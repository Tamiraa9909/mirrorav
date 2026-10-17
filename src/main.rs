//! MediaFairy mirrorav — HLS mirror runner (Telegram bot, MTProto).
//!
//! /mirror <url>
//!   -> resolve (page -> packer -> m3u8 -> variants + duration + thumbnail)
//!   -> photo card (title / size est / duration / category) + inline buttons
//!   -> button callback -> parallel download -> ffmpeg concat -> MTProto upload
//!
//! Admin-only, long-lived via MTProto stream (no webhook, no HTTP bot API).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use grammers_client::media::{Attribute, InputMedia};
use grammers_client::message::{Button, InputMessage, ReplyMarkup};
use grammers_client::update::{CallbackQuery, Message, Update};
use grammers_client::Client;
use grammers_mtsender::{SenderPool, UpdatesConfiguration};
use grammers_session::storages::SqliteSession;
use regex::Regex;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use wreq::Client as HttpClient;
use wreq_util::emulate::Profile;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Res<T> = Result<T, BoxError>;

const REFERER: &str = "https://missav.ws/";

// ---------------------------------------------------------------- config
#[derive(Clone)]
struct Cfg {
    bot_token: String,
    admin_id: String,
    api_id: i32,
    api_hash: String,
    parallel: usize,
    work_dir: String,
    session_file: String,
}

fn cfg() -> Res<Cfg> {
    Ok(Cfg {
        bot_token: std::env::var("BOT_TOKEN")?,
        admin_id: std::env::var("ADMIN_ID").unwrap_or_default(),
        api_id: std::env::var("API_ID")?.parse()?,
        api_hash: std::env::var("API_HASH")?,
        parallel: std::env::var("PARALLEL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6),
        work_dir: std::env::var("WORK_DIR").unwrap_or_else(|_| "./tmp".into()),
        session_file: std::env::var("SESSION_PATH")
            .unwrap_or_else(|_| "./mirrorav.session".into()),
    })
}

fn http() -> Res<HttpClient> {
    Ok(HttpClient::builder()
        .emulation(Profile::Safari17_0)
        .timeout(Duration::from_secs(120))
        .build()?)
}

// ---------------------------------------------------------------- page & m3u8
async fn get_page(http: &HttpClient, url: &str) -> Res<String> {
    if let Ok(r) = http.get(url).header("referer", REFERER).send().await {
        if r.status() == 200 {
            let body = r.text().await?;
            let head: String = body.chars().take(2500).collect();
            if !head.contains("Just a moment") {
                return Ok(body);
            }
            warn!("direct fetch blocked by challenge, fallback reader");
        } else {
            warn!("direct fetch status {}", r.status());
        }
    }
    let r = http
        .get(format!("https://r.jina.ai/{url}"))
        .header("x-return-format", "html")
        .send()
        .await?;
    Ok(r.text().await?)
}

fn unpack_packer(payload: &str, base: u32, keys: &[&str]) -> String {
    let mut map: BTreeMap<u32, &str> = BTreeMap::new();
    for (i, v) in keys.iter().enumerate() {
        map.insert(i as u32, v);
    }
    let re = Regex::new(r"\b[0-9a-e]+\b").unwrap();
    re.replace_all(payload, |c: &regex::Captures| {
        let tok = &c[0];
        match u32::from_str_radix(tok, base) {
            Ok(n) => map.get(&n).copied().unwrap_or(tok).to_string(),
            Err(_) => tok.to_string(),
        }
    })
    .to_string()
}

fn extract_playlists(page: &str) -> Vec<String> {
    let plain = Regex::new(r#"https://[\w.-]+/[^\s'";]+\.m3u8"#).unwrap();
    let mut urls: Vec<String> =
        plain.find_iter(page).map(|m| m.as_str().to_string()).collect();

    let block = Regex::new(r"(?s)eval\(function\(p,a,c,k,e,d\)(.{0,3000}?)\}\)\)").unwrap();
    let args = Regex::new(
        r"(?s)\('((?:\\.|[^'\\])*)',(\d+),\d+,'((?:\\.|[^'\\])*)'\.split\('\|'\)",
    )
    .unwrap();
    if let Some(bm) = block.captures(page) {
        if let Some(am) = args.captures(&bm[1]) {
            let payload = am[1].replace("\\'", "'");
            let base: u32 = am[2].parse().unwrap_or(10);
            let keys_src = am[3].replace("\\'", "'");
            let keys: Vec<&str> = keys_src.split('|').collect();
            let unpacked = unpack_packer(&payload, base, &keys);
            urls.extend(plain.find_iter(&unpacked).map(|m| m.as_str().to_string()));
        }
    }

    let mut seen = std::collections::HashSet::new();
    let mut ordered: Vec<String> = Vec::new();
    for u in urls {
        if seen.insert(u.clone()) {
            ordered.push(u);
        }
    }
    ordered.sort_by_key(|u| !u.contains("playlist.m3u8"));
    ordered
}

/// master playlist -> (height, bandwidth, url)
fn parse_master_variants(master: &str, base: &str) -> Vec<(u32, u64, String)> {
    let res_re = Regex::new(r"RESOLUTION=\d+x(\d+)").unwrap();
    let bw_re = Regex::new(r"BANDWIDTH=(\d+)").unwrap();
    let mut out = Vec::new();
    let mut cur: Option<(u32, u64)> = None;
    for line in master.lines() {
        let line = line.trim();
        if let Some(at) = line.strip_prefix("#EXT-X-STREAM-INF") {
            let h = res_re
                .captures(at)
                .and_then(|c| c[1].parse().ok())
                .unwrap_or(0);
            let bw = bw_re
                .captures(at)
                .and_then(|c| c[1].parse().ok())
                .unwrap_or(0);
            cur = Some((h, bw));
        } else if !line.is_empty() && !line.starts_with('#') {
            if let Some((h, bw)) = cur.take() {
                let url = if line.starts_with("http") {
                    line.to_string()
                } else {
                    format!("{}/{}", base.trim_end_matches('/'), line)
                };
                out.push((h, bw, url));
            }
        }
    }
    out
}

fn parse_segments(text: &str, base: &str) -> Res<(Vec<String>, Vec<f64>)> {
    if text.contains("METHOD=AES") {
        return Err("playlist is AES-encrypted — not supported".into());
    }
    let mut segs = Vec::new();
    let mut durs = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(d) = line.strip_prefix("#EXTINF:") {
            let num = d.split(',').next().unwrap_or("0").trim();
            durs.push(num.parse().unwrap_or(0.0));
        } else if !line.starts_with('#') {
            segs.push(if line.starts_with("http") {
                line.to_string()
            } else {
                format!("{}/{}", base.trim_end_matches('/'), line)
            });
        }
    }
    Ok((segs, durs))
}

fn page_title(page: &str) -> String {
    let re = Regex::new(r"<title>([^<]+)").unwrap();
    if let Some(m) = re.captures(page) {
        let t = m[1].trim().trim_end_matches(|c| c == '-' || c == ' ').trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    let re = Regex::new(r"(?m)^Title: (.+)$").unwrap();
    re.captures(page)
        .map(|m| m[1].to_string())
        .unwrap_or_else(|| "mirror".into())
}

fn page_category(page: &str) -> String {
    let re = Regex::new(r#"/tags/([\w-]+)"#).unwrap();
    let mut seen = std::collections::HashSet::new();
    let mut tags = Vec::new();
    for m in re.captures_iter(page) {
        let t = m[1].to_string();
        if seen.insert(t.clone()) {
            tags.push(t);
        }
        if tags.len() >= 6 {
            break;
        }
    }
    if tags.is_empty() {
        "-".into()
    } else {
        tags.join(", ")
    }
}

fn og_image(page: &str) -> Option<String> {
    let re = Regex::new(r#"og:image" content="([^"]+)""#).unwrap();
    if let Some(m) = re.captures(page) {
        return Some(m[1].to_string());
    }
    let re2 = Regex::new(r#"content="([^"]+)"[^>]*og:image"#).unwrap();
    re2.captures(page).map(|m| m[1].to_string())
}

/// Full untruncated title from og:title (falls back to <title> tag).
fn og_title(page: &str) -> Option<String> {
    let re = Regex::new(r#"og:title" content="([^"]+)""#).unwrap();
    if let Some(m) = re.captures(page) {
        return Some(m[1].to_string());
    }
    let re2 = Regex::new(r#"content="([^"]+)"[^>]*og:title"#).unwrap();
    re2.captures(page).map(|m| m[1].to_string())
}

fn unescape_html(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn fmt_dur(secs: f64) -> String {
    let t = secs.max(0.0) as u64;
    format!("{}:{:02}", t / 60, t % 60)
}

// ---------------------------------------------------------------- download
async fn download_segments(
    http: HttpClient,
    segs: Vec<String>,
    dir: PathBuf,
    done: Arc<AtomicUsize>,
    par: usize,
) -> Res<u64> {
    use tokio::sync::Semaphore;
    let sem = Arc::new(Semaphore::new(par));
    let mut handles: Vec<tokio::task::JoinHandle<Result<u64, BoxError>>> =
        Vec::with_capacity(segs.len());

    for (i, url) in segs.into_iter().enumerate() {
        let http = http.clone();
        let dir = dir.clone();
        let sem = sem.clone();
        let done = done.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let fname = dir.join(format!("{i:06}.ts"));
            let mut last_err = String::from("unknown");
            for attempt in 0..4u32 {
                match http.get(&url).header("referer", REFERER).send().await {
                    Ok(r) if r.status() == 200 => match r.bytes().await {
                        Ok(b) if b.len() > 1 => {
                            let n = b.len() as u64;
                            if tokio::fs::write(&fname, &b).await.is_ok() {
                                done.fetch_add(1, Ordering::Relaxed);
                                return Ok(n);
                            }
                            last_err = "fs write failed".into();
                        }
                        Ok(_) => last_err = "empty segment".into(),
                        Err(e) => last_err = e.to_string(),
                    },
                    Ok(r) => last_err = format!("HTTP {}", r.status()),
                    Err(e) => last_err = e.to_string(),
                }
                tokio::time::sleep(Duration::from_millis(700 * (attempt as u64 + 1))).await;
            }
            Err(format!("segment {i}: {last_err}").into())
        }));
    }

    let mut bytes = 0u64;
    for h in handles {
        match h.await {
            Ok(r) => bytes += r?,
            Err(e) => return Err(e.to_string().into()),
        }
    }
    Ok(bytes)
}

fn sanitize_name(title: &str) -> String {
    let mut t = String::new();
    for c in title.trim().chars() {
        if c.is_alphanumeric() {
            t.push(c);
        } else if c == ' ' {
            t.push('_');
        } else if c == '-' || c == '.' || c == '_' {
            t.push(c);
        }
    }
    let t: String = t.trim_matches(|c| c == '_' || c == '.').to_string();
    let mut t = t.chars().take(120).collect::<String>();
    if t.is_empty() {
        t = "mirror".into();
    }
    format!("{t}.mp4")
}

async fn ffmpeg_concat(dir: &Path, out: &Path) -> Res<()> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "ts").unwrap_or(false))
        .collect();
    files.sort();

    let list = files
        .iter()
        .map(|f| format!("file '{}'\n", f.display()))
        .collect::<String>();
    tokio::fs::write(dir.join("list.txt"), &list).await?;

    let res = tokio::process::Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            &dir.join("list.txt").to_string_lossy(),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            &out.to_string_lossy(),
        ])
        .output()
        .await;

    match res {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => {
            let tail: String = String::from_utf8_lossy(&o.stderr)
                .chars()
                .rev()
                .take(200)
                .collect::<String>()
                .chars()
                .rev()
                .collect();
            warn!("ffmpeg failed: {tail} — raw concat fallback");
            raw_concat(&files, out).await
        }
        Err(e) => {
            warn!("ffmpeg spawn failed: {e} — raw concat fallback");
            raw_concat(&files, out).await
        }
    }
}

async fn raw_concat(files: &[PathBuf], out: &Path) -> Res<()> {
    use tokio::io::AsyncWriteExt;
    let mut out_f = tokio::fs::File::create(out).await?;
    for f in files {
        let data = tokio::fs::read(f).await?;
        out_f.write_all(&data).await?;
    }
    out_f.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------- upload progress
/// Counts bytes as they flow through, so upload progress is measured, not guessed.
struct ProgressReader<R> {
    inner: R,
    counter: Arc<AtomicU64>,
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            let n = (buf.filled().len() - before) as u64;
            self.counter.fetch_add(n, Ordering::Relaxed);
        }
        res
    }
}

// ---------------------------------------------------------------- state
#[derive(Clone)]
struct Variant {
    h: u32,
    bw: u64,
    url: String,
}

#[derive(Clone)]
struct Pending {
    title: String,
    category: String,
    dur_secs: f64,
    variants: Vec<Variant>,
}

#[derive(Clone)]
struct App {
    client: Client,
    cfg: Cfg,
    http: HttpClient,
    busy: Arc<Mutex<()>>,
    pending: Arc<Mutex<HashMap<String, Pending>>>,
    job_counter: Arc<AtomicU32>,
}

async fn set_prog(shared: &Arc<Mutex<String>>, text: &str) {
    *shared.lock().await = text.to_string();
}

async fn shared_lock(s: &Arc<Mutex<String>>) -> String {
    s.lock().await.clone()
}

async fn download_and_prepare(
    http: HttpClient,
    work_dir: &str,
    parallel: usize,
    job: &str,
    variant: &Variant,
    shared: Arc<Mutex<String>>,
) -> Res<(PathBuf, PathBuf, u64)> {
    set_prog(&shared, "🎬 reading playlist…").await;
    let vt = http
        .get(&variant.url)
        .header("referer", REFERER)
        .send()
        .await?
        .text()
        .await?;
    if vt.contains("METHOD=AES") {
        return Err("playlist is AES-encrypted — not supported".into());
    }
    let vbase = variant.url.rsplit_once('/').map(|x| x.0).unwrap_or_default();
    let (segs, durs) = parse_segments(&vt, &vbase)?;
    if segs.is_empty() {
        return Err("no segments in playlist".into());
    }
    let dur_secs = durs.iter().sum::<f64>() as u64;
    let total = segs.len();
    set_prog(
        &shared,
        &format!("🎞 {} segments • ~{:.0} min", total, dur_secs as f64 / 60.0),
    )
    .await;

    let tmp_dir = PathBuf::from(work_dir).join(format!("mf_{job}"));
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }
    std::fs::create_dir_all(&tmp_dir)?;

    let done = Arc::new(AtomicUsize::new(0));
    let dl_total = total;
    let done2 = done.clone();
    let stop_dl = Arc::new(AtomicBool::new(false));
    let stop_dl2 = stop_dl.clone();
    let shared2 = shared.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if stop_dl2.load(Ordering::Relaxed) {
                break;
            }
            let d = done2.load(Ordering::Relaxed);
            if d > 0 && (d % 12 == 0 || d == dl_total) {
                set_prog(
                    &shared2,
                    &format!("⬇️ downloading {d}/{dl_total} ({}%)", d * 100 / dl_total),
                )
                .await;
            }
            if d == dl_total {
                break;
            }
        }
    });

    let t0 = std::time::Instant::now();
    let bytes = download_segments(http, segs, tmp_dir.clone(), done, parallel).await?;
    stop_dl.store(true, Ordering::Relaxed);
    set_prog(
        &shared,
        &format!(
            "🧩 got {:.0} MB in {:.0}s — merging…",
            bytes as f64 / 1e6,
            t0.elapsed().as_secs_f64()
        ),
    )
    .await;

    let out = tmp_dir.join(sanitize_name("mirrorav"));
    ffmpeg_concat(&tmp_dir, &out).await?;
    Ok((out, tmp_dir, dur_secs))
}

// ---------------------------------------------------------------- resolve -> menu
async fn resolve_and_menu(app: App, msg: Message, url: String) {
    let status = match msg.respond("🕷️ resolving…").await {
        Ok(s) => s,
        Err(e) => {
            error!("respond: {e}");
            return;
        }
    };

    let result: Res<()> = async {
        let job = format!(
            "{:06x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as u64)
                .unwrap_or(0)
                ^ (app.job_counter.fetch_add(1, Ordering::Relaxed) as u64).wrapping_mul(7919)
        );

        let mut page = get_page(&app.http, &url).await?;

        let _ = status.edit("🔗 locating playlist…").await;
        let mut pls = extract_playlists(&page);
        if pls.is_empty() {
            // fallback reader: halaman batik anti-bot lain yang lolos 200
            warn!("no m3u8 on direct page, trying reader fallback");
            let r = app
                .http
                .get(format!("https://r.jina.ai/{url}"))
                .header("x-return-format", "html")
                .send()
                .await?;
            page = r.text().await?;
            pls = extract_playlists(&page);
        }
        if pls.is_empty() {
            return Err(format!("no m3u8 found (page {} bytes)", page.len()).into());
        }

        // metadata from the final page (direct or reader fallback)
        let title = og_title(&page)
            .map(|t| unescape_html(&t).trim().to_string())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| page_title(&page));
        let category = page_category(&page);

        let master_url = pls
            .iter()
            .find(|u| u.contains("playlist.m3u8"))
            .unwrap_or_else(|| pls.first().unwrap())
            .clone();

        let _ = status.edit("🎚 parsing variants…").await;
        let master = app
            .http
            .get(&master_url)
            .header("referer", REFERER)
            .send()
            .await?
            .text()
            .await?;
        if master.contains("METHOD=AES") {
            return Err("playlist is AES-encrypted — not supported".into());
        }
        let base = master_url.rsplit_once('/').map(|x| x.0).unwrap_or_default();
        let mut variants: Vec<Variant> = parse_master_variants(&master, base)
            .into_iter()
            .map(|(h, bw, url)| Variant { h, bw, url })
            .collect();
        variants.sort_by_key(|v| v.h);

        let dur_secs: f64;
        if variants.is_empty() {
            // langsung media playlist (tanpa varian)
            let (_, durs) = parse_segments(&master, base)?;
            dur_secs = durs.iter().sum();
            variants.push(Variant {
                h: 720,
                bw: 0,
                url: master_url.clone(),
            });
        } else {
            let best = variants.last().unwrap();
            let vt = app
                .http
                .get(&best.url)
                .header("referer", REFERER)
                .send()
                .await?
                .text()
                .await?;
            let vbase = best.url.rsplit_once('/').map(|x| x.0).unwrap_or_default();
            let (_, durs) = parse_segments(&vt, &vbase)?;
            dur_secs = durs.iter().sum();
        }
        if dur_secs <= 0.0 {
            return Err("empty playlist".into());
        }

        // thumbnail
        let mut thumb_tmp: Option<PathBuf> = None;
        if let Some(turl) = og_image(&page) {
            if let Ok(r) = app.http.get(&turl).header("referer", REFERER).send().await {
                if r.status() == 200 {
                    if let Ok(b) = r.bytes().await {
                        if b.len() > 1024 {
                            let p = PathBuf::from(&app.cfg.work_dir).join(format!("thumb_{job}.jpg"));
                            if tokio::fs::write(&p, &b).await.is_ok() {
                                thumb_tmp = Some(p);
                            }
                        }
                    }
                }
            }
        }

        // simpan pending untuk callback
        app.pending.lock().await.insert(
            job.clone(),
            Pending {
                title: title.clone(),
                category: category.clone(),
                dur_secs,
                variants: variants.clone(),
            },
        );

        // caption + buttons
        let caption = format!(
            "📛 {title}\n\n🏷 {category}\n⏱ {}\n\n🔗 {url}\n\npick a quality 👇",
            fmt_dur(dur_secs)
        );
        let buttons: Vec<Button> = variants
            .iter()
            .map(|v| {
                let est_mb = ((dur_secs * v.bw as f64) / 8.0 / 1e6).round() as u64;
                let label = if v.bw > 0 {
                    format!("{}p ~{}MB", v.h, est_mb)
                } else {
                    format!("{}p", v.h)
                };
                Button::data(label, format!("q:{job}:{}", v.h).into_bytes())
            })
            .collect();
        let markup = ReplyMarkup::from_buttons_row(&buttons);

        let im = InputMessage::new().text(caption.clone()).reply_markup(markup);
        let sent = if let Some(tp) = &thumb_tmp {
            match app
                .client
                .upload_file(tp.to_str().unwrap_or_default())
                .await
            {
                Ok(uploaded) => match msg.respond(im.clone().photo(uploaded)).await {
                    Ok(m) => Ok(m),
                    Err(e) => {
                        warn!("photo send failed: {e} — text menu fallback");
                        msg.respond(im).await
                    }
                },
                Err(_) => msg.respond(im).await,
            }
        } else {
            msg.respond(im).await
        };

        if let Some(tp) = &thumb_tmp {
            let _ = tokio::fs::remove_file(tp).await;
        }

        match sent {
            Ok(_) => {
                let _ = status.delete().await;
                Ok(())
            }
            Err(e) => Err(format!("failed to send menu: {e}").into()),
        }
    }
    .await;

    if let Err(e) = result {
        error!("resolve failed: {e}");
        let _ = status.edit(format!("❌ {e}")).await;
    }
}

// ---------------------------------------------------------------- callback
async fn on_callback(app: App, cq: CallbackQuery) {
    if cq.sender_id().to_string() != app.cfg.admin_id {
        return;
    }
    let data = String::from_utf8_lossy(cq.data()).to_string();
    let Some(rest) = data.strip_prefix("q:") else {
        return;
    };
    let Some((job, res)) = rest.split_once(':') else {
        return;
    };
    let job = job.to_string();
    let pending = app.pending.lock().await.remove(&job);
    let Some(p) = pending else {
        if let Ok(m) = cq.load_message().await {
            let _ = m.respond("🕷️ menu expired — send /mirror again").await;
        }
        return;
    };

    let h: u32 = res.parse().unwrap_or(0);
    let Some(variant) = p.variants.iter().min_by_key(|v| (v.h as i64 - h as i64).abs()).cloned() else {
        return;
    };

    let app2 = app.clone();
    if app.busy.try_lock().is_err() {
        if let Ok(m) = cq.load_message().await {
            let _ = m.respond("🕷️ busy — wait for the current job to finish").await;
        }
        return;
    }

    tokio::spawn(async move {
        let _guard = app2.busy.lock().await;
        let loaded = match cq.load_message().await {
            Ok(m) => m,
            Err(e) => {
                error!("load_message: {e}");
                return;
            }
        };
        let status = match loaded
            .respond(format!("⬇️ downloading {}p — hang tight…", variant.h))
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!("respond: {e}");
                return;
            }
        };

        // reporter loop
        let shared: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let st = status.clone();
        let stop2 = stop.clone();
        let shared_task = shared.clone();
        tokio::spawn(async move {
            let mut last = String::new();
            loop {
                tokio::time::sleep(Duration::from_secs(12)).await;
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                let t = shared_lock(&shared_task).await;
                if !t.is_empty() && t != last {
                    let _ = st.edit(format!("{t}\n——— mirrorav 🕷")).await;
                    last = t;
                }
            }
        });

        match download_and_prepare(
            app2.http.clone(),
            &app2.cfg.work_dir,
            app2.cfg.parallel,
            &job,
            &variant,
            shared.clone(),
        )
        .await
        {
            Ok((out, tmp_dir, dur_secs)) => {
                set_prog(&shared, "\u{2b06}\u{fe0f} preparing upload\u{2026}").await;
                let total = std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0);
                let up_res = if total > 0 {
                    match tokio::fs::File::open(&out).await {
                        Ok(file) => {
                            let counter = Arc::new(AtomicU64::new(0));
                            let mut reader =
                                ProgressReader { inner: file, counter: counter.clone() };

                            // sampler: real percentage + speed + ETA from the counting reader
                            let counter2 = counter.clone();
                            let total2 = total;
                            let shared_up = shared.clone();
                            let up_stop = Arc::new(AtomicBool::new(false));
                            let up_stop2 = up_stop.clone();
                            let sampler = tokio::spawn(async move {
                                let mut last = 0u64;
                                let mut t_last = std::time::Instant::now();
                                loop {
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                    if up_stop2.load(Ordering::Relaxed) {
                                        break;
                                    }
                                    let cur = counter2.load(Ordering::Relaxed);
                                    let dt = t_last.elapsed().as_secs_f64().max(0.2);
                                    let speed = (cur - last) as f64 / dt / 1e6;
                                    last = cur;
                                    t_last = std::time::Instant::now();
                                    if cur > 0 && total2 > 0 {
                                        let pct = cur as f64 / total2 as f64 * 100.0;
                                        let eta = if speed > 0.01 {
                                            (total2.saturating_sub(cur) as f64 / 1e6 / speed)
                                                as u64
                                        } else {
                                            0
                                        };
                                        set_prog(
                                            &shared_up,
                                            &format!(
                                                "\u{2b06}\u{fe0f} uploading {:.1}/{:.1} MB ({:.0}%) \u{2022} {:.1} MB/s \u{2022} ETA {:02}:{:02}",
                                                cur as f64 / 1e6,
                                                total2 as f64 / 1e6,
                                                pct,
                                                speed,
                                                eta / 60,
                                                eta % 60
                                            ),
                                        )
                                        .await;
                                    }
                                }
                            });

                            let res = app2
                                .client
                                .upload_stream(
                                    &mut reader,
                                    total as usize,
                                    "mirrorav.mp4".to_string(),
                                )
                                .await;
                            up_stop.store(true, Ordering::Relaxed);
                            let _ = sampler.await;
                            res
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    app2
                        .client
                        .upload_file(out.to_str().unwrap_or_default())
                        .await
                };

                match up_res {
                    Ok(uploaded) => {
                        let size_mb = (total as f64 / 1e6).round() as u64;
                        let media = InputMedia::new()
                            .mime_type("video/mp4")
                            .caption(format!(
                                "{}\n\n{}\n{} \u{2022} {}p \u{2022} {} MB\n\nvia mirrorav",
                                p.title, p.category, fmt_dur(dur_secs as f64), variant.h, size_mb
                            ))
                            .document(uploaded)
                            .attribute(Attribute::Video {
                                round_message: false,
                                supports_streaming: true,
                                duration: Duration::from_secs(dur_secs),
                                w: 1280,
                                h: variant.h as i32,
                            });
                        if let Err(e) = loaded.respond_album(vec![media]).await {
                            error!("send: {e}");
                            let _ = status.edit(format!("\u{274c} send failed: {e}")).await;
                        } else {
                            let _ = status.delete().await;
                        }
                    }
                    Err(e) => {
                        error!("upload: {e}");
                        let _ = status.edit(format!("\u{274c} upload failed: {e}")).await;
                    }
                }
                let _ = std::fs::remove_dir_all(&tmp_dir);
            }
            Err(e) => {
                error!("job failed: {e}");
                let _ = status.edit(format!("❌ failed: {e}")).await;
            }
        }
        stop.store(true, Ordering::Relaxed);
    });
}

// ---------------------------------------------------------------- telegram
async fn health_server(port: u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("health bind {addr}: {e}");
            return;
        }
    };
    info!("health on {addr}");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let _ = sock.read(&mut buf).await;
            let body = "mirrorav alive\n";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}

async fn on_update(app: &App, update: Update) {
    match update {
        Update::NewMessage(message) => {
            if message.outgoing() {
                return;
            }
            if message.peer_id().to_string() != app.cfg.admin_id {
                return;
            }
            let text = message.text().to_string();

            if text.starts_with("/start") || text.starts_with("/help") {
                let _ = message
                    .reply(
                        "mirrorav 🕷

/mirror <url>
→ pick a quality via the buttons",
                    )
                    .await;
                return;
            }

            if let Some(rest) = text.strip_prefix("/mirror") {
                let Some(url) = rest.trim().split_whitespace().next() else {
                    let _ = message.reply("usage: /mirror <url>").await;
                    return;
                };
                let app2 = app.clone();
                let msg = message.clone();
                let url = url.to_string();
                tokio::spawn(async move {
                    resolve_and_menu(app2, msg, url).await;
                });
            }
        }
        Update::CallbackQuery(cq) => {
            let app2 = app.clone();
            tokio::spawn(async move {
                on_callback(app2, cq).await;
            });
        }
        _ => {}
    }
}

#[tokio::main]
async fn main() -> Res<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cfg = cfg()?;
    std::fs::create_dir_all(&cfg.work_dir)?;

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(7860);
    tokio::spawn(health_server(port));

    let http = http()?;
    let session = Arc::new(SqliteSession::open(cfg.session_file.as_str()).await?);
    let SenderPool { runner, updates, handle } =
        SenderPool::new(Arc::clone(&session), cfg.api_id);
    let client = Client::new(handle.clone());
    tokio::spawn(runner.run());

    if !client.is_authorized().await? {
        info!("signing in as bot…");
        client.bot_sign_in(&cfg.bot_token, &cfg.api_hash).await?;
    }
    info!("mirrorav up — admin {}", cfg.admin_id);

    let app = App {
        client,
        cfg,
        http,
        busy: Arc::new(Mutex::new(())),
        pending: Arc::new(Mutex::new(HashMap::new())),
        job_counter: Arc::new(AtomicU32::new(0)),
    };

    let mut stream = app
        .client
        .stream_updates(updates, UpdatesConfiguration { catch_up: true })
        .await?;

    loop {
        let update = match stream.next().await {
            Ok(u) => u,
            Err(e) => {
                error!("update stream: {e}");
                continue;
            }
        };
        on_update(&app, update).await;
    }
}
