//! MediaFairy mirrorav — HLS mirror runner (Telegram bot, MTProto).
//!
//! /mirror <url> [360p|480p|720p]
//!   -> fetch page (Safari TLS impersonation, reader fallback)
//!   -> unpack Dean-Edwards packer -> master.m3u8 -> variant -> segments
//!   -> parallel download -> ffmpeg concat -> MTProto upload (<=2GB)

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use grammers_client::media::{Attribute, InputMedia};
use grammers_client::update::{Message, Update};
use grammers_client::Client;
use grammers_mtsender::{SenderPool, UpdatesConfiguration};
use grammers_session::storages::SqliteSession;
use regex::Regex;
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
        admin_id: std::env::var("ADMIN_ID")
            .unwrap_or_default(),
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

    let block = Regex::new(r"eval\(function\(p,a,c,k,e,d\)(.{0,3000}?)\}\)\)").unwrap();
    let args = Regex::new(
        r"\('((?:\\.|[^'\\])*)',(\d+),\d+,'((?:\\.|[^'\\])*)'\.split\('\|'\)",
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

fn pick_quality(master: &str, want: &str, base: &str) -> Res<String> {
    let res_re = Regex::new(r"RESOLUTION=(\d+)x\d+").unwrap();
    let want_w: u32 = match want {
        "360p" => 640,
        "480p" => 854,
        _ => 1280,
    };
    let mut best: Option<(u32, String)> = None;
    let mut cur_w = 0u32;
    for line in master.lines() {
        if let Some(m) = res_re.captures(line) {
            cur_w = m[1].parse().unwrap_or(0);
        } else if !line.is_empty() && !line.starts_with('#') && cur_w > 0 {
            let better = match &best {
                None => true,
                Some((bw, _)) => cur_w.abs_diff(want_w) < bw.abs_diff(want_w),
            };
            if better {
                best = Some((cur_w, line.to_string()));
            }
        }
    }
    let best = best.ok_or("no variant playlists found")?.1;
    Ok(if best.starts_with("http") {
        best
    } else {
        format!("{}/{}", base.trim_end_matches('/'), best)
    })
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

// ---------------------------------------------------------------- job
async fn set_prog(shared: &Arc<Mutex<String>>, text: &str) {
    *shared.lock().await = text.to_string();
}

async fn do_job(
    http: HttpClient,
    work_dir: &str,
    parallel: usize,
    url: &str,
    want: &str,
    shared: Arc<Mutex<String>>,
) -> Res<(PathBuf, PathBuf, String, u64)> {
    set_prog(&shared, "🕷️ fetching page…").await;
    let page = get_page(&http, url).await?;
    let title = page_title(&page);
    let pls = extract_playlists(&page);
    if pls.is_empty() {
        return Err("no m3u8 found on page".into());
    }
    let master_url = pls
        .iter()
        .find(|u| u.contains("playlist.m3u8"))
        .unwrap_or_else(|| pls.first().unwrap())
        .clone();
    set_prog(&shared, &format!("📬 master: {master_url}")).await;

    let (media_text, var_base) = if master_url.contains("playlist.m3u8") {
        let master = http
            .get(&master_url)
            .header("referer", REFERER)
            .send()
            .await?
            .text()
            .await?;
        if master.contains("METHOD=AES") {
            return Err("playlist is AES-encrypted — not supported".into());
        }
        let base_dir = master_url
            .rsplit_once('/')
            .map(|x| x.0)
            .unwrap_or_default()
            .to_string();
        let variant = pick_quality(&master, want, &base_dir)?;
        set_prog(&shared, &format!("🔗 variant: {variant}")).await;
        let vt = http
            .get(&variant)
            .header("referer", REFERER)
            .send()
            .await?
            .text()
            .await?;
        let vbase = variant
            .rsplit_once('/')
            .map(|x| x.0)
            .unwrap_or_default()
            .to_string();
        (vt, vbase)
    } else {
        let vt = http
            .get(&master_url)
            .header("referer", REFERER)
            .send()
            .await?
            .text()
            .await?;
        let vbase = master_url
            .rsplit_once('/')
            .map(|x| x.0)
            .unwrap_or_default()
            .to_string();
        (vt, vbase)
    };

    let (segs, durs) = parse_segments(&media_text, &var_base)?;
    if segs.is_empty() {
        return Err("no segments in playlist".into());
    }
    let dur_secs = durs.iter().sum::<f64>() as u64;
    let total = segs.len();
    set_prog(
        &shared,
        &format!("🎞 {} segmen • ~{:.0} menit", total, dur_secs as f64 / 60.0),
    )
    .await;

    let tmp_dir = PathBuf::from(work_dir).join(format!("mf_{}", std::process::id()));
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
                set_prog(&shared2, &format!("⬇️ downloading {d}/{dl_total} ({}%)", d * 100 / dl_total)).await;
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
            "🧩 got {:.0} MB in {:.0}s — concat…",
            bytes as f64 / 1e6,
            t0.elapsed().as_secs_f64()
        ),
    )
    .await;

    let out = tmp_dir.join(sanitize_name(&title));
    ffmpeg_concat(&tmp_dir, &out).await?;
    Ok((out, tmp_dir, title, dur_secs))
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

#[derive(Clone)]
struct App {
    client: Client,
    cfg: Cfg,
    http: HttpClient,
    busy: Arc<Mutex<()>>,
}

async fn run_job(app: &App, status: grammers_client::message::Message, reply_ctx: &Message, url: String, want: String) {
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

    match do_job(
        app.http.clone(),
        &app.cfg.work_dir,
        app.cfg.parallel,
        &url,
        &want,
        shared.clone(),
    )
    .await
    {
        Ok((out, tmp_dir, title, dur_secs)) => {
            let _ = status.edit("⬆️ uploading…").await;
            match app.client.upload_file(out.to_str().unwrap_or_default()).await {
                Ok(uploaded) => {
                    let media = InputMedia::new()
                        .mime_type("video/mp4")
                        .caption(format!("{title}\n\nmirrorav 🕷 {want}"))
                        .document(uploaded)
                        .attribute(Attribute::Video {
                            round_message: false,
                            supports_streaming: true,
                            duration: Duration::from_secs(dur_secs),
                            w: 1280,
                            h: 720,
                        });
                    match reply_ctx.respond_album(vec![media]).await {
                        Ok(_) => info!("video terkirim"),
                        Err(e) => error!("send failed: {e}"),
                    }
                }
                Err(e) => error!("upload failed: {e}"),
            }
            let _ = std::fs::remove_dir_all(&tmp_dir);
        }
        Err(e) => {
            error!("job failed: {e}");
            let _ = status.edit(format!("❌ failed: {e}")).await;
        }
    }
    stop.store(true, Ordering::Relaxed);
}

async fn shared_lock(s: &Arc<Mutex<String>>) -> String {
    s.lock().await.clone()
}

async fn on_update(app: &App, update: Update) {
    let Update::NewMessage(message) = update else { return };
    if message.outgoing() {
        return;
    }
    let is_admin = message.peer_id().to_string() == app.cfg.admin_id;
    if !is_admin {
        return;
    }
    let text = message.text().to_string();

    if text.starts_with("/start") || text.starts_with("/help") {
        let _ = message
            .reply(
                "MediaFairy mirrorav 🕷\n\n/mirror <url> [360p|480p|720p]\ndefault 720p • admin-only",
            )
            .await;
        return;
    }

    if let Some(rest) = text.strip_prefix("/mirror") {
        let args = rest.trim();
        let (url, want) = match args.split_whitespace().collect::<Vec<_>>().as_slice() {
            [u] => (u.to_string(), "720p".to_string()),
            [u, q] => (u.to_string(), q.to_string()),
            _ => {
                let _ = message.reply("pakai: /mirror <url> [360p|480p|720p]").await;
                return;
            }
        };
        if app.busy.try_lock().is_err() {
            let _ = message.reply("🕷️ ada job berjalan — tunggu selesai").await;
            return;
        }

        let app2 = app.clone();
        tokio::spawn(async move {
            let _guard = app2.busy.lock().await;
            match message.reply(format!("🕷️ job diterima\n🔗 {url}\n🎚 kualitas: {want}")).await {
                Ok(status) => {
                    run_job(&app2, status.clone(), &message, url, want).await;
                }
                Err(e) => error!("reply failed: {e}"),
            }
            // safety net un-guarded by stop at end of run_job
        });
    }
}

#[tokio::main]
async fn main() -> Res<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cfg = cfg()?;
    std::fs::create_dir_all(&cfg.work_dir)?;

    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(7860);
    tokio::spawn(health_server(port));

    let http = http()?;
    let session = Arc::new(SqliteSession::open(cfg.session_file.as_str()).await?);
    let SenderPool { runner, updates, handle } = SenderPool::new(Arc::clone(&session), cfg.api_id);
    let client = Client::new(handle.clone());
    tokio::spawn(runner.run());

    if !client.is_authorized().await? {
        info!("signing in as bot…");
        client.bot_sign_in(&cfg.bot_token, &cfg.api_hash).await?;
    }
    info!("mirrorav up — admin {}", cfg.admin_id);

    let app = App { client, cfg, http, busy: Arc::new(Mutex::new(())) };
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
    Ok(())
}
