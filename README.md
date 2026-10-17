# mirrorav 🕷

Mirror runner Telegram — ambil video HLS dari sumber (missav dkk.), gabung jadi MP4, upload balik ke chat via MTProto (satu file sampai 2GB).

Rust + [wreq](https://github.com/0x676e67/wreq) (Safari-17 TLS impersonation) + [grammers](https://grammers.dev) (MTProto) + ffmpeg.

## Pakai

```
/mirror <url> [360p|480p|720p]
```

Default 720p. Admin-only (`ADMIN_ID`).

## Environment

| Var | Wajib | Default | Keterangan |
|---|---|---|---|
| `BOT_TOKEN` | ✅ | — | token bot Telegram |
| `API_ID` | ✅ | — | api id (my.telegram.org) |
| `API_HASH` | ✅ | — | api hash |
| `ADMIN_ID` | ✅ | — | telegram user id admin |
| `PARALLEL` | | `6` | paralel unduh segmen |
| `WORK_DIR` | | `./tmp` | folder kerja segmen |
| `SESSION_PATH` | | `./mirrorav.session` | file sesi MTProto |
| `PORT` | | `7860` | port health-check |

## Alur

1. Fetch halaman (impersonasi Safari TLS; fallback `r.jina.ai`).
2. Unpack Dean-Edwards packer → ekstrak `master.m3u8`.
3. Pilih resolusi terdekat dari varian.
4. Unduh paralel segmen TS (referer wajib).
5. `ffmpeg -f concat -c copy` → MP4 (fallback raw concat).
6. Upload MTProto as video streaming.

## Deploy

Docker multi-stage (builder Rust + runtime slim-ffmpeg). Di Railway: Connect GitHub → pilih repo → variabel di atas → deploy.
