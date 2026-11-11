# Rotation

**Self-hosted Plexamp listening analytics, Story Studio cards, and Plex smart playlists — powered by your Tautulli database and Plex Media Server.**

Built with pure **Python 3.11 standard library** (zero external pip dependencies), a single-file responsive web/PWA UI with offline support, a built-in **First-Launch Setup Wizard**, and an optional **Passcode Login Gate**.

---

## Features

- **Listening Telemetry:** All-time, yearly, `90d`, `30d`, and `7d` listening breakdowns — completion vs. skip rates, streaks, Listening Hurricane/spiral clock, 7×24 heatmap, listening persona, milestones, and discovery stats.
- **Leaderboards & Catalog:** Searchable top tracks, artists, and albums with clickable **Artist Deep-Dive** modals and inline audio previews streamed through Plex.
- **Story Studio (`9:16` Export):** Generate shareable Instagram Story / iMessage cards with light (`linen`) or dark (`vinyl`) themes and customizable handle (`@you • Plexamp`).
- **Plex Smart Playlists & Vault:** Curate and sync Rotation playlists directly to your Plex server (including auto-sync toggles and discovery/forgotten-gems views).
- **Zero-CLI Browser Setup:** On first launch (or anytime via **⚙️ Setup**), paste a download link to `tautulli.db`, upload it from your computer, or use a Docker-mounted path — then auto-detect your Plex Machine ID and Music library in one click.
- **Passcode Protection:** Lock the entire dashboard, audio streams, artwork proxy, playlist actions, and setup behind an HMAC-signed cookie session (`HttpOnly`).

---

## Quick Start

### Option 1: Docker Compose (Recommended)

```bash
git clone https://github.com/Erock002/Rotation.git
cd Rotation
cp .env.example .env
docker compose up -d --build
```

Open **`http://localhost:8888`**.  
If no `tautulli.db` is mounted yet, the **Setup Wizard** pops up automatically so you can finish configuration right in your browser.

### Option 2: Local Python 3.11+

```bash
git clone https://github.com/Erock002/Rotation.git
cd Rotation
python3 server.py
```

Open **`http://localhost:8888`**.

---

## Connecting Your Data (In-App Setup Wizard)

1. **Load `tautulli.db` (choose any of 3 ways):**
   - **Direct Download Link:** Paste an `http://` or `https://` URL pointing directly to `tautulli.db` and click **Fetch DB Link**.
     > Tip: On the machine hosting Tautulli, run `python3 -m http.server 8099` inside the Tautulli config folder, paste `http://YOUR-SERVER-IP:8099/tautulli.db`, fetch it, then press `Ctrl+C` to stop the temporary HTTP server.
   - **Browser Upload:** Select `tautulli.db` from your computer and click **Upload File**.
   - **Local / Docker Volume Path:** Set `TAUTULLI_DIR=/path/to/tautulli` in `.env` (mounts read-only to `/tautulli/tautulli.db`).
2. **Connect Plex (optional for stats; required for album art, track playback, and playlist sync):**
   - Enter your **Plex Server URL** and **`X-Plex-Token`**.
   - Click **🔍 Test & Auto-Detect** to automatically populate your **Server Name**, **Machine Identifier**, and **Music Library Section ID**.
3. **Personalize & Lock (optional):**
   - Customize your **Story Card Handle**.
   - Set a **Login Passcode** (4+ characters) to require sign-in before anyone with the URL can view stats or sync playlists.

---

## How to Find Your Tautulli DB & Plex Token

- **Where is `tautulli.db`?**
  - **Tautulli Web UI:** *Settings → Help & Info → Database File*
  - **Docker:** `/config/tautulli.db` inside your Tautulli volume
  - **macOS:** `~/Library/Application Support/Tautulli/tautulli.db`
  - **Windows:** `%LocalAppData%\Tautulli\tautulli.db`
- **Where is `X-Plex-Token`?**
  1. Open Plex Web and select any track or album.
  2. Click **`⋯` → Get Info → View XML**.
  3. Copy the token at the end of the browser URL after `X-Plex-Token=`.

---

## Environment Variables (`.env`)

All variables are optional if you configure Rotation through the browser Setup Wizard:

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8888` | HTTP server port |
| `TAUTULLI_DIR` | `./cache` | Host folder containing `tautulli.db` (mounted to `/tautulli:ro` in Docker) |
| `TAUTULLI_DB` | `/tautulli/tautulli.db` | Path to `tautulli.db` inside the server process |
| `PLEX_URL` | `http://127.0.0.1:32400` | Base URL of your Plex Media Server |
| `PLEX_TOKEN` | _(empty)_ | Plex `X-Plex-Token` |
| `MACHINE_ID` | _(empty)_ | Plex server `machineIdentifier` (auto-detected in Setup) |
| `MUSIC_SECTION_ID` | `1` | Plex music library section key (auto-detected in Setup) |
| `SERVER_NAME` | `Plex Server` | Friendly Plex server name shown in UI |
| `USER_HANDLE` | `@you • Plexamp` | Default handle stamped on Story Studio cards |
| `APP_PASSWORD` | _(empty)_ | Optional login passcode gate (can also be set in UI) |
| `NTFY_URL` | _(empty)_ | Optional ntfy topic URL for monthly recap notifications |
| `PUBLIC_URL` | _(empty)_ | Optional external URL linked inside monthly recap notifications |
| `CACHE_DIR` | `./cache` | Directory for runtime config (`config.json`), session secret, and cached artwork |

---

## Security & Privacy

- All runtime secrets (`cache/config.json`, `cache/.session_secret`), uploaded/fetched `tautulli.db` copies, playlist sync state, and cached artwork live inside `cache/`, which is gitignored alongside `.env`.
- Rotation reads `tautulli.db` strictly in read-only mode (`?mode=ro` with `immutable=1` fallback) so it never takes write locks on a live Tautulli database.
- Before exposing Rotation outside your LAN (Tailscale Funnel, Cloudflare Tunnel, or a reverse proxy), enable a passcode in Setup or set `APP_PASSWORD` in `.env`.
