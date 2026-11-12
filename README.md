# arena pong — relay server

The WebSocket room relay for **arena pong** (a deterministic polygon-pong game).
It carries no game state and needs no database: it assigns seats, pumps causes
between clients, reserves a dropped player's seat for a grace window, and keeps
itself warm.

- Game client: static build hosted on itch.io, points at this relay's `wss://`
  URL.
- One Node process, one dependency (`ws`).

## Run locally

```bash
npm install
npm start            # ws://localhost:8787
```

Health check: `GET /` returns `{"ok":true,"game":"arena-pong"}`.

## API

- WebSocket endpoint: `/ws?room=NAME`
- Client → server: `join {name}`, `input {u}`, `ping {t}`, and (host only)
  `start|seg|close|gone`
- Server → client: `welcome`, `roster`, `away`, `back`, `leave`, `rejected`,
  `pong`, plus the relayed game causes
- Max 8 seats per room; duplicate names rejected; fresh joins are rejected
  while a match is live (a reserved seat may reclaim its name).

## Deploy on Render (free tier)

This repo ships a `render.yaml` blueprint:

1. Render dashboard → **New → Blueprint** → pick this repo → **Apply**.
   (Or **New → Web Service**: build `npm install`, start `node index.mjs`,
   plan Free, health check path `/`.)
2. Copy the service URL, e.g. `https://arena-pong-relay.onrender.com`.
3. The game client connects to `wss://arena-pong-relay.onrender.com`.

Environment variables (all optional):

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8787` | listen port (Render injects it) |
| `SEAT_GRACE_MS` | `45000` | how long a dropped player's seat survives |

Free-tier notes: the service sleeps after ~15 min idle and wakes in ~60 s; the
game client retries with backoff and shows a "waking the relay" hint. While a
tab is open the client pings every 60 s, so a live session keeps the service
awake.
