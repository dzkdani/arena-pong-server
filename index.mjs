// arena-pong — room relay server (Node + ws). A PUMP with a bouncer:
// no game state lives here. Responsibilities:
//   1. rooms sockets by the `room` query param; at most 8 seats, unique
//      names, no fresh joins while a match is live,
//   2. assigns ids `p<n>` on `join {name}` (per-room counter, join order),
//   3. SEAT GRACE: a dropped socket keeps its seat for `graceMs` (default
//      45 s) so a flaky client (or a waking free-tier host) can reclaim the
//      same id/name — `away {id}` … `back {id}` … `leave {id}` on expiry,
//   4. relays CAUSES from the host only (`start|seg|close|gone`); `input`
//      from anyone with the owner rewritten from the socket identity,
//   5. answers `ping` with `pong` (client keepalive → Render idle timer).
// Guests replay causes; the server never reads clocks or rolls dice.

import http from "node:http";
import { WebSocketServer } from "ws";

const CAUSES = new Set(["start", "seg", "close", "gone"]);
const ROOM_MAX = 8;
const NAME_MAX = 16;
const MAX_PAYLOAD = 4096;
const RATE_WINDOW_MS = 1000;
const RATE_MAX = 240; // messages per socket per second

const sendTo = (ws, msg) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
};

const reject = (ws, reason) => {
  if (ws.readyState !== 1) return;
  ws.send(JSON.stringify({ type: "rejected", reason }), () => ws.close(4001, reason.slice(0, 120)));
};

export function createServer({ graceMs } = {}) {
  const seatGrace = Number(graceMs ?? process.env.SEAT_GRACE_MS ?? 45000);
  const httpServer = http.createServer((_req, res) => {
    res.writeHead(200, {
      "content-type": "application/json",
      "access-control-allow-origin": "*", // client wake probe from the itch page
    });
    res.end(JSON.stringify({ ok: true, game: "arena-pong" }));
  });
  const wss = new WebSocketServer({ server: httpServer, path: "/ws", maxPayload: MAX_PAYLOAD });
  httpServer.wss = wss; // exposed for closeServer() teardown
  const rooms = new Map(); // room -> { members: [{id, name, ws, host, away, timer}], seq, live }
  const seatTimers = new Set();
  httpServer._seatTimers = seatTimers;

  const rosterOf = (room) => ({
    type: "roster",
    members: room.members.map((m) => ({ id: m.id, name: m.name, host: m.host, away: !!m.away })),
  });
  const broadcast = (room, msg) => {
    for (const m of room.members) {
      if (m.ws && !m.away) sendTo(m.ws, msg);
    }
  };
  const promoteIfNeeded = (room) => {
    if (room.members.some((m) => m.host && !m.away)) return;
    const next = room.members.find((m) => !m.away) ?? null;
    if (next) next.host = true;
  };

  function scheduleExpiry(roomName, member) {
    const timer = setTimeout(() => {
      seatTimers.delete(timer);
      const room = rooms.get(roomName);
      if (!room || !member.away) return; // reclaimed or room gone
      room.members = room.members.filter((m) => m !== member);
      broadcast(room, { type: "leave", id: member.id });
      promoteIfNeeded(room);
      if (room.members.length > 0) broadcast(room, rosterOf(room));
      else rooms.delete(roomName);
    }, seatGrace);
    seatTimers.add(timer);
    member.timer = timer;
  }

  wss.on("connection", (ws, req) => {
    const roomName = new URL(req.url, "http://x").searchParams.get("room");
    if (!roomName) { reject(ws, "room required"); return; }

    let windowStart = Date.now();
    let windowCount = 0;

    ws.on("message", (raw) => {
      const now = Date.now();
      if (now - windowStart > RATE_WINDOW_MS) { windowStart = now; windowCount = 0; }
      if (++windowCount > RATE_MAX) return; // flood guard

      let msg;
      try { msg = JSON.parse(String(raw)); } catch { return; }
      const type = msg?.type;
      const room = rooms.get(roomName);

      if (type === "join") {
        if (ws.mid) return;
        if (typeof msg.name !== "string") return;
        const name = msg.name.trim().slice(0, NAME_MAX);
        if (!name) return;

        let r = room;
        if (!r) { r = { members: [], seq: 0, live: false }; rooms.set(roomName, r); }

        // 1) reclaim a reserved seat (same name, away within grace)
        const seat = r.members.find(
          (m) => m.away && m.name.toLowerCase() === name.toLowerCase(),
        );
        if (seat) {
          if (seat.timer) { clearTimeout(seat.timer); seatTimers.delete(seat.timer); seat.timer = null; }
          seat.away = false;
          seat.ws = ws;
          ws.mid = seat;
          promoteIfNeeded(r);
          sendTo(ws, { type: "welcome", id: seat.id, host: seat.host });
          broadcast(r, rosterOf(r));
          broadcast(r, { type: "back", id: seat.id });
          return;
        }

        // 2) fresh joins: locked out while a match is live, capped, unique
        if (r.live) { reject(ws, "match in progress"); return; }
        if (r.members.length >= ROOM_MAX) { reject(ws, "room full (8)"); return; }
        if (r.members.some((m) => m.name.toLowerCase() === name.toLowerCase())) {
          reject(ws, "name taken");
          return;
        }
        const member = {
          id: `p${++r.seq}`,
          name,
          ws,
          host: r.members.length === 0,
          away: false,
          timer: null,
        };
        ws.mid = member;
        r.members.push(member);
        sendTo(ws, { type: "welcome", id: member.id, host: member.host });
        broadcast(r, rosterOf(r)); // a join changes everyone's side order
        return;
      }

      const from = ws.mid;
      if (!from || !room) return;

      if (type === "ping") {
        sendTo(ws, { type: "pong", t: msg.t ?? null });
        return;
      }

      if (type === "input") {
        const u = Number(msg.u);
        if (!Number.isFinite(u)) return;
        for (const m of room.members) {
          if (!m.ws || m.away || m.ws === ws) continue;
          sendTo(m.ws, { type: "input", u: Math.max(0, Math.min(1, u)), owner: from.name });
        }
        return;
      }

      if (!CAUSES.has(type)) return; // leave/unknown/reserved are dropped
      if (!from.host) return;        // only the host may publish causes
      if (type === "start") room.live = true;
      if (type === "close" && Array.isArray(msg.owners) && msg.owners.length <= 1) {
        room.live = false; // match finished; the room reopens
      }
      for (const m of room.members) {
        if (!m.ws || m.away || m.ws === ws) continue;
        sendTo(m.ws, msg);
      }
    });

    ws.on("close", () => {
      const member = ws.mid;
      if (!member || member.away) return;
      const room = rooms.get(roomName);
      if (!room) return;
      member.away = true;
      member.ws = null;
      const wasHost = member.host;
      member.host = false;
      if (wasHost) promoteIfNeeded(room);
      broadcast(room, { type: "away", id: member.id });
      broadcast(room, rosterOf(room));
      scheduleExpiry(roomName, member);
    });
  });

  // heartbeat: half-open sockets must not hold a seat hostage forever
  const hb = setInterval(() => {
    for (const ws of wss.clients) {
      if (ws.isAlive === false) { ws.terminate(); continue; }
      ws.isAlive = false;
      try { ws.ping(); } catch {}
    }
  }, 30000);
  httpServer._hb = hb;
  wss.on("connection", (ws) => {
    ws.isAlive = true;
    ws.on("pong", () => { ws.isAlive = true; });
  });

  return httpServer;
}

/** Test teardown: drop sockets, stop timers, close wss + http. */
export function closeServer(httpServer) {
  clearInterval(httpServer._hb);
  for (const t of httpServer._seatTimers ?? []) clearTimeout(t);
  httpServer._seatTimers?.clear();
  const wss = httpServer.wss;
  for (const c of wss.clients) c.terminate();
  return new Promise((res) => {
    wss.close(() => httpServer.close(res));
  });
}

const isMain = process.argv[1] && import.meta.url === new URL(`file:///${process.argv[1].replaceAll("\\", "/")}`).href;
if (isMain) {
  const port = Number(process.env.PORT ?? 8787);
  const host = process.env.HOST ?? "0.0.0.0"; // VPS: HOST=127.0.0.1 behind a proxy
  createServer().listen(port, host, () => console.log(`arena-pong relay on ${host}:${port}`));
}
