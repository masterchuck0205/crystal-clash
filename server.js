'use strict';
/* Crystal Clash online server.
   The HOST's browser runs the battle and streams snapshots; the GUEST sends card placements.
   This server creates 2-player rooms (4-letter codes) and relays validated messages between them:
     create -> room(code)          join(code) -> start{role} to both      leave / disconnect -> peer-left
     snap (host->guest)   play, emote (guest->host)   end (host->guest)   rematch (host) -> start again */
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const compression = require('compression');
const { Server } = require('socket.io');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const MAX_ROOMS = 500;

const app = express();
app.disable('x-powered-by');
app.use(compression());
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: (process.env.CORS_ORIGIN || '*').split(',') },
  maxHttpBufferSize: 2e5, pingInterval: 8000, pingTimeout: 12000,
  perMessageDeflate: { threshold: 1024 }               // snapshots compress very well
});
const rooms = new Map();                               // code -> { host, guest, started }

app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size }));
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '10m', setHeaders: (res, p) => { if (p.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache'); } }));

function genCode() {
  for (let n = 0; n < 300; n++) {
    let c = ''; const b = crypto.randomBytes(4);
    for (let i = 0; i < 4; i++) c += ALPHABET[b[i] % ALPHABET.length];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function allow(sock, cat, perSec, burst) {
  const b = sock.data.b || (sock.data.b = {}), t = Date.now(); let k = b[cat];
  if (!k) k = b[cat] = { n: burst, t };
  k.n = Math.min(burst, k.n + (t - k.t) / 1000 * perSec); k.t = t;
  if (k.n < 1) return false; k.n -= 1; return true;
}
const isNum = v => typeof v === 'number' && Number.isFinite(v);
function leave(sock) {
  const code = sock.data.room; if (!code) return;
  sock.data.room = null; sock.data.role = null; sock.leave(code);
  const r = rooms.get(code); if (!r) return;
  const other = r.host === sock.id ? r.guest : r.host;
  if (other) { const o = io.sockets.sockets.get(other); if (o) { o.data.room = null; o.data.role = null; o.leave(code); o.emit('peer-left'); } }
  rooms.delete(code);                                   // a battle needs both players, so the room ends
}
function startMatch(r) {
  r.started = true;
  io.to(r.host).emit('start', { role: 'host' });
  io.to(r.guest).emit('start', { role: 'guest' });
}

io.on('connection', sock => {
  const other = () => { const r = rooms.get(sock.data.room); if (!r) return null; return io.sockets.sockets.get(sock.data.role === 'host' ? r.guest : r.host) || null; };

  sock.on('create', () => {
    if (!allow(sock, 'room', 1, 4)) return sock.emit('err', 'Slow down a little.');
    leave(sock);
    if (rooms.size >= MAX_ROOMS) return sock.emit('err', 'The server is busy. Try again in a moment.');
    const code = genCode(); if (!code) return sock.emit('err', 'Could not create a room.');
    rooms.set(code, { host: sock.id, guest: null, started: false });
    sock.join(code); sock.data.room = code; sock.data.role = 'host';
    sock.emit('room', code);
  });
  sock.on('join', raw => {
    if (!allow(sock, 'room', 1, 4)) return sock.emit('err', 'Slow down a little.');
    const code = String(raw == null ? '' : raw).toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
    const r = rooms.get(code);
    if (code.length !== 4 || !r) return sock.emit('err', 'No room with that code.');
    if (r.host === sock.id) return sock.emit('err', 'That is your own room - give the code to a friend.');
    if (r.guest) return sock.emit('err', 'That room is already full.');
    leave(sock);
    r.guest = sock.id; sock.join(code); sock.data.room = code; sock.data.role = 'guest';
    startMatch(r);
  });
  sock.on('leave', () => leave(sock));
  sock.on('disconnect', () => leave(sock));

  /* host -> guest */
  sock.on('snap', d => {
    if (sock.data.role !== 'host' || !allow(sock, 'snap', 40, 60) || !d || typeof d !== 'object' || !Array.isArray(d.u) || d.u.length > 200 || !Array.isArray(d.T)) return;
    const o = other(); if (o) o.volatile.emit('snap', d);
  });
  sock.on('end', d => {
    if (sock.data.role !== 'host' || !allow(sock, 'end', 2, 4) || !d || !isNum(d.w) || !Array.isArray(d.cr) || d.cr.length !== 2 || !d.cr.every(isNum)) return;
    const o = other(); if (o) o.emit('end', { w: d.w, cr: d.cr });
  });
  sock.on('rematch', () => {
    if (sock.data.role !== 'host' || !allow(sock, 'rematch', 1, 3)) return;
    const r = rooms.get(sock.data.room); if (r && r.guest) startMatch(r);
  });
  /* guest -> host */
  sock.on('play', d => {
    if (sock.data.role !== 'guest' || !allow(sock, 'play', 12, 24) || !d || typeof d.id !== 'string' || d.id.length > 24 || !isNum(d.x) || !isNum(d.y)) return;
    const o = other(); if (o) o.emit('play', { id: d.id, x: d.x, y: d.y });
  });
  sock.on('emote', e => {
    if (sock.data.role !== 'guest' || !allow(sock, 'emote', 1, 3) || typeof e !== 'string' || e.length > 8) return;
    const o = other(); if (o) o.emit('emote', e);
  });
});

server.listen(PORT, () => console.log('Crystal Clash server on port ' + PORT));
process.on('SIGTERM', () => { io.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
module.exports = { server, io, rooms };
