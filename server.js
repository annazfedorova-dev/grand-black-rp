// Grand Black RP — сервер для игры онлайн (без внешних библиотек)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const HTML = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
// одноразовые промокоды: кто и какой код уже использовал
const USED_FILE = path.join(__dirname, 'promo-used.json');
let USED = {}; try { USED = JSON.parse(fs.readFileSync(USED_FILE, 'utf8')); } catch (e) {}

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  if (req.url === '/promo' && req.method === 'POST') {
    let body = ''; req.on('data', d => { body += d; if (body.length > 2000) req.destroy(); });
    req.on('end', () => {
      let m = {}; try { m = JSON.parse(body); } catch (e) {}
      const h = String(m.h || '').slice(0, 40), nick = String(m.nick || 'игрок').slice(0, 24);
      res.writeHead(200, { 'content-type': 'application/json' });
      if (!h) { res.end('{"ok":false}'); return; }
      if (USED[h]) { res.end(JSON.stringify({ ok: false, by: USED[h] })); return; }
      USED[h] = nick; try { fs.writeFileSync(USED_FILE, JSON.stringify(USED)); } catch (e) {}
      res.end('{"ok":true}');
    });
    return;
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
  res.end(HTML);
});

// ---------- минимальный WebSocket ----------
function frame(op, payload) {
  const len = payload.length; let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
const clients = new Map(); let nextId = 1;
function send(c, obj) { if (!c.dead) try { c.sock.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) {} }
function broadcast(fromId, obj) { const buf = frame(1, Buffer.from(JSON.stringify(obj))); for (const [id, c] of clients) if (id !== fromId && !c.dead) try { c.sock.write(buf); } catch (e) {} }
function drop(c) {
  if (c.dead) return; c.dead = true; clients.delete(c.id);
  try { c.sock.end(); } catch (e) {}
  broadcast(c.id, { t: 'left', id: c.id });
}
function onMessage(c, text) {
  let m; try { m = JSON.parse(text); } catch (e) { return; }
  if (m && m.t === 'p' && m.p && typeof m.p === 'object') { c.p = m.p; broadcast(c.id, { t: 'p', id: c.id, p: m.p }); }
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!req.url.startsWith('/ws') || !key) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: 'p' + (nextId++), sock, p: null, buf: Buffer.alloc(0), frag: [], dead: false, alive: true };
  clients.set(c.id, c);
  send(c, { t: 'hello', id: c.id, peers: [...clients.values()].filter(o => o !== c && o.p).map(o => ({ id: o.id, p: o.p })) });
  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > 1 << 20) { drop(c); return; }
    while (c.buf.length >= 2) {
      const b0 = c.buf[0], b1 = c.buf[1], fin = b0 & 0x80, op = b0 & 0x0f, masked = b1 & 0x80;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 65536) { drop(c); return; }
      const need = off + (masked ? 4 : 0) + len; if (c.buf.length < need) return;
      let data = c.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const mk = c.buf.subarray(off, off + 4); data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= mk[i & 3]; }
      c.buf = c.buf.subarray(need);
      if (op === 8) { drop(c); return; }
      if (op === 9) { try { sock.write(frame(10, data)); } catch (e) {} continue; }
      if (op === 10) { c.alive = true; continue; }
      if (op === 1 || op === 0) { c.frag.push(data); if (fin) { const t = Buffer.concat(c.frag).toString('utf8'); c.frag = []; onMessage(c, t); } }
    }
  });
  sock.on('close', () => drop(c)); sock.on('error', () => drop(c));
});
// пинг, чтобы хостинг не обрывал соединение, и чистка «мёртвых» игроков
setInterval(() => { for (const c of clients.values()) { if (!c.alive) { drop(c); continue; } c.alive = false; try { c.sock.write(frame(9, Buffer.alloc(0))); } catch (e) {} } }, 25000);

server.listen(PORT, () => console.log('Grand Black RP работает на порту ' + PORT));
