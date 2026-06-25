const { WebSocketServer } = require('ws');
const { v4: uuidv4 }      = require('uuid');

const PORT = process.env.PORT || 8080;

// rooms: Map<roomId, { hostId, clients: Map<clientId, ws>, chat: [], state: {} }>
const rooms = new Map();

const wss = new WebSocketServer({ port: PORT });

// Удалить комнату и оповестить всех
function destroyRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  broadcast(roomId, { type: 'room_closed' });
  room.clients.forEach(ws => { try { ws.close(); } catch (_) {} });
  rooms.delete(roomId);
  console.log(`[Room ${roomId}] destroyed`);
}

// Отправить всем в комнате (кроме sender, если указан)
function broadcast(roomId, msg, excludeId = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const json = JSON.stringify(msg);
  room.clients.forEach((ws, clientId) => {
    if (clientId === excludeId) return;
    if (ws.readyState === 1) ws.send(json);
  });
}

// Список участников для отправки
function memberList(room) {
  return [
    { id: room.hostId, role: 'host' },
    ...[...room.clients.keys()]
      .filter(id => id !== room.hostId)
      .map(id => ({ id, role: 'guest' }))
  ];
}

wss.on('connection', ws => {
  const clientId = uuidv4();
  let currentRoom = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── PING (heartbeat) ───────────────────────────────────
    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
      return;
    }

    // ── CREATE ROOM ────────────────────────────────────
    if (msg.type === 'create_room') {
      const roomId = uuidv4().slice(0, 8).toUpperCase();
      rooms.set(roomId, {
        hostId:  clientId,
        clients: new Map([[clientId, ws]]),
        chat:    [],
        state:   { episode: null, dubbing: null, player: null, animeSlug: msg.animeSlug || null }
      });
      currentRoom = roomId;
      ws.send(JSON.stringify({ type: 'room_created', roomId, clientId, role: 'host' }));
      console.log(`[Room ${roomId}] created by ${clientId}`);
      return;
    }

    // ── JOIN ROOM ──────────────────────────────────────
    if (msg.type === 'join_room') {
      const roomId = msg.roomId;
      const room   = rooms.get(roomId);
      if (!room) {
        ws.send(JSON.stringify({ type: 'error', code: 'room_not_found' }));
        return;
      }
      room.clients.set(clientId, ws);
      currentRoom = roomId;

      // Отправить присоединившемуся текущее состояние + историю чата
      ws.send(JSON.stringify({
        type:    'room_joined',
        roomId,
        clientId,
        role:    'guest',
        state:   room.state,
        chat:    room.chat,
        members: memberList(room)
      }));

      // Если хост уже смотрит — сразу синхронизируем позицию нового гостя
      if (room.state.currentTime != null) {
        setTimeout(() => {
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({
              type:   'player_control',
              action: room.state.paused ? 'pause' : 'time',
              time:   room.state.currentTime,
            }));
          }
        }, 1500); // небольшая задержка — гость успевает загрузить плеер
      }

      // Оповестить всех об обновлении участников
      broadcast(roomId, { type: 'members_update', members: memberList(room) }, clientId);
      console.log(`[Room ${roomId}] ${clientId} joined`);
      return;
    }

    // Дальше — только участники комнаты
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) return;

    // ── PLAYER SYNC (только хост) ──────────────────────
    if (msg.type === 'player_sync') {
      if (clientId !== room.hostId) return; // гость не может управлять
      room.state = { ...room.state, ...msg.state };
      broadcast(currentRoom, { type: 'player_sync', state: room.state }, clientId);
      return;
    }

    // ── PLAYER CONTROL: play / pause / seek / time (только хост) ──
    if (msg.type === 'player_control') {
      if (clientId !== room.hostId) return;
      // Кешируем текущее время и статус паузы для новых гостей
      if ((msg.action === 'time' || msg.action === 'seek') && msg.time != null) {
        room.state = { ...room.state, currentTime: msg.time };
      }
      if (msg.action === 'pause') room.state = { ...room.state, paused: true };
      if (msg.action === 'play')  room.state = { ...room.state, paused: false };
      broadcast(currentRoom, { type: 'player_control', action: msg.action, time: msg.time }, clientId);
      return;
    }

    // ── CHAT MESSAGE ───────────────────────────────────
    if (msg.type === 'chat_message') {
      const text = String(msg.text || '').trim().slice(0, 500);
      if (!text) return;
      const role  = clientId === room.hostId ? 'host' : 'guest';
      const entry = { id: uuidv4().slice(0, 8), clientId, role, text, ts: Date.now() };
      room.chat.push(entry);
      if (room.chat.length > 200) room.chat.shift(); // лимит 200 сообщений
      broadcast(currentRoom, { type: 'chat_message', message: entry });
      return;
    }

    // ── LEAVE ROOM ─────────────────────────────────────
    if (msg.type === 'leave_room') {
      handleLeave();
      return;
    }
  });

  ws.on('close', () => handleLeave());
  ws.on('error', () => handleLeave());

  function handleLeave() {
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) { currentRoom = null; return; }

    room.clients.delete(clientId);
    console.log(`[Room ${currentRoom}] ${clientId} left`);

    // Если ушёл хост — уничтожить комнату
    if (clientId === room.hostId) {
      destroyRoom(currentRoom);
    } else if (room.clients.size === 0) {
      // Все вышли — удалить комнату
      rooms.delete(currentRoom);
      console.log(`[Room ${currentRoom}] empty, deleted`);
    } else {
      // Уведомить оставшихся
      broadcast(currentRoom, { type: 'members_update', members: memberList(room) });
    }
    currentRoom = null;
  }
});

console.log(`AnimeHub WatchParty server running on port ${PORT}`);
