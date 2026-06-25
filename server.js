const { WebSocketServer } = require('ws');
const { v4: uuidv4 }      = require('uuid');

const PORT = process.env.PORT || 8080;

/* ====================================================
   AnimeHub WatchParty — server.js

   НОВАЯ МОДЕЛЬ (без принудительной синхронизации плеера):
   - Каждый участник (хост и гости) смотрит и управляет своим
     собственным плеером полностью самостоятельно — выбирает
     серию/озвучку/плеер/перемотку сам, без ограничений.
   - Участники периодически шлют на сервер свой личный статус
     просмотра ({episode, dubbing, player, currentTime, paused,
     animeTitle}) — сервер просто ретранслирует его остальным,
     чтобы в общем лобби/чате было видно "кто на чём смотрит".
   - Чат общий для всех, как и раньше.
   - При выходе хоста комната больше НЕ уничтожается — раз нет
     принудительного управления, "хост" — это просто создатель
     комнаты для истории, а не единственный, кто может ей рулить.
     Комната живёт, пока в ней есть хотя бы один участник.
   ==================================================== */

// rooms: Map<roomId, { hostId, clients: Map<clientId, ws>, chat: [], statuses: Map<clientId, status> }>
const rooms = new Map();

const wss = new WebSocketServer({ port: PORT });

// Таймаут неактивности соединения (мс). Если от клиента не было ни
// одного сообщения (включая ping) дольше этого срока — считаем его
// мёртвым и закрываем. Защищает от "фантомных" участников в лобби
// при частых reload/закрытии вкладки без штатного leave_room.
const IDLE_TIMEOUT_MS = 60000;

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

// Список участников со статусами для отправки клиенту
function memberList(room) {
  return [...room.clients.keys()].map(id => ({
    id,
    role:   id === room.hostId ? 'host' : 'guest',
    status: room.statuses.get(id) || null,
  }));
}

wss.on('connection', ws => {
  const clientId = uuidv4();
  let currentRoom = null;
  let lastActivity = Date.now();

  const idleCheck = setInterval(() => {
    if (Date.now() - lastActivity > IDLE_TIMEOUT_MS) {
      try { ws.terminate(); } catch (_) {}
    }
  }, 15000);

  ws.on('message', raw => {
    lastActivity = Date.now();
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
        hostId:   clientId,
        clients:  new Map([[clientId, ws]]),
        chat:     [],
        statuses: new Map(),
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

      ws.send(JSON.stringify({
        type:    'room_joined',
        roomId,
        clientId,
        role:    clientId === room.hostId ? 'host' : 'guest',
        chat:    room.chat,
        members: memberList(room),
      }));

      // Оповестить всех об обновлении участников
      broadcast(roomId, { type: 'members_update', members: memberList(room) }, clientId);
      console.log(`[Room ${roomId}] ${clientId} joined`);
      return;
    }

    // Дальше — только участники комнаты
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) return;

    // ── STATUS UPDATE: каждый участник шлёт СВОЙ статус просмотра ──
    // (серия/озвучка/плеер/время/пауза/название аниме). Никаких
    // ограничений на роль — гость и хост абсолютно равноправны.
    if (msg.type === 'status_update') {
      const status = {
        animeTitle:  msg.status && msg.status.animeTitle  != null ? String(msg.status.animeTitle).slice(0, 200) : null,
        episode:     msg.status && msg.status.episode     != null ? String(msg.status.episode).slice(0, 20)   : null,
        dubbing:     msg.status && msg.status.dubbing     != null ? String(msg.status.dubbing).slice(0, 100)  : null,
        player:      msg.status && msg.status.player      != null ? String(msg.status.player).slice(0, 100)   : null,
        currentTime: msg.status && typeof msg.status.currentTime === 'number' ? msg.status.currentTime        : null,
        paused:      !!(msg.status && msg.status.paused),
        updatedAt:   Date.now(),
      };
      room.statuses.set(clientId, status);
      broadcast(currentRoom, { type: 'status_update', clientId, status }, clientId);
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

  ws.on('close', () => { clearInterval(idleCheck); handleLeave(); });
  ws.on('error', () => { clearInterval(idleCheck); handleLeave(); });

  function handleLeave() {
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) { currentRoom = null; return; }

    room.clients.delete(clientId);
    room.statuses.delete(clientId);
    console.log(`[Room ${currentRoom}] ${clientId} left`);

    if (room.clients.size === 0) {
      // Комната пуста (включая хоста) — удаляем.
      rooms.delete(currentRoom);
      console.log(`[Room ${currentRoom}] empty, deleted`);
    } else {
      // Комната продолжает жить, даже если ушёл хост — управления
      // плеером больше нет, так что "хост" не более чем создатель
      // комнаты. Просто уведомляем оставшихся об обновлении списка.
      broadcast(currentRoom, { type: 'members_update', members: memberList(room) });
    }
    currentRoom = null;
  }
});

console.log(`AnimeHub WatchParty server running on port ${PORT}`);
