const path = require('path');
const os = require('os');
const fs = require('fs');
const express = require('express');
const cookieParser = require('cookie-parser');
const bcrypt = require('bcryptjs');
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

// ---------- хранилище: S3 (постоянное, переживает передеплой), с запасным
// вариантом — временный файл ОС, если переменные S3 не заданы (например,
// при локальной проверке без реального хранилища) ----------
const S3_BUCKET = process.env.S3_BUCKET;
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY;
const S3_SECRET_KEY = process.env.S3_SECRET_KEY;
const S3_ENDPOINT = process.env.S3_ENDPOINT || 'https://s3.twcstorage.ru';
const S3_REGION = process.env.S3_REGION || 'ru-1';
const S3_OBJECT_KEY = 'store.json';
const USE_S3 = !!(S3_BUCKET && S3_ACCESS_KEY && S3_SECRET_KEY);

const FALLBACK_DIR = process.env.DATA_DIR || path.join(os.tmpdir(), 'studio-journal-data');
const FALLBACK_PATH = path.join(FALLBACK_DIR, 'store.json');

let s3Client = null;
if (USE_S3) {
  s3Client = new S3Client({
    region: S3_REGION,
    endpoint: S3_ENDPOINT,
    forcePathStyle: true,
    credentials: { accessKeyId: S3_ACCESS_KEY, secretAccessKey: S3_SECRET_KEY },
  });
  console.log(`Хранилище: S3, бакет "${S3_BUCKET}" (${S3_ENDPOINT}) — данные переживут передеплой.`);
} else {
  console.warn('[ВНИМАНИЕ] Переменные S3 (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY) не заданы — используется временное хранилище на диске. Данные будут потеряны при следующем деплое! Задайте переменные в панели Timeweb.');
  if (!fs.existsSync(FALLBACK_DIR)) fs.mkdirSync(FALLBACK_DIR, { recursive: true });
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

// Не даём запросу к S3 висеть бесконечно: если сеть/хранилище подвисли (не ответили ни успехом,
// ни ошибкой), через указанное время считаем попытку неудачной, вместо того чтобы ждать вечно —
// иначе из-за одного зависшего запроса "крутилка" сохранения (или даже запуск сервера) не
// заканчивалась бы вообще никогда.
function withTimeout(promise, ms) {
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => { if (!settled) { settled = true; resolve({ timedOut: true }); } }, ms);
    promise.then(
      (v) => { if (!settled) { settled = true; clearTimeout(timer); resolve({ timedOut: false, value: v }); } },
      (e) => { if (!settled) { settled = true; clearTimeout(timer); resolve({ timedOut: false, error: e }); } }
    );
  });
}
const S3_TIMEOUT_MS = 10000;

async function loadStoreFromBackend() {
  if (USE_S3) {
    const outcome = await withTimeout(
      s3Client.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: S3_OBJECT_KEY })),
      S3_TIMEOUT_MS
    );
    if (outcome.timedOut) {
      console.error(`Не удалось прочитать данные из S3: не ответил за ${S3_TIMEOUT_MS}мс — начинаем с пустого хранилища.`);
      return { kv: {}, credentials: {} };
    }
    const e = outcome.error;
    if (e) {
      if (e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) {
        console.log('В S3 ещё нет сохранённых данных — начинаем с пустого хранилища.');
      } else {
        console.error('Не удалось прочитать данные из S3, начинаем с пустого хранилища:', e.message);
      }
      return { kv: {}, credentials: {} };
    }
    try {
      const raw = await streamToString(outcome.value.Body);
      const parsed = JSON.parse(raw);
      return { kv: parsed.kv || {}, credentials: parsed.credentials || {} };
    } catch (e2) {
      console.error('Не удалось разобрать данные из S3, начинаем с пустого хранилища:', e2.message);
      return { kv: {}, credentials: {} };
    }
  }
  if (!fs.existsSync(FALLBACK_PATH)) return { kv: {}, credentials: {} };
  try {
    const raw = fs.readFileSync(FALLBACK_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return { kv: parsed.kv || {}, credentials: parsed.credentials || {} };
  } catch (e) {
    console.error('Не удалось прочитать файл данных, начинаем с пустого хранилища:', e.message);
    return { kv: {}, credentials: {} };
  }
}

let store = { kv: {}, credentials: {} };

// ---------- автоматический бэкап по расписанию (снимок всего store.kv раз в сутки) ----------
const AUTO_BACKUP_PREFIX = 'backup:auto:';
const AUTO_BACKUP_RETENTION_DAYS = 14;
function isoDate(d) { return d.toISOString().slice(0, 10); }
function snapshotOfStore() {
  // Копируем весь текущий kv (кроме самих бэкапов, чтобы не вкладывать бэкапы в бэкапы)
  const snapshot = {};
  for (const k of Object.keys(store.kv)) {
    if (!k.startsWith(AUTO_BACKUP_PREFIX) && !k.startsWith(FREQUENT_BACKUP_PREFIX)) snapshot[k] = store.kv[k];
  }
  return snapshot;
}
async function runAutoBackup() {
  try {
    const today = isoDate(new Date());
    const key = AUTO_BACKUP_PREFIX + today;
    store.kv[key] = JSON.stringify({ takenAt: new Date().toISOString(), data: snapshotOfStore() });
    // Чистим бэкапы старше срока хранения
    const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - AUTO_BACKUP_RETENTION_DAYS);
    const cutoffStr = isoDate(cutoff);
    Object.keys(store.kv).forEach(k => {
      if (k.startsWith(AUTO_BACKUP_PREFIX)) {
        const dateStr = k.slice(AUTO_BACKUP_PREFIX.length);
        if (dateStr < cutoffStr) delete store.kv[k];
      }
    });
    await persist();
    console.log(`Автоматический бэкап создан: ${key}`);
  } catch (e) {
    console.error('Ошибка автоматического бэкапа:', e.message);
  }
}

// ---------- частые снимки (каждые 15 минут, короткое хранение) ----------
// Дополняют суточные бэкапы выше: если что-то потёрлось сегодня, не обязательно ждать завтра —
// можно откатиться на состояние 15-минутной давности. Храним недолго (по умолчанию 48 часов),
// иначе снимков накопится слишком много и они займут много места.
const FREQUENT_BACKUP_PREFIX = 'backup:freq:';
const FREQUENT_BACKUP_INTERVAL_MS = 15 * 60 * 1000;
const FREQUENT_BACKUP_RETENTION_MS = 48 * 60 * 60 * 1000;
async function runFrequentBackup() {
  try {
    const now = new Date();
    const key = FREQUENT_BACKUP_PREFIX + now.toISOString();
    store.kv[key] = JSON.stringify({ takenAt: now.toISOString(), data: snapshotOfStore() });
    const cutoffMs = now.getTime() - FREQUENT_BACKUP_RETENTION_MS;
    Object.keys(store.kv).forEach(k => {
      if (k.startsWith(FREQUENT_BACKUP_PREFIX)) {
        const tsStr = k.slice(FREQUENT_BACKUP_PREFIX.length);
        const ts = Date.parse(tsStr);
        if (!ts || ts < cutoffMs) delete store.kv[k];
      }
    });
    await persist();
    console.log(`Частый снимок создан: ${key}`);
  } catch (e) {
    console.error('Ошибка частого снимка:', e.message);
  }
}

// Пишем по очереди (без параллельных записей), чтобы не гонять одновременные PUT
let writeQueue = Promise.resolve();
// persist() теперь возвращает true/false — реально ли данные записались в хранилище (S3 или
// локальный файл). Раньше ошибка записи просто логировалась на сервере, а вызывающий код (в
// т.ч. обработчики PUT/DELETE ниже) об этом не знал и отвечал браузеру "ok:true" в любом случае —
// из-за этого сбой записи в S3 выглядел для сотрудника как "сохранилось", а после перезапуска
// сервера (деплой, технический рестарт) данные, которые реально не долетели до S3, пропадали.
// withTimeout()/S3_TIMEOUT_MS объявлены выше, рядом с loadStoreFromBackend — не даём записи
// висеть бесконечно по той же причине (иначе "крутилка" сохранения не кончалась бы никогда,
// и все последующие сохранения вставали бы в очередь позади зависшего).
function persist() {
  writeQueue = writeQueue.then(async () => {
    const body = JSON.stringify(store);
    if (USE_S3) {
      const outcome = await withTimeout(
        s3Client.send(new PutObjectCommand({ Bucket: S3_BUCKET, Key: S3_OBJECT_KEY, Body: body, ContentType: 'application/json' })),
        S3_TIMEOUT_MS
      );
      if (outcome.timedOut) {
        console.error(`Ошибка сохранения данных в S3: не ответил за ${S3_TIMEOUT_MS}мс (похоже, зависло соединение)`);
        return false;
      }
      if (outcome.error) {
        console.error('Ошибка сохранения данных в S3:', outcome.error.message);
        return false;
      }
      return true;
    }
    return await new Promise((resolve) => {
      const tmpPath = FALLBACK_PATH + '.tmp';
      fs.writeFile(tmpPath, body, (err) => {
        if (err) { console.error('Ошибка записи данных:', err.message); return resolve(false); }
        fs.rename(tmpPath, FALLBACK_PATH, (err2) => {
          if (err2) { console.error('Ошибка сохранения данных:', err2.message); return resolve(false); }
          resolve(true);
        });
      });
    });
  });
  return writeQueue;
}

const SESSION_SECRET = process.env.SESSION_SECRET || 'change-me-in-production-please';
if (!process.env.SESSION_SECRET) {
  console.warn('[ВНИМАНИЕ] SESSION_SECRET не задан в переменных окружения — используется значение по умолчанию. Задайте свой секрет в панели Timeweb (Приложения → Переменные окружения).');
}

const app = express();
app.use(express.json({ limit: '5mb' }));
app.use(cookieParser(SESSION_SECRET));

// Порт открывается сразу (см. app.listen в конце файла), не дожидаясь загрузки данных из S3,
// чтобы проверка готовности контейнера на хостинге не считала сервер неживым, пока он честно
// ждёт ответа от S3 (до S3_TIMEOUT_MS). Пока данные ещё не подгружены, все запросы к /api/*
// получают понятный ответ 503 вместо того, чтобы работать с пустыми/неполными данными.
let serverReady = false;
app.use('/api', (req, res, next) => {
  if (!serverReady) return res.status(503).json({ error: 'Сервер ещё загружается, попробуйте через несколько секунд' });
  next();
});

// ---------- вход пользователя: подписанная cookie вместо серверной сессии в памяти ----------
// Раньше вход хранился в express-session (память процесса Node). Проблема: при любом
// перезапуске/передеплое/пересоздании контейнера на Timeweb (а также если приложение
// когда-нибудь будет работать в нескольких экземплярах) эта память обнуляется — и все,
// кто уже вошёл, внезапно перестают сохранять изменения (сервер отвечает 401, будто
// они не входили). Подписанная cookie не зависит от памяти сервера: она сама содержит
// id сотрудника и проверяется секретным ключом, поэтому вход переживает перезапуск сервера.
const AUTH_COOKIE = 'auth';
const AUTH_COOKIE_MAX_AGE = 30 * 24 * 60 * 60 * 1000; // 30 дней
function setAuthCookie(res, employeeId) {
  res.cookie(AUTH_COOKIE, employeeId, {
    signed: true,
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    maxAge: AUTH_COOKIE_MAX_AGE,
  });
}

// ---------- helpers ----------
function getEmployees() {
  const raw = store.kv['roster:employees'];
  return raw ? JSON.parse(raw) : [];
}
function findEmployee(id) {
  return getEmployees().find(e => e.id === id) || null;
}
function hasAnyCredential() {
  return Object.keys(store.credentials).length > 0;
}
function currentEmployee(req) {
  const employeeId = req.signedCookies && req.signedCookies[AUTH_COOKIE];
  if (!employeeId) return null;
  return findEmployee(employeeId);
}
function requireAuth(req, res, next) {
  const emp = currentEmployee(req);
  if (!emp) return res.status(401).json({ error: 'Не авторизован' });
  req.employee = emp;
  next();
}
function isPrivilegedRole(role) {
  return role === 'Руководитель' || role === 'Ассистент';
}
// Ключи, которые может читать/писать только Руководитель/Ассистент
function isRestrictedKey(key) {
  return key.startsWith('funds:') || key.startsWith('personal:') || key.startsWith('backup:') || key.startsWith('audit:');
}
// Ключ, который может ИЗМЕНЯТЬ только Руководитель/Ассистент (но читать может любой залогиненный)
function isRosterKey(key) {
  return key === 'roster:employees';
}

// Хранилище — это просто пары ключ→JSON-строка, сервер не разбирает содержимое большинства
// ключей. Но для двух самых «дорогих» ключей (общий список задач и общий список проектов) есть
// смысл проверить даже на этом уровне: не даём одним запросом стереть чужую задачу/чужой проект
// в обход правил, которые интерфейс и так соблюдает (это не полноценная замена переработки
// хранения на отдельные проверяемые запросы для каждого действия, а точечная защита от
// злоупотребления самым разрушительным — массовым/чужим удалением).
function parseJsonArray(raw) {
  if (typeof raw !== 'string') return null;
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : null;
  } catch (e) {
    return null;
  }
}
function findDisallowedDeletion(key, oldRaw, newRaw, employee) {
  if (isPrivilegedRole(employee.role)) return null;
  if (key !== 'tasks:work' && key !== 'tasks:projects') return null;
  if (oldRaw === undefined) return null; // ключа ещё не было — нечего защищать
  const oldArr = parseJsonArray(oldRaw);
  const newArr = parseJsonArray(newRaw);
  if (!oldArr || !newArr) return null; // не похоже на ожидаемый формат — не наша забота
  const newIds = new Set(newArr.filter(x => x && x.id).map(x => x.id));
  const removed = oldArr.filter(x => x && x.id && !newIds.has(x.id));
  if (!removed.length) return null;
  if (key === 'tasks:projects') {
    return 'Удалять проекты может только Руководитель или Ассистент';
  }
  // key === 'tasks:work': удалить задачу может только тот, кто её поставил (или Руководитель/Ассистент).
  // Удаление задачи вместе с подзадачами (и серии повторяющихся задач) — обычное дело в
  // интерфейсе, а подзадача/копия из серии не обязана иметь того же автора буквально в поле
  // creatorId — поэтому разрешаем удаление и тогда, когда сам автор есть где-то по цепочке
  // родителей среди удаляемых задач (именно так это и работает в интерфейсе).
  function creatorChainIncludes(task, employeeId) {
    let current = task;
    const seen = new Set();
    while (current) {
      if (current.creatorId === employeeId) return true;
      if (!current.parentId || seen.has(current.id)) return false;
      seen.add(current.id);
      current = oldArr.find(t => t.id === current.parentId);
    }
    return false;
  }
  const forbidden = removed.find(t => !creatorChainIncludes(t, employee.id));
  if (forbidden) return 'Удалить задачу может только тот, кто её поставил — либо Руководитель или Ассистент';
  return null;
}

// ---------- публично: список сотрудников (без паролей) ----------
app.get('/api/employees', (req, res) => {
  const list = getEmployees().map(e => ({ id: e.id, name: e.name, role: e.role, hasPassword: !!store.credentials[e.id] }));
  res.json({ employees: list, bootstrapNeeded: !hasAnyCredential() });
});

// ---------- вход ----------
app.post('/api/login', (req, res) => {
  const { employeeId, password } = req.body || {};
  if (!employeeId || !password) return res.status(400).json({ error: 'Укажите сотрудника и пароль' });
  const emp = findEmployee(employeeId);
  if (!emp) return res.status(404).json({ error: 'Сотрудник не найден' });
  const hash = store.credentials[employeeId];
  if (!hash) return res.status(401).json({ error: 'У этого сотрудника ещё не задан пароль' });
  if (!bcrypt.compareSync(password, hash)) {
    return res.status(401).json({ error: 'Неверный пароль' });
  }
  setAuthCookie(res, employeeId);
  res.json({ ok: true, employee: { id: emp.id, name: emp.name, role: emp.role } });
});

app.post('/api/logout', (req, res) => {
  res.clearCookie(AUTH_COOKIE);
  res.json({ ok: true });
});

app.get('/api/me', (req, res) => {
  const emp = currentEmployee(req);
  if (!emp) return res.status(401).json({ error: 'Не авторизован' });
  res.json({ employee: { id: emp.id, name: emp.name, role: emp.role } });
});

// Установка / смена / снятие пароля.
// - Бутстрап: если вообще ни у кого нет пароля, разрешаем задать самый первый без сессии.
// - После бутстрапа:
//     * Руководитель/Ассистент могут задать/сбросить пароль ЛЮБОГО сотрудника без старого пароля.
//     * Сотрудник может сменить СВОЙ пароль, но должен указать текущий (если он уже задан).
app.post('/api/employees/:id/password', async (req, res) => {
  const targetId = req.params.id;
  const target = findEmployee(targetId);
  if (!target) return res.status(404).json({ error: 'Сотрудник не найден' });
  const { newPassword, currentPassword } = req.body || {};

  const bootstrapOpen = !hasAnyCredential();
  const caller = currentEmployee(req);

  if (!bootstrapOpen) {
    if (!caller) return res.status(401).json({ error: 'Не авторизован' });
    const isSelf = caller.id === targetId;
    const callerPrivileged = isPrivilegedRole(caller.role);
    if (!isSelf && !callerPrivileged) {
      return res.status(403).json({ error: 'Менять пароль другого сотрудника может только Руководитель или Ассистент' });
    }
    if (isSelf) {
      const existingHash = store.credentials[targetId];
      if (existingHash) {
        if (!currentPassword || !bcrypt.compareSync(currentPassword, existingHash)) {
          return res.status(401).json({ error: 'Текущий пароль неверен' });
        }
      }
    }
  }

  if (!newPassword) {
    const oldHash = store.credentials[targetId];
    delete store.credentials[targetId];
    if (!(await persist())) {
      if (oldHash !== undefined) store.credentials[targetId] = oldHash;
      return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
    }
    return res.json({ ok: true, removed: true });
  }
  const oldHash = store.credentials[targetId];
  store.credentials[targetId] = bcrypt.hashSync(newPassword, 10);
  if (!(await persist())) {
    if (oldHash === undefined) delete store.credentials[targetId]; else store.credentials[targetId] = oldHash;
    return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
  }
  res.json({ ok: true });
});

// ---------- универсальное хранилище ключ-значение (замена window.storage) ----------
app.get('/api/storage/:key', requireAuth, (req, res) => {
  const key = req.params.key;
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  const value = store.kv[key];
  if (value === undefined) return res.status(404).json({ error: 'not found' });
  res.json({ key, value });
});

app.put('/api/storage/:key', requireAuth, async (req, res) => {
  const key = req.params.key;
  const { value } = req.body || {};
  if (typeof value !== 'string') return res.status(400).json({ error: 'value должен быть строкой' });
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  if (isRosterKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Изменять список сотрудников может только Руководитель или Ассистент' });
  }
  const deletionError = findDisallowedDeletion(key, store.kv[key], value, req.employee);
  if (deletionError) {
    return res.status(403).json({ error: deletionError });
  }
  // Важно: сохраняем старое значение и дожидаемся РЕАЛЬНОЙ записи в хранилище (S3/диск), прежде
  // чем отвечать браузеру "ok". Если запись не удалась — откатываем в памяти и честно сообщаем об
  // ошибке, а не делаем вид, что всё сохранилось (раньше именно так терялись изменения).
  const oldValue = store.kv[key];
  store.kv[key] = value;
  const saved = await persist();
  if (!saved) {
    if (oldValue === undefined) delete store.kv[key]; else store.kv[key] = oldValue;
    return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
  }
  res.json({ ok: true });
});

app.delete('/api/storage/:key', requireAuth, async (req, res) => {
  const key = req.params.key;
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  if (isRosterKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Изменять список сотрудников может только Руководитель или Ассистент' });
  }
  // Полное удаление ключа с общим списком задач/проектов стёрло бы вообще всё сразу — тот же
  // риск, что и при точечном удалении через PUT, только хуже, так что для него действует то же
  // правило (и приложение само никогда не удаляет эти ключи целиком, только перезаписывает).
  if ((key === 'tasks:work' || key === 'tasks:projects') && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  const oldValue = store.kv[key];
  const hadValue = Object.prototype.hasOwnProperty.call(store.kv, key);
  delete store.kv[key];
  const saved = await persist();
  if (!saved) {
    if (hadValue) store.kv[key] = oldValue;
    return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
  }
  res.json({ ok: true });
});

// ---------- список автоматических бэкапов (только Руководитель/Ассистент) ----------
app.get('/api/auto-backups', requireAuth, (req, res) => {
  if (!isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  const list = Object.keys(store.kv)
    .filter(k => k.startsWith(AUTO_BACKUP_PREFIX))
    .map(k => {
      const date = k.slice(AUTO_BACKUP_PREFIX.length);
      let takenAt = null;
      try { takenAt = JSON.parse(store.kv[k]).takenAt; } catch (e) {}
      return { key: k, date, takenAt };
    })
    .sort((a, b) => b.date.localeCompare(a.date));
  const frequentList = Object.keys(store.kv)
    .filter(k => k.startsWith(FREQUENT_BACKUP_PREFIX))
    .map(k => {
      let takenAt = null;
      try { takenAt = JSON.parse(store.kv[k]).takenAt; } catch (e) {}
      return { key: k, takenAt: takenAt || k.slice(FREQUENT_BACKUP_PREFIX.length) };
    })
    .sort((a, b) => b.takenAt.localeCompare(a.takenAt));
  res.json({ backups: list, frequentBackups: frequentList });
});

// ---------- статика (сам интерфейс) ----------
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---------- запуск ----------
// ВАЖНО: порт открываем СРАЗУ, не дожидаясь загрузки данных из S3 (см. комментарий и флаг
// serverReady у app.use(cookieParser...) в начале файла) — иначе проверка готовности контейнера на
// Timeweb не дожидается открытия порта и считает деплой неудачным.
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Единый журнал: сервер запущен на порту ${PORT} (слушает 0.0.0.0), данные ещё загружаются...`);
});

(async function main() {
  store = await loadStoreFromBackend();

  // Сеем список ролей по умолчанию при самом первом запуске, чтобы было кого выбрать при входе
  if (!store.kv['roster:employees']) {
    const DEFAULT_ROLES = ['Руководитель', 'Ассистент', 'Маркетолог', 'Старший администратор', 'Менеджер', 'Администратор 1 смена', 'Администратор 2 смена'];
    const employees = DEFAULT_ROLES.map((role, i) => ({ id: `emp-seed-${i}-${Date.now()}`, name: role, role }));
    store.kv['roster:employees'] = JSON.stringify(employees);
    await persist();
    console.log('Список сотрудников по умолчанию создан (6 ролей). Задайте пароли через экран входа.');
  }

  serverReady = true;
  console.log('Данные загружены, сервер готов к работе.');

  // Автобэкап: один раз вскоре после старта (на случай долгого простоя сервера), затем раз в сутки
  setTimeout(runAutoBackup, 60 * 1000);
  setInterval(runAutoBackup, 24 * 60 * 60 * 1000);
  // Частые снимки — каждые 15 минут (первый почти сразу после старта)
  setTimeout(runFrequentBackup, 90 * 1000);
  setInterval(runFrequentBackup, FREQUENT_BACKUP_INTERVAL_MS);
})();
