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
// Снимки (бэкапы) лежат в ОТДЕЛЬНОМ объекте. Раньше они были внутри store.json, а каждый снимок —
// полная копия всех данных (до ~200 копий), поэтому файл раздувался, и каждое сохранение задачи
// закачивало в S3 сотни килобайт/мегабайты ради одной галочки — отсюда долгие сохранения.
const S3_BACKUPS_KEY = 'store-backups.json';
function isBackupKey(k) { return k.startsWith('backup:'); }
const USE_S3 = !!(S3_BUCKET && S3_ACCESS_KEY && S3_SECRET_KEY);

const FALLBACK_DIR = process.env.DATA_DIR || path.join(os.tmpdir(), 'studio-journal-data');
const FALLBACK_PATH = path.join(FALLBACK_DIR, 'store.json');
const FALLBACK_BACKUPS_PATH = path.join(FALLBACK_DIR, 'store-backups.json');

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

// ---------- резервная копия на почту (независимый "второй карман" на случай новых сюрпризов
// с S3 — присылается раз в день целиком на email, тем же способом, каким человек сам может
// скачать "Резервную копию" из интерфейса) ----------
// Используем Resend (https://resend.com) — так же, как уже настроено в других приложениях
// Карины — вместо SMTP: проще (не нужен пароль приложения Gmail), те же переменные окружения.
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const BACKUP_EMAIL_TO = process.env.BACKUP_EMAIL_TO;
// Resend без подтверждённого домена разрешает отправлять только с onboarding@resend.dev —
// этого достаточно для бэкапа самому себе. Если в другом приложении уже настроен свой домен,
// можно переопределить через BACKUP_EMAIL_FROM.
const BACKUP_EMAIL_FROM = process.env.BACKUP_EMAIL_FROM || 'Единый журнал <onboarding@resend.dev>';
const EMAIL_BACKUP_ENABLED = !!(RESEND_API_KEY && BACKUP_EMAIL_TO);
if (EMAIL_BACKUP_ENABLED) {
  console.log(`Резервная копия на почту: включена, будет отправляться на ${BACKUP_EMAIL_TO} через Resend.`);
} else {
  console.warn('[ВНИМАНИЕ] Резервная копия на почту выключена — не заданы RESEND_API_KEY/BACKUP_EMAIL_TO в переменных окружения.');
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

// Читает один JSON-объект хранилища (S3-ключ или локальный файл). Возвращает одно из трёх:
//   {state:'ok', parsed}  — прочитали и разобрали;
//   {state:'missing'}     — объекта точно нет (NoSuchKey/404 или файла нет);
//   {state:'error'}       — не получилось прочитать (таймаут, сеть, битый JSON) — НЕ значит "пусто".
async function readJsonObject(s3Key, filePath) {
  if (USE_S3) {
    const outcome = await withTimeout(
      s3Client.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: s3Key })),
      S3_TIMEOUT_MS
    );
    if (outcome.timedOut) {
      console.error(`Не удалось прочитать "${s3Key}" из S3: не ответил за ${S3_TIMEOUT_MS}мс.`);
      return { state: 'error' };
    }
    const e = outcome.error;
    if (e) {
      if (e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) return { state: 'missing' };
      console.error(`Не удалось прочитать "${s3Key}" из S3:`, e.message);
      return { state: 'error' };
    }
    try {
      const raw = await streamToString(outcome.value.Body);
      return { state: 'ok', parsed: JSON.parse(raw) };
    } catch (e2) {
      console.error(`Не удалось разобрать "${s3Key}" из S3 (файл повреждён или пуст):`, e2.message);
      return { state: 'error' };
    }
  }
  if (!fs.existsSync(filePath)) return { state: 'missing' };
  try {
    return { state: 'ok', parsed: JSON.parse(fs.readFileSync(filePath, 'utf8')) };
  } catch (e) {
    console.error(`Не удалось прочитать файл данных "${filePath}":`, e.message);
    return { state: 'error' };
  }
}

// ВАЖНО (урок из реального инцидента с потерей данных): эта функция ДОЛЖНА различать два разных
// случая —
//   1) "в хранилище точно ничего нет" (ключа не существует — NoSuchKey/404): это по-настоящему
//      пустое хранилище, с ним safe работать как с первым запуском.
//   2) "не получилось прочитать" (таймаут, сеть, неверные ключи доступа, повреждённый JSON):
//      это НЕ означает, что данных нет — они могут быть целы, просто сейчас недоступны.
// Явно помечаем тип пустоты через `confirmedEmpty`; только настоящий "первый запуск" разрешает
// серверу создавать данные по умолчанию и писать поверх.
// Данные лежат в ДВУХ объектах: основной (store.json — живые данные и пароли) и отдельный файл
// со снимками (store-backups.json). Ненадёжное чтение ЛЮБОГО из них считается сбоем — иначе, не
// прочитав снимки, сервер потом записал бы новый файл снимков поверх старого и потерял их.
async function loadStoreFromBackend() {
  const live = await readJsonObject(S3_OBJECT_KEY, FALLBACK_PATH);
  const backups = await readJsonObject(S3_BACKUPS_KEY, FALLBACK_BACKUPS_PATH);
  if (live.state === 'error' || backups.state === 'error') {
    return { kv: {}, credentials: {}, confirmedEmpty: false, legacyBackupsInLive: false };
  }
  if (live.state === 'missing' && backups.state === 'missing') {
    console.log('В хранилище ещё нет сохранённых данных — это действительно первый запуск.');
    return { kv: {}, credentials: {}, confirmedEmpty: true, legacyBackupsInLive: false };
  }
  const liveKv = (live.parsed && live.parsed.kv) || {};
  const backupsKv = (backups.parsed && backups.parsed.kv) || {};
  // Старый формат: снимки лежали прямо в store.json. Их надо сперва перенести в отдельный файл.
  const legacyBackupsInLive = Object.keys(liveKv).some(isBackupKey);
  return {
    kv: Object.assign({}, backupsKv, liveKv),
    credentials: (live.parsed && live.parsed.credentials) || {},
    confirmedEmpty: false,
    legacyBackupsInLive,
  };
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
    if (!k.startsWith(AUTO_BACKUP_PREFIX) && !k.startsWith(FREQUENT_BACKUP_PREFIX) && !k.startsWith('integrations:')) snapshot[k] = store.kv[k];
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
    await saveBackups();
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
// Не делаем новый снимок, если с прошлого снимка ничего не менялось: раньше каждые 15 минут
// (даже ночью) добавлялась ещё одна полная копия одних и тех же данных.
let lastFrequentSig = null;
async function runFrequentBackup() {
  try {
    const now = new Date();
    const key = FREQUENT_BACKUP_PREFIX + now.toISOString();
    const snapshot = snapshotOfStore();
    const sig = require('crypto').createHash('sha1').update(JSON.stringify(snapshot)).digest('hex');
    let changed = false;
    if (sig !== lastFrequentSig) {
      store.kv[key] = JSON.stringify({ takenAt: now.toISOString(), data: snapshot });
      lastFrequentSig = sig;
      changed = true;
    }
    const cutoffMs = now.getTime() - FREQUENT_BACKUP_RETENTION_MS;
    Object.keys(store.kv).forEach(k => {
      if (k.startsWith(FREQUENT_BACKUP_PREFIX)) {
        const tsStr = k.slice(FREQUENT_BACKUP_PREFIX.length);
        const ts = Date.parse(tsStr);
        if (!ts || ts < cutoffMs) { delete store.kv[k]; changed = true; }
      }
    });
    if (!changed) return;
    await saveBackups();
    console.log(`Частый снимок создан: ${key}`);
  } catch (e) {
    console.error('Ошибка частого снимка:', e.message);
  }
}

// ---------- email-бэкап (раз в день отдельно от S3 — "второй карман" на случай новых проблем
// именно с S3/хранилищем) ----------
// Собираем файл в ТОМ ЖЕ формате, что и кнопка "Скачать резервную копию" в интерфейсе
// (exportAllData/importAllData на клиенте), чтобы в случае чего его можно было загрузить обратно
// через "Восстановить из файла" без всякой ручной возни с форматом.
function safeParseArray(raw) {
  if (typeof raw !== 'string') return [];
  try { const v = JSON.parse(raw); return Array.isArray(v) ? v : []; } catch (e) { return []; }
}
function buildExportPayload() {
  const kv = store.kv;
  return {
    app: 'studio-journal',
    version: 2,
    exportedAt: new Date().toISOString(),
    employees: safeParseArray(kv['roster:employees']),
    events: safeParseArray(kv['calendar:events']),
    tasksWork: safeParseArray(kv['tasks:work']),
    projectsWork: safeParseArray(kv['tasks:projects']),
    blocksWork: safeParseArray(kv['tasks:blocks']),
    ideaBankWork: safeParseArray(kv['tasks:ideabank']),
    checklistsWork: safeParseArray(kv['tasks:checklists']),
    quickIdeasWork: safeParseArray(kv['tasks:quickideas']),
    fundsEntries: safeParseArray(kv['funds:ledger']),
    trash: safeParseArray(kv['trash:items']),
    // Личные данные сотрудников (зашифрованные на клиенте) переносим как есть, без расшифровки —
    // сервер и так их содержимое не видит.
    personalBlobRaw: kv['tasks:personal:blob'] || undefined,
    personalSalt: kv['tasks:personal:salt'] || undefined,
  };
}
async function sendBackupEmail(trigger) {
  if (!EMAIL_BACKUP_ENABLED) return { ok: false, error: 'email-бэкап не настроен (нет RESEND_API_KEY/BACKUP_EMAIL_TO)' };
  try {
    const payload = buildExportPayload();
    const dateStr = new Date().toISOString().slice(0, 10);
    const taskCount = payload.tasksWork.length;
    const projectCount = payload.projectsWork.length;
    const empCount = payload.employees.length;
    const fileContent = JSON.stringify(payload, null, 2);
    const outcome = await withTimeout(
      fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          from: BACKUP_EMAIL_FROM,
          to: BACKUP_EMAIL_TO,
          subject: `Единый журнал — резервная копия за ${dateStr}`,
          text: `Автоматическая резервная копия данных «Единого журнала».\n\nСотрудников: ${empCount}\nПроектов: ${projectCount}\nЗадач: ${taskCount}\n\nЧтобы восстановить: откройте приложение (под Руководителем/Ассистентом) → значок 🗄️ в шапке → «Восстановить из файла» → выберите приложенный файл.\n\nЭто автоматическое письмо (${trigger || 'по расписанию'}), отвечать на него не нужно.`,
          attachments: [{
            filename: `studio-journal-backup-${dateStr}.json`,
            content: Buffer.from(fileContent, 'utf8').toString('base64'),
          }],
        }),
      }),
      S3_TIMEOUT_MS
    );
    if (outcome.timedOut) throw new Error(`Resend не ответил за ${S3_TIMEOUT_MS}мс`);
    if (outcome.error) throw outcome.error;
    const r = outcome.value;
    if (!r.ok) {
      const errText = await r.text().catch(() => '');
      throw new Error(`Resend вернул ${r.status}: ${errText}`);
    }
    console.log(`Резервная копия отправлена на почту (${BACKUP_EMAIL_TO}).`);
    return { ok: true };
  } catch (e) {
    console.error('Ошибка отправки резервной копии на почту:', e.message);
    return { ok: false, error: e.message };
  }
}

// Запись одного объекта хранилища (S3 или локальный файл). Возвращает true/false — реально ли
// записалось. Не даём записи висеть бесконечно (иначе "крутилка" сохранения не кончалась бы никогда).
async function writeJsonObject(s3Key, filePath, body, timeoutMs) {
  if (USE_S3) {
    const outcome = await withTimeout(
      s3Client.send(new PutObjectCommand({ Bucket: S3_BUCKET, Key: s3Key, Body: body, ContentType: 'application/json' })),
      timeoutMs
    );
    if (outcome.timedOut) {
      console.error(`Ошибка сохранения "${s3Key}" в S3: не ответил за ${timeoutMs}мс (похоже, зависло соединение)`);
      return false;
    }
    if (outcome.error) {
      console.error(`Ошибка сохранения "${s3Key}" в S3:`, outcome.error.message);
      return false;
    }
    return true;
  }
  return await new Promise((resolve) => {
    const tmpPath = filePath + '.tmp';
    fs.writeFile(tmpPath, body, (err) => {
      if (err) { console.error('Ошибка записи данных:', err.message); return resolve(false); }
      fs.rename(tmpPath, filePath, (err2) => {
        if (err2) { console.error('Ошибка сохранения данных:', err2.message); return resolve(false); }
        resolve(true);
      });
    });
  });
}

// true — файл со снимками уже содержит ВСЕ снимки, что есть в памяти, и их можно не дублировать в
// основном store.json. Пока false (например, сразу после перехода со старого формата, где снимки
// лежали внутри store.json) основной файл по-прежнему пишется целиком, со снимками — так что при
// любом сбое ничего не теряется.
let backupsFilePersisted = false;

// persist() возвращает true/false — реально ли данные записались в хранилище. Обработчики PUT/DELETE
// дожидаются результата и при неудаче откатывают изменение (иначе сбой записи выглядел бы как
// "сохранилось", а после рестарта данные пропадали).
// Параллельные сохранения СКЛЕИВАЮТСЯ: пока идёт закачка, все новые вызовы ждут одну общую следующую
// закачку (она берёт самое свежее состояние) — а не выстраиваются в очередь по одной полной закачке
// на каждое нажатие. Это ускоряет сохранение, когда несколько человек работают одновременно.
let persistTail = Promise.resolve();
let persistPending = null;
async function doPersist() {
  const kv = {};
  for (const k of Object.keys(store.kv)) {
    if (backupsFilePersisted && isBackupKey(k)) continue; // снимки живут в отдельном файле
    kv[k] = store.kv[k];
  }
  const body = JSON.stringify({ kv, credentials: store.credentials });
  const t0 = Date.now();
  const ok = await writeJsonObject(S3_OBJECT_KEY, FALLBACK_PATH, body, S3_TIMEOUT_MS);
  const ms = Date.now() - t0;
  if (ms > 1500) console.warn(`Медленное сохранение: ${Math.round(body.length / 1024)} КБ за ${ms}мс`);
  return ok;
}
function persist() {
  if (persistPending) return persistPending;
  const run = persistTail.then(() => { persistPending = null; return doPersist(); });
  persistPending = run;
  persistTail = run.catch(() => {});
  return run;
}

// Отдельный файл со снимками. Пишется только когда снимок реально создан/удалён по сроку — а не при
// каждом сохранении задачи — и своей очередью, чтобы долгая закачка снимков не задерживала
// обычные сохранения. Тайм-аут больше: файл может быть крупным.
const BACKUPS_TIMEOUT_MS = 45000;
let backupsTail = Promise.resolve();
function persistBackups() {
  const run = backupsTail.then(async () => {
    const kv = {};
    for (const k of Object.keys(store.kv)) if (isBackupKey(k)) kv[k] = store.kv[k];
    return await writeJsonObject(S3_BACKUPS_KEY, FALLBACK_BACKUPS_PATH, JSON.stringify({ kv }), BACKUPS_TIMEOUT_MS);
  });
  backupsTail = run.catch(() => {});
  return run;
}
// Сохраняет снимки. Если файл снимков записан успешно — и мы ещё держали снимки в основном файле
// (старый формат) — переписывает основной файл уже без них, чтобы он стал маленьким. Если запись
// снимков не удалась, а основной файл их ещё содержит — пишем по-старому целиком, чтобы не потерять.
async function saveBackups() {
  const ok = await persistBackups();
  if (ok) {
    if (!backupsFilePersisted) {
      backupsFilePersisted = true;
      await persist();
    }
  } else if (!backupsFilePersisted) {
    await persist();
  }
  return ok;
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
// Роль «Муж» / «Семья»: только просмотр календаря Руководителя (/api/family/calendar). Всё остальное — закрыто.
function isFamilyRole(role) {
  const r = String(role || '').trim().toLowerCase();
  return r === 'муж' || r === 'семья';
}
function requireAuth(req, res, next) {
  const emp = currentEmployee(req);
  if (!emp) return res.status(401).json({ error: 'Не авторизован' });
  if (isFamilyRole(emp.role)) return res.status(403).json({ error: 'Для вашей роли доступен только просмотр календаря' });
  req.employee = emp;
  next();
}
function isPrivilegedRole(role) {
  return role === 'Руководитель' || role === 'Ассистент';
}
// ---------- видимость задач по оргструктуре ----------
// Руководитель и Ассистент видят все задачи. Остальные — только свои (где они исполнитель или постановщик)
// и задачи своих подчинённых: Старший администратор — задачи Администраторов, РОП — задачи Менеджеров.
// Сервер сам отфильтровывает список при чтении и аккуратно «вшивает» правки обратно при записи, поэтому
// чужие задачи (и комментарии внутри них) недоступны даже через запрос в обход интерфейса.
const TASK_LIST_KEYS = new Set(['tasks:work', 'archive:tasks']);
function isRop(role) { return /^(роп|руководитель отдела продаж)$/i.test(String(role || '').trim()); }
function isSubordinateRole(managerRole, subRole) {
  const m = String(managerRole || '').trim(), r = String(subRole || '').trim();
  if (m === 'Старший администратор') return /^администратор/i.test(r);
  if (isRop(m)) return /менеджер/i.test(r) && !isRop(r);
  return false;
}
function makeTaskVisibility(employee) {
  if (isPrivilegedRole(employee.role)) return () => true;
  const byId = {};
  getEmployees().forEach(e => { if (e && e.id) byId[e.id] = e; });
  return (t) => {
    if (!t) return false;
    if (t.assigneeId === employee.id || t.creatorId === employee.id) return true;
    // задачи по заявкам B2B общие для менеджеров по продажам (база заявок у них общая; иначе при передаче заявки задачи задваивались бы)
    if (t.b2bLeadId && employee.role === 'Менеджер') return true;
    const a = t.assigneeId ? byId[t.assigneeId] : null;
    return !!(a && isSubordinateRole(employee.role, a.role));
  };
}
// Сводит список, который прислал браузер (видит только «своё»), с полным списком на сервере: невидимые
// задачи остаются как были и на своих местах, видимые заменяются присланными.
function mergeVisibleTasks(oldAll, visible, clientArr) {
  const hiddenIds = new Set(oldAll.filter(t => t && !visible(t)).map(t => t.id));
  const incoming = clientArr.filter(t => t && t.id && !hiddenIds.has(t.id));
  const merged = [];
  let k = 0;
  oldAll.forEach(t => {
    if (t && !hiddenIds.has(t.id)) { if (k < incoming.length) merged.push(incoming[k++]); }
    else merged.push(t);
  });
  while (k < incoming.length) merged.push(incoming[k++]);
  return merged;
}
// Ключи, которые может читать/писать только Руководитель/Ассистент
// ---------- личные данные: у каждого сотрудника своё закрытое пространство ----------
// Страница по-прежнему обращается к ключам personal:tasks, personal:events и т. д., но сервер сам подменяет
// их на personal:<id сотрудника>:tasks … — id берётся из сессии, поэтому прочитать или записать чужие личные
// данные нельзя (даже Руководителю или Ассистенту). Из снимков (бэкапов) чужие личные данные при выдаче вырезаются.
const PERSONAL_LEGACY_RE = /^personal:(tasks|projects|funds|events|trash|archiveTasks|archiveEvents)$/;
// Исключение: личные ФИНАНСЫ Руководителя общие с Ассистентом — Ассистент работает с теми же записями
// (по решению владельца студии). Личные задачи, события и т. д. по-прежнему закрыты от всех, кроме хозяина.
// Ассистент ведёт личные финансы и личные события (календарь) Руководителя — для него эти ключи
// указывают на данные Руководителя. Личные ЗАДАЧИ, проекты, корзина и архив задач остаются закрытыми.
const ASSISTANT_SHARED_PERSONAL = ['personal:funds', 'personal:events', 'personal:archiveEvents'];
function personalKeyFor(key, empId, role) {
  if (ASSISTANT_SHARED_PERSONAL.includes(key) && role === 'Ассистент') {
    const leaderId = getLeaderId();
    if (leaderId) return `personal:${leaderId}:${key.slice('personal:'.length)}`;
  }
  return key.startsWith('personal:') ? `personal:${empId}:${key.slice('personal:'.length)}` : key;
}
function getLeaderId() {
  const emps = parseJsonArray(store.kv['roster:employees']) || [];
  const leader = emps.find(e => e && e.role === 'Руководитель');
  return leader ? leader.id : null;
}
function stripForeignPersonal(rawSnapshot, empId, role) {
  // Из снимка убираем все личные данные, кроме собственных сотрудника (и «старых» общих — только Руководителю).
  try {
    const parsed = JSON.parse(rawSnapshot);
    if (!parsed || typeof parsed !== 'object' || !parsed.data) return rawSnapshot;
    const leaderId = getLeaderId();
    Object.keys(parsed.data).forEach(k => {
      if (!k.startsWith('personal:')) return;
      const mine = k.startsWith(`personal:${empId}:`) || (role === 'Ассистент' && ASSISTANT_SHARED_PERSONAL.some(sk => k === `personal:${leaderId}:${sk.slice('personal:'.length)}`)) || (PERSONAL_LEGACY_RE.test(k) && empId === leaderId);
      if (!mine) delete parsed.data[k];
    });
    return JSON.stringify(parsed);
  } catch (e) {
    return JSON.stringify({ error: 'не удалось подготовить снимок' });
  }
}
// Разовый перенос прежних общих личных данных к Руководителю (по решению владельца студии).
// Один раз: в блоках «Дела менеджеров» и «Дела администраторов» включаем упрощённые задачи и
// заводим в них проект «Задачи» (дальше — в приложении, кнопкой ⚡ у блока).
async function migrateSimpleTaskBlocks() {
  try {
    if (store.kv['migrations:simpleBlocksV1']) return;
    const blocks = parseJsonArray(store.kv['tasks:blocks']);
    if (!blocks) return; // блоков ещё нет — повторим при следующем запуске
    const projects = parseJsonArray(store.kv['tasks:projects']) || [];
    const re = /^\s*дела\s+(менеджер|администратор)/i;
    let changed = 0;
    const newProjects = [];
    const nextBlocks = blocks.map(b => {
      if (!b || !re.test(String(b.name || ''))) return b;
      if (!projects.concat(newProjects).some(p => p.blockId === b.id && (p.simpleProject || String(p.name || '').trim().toLowerCase() === 'задачи'))) {
        newProjects.push({ id: 'simple-' + b.id, name: 'Задачи', blockId: b.id, simpleProject: true });
      }
      if (!b.simpleTasks) changed++;
      return { ...b, simpleTasks: true };
    });
    const oldBlocks = store.kv['tasks:blocks'];
    const oldProjects = store.kv['tasks:projects'];
    store.kv['tasks:blocks'] = JSON.stringify(nextBlocks);
    if (newProjects.length) store.kv['tasks:projects'] = JSON.stringify(projects.concat(newProjects));
    store.kv['migrations:simpleBlocksV1'] = '1';
    const ok = await persist();
    if (!ok) {
      store.kv['tasks:blocks'] = oldBlocks;
      if (oldProjects === undefined) delete store.kv['tasks:projects']; else store.kv['tasks:projects'] = oldProjects;
      delete store.kv['migrations:simpleBlocksV1'];
      console.error('Упрощённые задачи в блоках: не удалось сохранить — попробуем при следующем запуске.');
      return;
    }
    console.log(`Упрощённые задачи: включено в блоках ${changed}, добавлено проектов «Задачи»: ${newProjects.length}.`);
  } catch (e) { console.error('Ошибка включения упрощённых задач:', e); }
}
async function migratePersonalToLeader() {
  try {
    const legacyKeys = Object.keys(store.kv).filter(k => PERSONAL_LEGACY_RE.test(k));
    if (!legacyKeys.length) return;
    const leaderId = getLeaderId();
    if (!leaderId) { console.error('Перенос личных данных: не найден Руководитель — пропускаем.'); return; }
    await runFrequentBackup();
    const old = {};
    legacyKeys.forEach(k => {
      const target = personalKeyFor(k, leaderId);
      old[k] = store.kv[k];
      if (store.kv[target] === undefined) store.kv[target] = store.kv[k];
      delete store.kv[k];
    });
    const ok = await persist();
    if (!ok) {
      legacyKeys.forEach(k => { store.kv[k] = old[k]; delete store.kv[personalKeyFor(k, leaderId)]; });
      console.error('Перенос личных данных не удалось сохранить — оставлено как было, попробуем при следующем запуске.');
      return;
    }
    console.log(`Личные данные перенесены к Руководителю (ключей: ${legacyKeys.length}).`);
  } catch (e) {
    console.error('Ошибка переноса личных данных:', e && e.message);
  }
}
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
// Защита от «записали пусто поверх данных» (так 7 октября пропал календарь: страница после сбоя
// чтения приняла «не прочиталось» за «пусто» и сохранила []). Для списков, потеря которых катастрофична:
//  - заменить непустой список (>=5 записей) на пустой можно только с заголовком X-Confirm-Empty: 1
//    (его ставит сама страница в осознанных действиях: восстановление из файла, удаление последнего);
//  - список сотрудников нельзя подменить целиком (ни один прежний id не остался) без подтверждения;
//  - при заметном сокращении (>=10 записей и >=30%) заранее делается снимок до изменения;
//  - любое сокращение на 3+ и каждый отказ пишутся в журнал сервера (кто, что, с какого браузера).
const SHRINK_GUARDED_KEYS = new Set(['calendar:events', 'tasks:work', 'tasks:projects', 'tasks:blocks',
  'roster:employees', 'personal:events', 'personal:tasks', 'personal:projects']);
function checkShrinkGuard(key, oldRaw, newRaw, req) {
  if (!SHRINK_GUARDED_KEYS.has(key)) return null;
  const oldArr = parseJsonArray(oldRaw);
  const newArr = parseJsonArray(newRaw);
  if (!oldArr || !newArr) return null;
  const confirmed = req.get('x-confirm-empty') === '1';
  const who = `${req.employee.name} (${req.employee.role})`;
  const ua = String(req.get('user-agent') || '').slice(0, 120);
  let problem = null;
  if (newArr.length === 0 && oldArr.length >= 5 && !confirmed) {
    problem = `Похоже на случайное стирание всего списка (было ${oldArr.length}, стало 0) — запись отклонена. Обновите страницу; данные на сервере не тронуты.`;
  } else if (key === 'roster:employees' && oldArr.length >= 2 && !confirmed) {
    const newIds = new Set(newArr.map(e => e && e.id));
    if (!oldArr.some(e => e && newIds.has(e.id))) {
      problem = 'Похоже на подмену всего списка сотрудников (ни один прежний сотрудник не остался) — запись отклонена. Обновите страницу.';
    }
  }
  if (problem) {
    console.warn(`[отказ] ${key}: ${oldArr.length} → ${newArr.length}; ${who}; UA=${ua}`);
    return problem;
  }
  const removed = oldArr.length - newArr.length;
  if (removed >= 3) console.warn(`[изменение] ${key}: ${oldArr.length} → ${newArr.length}; ${who}; UA=${ua}`);
  if (removed >= 10 && removed >= oldArr.length * 0.3) {
    // снимок состояния ДО этой записи (store.kv ещё старый); не ждём — запись в S3 идёт в фоне
    runFrequentBackup().catch(e => console.error('pre-change backup failed', e && e.message));
  }
  return null;
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

// Календарь Руководителя для семьи: личные дела + события, где участвует сам Руководитель. Только чтение.
app.get('/api/family/calendar', (req, res) => {
  const emp = currentEmployee(req);
  if (!emp) return res.status(401).json({ error: 'Не авторизован' });
  if (!isFamilyRole(emp.role)) return res.status(403).json({ error: 'Недоступно' });
  const leaderId = getLeaderId();
  const leader = getEmployees().find(e => e.id === leaderId);
  if (!leaderId) return res.json({ owner: null, events: [] });
  const shared = (parseJsonArray(store.kv['calendar:events']) || [])
    .filter(e => e && Array.isArray(e.employeeIds) && e.employeeIds.includes(leaderId) && e.category !== 'personal');
  const personal = parseJsonArray(store.kv[`personal:${leaderId}:events`]) || [];
  const pick = e => ({
    id: e.id, title: e.title, date: e.date, startTime: e.startTime || '', endTime: e.endTime || '',
    category: e.category, format: e.format || '', location: e.location || '', notes: e.notes || '',
  });
  res.json({ owner: { name: leader ? leader.name : '' }, events: shared.concat(personal).filter(e => e && e.date).map(pick) });
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
// Регламенты (кто сейчас держит какой регламент) — только Руководитель, Ассистент, Старший администратор
function regsKeyDenied(key, role) {
  if (key === 'regs:list') return !['Руководитель', 'Ассистент', 'Старший администратор'].includes(role);
  // B2B-заявки и клиентская база — Руководитель, Ассистент и менеджеры по продажам
  if (key === 'b2b:data') return !['Руководитель', 'Ассистент', 'Менеджер'].includes(role);
  return false;
}
app.get('/api/storage/:key', requireAuth, (req, res) => {
  const key = req.params.key;
  if (key.startsWith('integrations:')) return res.status(403).json({ error: 'Нет доступа' });
  if (regsKeyDenied(key, req.employee.role)) return res.status(403).json({ error: 'Нет доступа' });
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  let value = store.kv[personalKeyFor(key, req.employee.id, req.employee.role)];
  if (value === undefined) return res.status(404).json({ error: 'not found' });
  if (TASK_LIST_KEYS.has(key) && !isPrivilegedRole(req.employee.role)) {
    const arr = parseJsonArray(value);
    if (arr) value = JSON.stringify(arr.filter(makeTaskVisibility(req.employee)));
  }
  if (key.startsWith('backup:')) value = stripForeignPersonal(value, req.employee.id, req.employee.role);
  res.json({ key, value });
});

app.put('/api/storage/:key', requireAuth, async (req, res) => {
  const key = req.params.key;
  let { value } = req.body || {};
  if (typeof value !== 'string') return res.status(400).json({ error: 'value должен быть строкой' });
  if (key.startsWith('integrations:')) return res.status(403).json({ error: 'Нет доступа' });
  if (regsKeyDenied(key, req.employee.role)) return res.status(403).json({ error: 'Нет доступа' });
  if (key === 'settings:eventPurposes' && !isPrivilegedRole(req.employee.role)) return res.status(403).json({ error: 'Список назначений меняют только Руководитель и Ассистент' });
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  if (isRosterKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Изменять список сотрудников может только Руководитель или Ассистент' });
  }
  const realKey = personalKeyFor(key, req.employee.id, req.employee.role);
  // Для обычных сотрудников список задач приходит «урезанным» (только видимое им) — сверяем с видимой частью
  // старого списка, а перед записью вшиваем правки в полный список.
  let oldForChecks = store.kv[realKey];
  if (TASK_LIST_KEYS.has(key) && !isPrivilegedRole(req.employee.role)) {
    const oldAll = parseJsonArray(store.kv[realKey]);
    const clientArr = parseJsonArray(value);
    if (!clientArr) return res.status(400).json({ error: 'ожидался список задач' });
    const visible = makeTaskVisibility(req.employee);
    if (oldAll) {
      oldForChecks = JSON.stringify(oldAll.filter(visible));
      req.mergedTasksValue = JSON.stringify(mergeVisibleTasks(oldAll, visible, clientArr));
    }
  }
  const deletionError = findDisallowedDeletion(key, oldForChecks, value, req.employee);
  if (deletionError) {
    return res.status(403).json({ error: deletionError });
  }
  const shrinkError = checkShrinkGuard(key, oldForChecks, value, req);
  if (!shrinkError && req.mergedTasksValue) value = req.mergedTasksValue;
  if (shrinkError) return res.status(409).json({ error: shrinkError });
  // Важно: сохраняем старое значение и дожидаемся РЕАЛЬНОЙ записи в хранилище (S3/диск), прежде
  // чем отвечать браузеру "ok". Если запись не удалась — откатываем в памяти и честно сообщаем об
  // ошибке, а не делаем вид, что всё сохранилось (раньше именно так терялись изменения).
  const oldValue = store.kv[realKey];
  store.kv[realKey] = value;
  const saved = await (isBackupKey(key) ? saveBackups() : persist());
  if (!saved) {
    if (oldValue === undefined) delete store.kv[realKey]; else store.kv[realKey] = oldValue;
    return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
  }
  res.json({ ok: true });
});

app.delete('/api/storage/:key', requireAuth, async (req, res) => {
  const key = req.params.key;
  if (key.startsWith('integrations:')) return res.status(403).json({ error: 'Нет доступа' });
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
  const realKey = personalKeyFor(key, req.employee.id, req.employee.role);
  const oldValue = store.kv[realKey];
  const hadValue = Object.prototype.hasOwnProperty.call(store.kv, realKey);
  delete store.kv[realKey];
  const saved = await (isBackupKey(key) ? saveBackups() : persist());
  if (!saved) {
    if (hadValue) store.kv[realKey] = oldValue;
    return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
  }
  res.json({ ok: true });
});

// ---------- список автоматических бэкапов (только Руководитель/Ассистент) ----------
// Поиск по задачам всей студии (только чтение): любой сотрудник может найти и открыть чужую задачу, чтобы увидеть
// детали (исполнитель, срок, ссылки). Переписка по задаче (комментарии, отчёт исполнителя, замечания при возврате,
// причины просрочки) и суммы покупок НЕ отдаются — они остаются видны только тем, кому положено по оргструктуре.
const SEARCH_TASK_FIELDS = ['id','title','description','link','links','assigneeId','creatorId','createdDate','deadline','deadlineTime',
  'priority','status','projectId','blockId','parentId','managementStage','completedAt','approvedAt','fromChecklist','inReport','sortOrder'];
app.get('/api/tasks/search-all', requireAuth, (req, res) => {
  const arr = parseJsonArray(store.kv['tasks:work']) || [];
  const tasks = arr.filter(t => t && t.id).map(t => {
    const o = {};
    SEARCH_TASK_FIELDS.forEach(k => { if (t[k] !== undefined) o[k] = t[k]; });
    return o;
  });
  res.json({ tasks });
});

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

// ---------- email-бэкап: статус и ручная отправка "прямо сейчас" (только Руководитель/Ассистент) ----------
app.get('/api/email-backup/status', requireAuth, (req, res) => {
  if (!isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  res.json({ enabled: EMAIL_BACKUP_ENABLED, to: EMAIL_BACKUP_ENABLED ? BACKUP_EMAIL_TO : null });
});
app.post('/api/email-backup/send-now', requireAuth, async (req, res) => {
  if (!isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  const result = await sendBackupEmail('отправлено вручную из приложения');
  if (!result.ok) return res.status(500).json({ error: result.error || 'Не удалось отправить письмо' });
  res.json({ ok: true });
});

// ---------- статика (сам интерфейс) ----------
// ================= Telegram: приём сообщений в B2B и ответы из карточки заявки =================
// Бот работает через long polling (getUpdates) — публичный адрес и webhook не нужны. Токен хранится на сервере
// (ключ integrations:telegram, из браузера недоступен и в снимки не попадает) либо в переменной TELEGRAM_BOT_TOKEN.
const TG_API_BASE = process.env.TELEGRAM_API_BASE || 'https://api.telegram.org';
const TG_KEY = 'integrations:telegram';
const TG_DEFAULT_GREETING = 'Здравствуйте! Это студия «Свободное Пространство». Напишите, чем можем помочь — менеджер ответит в рабочее время.';
const tg = { token: '', username: '', error: '', offset: 0, polling: false, gen: 0 };
const B2B_ROLES = ['Руководитель', 'Ассистент', 'Менеджер'];
const tgSleep = ms => new Promise(r => setTimeout(r, ms));
function tgConfig() { try { return JSON.parse(store.kv[TG_KEY] || '{}'); } catch (e) { return {}; } }
async function tgApi(method, body, timeoutMs) {
  const r = await fetch(`${TG_API_BASE}/bot${tg.token}/${method}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body || {}),
    signal: AbortSignal.timeout(timeoutMs || 20000) });
  let j; try { j = await r.json(); } catch (e) { throw new Error('Telegram ответил не в том формате (HTTP ' + r.status + ')'); }
  if (!j.ok) throw new Error(j.description || ('Ошибка Telegram ' + r.status));
  return j.result;
}
// «день» студии — по Омску (UTC+6), независимо от часового пояса сервера
function omskISO(offsetDays) { const d = new Date(Date.now() + 6 * 3600e3 + (offsetDays || 0) * 86400e3); return d.toISOString().slice(0, 10); }
function isoDiffDays(a, b) { return Math.round((Date.parse(a + 'T00:00:00Z') - Date.parse(b + 'T00:00:00Z')) / 86400e3); }
function isoPlus(iso, n) { return new Date(Date.parse(iso + 'T00:00:00Z') + n * 86400e3).toISOString().slice(0, 10); }
function b2bReadData() {
  let o = {}; try { o = JSON.parse(store.kv['b2b:data'] || '{}'); } catch (e) { o = {}; }
  if (!o || typeof o !== 'object' || Array.isArray(o)) o = {};
  o.clients = Array.isArray(o.clients) ? o.clients : []; o.leads = Array.isArray(o.leads) ? o.leads : [];
  o.schedule = o.schedule && typeof o.schedule === 'object' ? o.schedule : {};
  o.pipelines = Array.isArray(o.pipelines) && o.pipelines.length ? o.pipelines : [{ id: 'main', name: 'Основная' }];
  return o;
}
async function b2bWriteData(o) {
  const old = store.kv['b2b:data'];
  store.kv['b2b:data'] = JSON.stringify(o);
  const ok = await persist();
  if (!ok) { if (old === undefined) delete store.kv['b2b:data']; else store.kv['b2b:data'] = old; }
  return ok;
}
// график менеджеров: если из Расписания пришёл календарь дежурств (d.schedule.__duty) — работает только он;
// день, которого в календаре нет, считается «не задан» (null), старый график 2/2 тогда не используется
function srvWorks(d, empId, iso) {
  const du = d.schedule && d.schedule.__duty;
  if (du && typeof du === 'object' && Object.keys(du).length) { if (!empId) return null; return du[iso] ? du[iso] === empId : null; }
  const a = d.schedule[empId]; if (!a) return null; return (((isoDiffDays(iso, a) % 4) + 4) % 4) < 2; }
function srvNextWork(d, empId, from, inclusive) {
  const start = inclusive ? from : isoPlus(from, 1);
  for (let i = 0; i < 8; i++) { const x = isoPlus(start, i); if (srvWorks(d, empId, x) !== false) return x; }
  return start;
}
function srvPickManager(d, iso) {
  const mgrs = getEmployees().filter(e => e.role === 'Менеджер' && srvWorks(d, e.id, iso) === true);
  if (!mgrs.length) return '';
  const open = l => ['new', 'meet', 'kp', 'sent'].includes(l.stage);
  const load = id => d.leads.filter(l => open(l) && l.managerId === id).length;
  return mgrs.sort((a, b) => load(a.id) - load(b.id))[0].id;
}
const tgChain = { p: Promise.resolve() };
function tgSerial(fn) { const run = tgChain.p.then(fn); tgChain.p = run.catch(() => {}); return run; }
const OPEN_STAGES = ['new', 'meet', 'kp', 'sent'];
function tgPushMsg(lead, msg) {
  lead.msgs = (lead.msgs || []).concat([msg]).slice(-300);
  lead.lastMsgAt = msg.ts;
  if (msg.dir === 'in') lead.unread = (lead.unread || 0) + 1;
  lead.log = (lead.log || []).concat([{ date: omskISO(), ts: msg.ts, stage: null, note: (msg.dir === 'in' ? '💬 Сообщение от клиента (Telegram)' : '📤 Ответ клиенту (Telegram)'), by: msg.by || null }]).slice(-400);
}
// Входящее сообщение → заявка. Возвращает {lead, firstContact}
async function tgHandleIncoming(chat, from, text, startPayload) {
  return tgSerial(async () => {
    const d = b2bReadData();
    const today = omskISO();
    const name = [from && from.first_name, from && from.last_name].filter(Boolean).join(' ').trim() || (from && from.username ? '@' + from.username : 'Клиент из Telegram');
    let lead = null, client = null, first = false;
    // привязка по ссылке из карточки заявки: t.me/бот?start=L_<id заявки>
    const m = /^L_(.+)$/.exec(startPayload || '');
    if (m) { lead = d.leads.find(l => l.id === m[1]) || null; if (lead) client = d.clients.find(c => c.id === lead.clientId) || null; }
    if (!client) client = d.clients.find(c => c.tg && String(c.tg.chatId) === String(chat.id)) || null;
    if (client && !lead) {
      const mine = d.leads.filter(l => l.clientId === client.id);
      lead = mine.filter(l => OPEN_STAGES.includes(l.stage)).sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''))[0] || null;
    }
    if (client) { client.tg = { chatId: chat.id, username: (from && from.username) || '' }; }
    if (!client) {
      client = { id: 'c' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), name, person: name, phone: '', instagram: '', note: 'Пришёл из Telegram', createdAt: today, tg: { chatId: chat.id, username: (from && from.username) || '' } };
      d.clients.push(client);
    }
    if (!lead) {
      const mgr = srvPickManager(d, today);
      lead = { id: 'l' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), clientId: client.id, stage: 'new', direction: 'in', source: 'Telegram', createdAt: today,
        managerId: mgr, deadline: srvNextWork(d, mgr, today, false), forecast: 0, comment: '', tasks: [], tags: [], links: [], custom: {}, pipelineId: d.pipelines[0].id,
        log: [{ date: today, ts: Date.now(), stage: 'new', note: 'Заявка из Telegram' + (mgr ? ' · ответственный по графику' : ''), by: null }], msgs: [] };
      d.leads.push(lead); first = true;
    }
    if (text) tgPushMsg(lead, { id: 'm' + Date.now().toString(36) + Math.random().toString(36).slice(2, 5), dir: 'in', text: String(text).slice(0, 4000), ts: Date.now(), from: name });
    const ok = await b2bWriteData(d);
    return { ok, lead, first };
  });
}
async function tgHandleUpdate(u) {
  const msg = u.message; if (!msg || !msg.chat || msg.chat.type !== 'private' || (msg.from && msg.from.is_bot)) return;
  let text = msg.text || msg.caption || '';
  let payload = '';
  const sm = /^\/start(?:\s+(\S+))?/.exec(text);
  if (sm) { payload = sm[1] || ''; text = ''; }
  else if (!text) text = msg.photo ? '[фото]' : msg.document ? '[файл: ' + (msg.document.file_name || 'документ') + ']' : msg.voice ? '[голосовое сообщение]' : msg.video ? '[видео]' : msg.sticker ? '[стикер]' : msg.contact ? '[контакт: ' + (msg.contact.phone_number || '') + ']' : '[вложение]';
  const r = await tgHandleIncoming(msg.chat, msg.from, text, payload);
  if (r && r.ok && (r.first || sm)) {
    const greeting = tgConfig().greeting || TG_DEFAULT_GREETING;
    try { await tgApi('sendMessage', { chat_id: msg.chat.id, text: greeting }); } catch (e) { /* приветствие не критично */ }
  }
}
async function tgPollLoop(gen) {
  tg.polling = true;
  try { await tgApi('deleteWebhook', { drop_pending_updates: false }); } catch (e) { tg.error = e.message; }
  while (tg.token && tg.gen === gen) {
    try {
      const ups = await tgApi('getUpdates', { offset: tg.offset, timeout: 25, allowed_updates: ['message'] }, 40000);
      tg.error = '';
      for (const u of ups) { tg.offset = u.update_id + 1; try { await tgHandleUpdate(u); } catch (e) { console.error('tg update', e && e.message); } }
    } catch (e) { tg.error = e.message; await tgSleep(8000); }
  }
  if (tg.gen === gen) tg.polling = false;
}
async function tgStart() {
  const token = (process.env.TELEGRAM_BOT_TOKEN || tgConfig().token || '').trim();
  tg.gen++; tg.token = token; tg.username = ''; tg.error = '';
  if (!token) return;
  try { const me = await tgApi('getMe'); tg.username = me.username || ''; }
  catch (e) { tg.error = e.message; if (/Unauthorized|not found/i.test(e.message)) { tg.token = ''; return; } }
  tgPollLoop(tg.gen);
}
app.get('/api/integrations/telegram/status', requireAuth, (req, res) => {
  if (!B2B_ROLES.includes(req.employee.role)) return res.status(403).json({ error: 'Нет доступа' });
  const cfg = tgConfig();
  res.json({ connected: !!tg.token, username: tg.username, error: tg.error, greeting: cfg.greeting || TG_DEFAULT_GREETING, fromEnv: !!process.env.TELEGRAM_BOT_TOKEN });
});
app.post('/api/integrations/telegram/config', requireAuth, async (req, res) => {
  if (!isPrivilegedRole(req.employee.role)) return res.status(403).json({ error: 'Подключать мессенджеры могут только Руководитель и Ассистент' });
  const body = req.body || {};
  const cfg = tgConfig();
  if (typeof body.greeting === 'string') cfg.greeting = body.greeting.trim().slice(0, 1000);
  if (typeof body.token === 'string') {
    const t = body.token.trim();
    if (t) {
      // проверяем токен, прежде чем сохранять
      const prev = tg.token; tg.token = t;
      try { await tgApi('getMe'); } catch (e) { tg.token = prev; return res.status(400).json({ error: 'Telegram не принял токен: ' + e.message }); }
      cfg.token = t;
    } else delete cfg.token;
  }
  const old = store.kv[TG_KEY];
  store.kv[TG_KEY] = JSON.stringify(cfg);
  if (!(await persist())) { if (old === undefined) delete store.kv[TG_KEY]; else store.kv[TG_KEY] = old; return res.status(500).json({ error: 'Не удалось сохранить настройки' }); }
  if (typeof body.token === 'string') await tgStart();
  res.json({ ok: true, connected: !!tg.token, username: tg.username });
});
app.post('/api/integrations/telegram/send', requireAuth, async (req, res) => {
  if (!B2B_ROLES.includes(req.employee.role)) return res.status(403).json({ error: 'Нет доступа' });
  const { leadId, text } = req.body || {};
  const msgText = String(text || '').trim();
  if (!leadId || !msgText) return res.status(400).json({ error: 'Нужны заявка и текст' });
  if (!tg.token) return res.status(400).json({ error: 'Telegram не подключён' });
  const d0 = b2bReadData();
  const lead0 = d0.leads.find(l => l.id === leadId);
  const cl0 = lead0 && d0.clients.find(c => c.id === lead0.clientId);
  if (!cl0 || !cl0.tg || !cl0.tg.chatId) return res.status(400).json({ error: 'Клиент ещё не написал боту — отправьте ему ссылку из карточки' });
  try { await tgApi('sendMessage', { chat_id: cl0.tg.chatId, text: msgText.slice(0, 4000) }); }
  catch (e) { return res.status(502).json({ error: /blocked|deactivated/i.test(e.message) ? 'Клиент заблокировал бота' : 'Telegram: ' + e.message }); }
  const r = await tgSerial(async () => {
    const d = b2bReadData(); const lead = d.leads.find(l => l.id === leadId);
    if (!lead) return false;
    tgPushMsg(lead, { id: 'm' + Date.now().toString(36) + Math.random().toString(36).slice(2, 5), dir: 'out', text: msgText.slice(0, 4000), ts: Date.now(), by: req.employee.id, from: req.employee.name });
    lead.unread = 0;
    return await b2bWriteData(d);
  });
  if (!r) return res.status(500).json({ error: 'Сообщение отправлено, но не записалось в историю' });
  res.json({ ok: true });
});

app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---------- приём заявок и задач из «Расписания студии» (одностороннее обновление) ----------
// Расписание шлёт сюда заявки (клиент + этап + задачи), а журнал сам создаёт/обновляет их в B2B.
// Задачи менеджерам в разделе «Задачи» появляются штатным механизмом B2B (b2bReconcileTasks).
// Безопасность: отдельный секрет SCHEDULE_SYNC_TOKEN (переменная окружения), без cookie-входа;
// ничего не удаляется; заявки журнала, заведённые не из Расписания, не трогаются; перед записью —
// снимок данных (runFrequentBackup); запись в хранилище откатывается при ошибке (b2bWriteData).
const SYNC_TOKEN = (process.env.SCHEDULE_SYNC_TOKEN || '').trim();
const syncFails = new Map(); // ip → {n, until}
const SYNC_STAGE = { new: 'new', brief: 'kp', kp: 'sent', thinking: 'sent', sold: 'won', lost: 'lost' };
const SYNC_LOST_ID = [[/дорого/i, 'price'], [/друг(ую|ой) студи/i, 'competitor'], [/не отвеч|пропал/i, 'noanswer'], [/передума|отлож/i, 'later'], [/формат|программ/i, 'notfit']];
function syncStr(v, n) { return String(v == null ? '' : v).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').trim().slice(0, n); }
function syncDate(v) { return /^\d{4}-\d{2}-\d{2}$/.test(String(v || '')) ? String(v) : ''; }
function syncSameToken(a) {
  const crypto = require('crypto');
  const x = crypto.createHash('sha256').update(String(a || '')).digest();
  const y = crypto.createHash('sha256').update(SYNC_TOKEN).digest();
  return crypto.timingSafeEqual(x, y);
}
function syncCors(req, res) {
  res.set('Access-Control-Allow-Origin', '*'); // защита — токен в заголовке, cookie не используются
  res.set('Access-Control-Allow-Headers', 'content-type, x-sync-token');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.set('Access-Control-Max-Age', '600');
}
function syncFindManagerId(name) {
  const n = syncStr(name, 80).toLowerCase(); if (!n) return '';
  const e = getEmployees().find(x => x && x.name && String(x.name).trim().toLowerCase() === n && ['Менеджер', 'Руководитель', 'Ассистент'].includes(x.role));
  return e ? e.id : '';
}
function syncApplyLead(d, p, today, map) {
  const stage = SYNC_STAGE[p.stage]; if (!stage) return null;
  const log = (lead, st, note) => { lead.log = (lead.log || []).concat([{ date: today, ts: Date.now(), stage: st, note, by: null }]); };
  let lead = d.leads.find(l => l.ext && l.ext.src === 'schedule' && l.ext.id === p.id);
  let created = false, changed = false;
  const mgrId = syncFindManagerId(p.manager);
  const open = st => OPEN_STAGES.includes(st);
  const tasksIn = (Array.isArray(p.tasks) ? p.tasks : []).slice(0, 60).map(t => ({
    id: syncStr(t && t.id, 60), text: syncStr(t && t.title, 300), deadline: syncDate(t && t.due), done: !!(t && t.done), managerId: syncFindManagerId(t && t.assignee) || mgrId,
  })).filter(t => t.id && t.text);
  if (!lead && map.leads[p.id]) return null; // заявку уже заводили, но в журнале её удалили — не воскрешаем
  if (!lead) {
    const contact = syncStr(p.contact, 120);
    const pk = String(contact).replace(/\D/g, '').slice(-10);
    let client = pk.length === 10 ? d.clients.find(c => String(c.phone || '').replace(/\D/g, '').slice(-10) === pk) : null;
    if (!client) {
      const nm = syncStr(p.client, 120) || contact || 'Клиент из Расписания';
      client = d.clients.find(c => (c.name || '').trim().toLowerCase() === nm.toLowerCase()) || null;
      if (!client) {
        client = { id: 'c' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), name: nm, person: '', phone: pk.length === 10 ? contact : '', instagram: pk.length === 10 ? '' : (contact.startsWith('@') ? contact : ''), note: 'Из Расписания студии' + (pk.length === 10 || !contact ? '' : ' · ' + contact), createdAt: today };
        d.clients.push(client);
      }
    }
    const mgr = mgrId || srvPickManager(d, today);
    const openTasks = tasksIn.filter(t => !t.done && t.deadline).map(t => t.deadline).sort();
    lead = { id: 'l' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), clientId: client.id, stage, direction: 'in', source: syncStr(p.source, 60) || 'Расписание', createdAt: syncDate(p.createdAt) || today,
      managerId: mgr, deadline: open(stage) ? (openTasks[0] || srvNextWork(d, mgr, today, true)) : '', forecast: Math.max(0, Number(p.revenue) || 0), comment: syncStr(p.comment, 1000),
      product: syncStr(p.product, 80), tasks: [], tags: ['Расписание'], links: [], custom: {}, pipelineId: d.pipelines[0].id, log: [], msgs: [], ext: { src: 'schedule', id: p.id, stage: p.stage, revenue: Math.max(0, Number(p.revenue) || 0) } };
    log(lead, stage, 'Заявка из Расписания студии' + (p.product ? ' · ' + syncStr(p.product, 80) : '') + (mgr ? ' · ответственный по графику' : ''));
    if (!open(stage)) { lead.doneAt = syncDate(p.saleDate) || today; if (stage === 'won') lead.dealSum = lead.forecast;
      if (stage === 'lost') { const why = syncStr(p.lostReason, 200); const m = SYNC_LOST_ID.find(r => r[0].test(why)); lead.lostReasonId = m ? m[1] : 'other'; lead.lostReason = m ? '' : why; } }
    d.leads.push(lead); map.leads[p.id] = lead.id; created = true; changed = true;
  } else {
    const ext = lead.ext || (lead.ext = { src: 'schedule', id: p.id });
    // этапы внутри B2B ведутся в журнале; из Расписания принимаем только итог — «Сделка» или «Отказ»,
    // и не переоткрываем заявку, которую в журнале уже закрыли
    if (ext.stage !== p.stage && (!open(stage) || (!open(lead.stage) && (ext.stage === 'sold' || ext.stage === 'lost')))) {
      const was = lead.stage;
      lead.stage = stage; ext.stage = p.stage;
      if (!open(stage)) {
        lead.prevStage = was; lead.doneAt = syncDate(p.saleDate) || today; lead.deadline = '';
        if (stage === 'won') lead.dealSum = Math.max(0, Number(p.revenue) || 0);
        if (stage === 'lost') { const why = syncStr(p.lostReason, 200); const m = SYNC_LOST_ID.find(r => r[0].test(why)); lead.lostReasonId = m ? m[1] : 'other'; lead.lostReason = m ? '' : why; }
      } else {
        lead.doneAt = null; lead.lostReason = ''; lead.lostReasonId = '';
        const ds = tasksIn.filter(t => !t.done && t.deadline).map(t => t.deadline).sort();
        lead.deadline = ds[0] || srvNextWork(d, lead.managerId, today, true);
      }
      log(lead, stage, 'Этап изменён в Расписании студии: ' + B2B_STAGE_LABELS_SRV[stage]);
      changed = true;
    } else if (ext.stage !== p.stage) { ext.stage = p.stage; changed = true; }
    if (Math.max(0, Number(p.revenue) || 0) !== (ext.revenue || 0)) {
      ext.revenue = Math.max(0, Number(p.revenue) || 0);
      if (lead.stage === 'won') lead.dealSum = ext.revenue; else lead.forecast = ext.revenue;
      changed = true;
    }
  }
  // задачи: создаём новые, обновляем только невыполненные (ничего не удаляем, правки журнала по выполненным не трогаем)
  lead.tasks = Array.isArray(lead.tasks) ? lead.tasks : [];
  tasksIn.forEach(t => {
    const id = 'sch' + t.id;
    const hash = [t.text, t.deadline, t.managerId, t.done ? 1 : 0].join('|');
    const cur = lead.tasks.find(x => x.id === id);
    if (!cur) {
      if (!open(lead.stage) && !t.done) return; // у закрытой заявки новых открытых задач не заводим
      lead.tasks.push({ id, type: 'other', text: t.text, deadline: t.deadline, time: '', managerId: t.managerId || lead.managerId || '', done: t.done, extHash: hash });
      changed = true;
    } else if (cur.extHash !== hash) {
      if (!cur.done) { cur.text = t.text; cur.deadline = t.deadline; if (t.managerId) cur.managerId = t.managerId; if (t.done) cur.done = true; }
      cur.extHash = hash; changed = true;
    }
  });
  if (!open(lead.stage)) lead.tasks.forEach(t => { if (String(t.id).startsWith('sch') && !t.done) { t.done = true; changed = true; } });
  return { lead, created, changed };
}

// ---------- B2C: задачи по частным заявкам попадают менеджерам в обычные «Задачи» журнала ----------
// Сама B2C-заявка остаётся в Расписании; в журнале — только задача «кому и что сделать/написать».
// Соответствие «задача расписания → задача журнала» хранится на сервере (ключ integrations:…, клиенту недоступен),
// поэтому правки задачи в интерфейсе журнала ничего не ломают и дублей не создают.
const SYNC_MAP_KEY = 'integrations:schedule-map';
function syncReadMap() {
  let m = {}; try { m = JSON.parse(store.kv[SYNC_MAP_KEY] || '{}'); } catch (e) { m = {}; }
  if (!m || typeof m !== 'object' || Array.isArray(m)) m = {};
  m.tasks = m.tasks && typeof m.tasks === 'object' ? m.tasks : {};
  m.leads = m.leads && typeof m.leads === 'object' ? m.leads : {};
  return m;
}
const SYNC_DONE_ST = ['done', 'review', 'approval'];
function syncApplyTasks(tasksArr, payloadTasks, map, d, today, unmatched, touched) {
  let changed = false;
  const leader = getEmployees().find(e => e && e.role === 'Руководитель');
  const proj = (parseJsonArray(store.kv['tasks:projects']) || []).find(p => p && !p.archived && /b2c|расписан/i.test(p.name || ''));
  for (const pt of (payloadTasks || []).slice(0, 400)) {
    if (!pt || typeof pt !== 'object') continue;
    const id = syncStr(pt.id, 60), title = syncStr(pt.title, 300); if (!id || !title) continue;
    const due = syncDate(pt.due);
    const empId = syncFindManagerId(pt.assignee);
    if (!empId && pt.assignee) unmatched.add(syncStr(pt.assignee, 60));
    const assignee = empId || srvPickManager(d, today) || (leader ? leader.id : null);
    const who = syncStr(pt.client, 120) || syncStr(pt.contact, 120) || 'клиент';
    const fullTitle = `Расписание: ${who} — ${title}`.slice(0, 300);
    const desc = `Задача по заявке из Расписания студии (B2C).\nКлиент: ${syncStr(pt.contact, 120) || '—'}${pt.product ? '\nПродукт: ' + syncStr(pt.product, 80) : ''}${pt.stage ? '\nЭтап: ' + syncStr(pt.stage, 60) : ''}${Number(pt.amount) > 0 ? '\nСумма: ' + Math.round(Number(pt.amount)) + ' ₽' : ''}${pt.comment ? '\nКомментарий: ' + syncStr(pt.comment, 500) : ''}`;
    const hash = [fullTitle, due, assignee, pt.done ? 1 : 0].join('|');
    const rec = map.tasks[id];
    if (!rec) {
      if (pt.done) continue;
      const jid = 't' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
      tasksArr.push({ id: jid, title: fullTitle, description: desc, projectId: proj ? proj.id : null, parentId: null, assigneeId: assignee, creatorId: leader ? leader.id : assignee,
        createdDate: today, deadline: due || today, deadlineTime: '', priority: 'med', status: 'progress', completedAt: null, eventId: null, purchaseAmount: null, purchaseStore: '',
        purchaseExpenseId: null, comments: [], sortOrder: 0, simple: false });
      map.tasks[id] = { jid, hash }; touched.add(id); changed = true;
    } else if (!tasksArr.some(t => t && t.id === rec.jid)) {
      // задачи нет в журнале: если мы её ещё ни разу не видели там — её затёрла одновременная запись, создаём заново;
      // если видели — значит, её удалили вручную, не воскрешаем
      if (!rec.seen && !pt.done) {
        tasksArr.push({ id: rec.jid, title: fullTitle, description: desc, projectId: proj ? proj.id : null, parentId: null, assigneeId: assignee, creatorId: leader ? leader.id : assignee,
          createdDate: today, deadline: due || today, deadlineTime: '', priority: 'med', status: 'progress', completedAt: null, eventId: null, purchaseAmount: null, purchaseStore: '',
          purchaseExpenseId: null, comments: [], sortOrder: 0, simple: false });
        rec.hash = hash; touched.add(id); changed = true;
      }
    } else if (rec.hash !== hash) {
      const jt = tasksArr.find(t => t && t.id === rec.jid);
      if (jt && !SYNC_DONE_ST.includes(jt.status)) {
        if (pt.done) { jt.status = 'done'; jt.completedAt = today; }
        else { jt.title = fullTitle; jt.description = desc; if (due) jt.deadline = due; if (assignee) jt.assigneeId = assignee; }
      }
      rec.hash = hash; touched.add(id); changed = true;
    }
  }
  return changed;
}
const B2B_STAGE_LABELS_SRV = { new: 'Новая заявка', meet: 'Встреча / звонок', kp: 'Готовим КП', sent: 'КП отправлено', won: 'Сделка', lost: 'Отказ' };
app.options('/api/integrations/schedule/sync', (req, res) => { syncCors(req, res); res.sendStatus(204); });
app.post('/api/integrations/schedule/sync', async (req, res) => {
  syncCors(req, res);
  if (!SYNC_TOKEN) return res.status(503).json({ error: 'Связка не настроена: задайте SCHEDULE_SYNC_TOKEN в переменных окружения' });
  const ip = String(req.ip || '');
  const f = syncFails.get(ip);
  if (!syncSameToken(req.get('x-sync-token'))) {
    // неверный ключ: после 10 попыток подряд отвечаем 429 (верный ключ при этом всегда проходит — нельзя заблокировать связку чужими попытками)
    for (const [k, v] of syncFails) if (v.until < Date.now()) syncFails.delete(k);
    if (f && f.until > Date.now() && f.n >= 10) return res.status(429).json({ error: 'Слишком много неверных попыток, подождите 10 минут' });
    const n = (f && f.until > Date.now() ? f.n : 0) + 1; syncFails.set(ip, { n, until: Date.now() + 10 * 60e3 });
    return res.status(401).json({ error: 'Неверный ключ' });
  }
  syncFails.delete(ip);
  const body = req.body || {};
  if (body.ping) return res.json({ ok: true, pong: true });
  if (body.format !== 'sp-sync' || body.version !== 1 || !Array.isArray(body.leads)) return res.status(400).json({ error: 'Неверный формат данных' });
  if (body.leads.length > 300) return res.status(400).json({ error: 'Слишком много заявок за раз' });
  try {
    const out = await tgSerial(async () => {
      const d = b2bReadData();
      const before = JSON.stringify(d);
      const tasksArr = parseJsonArray(store.kv['tasks:work']) || [];
      const tasksBefore = JSON.stringify(tasksArr);
      const map = syncReadMap(); const mapBefore = JSON.stringify(map);
      const today = omskISO();
      // график менеджеров ведётся только в календаре Расписания: кладём его в d.schedule.__duty (дата → id сотрудника);
      // дальше им пользуются и сервер (назначение заявок), и интерфейс журнала (b2bWorks)
      if (body.duty && typeof body.duty === 'object' && !Array.isArray(body.duty)) {
        const duty = {}; let n = 0;
        for (const iso of Object.keys(body.duty)) {
          if (!syncDate(iso) || n >= 200) continue;
          const id = syncFindManagerId(body.duty[iso]); if (id) { duty[iso] = id; n++; }
        }
        if (n) { d.schedule = d.schedule && typeof d.schedule === 'object' ? d.schedule : {}; d.schedule.__duty = duty; }
      }
      const unmatched = new Set();
      const mapOut = []; let created = 0, updated = 0;
      for (const p of body.leads) {
        if (!p || typeof p !== 'object') continue;
        p.id = syncStr(p.id, 60); if (!p.id) continue;
        const r = syncApplyLead(d, p, today, map);
        if (!r) continue;
        mapOut.push({ id: p.id, journalId: r.lead.id });
        if (r.created) created++; else if (r.changed) updated++;
        (r.lead.tasks || []).forEach(() => {});
      }
      const touched = new Set();
      syncApplyTasks(tasksArr, Array.isArray(body.tasks) ? body.tasks : [], map, d, today, unmatched, touched);
      // ответ: актуальные этапы B2B-заявок и состояние задач в журнале (для показа в Расписании)
      const track = body.track && typeof body.track === 'object' ? body.track : {};
      const stages = {}; (Array.isArray(track.leads) ? track.leads : []).slice(0, 500).forEach(id => {
        const l = d.leads.find(x => x.ext && x.ext.src === 'schedule' && x.ext.id === id);
        if (l) stages[id] = { stage: l.stage, label: B2B_STAGE_LABELS_SRV[l.stage] || l.stage, manager: (getEmployees().find(e => e.id === l.managerId) || {}).name || '' };
      });
      const taskStates = {}; (Array.isArray(track.tasks) ? track.tasks : []).slice(0, 800).forEach(id => {
        const rec = map.tasks[id]; if (!rec) return;
        const jt = tasksArr.find(t => t && t.id === rec.jid);
        if (jt && !rec.seen && !touched.has(id)) { rec.seen = true; }
        taskStates[id] = !jt ? (rec.seen ? 'deleted' : 'lost') : (SYNC_DONE_ST.includes(jt.status) ? 'done' : 'open');
      });
      const result = { ok: true, created, updated, leads: mapOut, stages, taskStates, unmatched: Array.from(unmatched) };
      const bChanged = JSON.stringify(d) !== before, tChanged = JSON.stringify(tasksArr) !== tasksBefore, mChanged = JSON.stringify(map) !== mapBefore;
      if (!bChanged && !tChanged && !mChanged) return result;
      runFrequentBackup().catch(e => console.error('sync pre-backup failed', e && e.message));
      const old = { b: store.kv['b2b:data'], t: store.kv['tasks:work'], m: store.kv[SYNC_MAP_KEY] };
      if (bChanged) store.kv['b2b:data'] = JSON.stringify(d);
      if (tChanged) store.kv['tasks:work'] = JSON.stringify(tasksArr);
      if (mChanged) store.kv[SYNC_MAP_KEY] = JSON.stringify(map);
      const ok = await persist();
      if (!ok) {
        [['b2b:data', old.b], ['tasks:work', old.t], [SYNC_MAP_KEY, old.m]].forEach(([k, v]) => { if (v === undefined) delete store.kv[k]; else store.kv[k] = v; });
        return { ok: false };
      }
      return result;
    });
    if (!out.ok) return res.status(500).json({ error: 'Не удалось сохранить данные в хранилище. Попробуйте ещё раз.' });
    console.log(`[Расписание→журнал] заявок создано ${out.created}, обновлено ${out.updated}`);
    res.json(out);
  } catch (e) {
    console.error('schedule sync', e && e.message);
    res.status(500).json({ error: 'Ошибка обработки' });
  }
});

// ---------- запуск ----------
// ВАЖНО: порт открываем СРАЗУ, не дожидаясь загрузки данных из S3 (см. комментарий и флаг
// serverReady у app.use(cookieParser...) в начале файла) — иначе проверка готовности контейнера на
// Timeweb не дожидается открытия порта и считает деплой неудачным.
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Единый журнал: сервер запущен на порту ${PORT} (слушает 0.0.0.0), данные ещё загружаются...`);
});

// Повторяем загрузку, пока не получим НАДЁЖНЫЙ результат — либо реальные данные, либо честно
// подтверждённую пустоту (confirmedEmpty: true, т.е. в хранилище точно ничего нет — настоящий
// первый запуск). Сбой чтения (таймаут, сеть и т.п.) НЕ считается поводом ни продолжать, ни тем
// более создавать/сохранять данные по умолчанию — так 4 октября 2026 были случайно стёрты
// реальные данные (сбойное чтение было принято за "ничего нет", сервер создал сотрудников по
// умолчанию и тут же сохранил их поверх настоящих). Теперь при сбое сервер просто пробует снова.
async function loadStoreReliably() {
  let attempt = 0;
  while (true) {
    attempt++;
    const result = await loadStoreFromBackend();
    const hasData = Object.keys(result.kv).length > 0 || Object.keys(result.credentials).length > 0;
    if (hasData || result.confirmedEmpty) return result;
    console.error(`Загрузка данных не удалась (попытка ${attempt}) — данные хранилища НЕ считаем пустыми, пробуем снова через 5с, сервер пока не принимает запросы.`);
    await new Promise(r => setTimeout(r, 5000));
  }
}

// Один профиль «Администратор» вместо «Администратор 1 смена» и «Администратор 2 смена».
// Остаётся первый профиль (его пароль сохраняется; если пароль был только у второго — берём его),
// второй удаляется, а все ссылки на него в данных (задачи, события, проекты…) переписываются на первый.
// Снимки (backup:*) не трогаем — они остаются историческими копиями. Перед изменением делается снимок.
async function mergeAdminShiftProfiles() {
  try {
    const employees = parseJsonArray(store.kv['roster:employees']);
    if (!employees) return;
    const NAMES = new Set(['Администратор 1 смена', 'Администратор 2 смена', 'Администратор']);
    const isAdminProfile = e => e && (NAMES.has(e.role) || NAMES.has(e.name));
    // Работаем только если остались старые профили «1 смена»/«2 смена». Одиночный или несколько
    // профилей «Администратор» (добавленных вручную позже) не трогаем.
    const OLD1 = 'Администратор 1 смена', OLD2 = 'Администратор 2 смена';
    const hasOld = employees.some(e => e && (e.role === OLD1 || e.role === OLD2 || e.name === OLD1 || e.name === OLD2));
    if (!hasOld) return;
    const group = employees.filter(isAdminProfile);
    await runFrequentBackup();
    // Остаётся профиль, у которого уже есть пароль (иначе — первый по списку).
    const keep = group.find(e => store.credentials[e.id] !== undefined) || group[0];
    const drops = group.filter(e => e !== keep);
    const oldKv = {};
    const oldCreds = { ...store.credentials };
    const newEmployees = employees.filter(e => !drops.includes(e)).map(e => {
      if (e !== keep) return e;
      return { ...e, role: 'Администратор', name: NAMES.has(e.name) ? 'Администратор' : e.name };
    });
    oldKv['roster:employees'] = store.kv['roster:employees'];
    store.kv['roster:employees'] = JSON.stringify(newEmployees);
    drops.forEach(drop => {
      Object.keys(store.kv).forEach(k => {
        if (k === 'roster:employees' || k.startsWith('backup:')) return;
        const v = store.kv[k];
        if (typeof v === 'string' && v.includes(drop.id)) {
          if (!(k in oldKv)) oldKv[k] = v;
          store.kv[k] = v.split(drop.id).join(keep.id);
        }
      });
      delete store.credentials[drop.id];
    });
    const ok = await persist();
    if (!ok) {
      Object.keys(oldKv).forEach(k => { store.kv[k] = oldKv[k]; });
      store.credentials = oldCreds;
      console.error('Объединение профилей администраторов не удалось сохранить — оставлено как было, попробуем при следующем запуске.');
      return;
    }
    console.log(`Профили администраторов объединены в один: «${newEmployees.find(e => e.id === keep.id).name}» (удалено дублей: ${drops.length}, ключей данных обновлено: ${Object.keys(oldKv).length - 1}).`);
  } catch (e) {
    console.error('Ошибка объединения профилей администраторов:', e && e.message);
  }
}

(async function main() {
  const loaded = await loadStoreReliably();
  store = { kv: loaded.kv, credentials: loaded.credentials };
  // Если снимки уже живут в отдельном файле (нет «старых» снимков внутри store.json) — основной файл
  // можно сразу писать без них. Иначе (старый формат) сначала переносим снимки в отдельный файл —
  // см. ниже, после запуска сервера — и только после УСПЕШНОЙ записи начинаем их из основного убирать.
  backupsFilePersisted = !loaded.legacyBackupsInLive;

  // Сеем список ролей по умолчанию ТОЛЬКО при подтверждённом первом запуске (см. loadStoreReliably
  // выше) — именно это различие и было источником потери данных 4 октября 2026.
  if (!store.kv['roster:employees']) {
    const DEFAULT_ROLES = ['Руководитель', 'Ассистент', 'Маркетолог', 'Старший администратор', 'Менеджер', 'Администратор'];
    const employees = DEFAULT_ROLES.map((role, i) => ({ id: `emp-seed-${i}-${Date.now()}`, name: role, role }));
    store.kv['roster:employees'] = JSON.stringify(employees);
    await persist();
    console.log('Список сотрудников по умолчанию создан (6 ролей). Задайте пароли через экран входа.');
  }

  await mergeAdminShiftProfiles();
  await migratePersonalToLeader();
  await migrateSimpleTaskBlocks();

  serverReady = true;
  console.log('Данные загружены, сервер готов к работе.');
  tgStart().catch(e => console.error('Telegram:', e && e.message));

  // Переход со старого формата: переносим снимки из store.json в отдельный файл (в фоне, сервер уже
  // принимает запросы). Пока перенос не удался, основной файл продолжает писаться целиком — ничего
  // не теряется; после успеха он «худеет» до размера одних живых данных.
  if (loaded.legacyBackupsInLive) {
    saveBackups().then(ok => console.log(ok
      ? 'Снимки перенесены в отдельный файл, основной файл данных теперь маленький.'
      : 'Не удалось перенести снимки в отдельный файл — попробуем при следующем снимке.'));
  }

  // Автобэкап: один раз вскоре после старта (на случай долгого простоя сервера), затем раз в сутки
  setTimeout(runAutoBackup, 60 * 1000);
  setInterval(runAutoBackup, 24 * 60 * 60 * 1000);
  // Частые снимки — каждые 15 минут (первый почти сразу после старта)
  setTimeout(runFrequentBackup, 90 * 1000);
  setInterval(runFrequentBackup, FREQUENT_BACKUP_INTERVAL_MS);
  // Email-бэкап: раз в день, независимо от S3-бэкапов выше (отдельный "второй карман" —
  // письмо уходит сразу на почту и оседает там, не завися от того, что происходит с S3).
  if (EMAIL_BACKUP_ENABLED) {
    setTimeout(() => sendBackupEmail('ежедневная отправка'), 120 * 1000);
    setInterval(() => sendBackupEmail('ежедневная отправка'), 24 * 60 * 60 * 1000);
  }
})();
