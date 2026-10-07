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
  const shrinkError = checkShrinkGuard(key, store.kv[key], value, req);
  if (shrinkError) return res.status(409).json({ error: shrinkError });
  // Важно: сохраняем старое значение и дожидаемся РЕАЛЬНОЙ записи в хранилище (S3/диск), прежде
  // чем отвечать браузеру "ok". Если запись не удалась — откатываем в памяти и честно сообщаем об
  // ошибке, а не делаем вид, что всё сохранилось (раньше именно так терялись изменения).
  const oldValue = store.kv[key];
  store.kv[key] = value;
  const saved = await (isBackupKey(key) ? saveBackups() : persist());
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
  const saved = await (isBackupKey(key) ? saveBackups() : persist());
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
    const group = employees.filter(isAdminProfile);
    const alreadyClean = group.length === 1 && group[0].role === 'Администратор';
    if (group.length === 0 || alreadyClean) return;
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

  serverReady = true;
  console.log('Данные загружены, сервер готов к работе.');

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
