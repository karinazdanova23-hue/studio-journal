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

// ВАЖНО (урок из реального инцидента с потерей данных): эта функция ДОЛЖНА различать два разных
// случая —
//   1) "в S3 точно ничего нет" (сервер только что создан, ключа не существует — NoSuchKey/404):
//      это по-настоящему пустое хранилище, с ним safe работать как с первым запуском.
//   2) "не получилось прочитать" (таймаут, сеть, неверные ключи доступа, повреждённый JSON):
//      это НЕ означает, что данных нет — они могут быть целы, просто сейчас недоступны.
// Раньше оба случая возвращали одинаковый пустой результат, и код выше (main()) не мог их
// отличить — после сбоя чтения он считал это "первым запуском", создавал сотрудников по
// умолчанию и тут же сохранял их, затирая реальные данные в S3. Теперь функция явно помечает
// тип пустоты через `confirmedEmpty`, и только настоящий "первый запуск" разрешает сервену
// создавать данные по умолчанию и писать поверх.
async function loadStoreFromBackend() {
  if (USE_S3) {
    const outcome = await withTimeout(
      s3Client.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: S3_OBJECT_KEY })),
      S3_TIMEOUT_MS
    );
    if (outcome.timedOut) {
      console.error(`Не удалось прочитать данные из S3: не ответил за ${S3_TIMEOUT_MS}мс.`);
      return { kv: {}, credentials: {}, confirmedEmpty: false };
    }
    const e = outcome.error;
    if (e) {
      if (e.name === 'NoSuchKey' || e.$metadata?.httpStatusCode === 404) {
        console.log('В S3 ещё нет сохранённых данных — это действительно первый запуск.');
        return { kv: {}, credentials: {}, confirmedEmpty: true };
      }
      console.error('Не удалось прочитать данные из S3:', e.message);
      return { kv: {}, credentials: {}, confirmedEmpty: false };
    }
    try {
      const raw = await streamToString(outcome.value.Body);
      const parsed = JSON.parse(raw);
      return { kv: parsed.kv || {}, credentials: parsed.credentials || {}, confirmedEmpty: false };
    } catch (e2) {
      console.error('Не удалось разобрать данные из S3 (файл повреждён или пуст):', e2.message);
      return { kv: {}, credentials: {}, confirmedEmpty: false };
    }
  }
  if (!fs.existsSync(FALLBACK_PATH)) return { kv: {}, credentials: {}, confirmedEmpty: true };
  try {
    const raw = fs.readFileSync(FALLBACK_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return { kv: parsed.kv || {}, credentials: parsed.credentials || {}, confirmedEmpty: false };
  } catch (e) {
    console.error('Не удалось прочитать файл данных:', e.message);
    return { kv: {}, credentials: {}, confirmedEmpty: false };
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

(async function main() {
  store = await loadStoreReliably();

  // Сеем список ролей по умолчанию ТОЛЬКО при подтверждённом первом запуске (см. loadStoreReliably
  // выше) — именно это различие и было источником потери данных 4 октября 2026.
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
  // Email-бэкап: раз в день, независимо от S3-бэкапов выше (отдельный "второй карман" —
  // письмо уходит сразу на почту и оседает там, не завися от того, что происходит с S3).
  if (EMAIL_BACKUP_ENABLED) {
    setTimeout(() => sendBackupEmail('ежедневная отправка'), 120 * 1000);
    setInterval(() => sendBackupEmail('ежедневная отправка'), 24 * 60 * 60 * 1000);
  }
})();
