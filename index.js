const path = require('path');
const fs = require('fs');
const express = require('express');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const bcrypt = require('bcryptjs');

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const STORE_PATH = path.join(DATA_DIR, 'store.json');

// ---------- простое файловое хранилище (без баз данных, без компиляции) ----------
function loadStore() {
  if (!fs.existsSync(STORE_PATH)) {
    return { kv: {}, credentials: {} };
  }
  try {
    const raw = fs.readFileSync(STORE_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return { kv: parsed.kv || {}, credentials: parsed.credentials || {} };
  } catch (e) {
    console.error('Не удалось прочитать файл данных, начинаем с пустого хранилища:', e.message);
    return { kv: {}, credentials: {} };
  }
}
let store = loadStore();

// Пишем на диск по очереди (без параллельных записей), атомарно через временный файл
let writeQueue = Promise.resolve();
function persist() {
  writeQueue = writeQueue.then(() => new Promise((resolve) => {
    const tmpPath = STORE_PATH + '.tmp';
    fs.writeFile(tmpPath, JSON.stringify(store), (err) => {
      if (err) { console.error('Ошибка записи данных:', err.message); return resolve(); }
      fs.rename(tmpPath, STORE_PATH, (err2) => {
        if (err2) console.error('Ошибка сохранения данных:', err2.message);
        resolve();
      });
    });
  }));
  return writeQueue;
}

// Сеем список ролей по умолчанию при самом первом запуске, чтобы было кого выбрать при входе
(function seedDefaultEmployees() {
  if (store.kv['roster:employees']) return;
  const DEFAULT_ROLES = ['Руководитель', 'Ассистент', 'Маркетолог', 'Старший администратор', 'Менеджер', 'Хостес'];
  const employees = DEFAULT_ROLES.map((role, i) => ({ id: `emp-seed-${i}-${Date.now()}`, name: role, role }));
  store.kv['roster:employees'] = JSON.stringify(employees);
  persist();
  console.log('Список сотрудников по умолчанию создан (6 ролей). Задайте пароли через экран входа.');
})();

const app = express();
app.use(express.json({ limit: '5mb' }));
app.use(cookieParser());

const SESSION_SECRET = process.env.SESSION_SECRET || 'change-me-in-production-please';
if (!process.env.SESSION_SECRET) {
  console.warn('[ВНИМАНИЕ] SESSION_SECRET не задан в переменных окружения — используется значение по умолчанию. Задайте свой секрет в панели Timeweb (Приложения → Переменные окружения).');
}

app.use(session({
  // Хранилище сессий по умолчанию (в памяти процесса): при перезапуске сервера
  // всем придётся войти заново — это нормально для небольшой команды.
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 30 * 24 * 60 * 60 * 1000, // 30 дней
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
  },
}));

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
  if (!req.session || !req.session.employeeId) return null;
  return findEmployee(req.session.employeeId);
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
  return key.startsWith('funds:') || key.startsWith('personal:');
}
// Ключ, который может ИЗМЕНЯТЬ только Руководитель/Ассистент (но читать может любой залогиненный)
function isRosterKey(key) {
  return key === 'roster:employees';
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
  req.session.employeeId = employeeId;
  res.json({ ok: true, employee: { id: emp.id, name: emp.name, role: emp.role } });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
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
app.post('/api/employees/:id/password', (req, res) => {
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
    delete store.credentials[targetId];
    persist();
    return res.json({ ok: true, removed: true });
  }
  store.credentials[targetId] = bcrypt.hashSync(newPassword, 10);
  persist();
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

app.put('/api/storage/:key', requireAuth, (req, res) => {
  const key = req.params.key;
  const { value } = req.body || {};
  if (typeof value !== 'string') return res.status(400).json({ error: 'value должен быть строкой' });
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  if (isRosterKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Изменять список сотрудников может только Руководитель или Ассистент' });
  }
  store.kv[key] = value;
  persist();
  res.json({ ok: true });
});

app.delete('/api/storage/:key', requireAuth, (req, res) => {
  const key = req.params.key;
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  delete store.kv[key];
  persist();
  res.json({ ok: true });
});

// ---------- статика (сам интерфейс) ----------
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Единый журнал: сервер запущен на порту ${PORT}`);
});
