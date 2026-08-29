const path = require('path');
const fs = require('fs');
const express = require('express');
const cookieParser = require('cookie-parser');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const Database = require('better-sqlite3');
const SqliteStore = require('better-sqlite3-session-store')(session);

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const DB_PATH = path.join(DATA_DIR, 'studio-journal.db');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS kv_store (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS credentials (
    employee_id TEXT PRIMARY KEY,
    bcrypt_hash TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions (
    sid TEXT PRIMARY KEY,
    sess TEXT NOT NULL,
    expire INTEGER NOT NULL
  );
`);

// Сеем список ролей по умолчанию при самом первом запуске, чтобы было кого выбрать при входе
(function seedDefaultEmployees() {
  const row = db.prepare('SELECT value FROM kv_store WHERE key = ?').get('roster:employees');
  if (row) return;
  const DEFAULT_ROLES = ['Руководитель', 'Ассистент', 'Маркетолог', 'Старший администратор', 'Менеджер', 'Хостес'];
  const employees = DEFAULT_ROLES.map((role, i) => ({ id: `emp-seed-${i}-${Date.now()}`, name: role, role }));
  db.prepare('INSERT INTO kv_store (key, value) VALUES (?, ?)').run('roster:employees', JSON.stringify(employees));
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
  store: new SqliteStore({ client: db, expired: { clear: true, intervalMs: 15 * 60 * 1000 } }),
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
  const row = db.prepare('SELECT value FROM kv_store WHERE key = ?').get('roster:employees');
  return row ? JSON.parse(row.value) : [];
}
function findEmployee(id) {
  return getEmployees().find(e => e.id === id) || null;
}
function hasAnyCredential() {
  const row = db.prepare('SELECT COUNT(*) AS c FROM credentials').get();
  return row.c > 0;
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
// Keys that only Руководитель/Ассистент may read or write
function isRestrictedKey(key) {
  return key.startsWith('funds:') || key.startsWith('personal:');
}
// Keys that only Руководитель/Ассистент may WRITE (but anyone logged in may read)
function isRosterKey(key) {
  return key === 'roster:employees';
}

// ---------- public: employee list (no passwords) ----------
app.get('/api/employees', (req, res) => {
  const creds = new Set(db.prepare('SELECT employee_id FROM credentials').all().map(r => r.employee_id));
  const list = getEmployees().map(e => ({ id: e.id, name: e.name, role: e.role, hasPassword: creds.has(e.id) }));
  res.json({ employees: list, bootstrapNeeded: !hasAnyCredential() });
});

// ---------- auth ----------
app.post('/api/login', (req, res) => {
  const { employeeId, password } = req.body || {};
  if (!employeeId || !password) return res.status(400).json({ error: 'Укажите сотрудника и пароль' });
  const emp = findEmployee(employeeId);
  if (!emp) return res.status(404).json({ error: 'Сотрудник не найден' });
  const cred = db.prepare('SELECT bcrypt_hash FROM credentials WHERE employee_id = ?').get(employeeId);
  if (!cred) return res.status(401).json({ error: 'У этого сотрудника ещё не задан пароль' });
  if (!bcrypt.compareSync(password, cred.bcrypt_hash)) {
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

// Set / change / remove a password.
// - Bootstrap: if nobody has a credential at all, anyone (even unauthenticated) may set the FIRST password.
// - Логика после бутстрапа:
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
      const existingCred = db.prepare('SELECT bcrypt_hash FROM credentials WHERE employee_id = ?').get(targetId);
      if (existingCred) {
        if (!currentPassword || !bcrypt.compareSync(currentPassword, existingCred.bcrypt_hash)) {
          return res.status(401).json({ error: 'Текущий пароль неверен' });
        }
      }
    }
  }

  if (!newPassword) {
    db.prepare('DELETE FROM credentials WHERE employee_id = ?').run(targetId);
    return res.json({ ok: true, removed: true });
  }
  const hash = bcrypt.hashSync(newPassword, 10);
  db.prepare('INSERT INTO credentials (employee_id, bcrypt_hash) VALUES (?, ?) ON CONFLICT(employee_id) DO UPDATE SET bcrypt_hash = excluded.bcrypt_hash').run(targetId, hash);
  res.json({ ok: true });
});

// ---------- generic key-value storage API (replaces window.storage) ----------
app.get('/api/storage/:key', requireAuth, (req, res) => {
  const key = req.params.key;
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  const row = db.prepare('SELECT value FROM kv_store WHERE key = ?').get(key);
  if (!row) return res.status(404).json({ error: 'not found' });
  res.json({ key, value: row.value });
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
  db.prepare('INSERT INTO kv_store (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value').run(key, value);
  res.json({ ok: true });
});

app.delete('/api/storage/:key', requireAuth, (req, res) => {
  const key = req.params.key;
  if (isRestrictedKey(key) && !isPrivilegedRole(req.employee.role)) {
    return res.status(403).json({ error: 'Доступно только Руководителю и Ассистенту' });
  }
  db.prepare('DELETE FROM kv_store WHERE key = ?').run(key);
  res.json({ ok: true });
});

// ---------- static front-end ----------
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Единый журнал: сервер запущен на порту ${PORT}`);
});
