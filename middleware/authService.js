/**
 * FA-1 Authentication Service
 * Simple session-based login for User A and User B.
 * Backend identifies the user from a signed session cookie — never trusts client-supplied user IDs.
 */

const crypto = require('crypto');

// Pre-seeded user credentials (never exposed to client)
const USER_CREDENTIALS = {
  user_a: { id: 'user_a', name: 'User A', email: 'usera@system.local', password: 'passA' },
  user_b: { id: 'user_b', name: 'User B', email: 'userb@system.local', password: 'passB' },
  user_c: { id: 'user_c', name: 'User C', email: 'userc@system.local', password: 'passC' },
  user_d: { id: 'user_d', name: 'User D', email: 'userd@system.local', password: 'passD' },
  user_e: { id: 'user_e', name: 'User E', email: 'usere@system.local', password: 'passE' }
};

// In-memory session store: sessionToken -> { userId, createdAt }
const sessions = new Map();

const SESSION_COOKIE = 'ds_session';
const SESSION_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

/** Generate a cryptographically-secure session token */
function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

/** Parse cookies from a raw Cookie header string */
function parseCookies(cookieHeader = '') {
  const cookies = {};
  cookieHeader.split(';').forEach(part => {
    const [key, ...rest] = part.trim().split('=');
    if (key) cookies[key.trim()] = decodeURIComponent(rest.join('=').trim());
  });
  return cookies;
}

/**
 * Attempt login. Returns session token on success, null on failure.
 */
function login(userId, password) {
  const user = USER_CREDENTIALS[userId];
  if (!user || user.password !== password) return null;

  const token = generateToken();
  sessions.set(token, { userId: user.id, createdAt: Date.now() });
  console.log(`[Auth] User '${userId}' logged in. Session created.`);
  return token;
}

/**
 * Logout by invalidating session token.
 */
function logout(token) {
  if (token && sessions.has(token)) {
    const { userId } = sessions.get(token);
    sessions.delete(token);
    console.log(`[Auth] User '${userId}' logged out. Session destroyed.`);
    return true;
  }
  return false;
}

/**
 * Resolve a session token to user info. Returns null if expired/invalid.
 */
function resolveSession(token) {
  if (!token || !sessions.has(token)) return null;
  const session = sessions.get(token);
  if (Date.now() - session.createdAt > SESSION_TTL_MS) {
    sessions.delete(token);
    return null;
  }
  const creds = USER_CREDENTIALS[session.userId];
  if (!creds) return null;
  // Return safe public user object (no password)
  return { id: creds.id, name: creds.name, email: creds.email };
}

/**
 * Express middleware: injects req.user if session cookie is valid.
 * Does NOT reject unauthenticated requests (use requireAuth for that).
 */
function sessionMiddleware(req, res, next) {
  const cookies = parseCookies(req.headers.cookie || '');
  const token = cookies[SESSION_COOKIE];
  req.user = resolveSession(token) || null;
  req.sessionToken = token || null;
  next();
}

/**
 * Express middleware: returns 401 if no authenticated session.
 */
function requireAuth(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ error: 'Unauthorized: Please login first', code: 401 });
  }
  next();
}

/**
 * Set session cookie on response.
 */
function setSessionCookie(res, token) {
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${token}; HttpOnly; Path=/; SameSite=Lax; Max-Age=${SESSION_TTL_MS / 1000}`);
}

/**
 * Clear session cookie on response.
 */
function clearSessionCookie(res) {
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0`);
}

/**
 * Get public user list (without passwords).
 */
function getAllUsers() {
  return Object.values(USER_CREDENTIALS).map(u => ({ id: u.id, name: u.name, email: u.email }));
}

module.exports = {
  login,
  logout,
  resolveSession,
  sessionMiddleware,
  requireAuth,
  setSessionCookie,
  clearSessionCookie,
  getAllUsers,
  SESSION_COOKIE
};
