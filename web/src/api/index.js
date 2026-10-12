// Endpoint bindings for the MAYFLY management REST API v1
// (docs/design/admin-api.md section 3).
import { createApiClient } from './client.js';

const TOKEN_KEY = 'mf-token';
const USER_KEY = 'mf-user';

export function getStoredToken() {
  return localStorage.getItem(TOKEN_KEY) || '';
}
export function getStoredUser() {
  return localStorage.getItem(USER_KEY) || '';
}
export function storeSession(token, username) {
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, username);
}
export function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
}

let unauthorizedHandler = null;
export function setUnauthorizedHandler(fn) {
  unauthorizedHandler = fn;
}

export const api = createApiClient({
  baseURL: '/api/v1',
  getToken: getStoredToken,
  onUnauthorized: () => {
    if (unauthorizedHandler) unauthorizedHandler();
  },
});

// ---------- 认证 / 安装向导 ----------
export const login = (username, password) => api.post('/login', { username, password });
export const setupStatus = () => api.get('/setup/status');
export const setupTestDb = (dsn) => api.post('/setup/test-db', { db: { type: 'mysql', dsn } });
export const setup = (dbType, dsn, adminPassword) =>
  api.post('/setup', { db: { type: dbType, dsn }, admin_password: adminPassword });

// ---------- 状态 ----------
export const getStatus = () => api.get('/status');

// ---------- 连接 / 会话 ----------
export const listConnections = (params) => api.get('/connections', params);
export const getConnection = (clientID) => api.get('/connections/' + encodeURIComponent(clientID));
export const kickConnection = (clientID) => api.del('/connections/' + encodeURIComponent(clientID));
export const getSession = (clientID) => api.get('/sessions/' + encodeURIComponent(clientID));
export const clearSessionApi = (clientID) => api.del('/sessions/' + encodeURIComponent(clientID));

// ---------- 订阅 / Retained ----------
export const listSubscriptions = (params) => api.get('/subscriptions', params);
export const listRetained = (params) => api.get('/retained', params);
export const deleteRetained = (topic) => api.del('/retained', { topic });

// ---------- 测试发布 ----------
export const publish = (topic, payload, payloadEncoding, retain, qos = 0) =>
  api.post('/publish', { topic, payload, payload_encoding: payloadEncoding, retain, qos });

// ---------- MQTT 用户 / ACL ----------
export const listUsers = (params) => api.get('/users', params);
export const createUser = (username, password, superuser) =>
  api.post('/users', { username, password, superuser });
export const updateUser = (username, body) => api.put('/users/' + encodeURIComponent(username), body);
export const deleteUser = (username) => api.del('/users/' + encodeURIComponent(username));

export const listAcls = (params) => api.get('/acls', params);
export const createAcl = (rule) => api.post('/acls', rule);
export const updateAcl = (id, rule) => api.put('/acls/' + id, rule);
export const deleteAcl = (id) => api.del('/acls/' + id);

// ---------- 认证/授权链 ----------
export const getChains = () => api.get('/auth/chains');
export const putChains = (cfg) => api.put('/auth/chains', cfg);
export const refreshAuth = () => api.post('/auth/refresh', {});

// ---------- API Key ----------
export const listApiKeys = (params) => api.get('/apikeys', params);
export const createApiKey = (name, scopes, expiresInDays) =>
  api.post('/apikeys', { name, scopes, expires_in_days: expiresInDays });
export const deleteApiKey = (id) => api.del('/apikeys/' + id);
