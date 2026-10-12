// API client for the MAYFLY management REST API (docs/design/admin-api.md).
// Framework-free factory so it can be unit-tested with a fake fetch.

export class ApiError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

// createApiClient builds a JSON client: Bearer token injection, query
// encoding, uniform {code, message} error unwrapping and 401 notification.
export function createApiClient({ baseURL = '/api/v1', getToken, onUnauthorized, fetchImpl } = {}) {
  const doFetch = fetchImpl || ((...args) => fetch(...args));

  async function request(method, path, { params, body } = {}) {
    let url = baseURL + path;
    if (params) {
      const qs = new URLSearchParams();
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined && v !== null && v !== '') qs.set(k, String(v));
      }
      const s = qs.toString();
      if (s) url += '?' + s;
    }
    const headers = { Accept: 'application/json' };
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    const token = getToken && getToken();
    if (token) headers['Authorization'] = 'Bearer ' + token;

    let res;
    try {
      res = await doFetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
      });
    } catch (e) {
      throw new ApiError(0, 'network_error', '网络请求失败:' + (e && e.message ? e.message : '无法连接服务器'));
    }

    if (res.status === 204) return null;

    let data = null;
    const text = await res.text();
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = null;
      }
    }
    if (!res.ok) {
      const code = (data && data.code) || 'error_' + res.status;
      const message = (data && data.message) || ('请求失败(HTTP ' + res.status + ')');
      if (res.status === 401 && onUnauthorized) onUnauthorized();
      throw new ApiError(res.status, code, message);
    }
    return data;
  }

  return {
    get: (path, params) => request('GET', path, { params }),
    post: (path, body) => request('POST', path, { body }),
    put: (path, body) => request('PUT', path, { body }),
    del: (path, params) => request('DELETE', path, { params }),
  };
}
