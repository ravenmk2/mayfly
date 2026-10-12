import { describe, it, expect, vi } from 'vitest';
import { createApiClient, ApiError } from '../src/api/client.js';

function fakeFetch(status, body) {
  return vi.fn(async () => ({
    status,
    ok: status >= 200 && status < 300,
    text: async () => (body === undefined ? '' : JSON.stringify(body)),
  }));
}

describe('api client', () => {
  it('sends GET with query params and bearer token', async () => {
    const f = fakeFetch(200, { items: [], total: 0 });
    const client = createApiClient({ baseURL: '/api/v1', getToken: () => 'tok', fetchImpl: f });
    const out = await client.get('/connections', { page: 2, limit: 20, client_id: 'dev', empty: '' });
    expect(out).toEqual({ items: [], total: 0 });
    const [url, opts] = f.mock.calls[0];
    expect(url).toBe('/api/v1/connections?page=2&limit=20&client_id=dev');
    expect(opts.method).toBe('GET');
    expect(opts.headers.Authorization).toBe('Bearer tok');
    expect(opts.body).toBeUndefined();
  });

  it('sends POST with JSON body and omits token when absent', async () => {
    const f = fakeFetch(200, { token: 'abc' });
    const client = createApiClient({ baseURL: '/api/v1', getToken: () => '', fetchImpl: f });
    await client.post('/login', { username: 'admin', password: 'x' });
    const [, opts] = f.mock.calls[0];
    expect(opts.headers['Content-Type']).toBe('application/json');
    expect(opts.headers.Authorization).toBeUndefined();
    expect(JSON.parse(opts.body)).toEqual({ username: 'admin', password: 'x' });
  });

  it('unwraps {code, message} error bodies into ApiError', async () => {
    const f = fakeFetch(404, { code: 'not_found', message: 'connection not found' });
    const client = createApiClient({ fetchImpl: f });
    const err = await client.get('/connections/nope').catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(404);
    expect(err.code).toBe('not_found');
    expect(err.message).toBe('connection not found');
  });

  it('falls back to a generic message for non-JSON errors', async () => {
    const f = vi.fn(async () => ({ status: 502, ok: false, text: async () => 'bad gateway' }));
    const client = createApiClient({ fetchImpl: f });
    const err = await client.get('/status').catch((e) => e);
    expect(err.status).toBe(502);
    expect(err.message).toContain('502');
  });

  it('notifies on 401 for token expiry handling', async () => {
    const onUnauthorized = vi.fn();
    const f = fakeFetch(401, { code: 'unauthorized', message: 'authentication required' });
    const client = createApiClient({ fetchImpl: f, onUnauthorized });
    await expect(client.get('/status')).rejects.toBeInstanceOf(ApiError);
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
  });

  it('returns null for 204 no-content responses', async () => {
    const f = vi.fn(async () => ({ status: 204, ok: true, text: async () => '' }));
    const client = createApiClient({ fetchImpl: f });
    await expect(client.del('/retained', { topic: 'a/b' })).resolves.toBeNull();
    expect(f.mock.calls[0][0]).toBe('/api/v1/retained?topic=a%2Fb');
  });

  it('maps network failures to ApiError with status 0', async () => {
    const f = vi.fn(async () => {
      throw new TypeError('fetch failed');
    });
    const client = createApiClient({ fetchImpl: f });
    const err = await client.get('/status').catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(0);
    expect(err.code).toBe('network_error');
  });
});
