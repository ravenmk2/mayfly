// Display formatting helpers shared by the console views.

// fmtNumber renders with thousands separators.
export function fmtNumber(n) {
  if (n === null || n === undefined || Number.isNaN(Number(n))) return '—';
  return Number(n).toLocaleString('en-US');
}

// fmtTime renders an RFC3339 timestamp as local "YYYY-MM-DD HH:mm:ss".
export function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  const p = (x) => String(x).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// fmtUptime renders seconds as "6d 4h" / "3h 12m" / "5m 20s".
export function fmtUptime(seconds) {
  const s = Math.max(0, Math.floor(Number(seconds) || 0));
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

// fmtBytes renders a byte count with binary units.
export function fmtBytes(n) {
  const v = Number(n) || 0;
  if (v < 1024) return v + ' B';
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let x = v;
  let i = -1;
  do {
    x /= 1024;
    i++;
  } while (x >= 1024 && i < units.length - 1);
  return x.toFixed(1) + ' ' + units[i];
}

// b64Preview decodes a base64 payload preview to UTF-8 text; undecodable
// bytes fall back to a hex dump prefix.
export function b64Preview(b64) {
  if (!b64) return '';
  try {
    const bin = atob(b64);
    const bytes = Uint8Array.from(bin, (ch) => ch.charCodeAt(0));
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    return text;
  } catch {
    try {
      const bin = atob(b64);
      return Array.from(bin, (ch) => ch.charCodeAt(0).toString(16).padStart(2, '0')).join(' ');
    } catch {
      return b64;
    }
  }
}

// protoVersion maps the MQTT protocol level byte to a label.
export function protoVersion(v) {
  if (v === 5) return '5.0';
  if (v === 4) return '3.1.1';
  return String(v ?? '—');
}
