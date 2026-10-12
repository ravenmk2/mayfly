import { defineStore } from 'pinia';
import { getStatus } from '../api/index.js';

const POLL_MS = 5000;
// 采样保留上限:5s 间隔下约 6 小时;1h/24h 视图从其中取窗口。
const MAX_SAMPLES = 4320;

// broker 运行状态:轮询 /status,维护消息速率采样供概览图表使用。
export const useStatusStore = defineStore('status', {
  state: () => ({
    latest: null,
    online: false,
    samples: [], // { t, inRate, outRate } —— 由计数器差分得到 msg/s
    timer: null,
    _prev: null,
  }),
  actions: {
    async poll() {
      try {
        const snap = await getStatus();
        const now = Date.now();
        if (this._prev) {
          const dt = (now - this._prev.t) / 1000;
          if (dt > 0) {
            const inRate = Math.max(0, (snap.messages_received - this._prev.in) / dt);
            const outRate = Math.max(0, (snap.messages_sent - this._prev.out) / dt);
            this.samples.push({ t: now, inRate: Math.round(inRate * 10) / 10, outRate: Math.round(outRate * 10) / 10 });
            if (this.samples.length > MAX_SAMPLES) {
              this.samples.splice(0, this.samples.length - MAX_SAMPLES);
            }
          }
        }
        this._prev = { t: now, in: snap.messages_received, out: snap.messages_sent };
        this.latest = snap;
        this.online = true;
      } catch (e) {
        this.online = false;
        if (e && e.status === 401) this.stop();
      }
    },
    start() {
      if (this.timer) return;
      this.poll();
      this.timer = setInterval(() => this.poll(), POLL_MS);
    },
    stop() {
      if (this.timer) {
        clearInterval(this.timer);
        this.timer = null;
      }
      this._prev = null;
      this.samples = [];
    },
  },
});
