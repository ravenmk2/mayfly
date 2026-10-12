<template>
  <div>
    <div class="stats-panel">
      <div class="stat-item" v-for="s in stats" :key="s.label">
        <div class="label"><el-icon :color="s.color"><component :is="s.icon" /></el-icon>{{ s.label }}</div>
        <div class="value">{{ s.value }}<small>{{ s.unit }}</small></div>
        <div class="trend" :class="s.trendCls">{{ s.trend }}</div>
      </div>
    </div>
    <div class="chart-row">
      <div class="panel">
        <h3>消息速率(msg/s)<span class="extra"><el-radio-group v-model="msgRange" size="small">
          <el-radio-button value="1h">1 小时</el-radio-button><el-radio-button value="24h">24 小时</el-radio-button>
        </el-radio-group></span></h3>
        <div ref="chartEl" style="height:280px"></div>
      </div>
      <div class="panel">
        <h3>监听器</h3>
        <el-table v-if="listeners.length" :data="listeners" size="small" :show-header="false">
          <el-table-column prop="name" width="90"></el-table-column>
          <el-table-column><template #default="{ row }"><span class="mono">{{ row.clients }} 连接</span></template></el-table-column>
          <el-table-column width="90" align="right"><template #default>
            <el-tag size="small" type="success" effect="light">运行中</el-tag>
          </template></el-table-column>
        </el-table>
        <el-empty v-else description="暂无在线连接,无法探测监听器" :image-size="60"></el-empty>
        <h3 style="margin-top:18px">资源</h3>
        <div class="quota-row"><span class="name">连接数</span>
          <el-progress :percentage="pct(latest && latest.connections_current, 10240)" :stroke-width="10" color="#f59e0b"/>
          <span class="mono">{{ fmtNumber(latest && latest.connections_current) }}</span></div>
        <div class="quota-row"><span class="name">Inflight</span>
          <el-progress :percentage="pct(latest && latest.inflight, 1024)" :stroke-width="10" color="#a78bfa"/>
          <span class="mono">{{ fmtNumber(latest && latest.inflight) }}</span></div>
        <div class="quota-row"><span class="name">Goroutines</span>
          <el-progress :percentage="pct(latest && latest.goroutines, 2000)" :stroke-width="10" color="#38bdf8"/>
          <span class="mono">{{ fmtNumber(latest && latest.goroutines) }}</span></div>
        <div class="quota-row"><span class="name">消息丢弃</span>
          <el-progress :percentage="0" :stroke-width="10" color="#34d399"/>
          <span class="mono">{{ fmtNumber(latest && latest.messages_dropped) }}</span></div>
        <div style="font-size:11.5px; color:var(--mf-dim); margin-top:6px">进度条上限为展示参考值,非服务端限制</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref, watch } from 'vue';
import * as echarts from 'echarts';
import { useStatusStore } from '../stores/status.js';
import { useThemeStore } from '../stores/theme.js';
import { listConnections } from '../api/index.js';
import { fmtNumber, fmtUptime } from '../utils/format.js';

const status = useStatusStore();
const theme = useThemeStore();
const latest = computed(() => status.latest);

const msgRange = ref('1h');
const chartEl = ref(null);
const listeners = ref([]);
let chart = null;

function pct(v, ref) {
  if (!v) return 0;
  return Math.min(100, Math.round((v / ref) * 100));
}

function trendOf(cur, prev, suffix = ' vs 上一采样') {
  if (prev === null || prev === undefined || prev === 0) return { text: '—', cls: 'flat' };
  const d = ((cur - prev) / prev) * 100;
  if (Math.abs(d) < 0.05) return { text: '— 持平' + suffix, cls: 'flat' };
  return d > 0
    ? { text: '▲ ' + d.toFixed(1) + '%' + suffix, cls: 'up' }
    : { text: '▼ ' + Math.abs(d).toFixed(1) + '%' + suffix, cls: 'down' };
}

const prevSnap = ref(null);
watch(latest, (v, old) => { prevSnap.value = old || null; });

const lastRate = computed(() => {
  const n = status.samples.length;
  return {
    inRate: n ? status.samples[n - 1].inRate : 0,
    outRate: n ? status.samples[n - 1].outRate : 0,
    prevIn: n > 1 ? status.samples[n - 2].inRate : null,
    prevOut: n > 1 ? status.samples[n - 2].outRate : null,
  };
});

const stats = computed(() => {
  const s = latest.value || {};
  const p = prevSnap.value || {};
  const tConn = trendOf(s.connections_current || 0, p.connections_current);
  const tIn = trendOf(lastRate.value.inRate, lastRate.value.prevIn);
  const tOut = trendOf(lastRate.value.outRate, lastRate.value.prevOut);
  return [
    { label: '在线连接', value: fmtNumber(s.connections_current), unit: '', trend: tConn.text, trendCls: tConn.cls, icon: 'Connection', color: '#f59e0b' },
    { label: '消息流入', value: fmtNumber(lastRate.value.inRate), unit: 'msg/s', trend: tIn.text, trendCls: tIn.cls, icon: 'Top', color: '#10b981' },
    { label: '消息流出', value: fmtNumber(lastRate.value.outRate), unit: 'msg/s', trend: tOut.text, trendCls: tOut.cls, icon: 'Bottom', color: '#38bdf8' },
    { label: '订阅总数', value: fmtNumber(s.subscriptions), unit: '', trend: '—', trendCls: 'flat', icon: 'Share', color: '#a78bfa' },
    { label: 'Retained', value: fmtNumber(s.retained), unit: '条', trend: '—', trendCls: 'flat', icon: 'Collection', color: '#fb923c' },
    { label: 'Inflight', value: fmtNumber(s.inflight), unit: '', trend: '—', trendCls: 'flat', icon: 'Timer', color: '#f472b6' },
    { label: '认证失败', value: fmtNumber(s.auth_failures), unit: '次', trend: '累计', trendCls: 'flat', icon: 'Lock', color: '#e45656' },
    { label: '运行时长', value: fmtUptime(s.uptime_seconds), unit: '', trend: s.version ? 'v' + s.version : '—', trendCls: 'flat', icon: 'Clock', color: '#8f887d' },
  ];
});

const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

function renderChart() {
  if (!chartEl.value) return;
  if (!chart) chart = echarts.init(chartEl.value);
  const windowMs = msgRange.value === '1h' ? 3600e3 : 86400e3;
  const cutoff = Date.now() - windowMs;
  const pts = status.samples.filter((p) => p.t >= cutoff);
  const grid = cssVar('--mf-chart-grid');
  const axis = cssVar('--mf-chart-axis');
  const times = pts.map((p) => {
    const d = new Date(p.t);
    const hm = d.getHours() + ':' + String(d.getMinutes()).padStart(2, '0');
    return msgRange.value === '1h'
      ? hm + ':' + String(d.getSeconds()).padStart(2, '0')
      : (d.getMonth() + 1) + '-' + d.getDate() + ' ' + hm;
  });
  chart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['流入', '流出'], right: 8, top: 0, textStyle: { color: axis } },
    grid: { left: 44, right: 14, top: 34, bottom: 24 },
    xAxis: { type: 'category', data: times, axisLine: { lineStyle: { color: grid } }, axisLabel: { color: axis } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: grid } }, axisLabel: { color: axis } },
    series: [
      { name: '流入', type: 'line', smooth: false, showSymbol: false, data: pts.map((p) => p.inRate),
        lineStyle: { color: '#10b981', width: 2 }, areaStyle: { color: 'rgba(16,185,129,.10)' } },
      { name: '流出', type: 'line', smooth: false, showSymbol: false, data: pts.map((p) => p.outRate),
        lineStyle: { color: '#f59e0b', width: 2 }, areaStyle: { color: 'rgba(245,158,11,.14)' } },
    ],
  }, true);
}

async function refreshListeners() {
  try {
    const res = await listConnections({ page: 1, limit: 500 });
    const seen = new Map();
    for (const c of res.items || []) {
      if (!c.connected) continue;
      const id = c.listener_id || 'unknown';
      seen.set(id, (seen.get(id) || 0) + 1);
    }
    listeners.value = Array.from(seen, ([name, clients]) => ({ name, clients }));
  } catch {
    // 监听器派生失败不打扰主面板
  }
}

const onResize = () => chart && chart.resize();
let listenersTimer = null;

onMounted(() => {
  renderChart();
  refreshListeners();
  listenersTimer = setInterval(refreshListeners, 30000);
  window.addEventListener('resize', onResize);
});
onUnmounted(() => {
  window.removeEventListener('resize', onResize);
  if (listenersTimer) clearInterval(listenersTimer);
  if (chart) { chart.dispose(); chart = null; }
});
watch(() => status.samples.length, renderChart);
watch(msgRange, renderChart);
watch(() => theme.dark, renderChart);
</script>
