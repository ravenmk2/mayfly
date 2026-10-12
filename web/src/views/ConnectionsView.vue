<template>
  <div class="panel">
    <div class="toolbar">
      <el-input v-model="query" :placeholder="searchBy === 'client_id' ? 'Client ID 搜索' : 'Username 搜索'"
                clearable style="width:300px" :prefix-icon="Search" @change="load(1)">
        <template #prepend>
          <el-select v-model="searchBy" style="width:110px">
            <el-option value="client_id" label="Client ID"></el-option>
            <el-option value="username" label="Username"></el-option>
          </el-select>
        </template>
      </el-input>
      <span class="spacer"></span>
      <span style="font-size:12.5px;color:var(--mf-dim)">共 {{ total }} 条</span>
    </div>
    <el-table :data="rows" stripe v-loading="loading">
      <el-table-column label="Client ID" min-width="180"><template #default="{ row }">
        <el-link type="primary" :underline="false" class="mono" @click="openConn(row)">{{ row.client_id }}</el-link>
      </template></el-table-column>
      <el-table-column label="状态" width="80"><template #default="{ row }">
        <el-tag size="small" :type="row.connected ? 'success' : 'info'">{{ row.connected ? '在线' : '离线' }}</el-tag>
      </template></el-table-column>
      <el-table-column prop="username" label="Username" width="130"></el-table-column>
      <el-table-column label="地址" width="160"><template #default="{ row }"><span class="mono">{{ row.remote_addr || '—' }}</span></template></el-table-column>
      <el-table-column label="协议" width="110"><template #default="{ row }">
        <el-tag size="small" effect="plain">{{ row.listener_id || '—' }} · {{ protoVersion(row.protocol_version) }}</el-tag>
      </template></el-table-column>
      <el-table-column prop="keepalive" label="Keepalive" width="90" align="center"></el-table-column>
      <el-table-column label="Inflight (入/出)" width="130" align="center"><template #default="{ row }">
        <span class="mono" style="color:#10b981">↑{{ row.inflight_in }}</span>&nbsp;<span class="mono" style="color:#38bdf8">↓{{ row.inflight_out }}</span>
      </template></el-table-column>
      <el-table-column label="连接时间" width="170"><template #default="{ row }">{{ fmtTime(row.connected_at) }}</template></el-table-column>
      <el-table-column label="操作" width="150" fixed="right"><template #default="{ row }">
        <el-button size="small" text type="primary" @click="openConn(row)">详情</el-button>
        <el-popconfirm title="确认踢除该连接?" @confirm="kick(row)">
          <template #reference><el-button size="small" text type="danger">踢除</el-button></template>
        </el-popconfirm>
      </template></el-table-column>
    </el-table>
    <div style="margin-top:14px; display:flex; justify-content:flex-end">
      <el-pagination layout="total, prev, pager, next" :total="total" :page-size="pageSize"
                     :current-page="page" @current-change="load"></el-pagination>
    </div>

    <el-drawer v-model="drawer" size="480px" :with-header="false">
      <template v-if="detail">
        <h3 style="margin-bottom:14px">连接详情</h3>
        <dl class="drawer-kv">
          <dt>Client ID</dt><dd class="mono">{{ detail.client_id }}</dd>
          <dt>Username</dt><dd>{{ detail.username || '—' }}</dd>
          <dt>对端地址</dt><dd class="mono">{{ detail.remote_addr || '—' }}</dd>
          <dt>协议</dt><dd>{{ detail.listener_id || '—' }} · MQTT {{ protoVersion(detail.protocol_version) }}</dd>
          <dt>Keepalive</dt><dd>{{ detail.keepalive }} s</dd>
          <dt>连接时间</dt><dd>{{ fmtTime(detail.connected_at) }}</dd>
          <dt>Clean Start</dt><dd>{{ detail.clean_start ? '是' : '否' }}</dd>
          <dt>Session Expiry</dt><dd>{{ expiryText(detail.session_expiry) }}</dd>
        </dl>
        <h4 style="margin-bottom:10px">配额使用</h4>
        <div class="quota-row"><span class="name">Inflight 发送</span><span class="mono">{{ detail.inflight_out }}</span></div>
        <div class="quota-row"><span class="name">Inflight 接收</span><span class="mono">{{ detail.inflight_in }}</span></div>
        <div class="quota-row"><span class="name">订阅数</span><span class="mono">{{ detail.subscriptions }}</span></div>
        <h4 style="margin:16px 0 10px">订阅({{ detailSubs.length }})</h4>
        <el-table :data="detailSubs" size="small">
          <el-table-column prop="filter" label="Filter"><template #default="{ row }"><span class="mono">{{ row.filter }}</span></template></el-table-column>
          <el-table-column prop="qos" label="QoS" width="70" align="center"></el-table-column>
        </el-table>
        <div style="margin-top:20px; display:flex; gap:10px">
          <el-button type="danger" plain style="flex:1" @click="kick(detail); drawer=false">踢除连接</el-button>
          <el-button plain style="flex:1" @click="clearSessionOf(detail)">清除会话</el-button>
        </div>
      </template>
    </el-drawer>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Search } from '@element-plus/icons-vue';
import { listConnections, getConnection, kickConnection, clearSessionApi } from '../api/index.js';
import { fmtTime, protoVersion } from '../utils/format.js';

const rows = ref([]);
const total = ref(0);
const page = ref(1);
const pageSize = 20;
const loading = ref(false);
const query = ref('');
const searchBy = ref('client_id');

const drawer = ref(false);
const detail = ref(null);
const detailSubs = ref([]);

function expiryText(v) {
  if (v >= 0xffffffff) return '永不';
  if (v === 0) return '断开即失效';
  return v + ' s';
}

async function load(p) {
  page.value = p || page.value;
  loading.value = true;
  try {
    const params = { page: page.value, limit: pageSize };
    if (query.value) params[searchBy.value] = query.value;
    const res = await listConnections(params);
    rows.value = res.items || [];
    total.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    loading.value = false;
  }
}

async function openConn(row) {
  try {
    const res = await getConnection(row.client_id);
    detail.value = res.connection;
    detailSubs.value = res.subscriptions || [];
    drawer.value = true;
  } catch (e) {
    ElMessage.error(e.message);
  }
}

async function kick(row) {
  try {
    await kickConnection(row.client_id);
    ElMessage.success(`已踢除 ${row.client_id}`);
    load();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

async function clearSessionOf(row) {
  try {
    await clearSessionApi(row.client_id);
    ElMessage.success(`已清除会话 ${row.client_id}`);
    drawer.value = false;
    load();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

onMounted(() => load(1));
</script>
