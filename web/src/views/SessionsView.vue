<template>
  <div class="panel">
    <div class="toolbar">
      <el-input v-model="query" placeholder="Client ID 搜索" clearable style="width:240px"
                :prefix-icon="Search" @change="load(1)"></el-input>
      <span class="spacer"></span>
      <el-checkbox v-model="offlineOnly" label="仅看离线持久会话"></el-checkbox>
    </div>
    <el-table :data="filtered" stripe v-loading="loading">
      <el-table-column label="Client ID" min-width="200"><template #default="{ row }">
        <span class="mono">{{ row.client_id }}</span>
      </template></el-table-column>
      <el-table-column label="状态" width="100"><template #default="{ row }">
        <el-tag size="small" :type="row.connected ? 'success' : 'info'">{{ row.connected ? '在线' : '离线' }}</el-tag>
      </template></el-table-column>
      <el-table-column label="Clean Start" width="110" align="center"><template #default="{ row }">
        <el-tag size="small" effect="plain" :type="row.clean_start ? 'info' : 'warning'">{{ row.clean_start ? 'clean' : '持久' }}</el-tag>
      </template></el-table-column>
      <el-table-column label="Session Expiry" width="140" align="center"><template #default="{ row }">{{ expiryText(row.session_expiry) }}</template></el-table-column>
      <el-table-column label="Inflight" width="90" align="center"><template #default="{ row }">{{ row.inflight_in + row.inflight_out }}</template></el-table-column>
      <el-table-column prop="subscriptions" label="订阅数" width="90" align="center"></el-table-column>
      <el-table-column label="创建时间" width="170"><template #default="{ row }">{{ fmtTime(row.created_at) }}</template></el-table-column>
      <el-table-column label="操作" width="110"><template #default="{ row }">
        <el-popconfirm title="清除该持久会话?" @confirm="clear(row)"><template #reference>
          <el-button size="small" text type="danger">清除</el-button></template></el-popconfirm>
      </template></el-table-column>
    </el-table>
    <div style="margin-top:14px; display:flex; justify-content:flex-end">
      <el-pagination layout="total, prev, pager, next" :total="total" :page-size="pageSize"
                     :current-page="page" @current-change="load"></el-pagination>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Search } from '@element-plus/icons-vue';
import { listConnections, clearSessionApi } from '../api/index.js';
import { fmtTime } from '../utils/format.js';

// 会话视图基于连接注册表:离线持久会话以 connected=false 出现在列表中。
const rows = ref([]);
const total = ref(0);
const page = ref(1);
const pageSize = 20;
const loading = ref(false);
const query = ref('');
const offlineOnly = ref(false);

const filtered = computed(() => (offlineOnly.value ? rows.value.filter((r) => !r.connected) : rows.value));

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
    if (query.value) params.client_id = query.value;
    const res = await listConnections(params);
    rows.value = res.items || [];
    total.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    loading.value = false;
  }
}

async function clear(row) {
  try {
    await clearSessionApi(row.client_id);
    ElMessage.success(`已清除会话 ${row.client_id}`);
    load();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

onMounted(() => load(1));
</script>
