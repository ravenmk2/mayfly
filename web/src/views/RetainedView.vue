<template>
  <div class="panel">
    <div class="toolbar">
      <el-input v-model="query" placeholder="Topic 过滤(子串匹配)" clearable style="width:280px"
                :prefix-icon="Search" @change="load(1)"></el-input>
      <span class="spacer"></span>
      <span style="font-size:12.5px;color:var(--mf-dim)">共 {{ total }} 条 retained 消息</span>
    </div>
    <el-table :data="rows" stripe v-loading="loading">
      <el-table-column label="Topic" min-width="230"><template #default="{ row }">
        <span class="mono">{{ row.topic }}</span>
      </template></el-table-column>
      <el-table-column label="Payload" min-width="200"><template #default="{ row }">
        <el-link type="primary" :underline="false" class="mono" @click="viewPayload(row)">{{ preview(row) }}</el-link>
      </template></el-table-column>
      <el-table-column label="大小" width="100" align="center"><template #default="{ row }">
        <span class="mono">{{ fmtBytes(row.payload_size) }}</span>
        <el-tag v-if="row.truncated" size="small" type="warning" effect="plain" style="margin-left:4px">截断</el-tag>
      </template></el-table-column>
      <el-table-column label="操作" width="110"><template #default="{ row }">
        <el-popconfirm title="删除该 retained 消息?" @confirm="remove(row)"><template #reference>
          <el-button size="small" text type="danger">删除</el-button></template></el-popconfirm>
      </template></el-table-column>
    </el-table>
    <div style="margin-top:14px; display:flex; justify-content:flex-end">
      <el-pagination layout="total, prev, pager, next" :total="total" :page-size="pageSize"
                     :current-page="page" @current-change="load"></el-pagination>
    </div>

    <el-dialog v-model="dlgPayload" title="Retained Payload" width="520px">
      <div class="payload-view">{{ activePayload }}</div>
      <div v-if="activeTruncated" style="margin-top:10px; font-size:12px; color:var(--mf-dim)">
        仅展示前 256 字节预览(完整 payload 共 {{ activeSize }} 字节)
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Search } from '@element-plus/icons-vue';
import { listRetained, deleteRetained } from '../api/index.js';
import { fmtBytes, b64Preview } from '../utils/format.js';

const rows = ref([]);
const total = ref(0);
const page = ref(1);
const pageSize = 20;
const loading = ref(false);
const query = ref('');

const dlgPayload = ref(false);
const activePayload = ref('');
const activeTruncated = ref(false);
const activeSize = ref(0);

function preview(row) {
  const text = b64Preview(row.payload_preview);
  return text.length > 48 ? text.slice(0, 48) + '…' : text || '(空)';
}

function viewPayload(row) {
  activePayload.value = b64Preview(row.payload_preview);
  activeTruncated.value = row.truncated;
  activeSize.value = row.payload_size;
  dlgPayload.value = true;
}

async function load(p) {
  page.value = p || page.value;
  loading.value = true;
  try {
    const params = { page: page.value, limit: pageSize };
    if (query.value) params.topic = query.value;
    const res = await listRetained(params);
    rows.value = res.items || [];
    total.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    loading.value = false;
  }
}

async function remove(row) {
  try {
    await deleteRetained(row.topic);
    ElMessage.success(`已删除 ${row.topic}`);
    load();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

onMounted(() => load(1));
</script>
