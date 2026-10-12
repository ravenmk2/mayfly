<template>
  <div class="panel">
    <div class="toolbar">
      <el-input v-model="query" :placeholder="searchBy === 'client_id' ? '按 Client ID 过滤' : '按 Topic Filter 过滤'"
                clearable style="width:320px" :prefix-icon="Search" @change="load(1)">
        <template #prepend>
          <el-select v-model="searchBy" style="width:130px">
            <el-option value="client_id" label="Client ID"></el-option>
            <el-option value="filter" label="Topic Filter"></el-option>
          </el-select>
        </template>
      </el-input>
      <span class="spacer"></span>
      <el-tag effect="plain" type="info">共享订阅组 {{ sharedGroups }} 个</el-tag>
    </div>
    <el-table :data="rows" stripe v-loading="loading">
      <el-table-column label="Client ID" min-width="170"><template #default="{ row }">
        <span class="mono">{{ row.client_id }}</span>
      </template></el-table-column>
      <el-table-column label="Topic Filter" min-width="230"><template #default="{ row }">
        <span class="mono">{{ row.filter }}</span>
        <el-tag v-if="row.shared" size="small" type="warning" effect="plain" style="margin-left:6px">共享·{{ row.shared }}</el-tag>
      </template></el-table-column>
      <el-table-column label="QoS" width="70" align="center"><template #default="{ row }">
        <el-tag size="small" :type="['success','warning','danger'][row.qos] || 'info'" effect="light">QoS {{ row.qos }}</el-tag>
      </template></el-table-column>
      <el-table-column label="NL/RAP" width="90" align="center"><template #default="{ row }">
        <span class="mono" style="color:var(--mf-dim)">{{ row.no_local ? 1 : 0 }}/{{ row.retain_as_published ? 1 : 0 }}</span>
      </template></el-table-column>
      <el-table-column label="Retain Handling" width="130" align="center"><template #default="{ row }">
        <span class="mono" style="color:var(--mf-dim)">{{ row.retain_handling }}</span>
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
import { listSubscriptions } from '../api/index.js';

const rows = ref([]);
const total = ref(0);
const page = ref(1);
const pageSize = 20;
const loading = ref(false);
const query = ref('');
const searchBy = ref('client_id');

const sharedGroups = computed(() => new Set(rows.value.map((r) => r.shared).filter(Boolean)).size);

async function load(p) {
  page.value = p || page.value;
  loading.value = true;
  try {
    const params = { page: page.value, limit: pageSize };
    if (query.value) params[searchBy.value] = query.value;
    const res = await listSubscriptions(params);
    rows.value = res.items || [];
    total.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    loading.value = false;
  }
}

onMounted(() => load(1));
</script>
