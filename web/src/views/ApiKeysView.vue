<template>
  <div class="panel">
    <div class="toolbar">
      <el-alert type="warning" :closable="false" style="flex:1; padding:4px 12px"
        title="API Key 仅在创建时完整展示一次,请妥善保存"></el-alert>
      <el-button type="primary" :icon="Plus" @click="openDlg">创建 Key</el-button>
    </div>
    <el-table :data="rows" stripe v-loading="loading">
      <el-table-column prop="name" label="名称" min-width="130"></el-table-column>
      <el-table-column label="Key" min-width="150"><template #default="{ row }">
        <span class="mono">{{ row.prefix }}••••••••</span>
      </template></el-table-column>
      <el-table-column label="权限" min-width="140"><template #default="{ row }">
        <el-tag v-for="s in row.scopes" :key="s" size="small" effect="plain" style="margin-right:4px">{{ s }}</el-tag>
      </template></el-table-column>
      <el-table-column label="创建时间" width="165"><template #default="{ row }">{{ fmtTime(row.created_at) }}</template></el-table-column>
      <el-table-column label="过期时间" width="165"><template #default="{ row }">
        {{ row.expires_at ? fmtTime(row.expires_at) : '永不过期' }}
      </template></el-table-column>
      <el-table-column label="操作" width="100"><template #default="{ row }">
        <el-popconfirm title="吊销该 Key?" @confirm="revoke(row)"><template #reference>
          <el-button size="small" text type="danger">吊销</el-button></template></el-popconfirm>
      </template></el-table-column>
    </el-table>
    <div style="margin-top:14px; display:flex; justify-content:flex-end">
      <el-pagination layout="total, prev, pager, next" :total="total" :page-size="pageSize"
                     :current-page="page" @current-change="load"></el-pagination>
    </div>

    <el-dialog v-model="dlgCreate" title="创建 API Key" width="440px">
      <el-form label-width="80px">
        <el-form-item label="名称"><el-input v-model="form.name" placeholder="ci-pipeline"></el-input></el-form-item>
        <el-form-item label="权限">
          <el-checkbox-group v-model="form.scopes">
            <el-checkbox value="read">read</el-checkbox>
            <el-checkbox value="write">write</el-checkbox>
            <el-checkbox value="admin">admin</el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item label="有效期">
          <el-select v-model="form.expiry" style="width:100%">
            <el-option value="never" label="永不过期"></el-option>
            <el-option value="30" label="30 天"></el-option>
            <el-option value="90" label="90 天"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlgCreate=false">取消</el-button>
        <el-button type="primary" @click="create">创建</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="dlgKey" title="API Key 已创建" width="520px" :close-on-click-modal="false">
      <el-alert type="warning" :closable="false" style="margin-bottom:12px"
        title="完整 Key 仅此一次展示,关闭后无法再次查看,请立即复制保存"></el-alert>
      <div class="payload-view">{{ createdKey }}</div>
      <template #footer>
        <el-button type="primary" @click="copyKey">复制并关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Plus } from '@element-plus/icons-vue';
import { listApiKeys, createApiKey, deleteApiKey } from '../api/index.js';
import { fmtTime } from '../utils/format.js';

const rows = ref([]);
const total = ref(0);
const page = ref(1);
const pageSize = 20;
const loading = ref(false);

const dlgCreate = ref(false);
const dlgKey = ref(false);
const createdKey = ref('');
const form = reactive({ name: '', scopes: ['read'], expiry: 'never' });

async function load(p) {
  page.value = p || page.value;
  loading.value = true;
  try {
    const res = await listApiKeys({ page: page.value, limit: pageSize });
    rows.value = res.items || [];
    total.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    loading.value = false;
  }
}

function openDlg() {
  form.name = '';
  form.scopes = ['read'];
  form.expiry = 'never';
  dlgCreate.value = true;
}

async function create() {
  if (!form.name) {
    ElMessage.warning('请输入名称');
    return;
  }
  if (!form.scopes.length) {
    ElMessage.warning('请至少选择一个权限');
    return;
  }
  try {
    const res = await createApiKey(form.name, form.scopes, form.expiry === 'never' ? 0 : Number(form.expiry));
    createdKey.value = res.key;
    dlgCreate.value = false;
    dlgKey.value = true;
    load();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

async function copyKey() {
  try {
    await navigator.clipboard.writeText(createdKey.value);
    ElMessage.success('已复制');
  } catch {
    // 剪贴板不可用时由用户手动选择复制
  }
  dlgKey.value = false;
  createdKey.value = '';
}

async function revoke(row) {
  try {
    await deleteApiKey(row.id);
    ElMessage.success(`已吊销 ${row.name}`);
    load();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

onMounted(() => load(1));
</script>
