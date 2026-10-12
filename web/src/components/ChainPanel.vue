<template>
  <div class="panel">
    <h3>{{ title }}<span class="chain-note">{{ note }}</span>
      <span class="extra">
        <slot name="extra"></slot>
        <el-dropdown trigger="click" @command="onAdd">
          <el-button size="small" type="primary"><el-icon style="margin-right:4px"><Plus /></el-icon>添加源</el-button>
          <template #dropdown><el-dropdown-menu>
            <el-dropdown-item command="file">配置文件(JSON / YAML / TOML)</el-dropdown-item>
            <el-dropdown-item command="db" :disabled="hasDb">内置数据库(安装时选定,仅可一次)</el-dropdown-item>
          </el-dropdown-menu></template>
        </el-dropdown>
      </span>
    </h3>
    <el-empty v-if="!items.length" description="链为空" :image-size="60"></el-empty>
    <template v-for="(s, i) in items" :key="s.type + ':' + (s.path || s.dsn)">
      <div class="chain-item" :class="{ off: !s.enabled }">
        <span class="chain-seq">{{ i + 1 }}</span>
        <el-tag size="small" :type="s.type === 'db' ? 'warning' : 'info'" effect="plain" style="flex-shrink:0">
          {{ s.type === 'db' ? 'DB' : 'FILE' }}
        </el-tag>
        <div class="chain-meta">
          <div class="chain-name">{{ nameOf(s) }}</div>
          <div class="chain-detail mono">{{ detailOf(s) }}</div>
        </div>
        <span class="chain-status" :class="statusCls(s)"><span class="sdot"></span>{{ statusText(s) }}</span>
        <el-switch :model-value="s.enabled" size="small" style="flex-shrink:0"
                   @change="$emit('toggle', i)"></el-switch>
        <span class="chain-ops">
          <el-button size="small" text :disabled="i === 0" @click="$emit('move', i, -1)"><el-icon><Top /></el-icon></el-button>
          <el-button size="small" text :disabled="i === items.length - 1" @click="$emit('move', i, 1)"><el-icon><Bottom /></el-icon></el-button>
          <el-popconfirm v-if="s.type !== 'db'" title="移除该源?" @confirm="$emit('remove', i)">
            <template #reference><el-button size="small" text type="danger"><el-icon><Delete /></el-icon></el-button></template>
          </el-popconfirm>
          <el-tooltip v-else content="内置数据库不可删除,可禁用" placement="top">
            <span><el-button size="small" text disabled><el-icon><Delete /></el-icon></el-button></span>
          </el-tooltip>
        </span>
      </div>
      <div v-if="i < items.length - 1" class="chain-fall">↓ miss → fall through</div>
    </template>

    <el-dialog v-model="dlgFile" title="添加配置文件源" width="440px" append-to-body>
      <el-form label-width="80px">
        <el-form-item label="文件路径">
          <el-input v-model="filePath" class="mono" placeholder="/etc/mayfly/rules.yaml"></el-input>
        </el-form-item>
      </el-form>
      <el-alert type="info" :closable="false" title="支持 JSON / YAML / TOML 与 users 文件,按扩展名识别"></el-alert>
      <template #footer>
        <el-button @click="dlgFile=false">取消</el-button>
        <el-button type="primary" @click="confirmFile">添加</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Plus, Top, Bottom, Delete } from '@element-plus/icons-vue';

defineProps({
  title: { type: String, required: true },
  note: { type: String, default: '' },
  items: { type: Array, default: () => [] },
  hasDb: { type: Boolean, default: false },
});
const emit = defineEmits(['move', 'remove', 'toggle', 'add']);

const dlgFile = ref(false);
const filePath = ref('');

function basename(p) {
  if (!p) return '(未命名)';
  const parts = String(p).split(/[\\/]/);
  return parts[parts.length - 1];
}

function nameOf(s) {
  if (s.type === 'db') return '内置数据库 (' + (s.backend || 'db') + ')';
  return basename(s.path);
}

function detailOf(s) {
  const stats = `allow ${s.allow ?? 0} · deny ${s.deny ?? 0} · miss ${s.miss ?? 0}`;
  if (s.type === 'db') return (s.dsn || '') + ' · ' + stats;
  return (s.path || '') + ' · ' + stats;
}

function statusCls(s) {
  if (!s.enabled) return 'off';
  return s.status === 'active' ? 'ok' : 'fail';
}

function statusText(s) {
  if (!s.enabled) return '已禁用';
  return s.status === 'active' ? '正常' : '异常';
}

function onAdd(kind) {
  if (kind === 'db') {
    emit('add', { type: 'db' });
  } else {
    filePath.value = '';
    dlgFile.value = true;
  }
}

function confirmFile() {
  if (!filePath.value.trim()) {
    ElMessage.warning('请输入文件路径');
    return;
  }
  emit('add', { type: 'file', path: filePath.value.trim() });
  dlgFile.value = false;
}
</script>
