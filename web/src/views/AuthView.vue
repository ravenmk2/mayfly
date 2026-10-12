<template>
  <div>
    <div class="chain-grid">
      <chain-panel title="认证链" note="密码错即拒并停止;未找到 → fall through"
        :items="authnChain" :has-db="authnHasDb"
        @move="(i,d) => moveSource(authnChain, i, d)" @remove="(i) => removeSource(authnChain, i)"
        @toggle="(i) => toggleSource(authnChain, i)" @add="(c) => addSource(authnChain, c)">
        <template #extra>
          <el-checkbox :model-value="anonymousAllow" size="small" style="margin-right:10px"
                       @change="toggleAnonymous">允许匿名</el-checkbox>
        </template>
      </chain-panel>
      <chain-panel title="授权链" note="源内按序首条命中;源间 fall through;全 miss → 默认拒绝"
        :items="authzChain" :has-db="authzHasDb"
        @move="(i,d) => moveSource(authzChain, i, d)" @remove="(i) => removeSource(authzChain, i)"
        @toggle="(i) => toggleSource(authzChain, i)" @add="(c) => addSource(authzChain, c)">
      </chain-panel>
    </div>

    <el-tabs v-model="tab" class="panel" style="padding-top:6px">
      <!-- ============ 认证用户 ============ -->
      <el-tab-pane label="认证用户" name="users">
        <div class="toolbar">
          <el-tag v-if="dbBackend" effect="plain" type="warning">内置数据库 · {{ dbBackend }}</el-tag>
          <el-tag v-else effect="plain" type="danger">未配置内置数据库源</el-tag>
          <span v-if="dbBackend" style="font-size:12.5px;color:var(--mf-dim)">认证链位置 #{{ dbAuthnIndex }}</span>
          <span class="spacer"></span>
          <el-button :loading="refreshing" @click="refreshCache">刷新缓存</el-button>
          <el-button type="primary" :icon="Plus" :disabled="!dbBackend" @click="openUserDlg()">新建用户</el-button>
        </div>
        <el-table :data="users" stripe v-loading="usersLoading">
          <el-table-column label="Username" min-width="140"><template #default="{ row }">
            <span class="mono">{{ row.username }}</span>
            <el-tag v-if="row.superuser" size="small" type="danger" effect="plain" style="margin-left:6px">superuser</el-tag>
          </template></el-table-column>
          <el-table-column label="密码" min-width="130"><template #default>
            <span style="color:var(--mf-dim)">bcrypt ••••••</span>
          </template></el-table-column>
          <el-table-column label="创建时间" width="170"><template #default="{ row }">{{ fmtTime(row.created_at) }}</template></el-table-column>
          <el-table-column label="操作" width="150"><template #default="{ row }">
            <el-button size="small" text type="primary" @click="openUserDlg(row)">改密码</el-button>
            <el-popconfirm title="删除该用户?" @confirm="removeUser(row)"><template #reference>
              <el-button size="small" text type="danger">删除</el-button></template></el-popconfirm>
          </template></el-table-column>
        </el-table>
        <div style="margin-top:14px; display:flex; justify-content:flex-end">
          <el-pagination layout="total, prev, pager, next" :total="usersTotal" :page-size="pageSize"
                         :current-page="usersPage" @current-change="loadUsers"></el-pagination>
        </div>
      </el-tab-pane>

      <!-- ============ ACL 规则 ============ -->
      <el-tab-pane label="ACL 规则" name="acl">
        <div class="toolbar">
          <el-alert type="info" :closable="false" style="flex:1; padding:4px 12px"
            title="按顺序首条命中;支持 {user}(username)、{clientid} 占位符;未命中默认拒绝"></el-alert>
          <el-button type="primary" :icon="Plus" :disabled="!dbBackend" @click="openAclDlg()">新建规则</el-button>
        </div>
        <el-table :data="acls" stripe v-loading="aclsLoading">
          <el-table-column prop="seq" label="#" width="60" align="center"></el-table-column>
          <el-table-column label="主体" min-width="130"><template #default="{ row }">
            <el-tag v-if="!row.username && !row.clientid" size="small" effect="plain">所有用户</el-tag>
            <span v-else class="mono">{{ row.username || row.clientid }}</span>
          </template></el-table-column>
          <el-table-column label="动作" width="90" align="center"><template #default="{ row }">
            <el-tag size="small" :type="row.action === 'allow' ? 'success' : 'danger'">{{ row.action }}</el-tag>
          </template></el-table-column>
          <el-table-column prop="permission" label="权限" width="100" align="center"></el-table-column>
          <el-table-column label="Topic Filter" min-width="220"><template #default="{ row }">
            <span class="mono">{{ row.topic }}</span>
          </template></el-table-column>
          <el-table-column label="操作" width="160"><template #default="{ row, $index }">
            <el-button size="small" text :disabled="$index === 0" @click="moveAcl(row, -1)">上移</el-button>
            <el-button size="small" text type="primary" @click="openAclDlg(row)">编辑</el-button>
            <el-popconfirm title="删除该规则?" @confirm="removeAcl(row)"><template #reference>
              <el-button size="small" text type="danger">删除</el-button></template></el-popconfirm>
          </template></el-table-column>
        </el-table>
        <div style="margin-top:14px; display:flex; justify-content:flex-end">
          <el-pagination layout="total, prev, pager, next" :total="aclsTotal" :page-size="pageSize"
                         :current-page="aclsPage" @current-change="loadAcls"></el-pagination>
        </div>
      </el-tab-pane>

      <!-- ============ 规则演练 ============ -->
      <el-tab-pane label="规则演练" name="playground">
        <el-alert type="info" :closable="false" style="margin-bottom:14px"
          title="file 源规则与匹配表达式实时演练:修改右侧输入,查看每条规则的逐段比对与链式首条命中过程"></el-alert>
        <div>
          <div style="margin-bottom:16px">
            <h4 style="margin-bottom:10px">file 源示例(与下方规则一致)</h4>
            <el-tabs v-model="pgSrcTab">
              <el-tab-pane label="acl.yaml" name="yaml"><div class="payload-view">{{ playgroundSources.yaml }}</div></el-tab-pane>
              <el-tab-pane label="acl.json" name="json"><div class="payload-view">{{ playgroundSources.json }}</div></el-tab-pane>
              <el-tab-pane label="acl.toml" name="toml"><div class="payload-view">{{ playgroundSources.toml }}</div></el-tab-pane>
              <el-tab-pane label="users.yaml" name="users"><div class="payload-view">{{ playgroundSources.users }}</div></el-tab-pane>
            </el-tabs>
          </div>
          <div>
            <h4 style="margin-bottom:10px">匹配演练</h4>
            <el-form inline style="margin-bottom:6px">
              <el-form-item label="Username"><el-input v-model="pgForm.username" style="width:120px" class="mono"></el-input></el-form-item>
              <el-form-item label="Client ID"><el-input v-model="pgForm.clientid" style="width:140px" class="mono"></el-input></el-form-item>
              <el-form-item label="操作">
                <el-select v-model="pgForm.op" style="width:110px">
                  <el-option value="publish" label="publish"></el-option>
                  <el-option value="subscribe" label="subscribe"></el-option>
                </el-select>
              </el-form-item>
              <el-form-item label="Topic"><el-input v-model="pgForm.topic" style="width:220px" class="mono"></el-input></el-form-item>
            </el-form>
            <div style="margin-bottom:12px">
              <el-tag v-for="t in playgroundExamples" :key="t.topic + t.op" size="small" effect="plain"
                      style="margin-right:6px; cursor:pointer" @click="Object.assign(pgForm, t)">{{ t.topic }}({{ t.op }})</el-tag>
            </div>
            <el-alert :type="pgEval.decision === 'allow' ? 'success' : 'error'" :closable="false" style="margin-bottom:12px"
              :title="pgEval.hit ? `命中规则 #${pgEval.hit.seq} → ${pgEval.hit.action.toUpperCase()}(链停止)` : '无规则命中 → 默认拒绝 DENY'"></el-alert>
            <el-table :data="pgEval.rows" size="small" :row-class-name="({row}) => row.hit ? 'hit-row' : ''">
              <el-table-column prop="seq" label="#" width="46" align="center"></el-table-column>
              <el-table-column label="主体" width="110"><template #default="{ row }">
                <el-tag v-if="row.rule.scope === 'all'" size="small" effect="plain">所有用户</el-tag>
                <span v-else class="mono">{{ row.rule.value }}<span style="color:var(--mf-dim)">({{ row.rule.scope === 'user' ? 'u' : 'c' }})</span></span>
              </template></el-table-column>
              <el-table-column label="动作/权限" width="120"><template #default="{ row }">
                <el-tag size="small" :type="row.rule.action === 'allow' ? 'success' : 'danger'">{{ row.rule.action }}</el-tag>
                <span style="color:var(--mf-dim); font-size:12px"> {{ row.rule.perm }}</span>
              </template></el-table-column>
              <el-table-column label="逐段比对" min-width="240"><template #default="{ row }">
                <template v-if="row.segs"><span v-for="(s,i) in row.segs" :key="i" class="seg" :class="s.ok ? 'seg-ok' : 'seg-no'">{{ s.label }}</span></template>
                <span v-else style="color:var(--mf-dim); font-size:12px">未评估(前面条件已跳过)</span>
              </template></el-table-column>
              <el-table-column label="结论" width="150"><template #default="{ row }">
                <span v-if="row.hit" style="font-weight:600" :style="{color: row.rule.action === 'allow' ? '#0e9f6e' : '#e45656'}">命中 · 链停止</span>
                <span v-else style="color:var(--mf-dim); font-size:12px">{{ row.skip }}</span>
              </template></el-table-column>
            </el-table>
          </div>
        </div>
      </el-tab-pane>
    </el-tabs>

    <!-- 新建/编辑用户 -->
    <el-dialog v-model="dlgUser" :title="userForm.editing ? '修改用户' : '新建用户'" width="420px">
      <el-form label-width="80px">
        <el-form-item label="Username">
          <el-input v-model="userForm.username" :disabled="!!userForm.editing" placeholder="device-001"></el-input>
        </el-form-item>
        <el-form-item :label="userForm.editing ? '新密码' : '密码'">
          <el-input v-model="userForm.password" type="password" show-password placeholder="将以 bcrypt 存储"></el-input>
        </el-form-item>
        <el-form-item label="Superuser"><el-switch v-model="userForm.superuser"></el-switch></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlgUser=false">取消</el-button>
        <el-button type="primary" @click="saveUser">{{ userForm.editing ? '保存' : '创建' }}</el-button>
      </template>
    </el-dialog>

    <!-- 新建/编辑 ACL 规则 -->
    <el-dialog v-model="dlgAcl" :title="aclForm.editing ? '编辑 ACL 规则' : '新建 ACL 规则'" width="460px">
      <el-form label-width="90px">
        <el-form-item label="主体类型">
          <el-radio-group v-model="aclForm.subjectType">
            <el-radio-button value="all">所有用户</el-radio-button>
            <el-radio-button value="user">指定用户</el-radio-button>
            <el-radio-button value="cid">指定 ClientID</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="aclForm.subjectType !== 'all'" :label="aclForm.subjectType === 'user' ? 'Username' : 'Client ID'">
          <el-input v-model="aclForm.subject" class="mono"></el-input>
        </el-form-item>
        <el-form-item label="动作">
          <el-radio-group v-model="aclForm.action">
            <el-radio-button value="allow">allow</el-radio-button>
            <el-radio-button value="deny">deny</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="权限">
          <el-select v-model="aclForm.permission" style="width:100%">
            <el-option value="publish" label="publish(发布)"></el-option>
            <el-option value="subscribe" label="subscribe(订阅)"></el-option>
            <el-option value="pubsub" label="pubsub(两者)"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Topic"><el-input v-model="aclForm.topic" class="mono" placeholder="sensors/{user}/#"></el-input></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlgAcl=false">取消</el-button>
        <el-button type="primary" @click="saveAcl">{{ aclForm.editing ? '保存' : '添加' }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Plus } from '@element-plus/icons-vue';
import ChainPanel from '../components/ChainPanel.vue';
import {
  getChains, putChains, refreshAuth,
  listUsers, createUser, updateUser, deleteUser,
  listAcls, createAcl, updateAcl, deleteAcl,
} from '../api/index.js';
import { fmtTime } from '../utils/format.js';
import { evalRules, playgroundRules, playgroundExamples, playgroundSources } from '../utils/aclPlayground.js';

const pageSize = 20;

/* ---------- 认证/授权链 ---------- */
const anonymousAllow = ref(false);
const authnChain = ref([]);
const authzChain = ref([]);

const hasDb = (chain) => chain.some((s) => s.type === 'db');
const authnHasDb = computed(() => hasDb(authnChain.value));
const authzHasDb = computed(() => hasDb(authzChain.value));

const dbBackend = computed(() => {
  const s = [...authnChain.value, ...authzChain.value].find((x) => x.type === 'db');
  return s ? (s.backend || 'db') : '';
});
const dbAuthnIndex = computed(() => authnChain.value.findIndex((s) => s.type === 'db') + 1);

async function loadChains() {
  try {
    const res = await getChains();
    anonymousAllow.value = !!res.anonymous_allow;
    authnChain.value = res.authentication || [];
    authzChain.value = res.authorization || [];
  } catch (e) {
    ElMessage.error(e.message);
  }
}

// 链上所有编辑(调序/启停/增删)立即整体 PUT,运行时生效,重启回到 mayfly.yaml。
async function applyChains(okText) {
  const toCfg = (items) => items.map((s) => ({ type: s.type, enabled: !!s.enabled, path: s.path || '', dsn: s.dsn || '' }));
  try {
    const res = await putChains({
      anonymous_allow: anonymousAllow.value,
      authentication: toCfg(authnChain.value),
      authorization: toCfg(authzChain.value),
    });
    anonymousAllow.value = !!res.anonymous_allow;
    authnChain.value = res.authentication || [];
    authzChain.value = res.authorization || [];
    if (okText) ElMessage.success(okText);
  } catch (e) {
    ElMessage.error(e.message);
    loadChains();
  }
}

function moveSource(chain, i, d) {
  const j = i + d;
  [chain.value[i], chain.value[j]] = [chain.value[j], chain.value[i]];
  applyChains('已调整顺序');
}
function toggleSource(chain, i) {
  chain.value[i].enabled = !chain.value[i].enabled;
  applyChains(chain.value[i].enabled ? '已启用该源' : '已禁用该源');
}
function removeSource(chain, i) {
  chain.value.splice(i, 1);
  applyChains('已移除源');
}
function addSource(chain, c) {
  if (c.type === 'db' && hasDb(chain.value)) return;
  chain.value.push({ type: c.type, enabled: true, path: c.path || '', dsn: c.dsn || '', status: 'disabled' });
  applyChains('已添加源');
}
function toggleAnonymous(v) {
  anonymousAllow.value = !!v;
  applyChains(v ? '已允许匿名连接' : '已禁止匿名连接');
}

const refreshing = ref(false);
async function refreshCache() {
  refreshing.value = true;
  try {
    await refreshAuth();
    ElMessage.success('认证/授权缓存已刷新');
    loadChains();
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    refreshing.value = false;
  }
}

/* ---------- 认证用户 ---------- */
const tab = ref('users');
const users = ref([]);
const usersTotal = ref(0);
const usersPage = ref(1);
const usersLoading = ref(false);
const dlgUser = ref(false);
const userForm = reactive({ editing: '', username: '', password: '', superuser: false });

async function loadUsers(p) {
  usersPage.value = p || usersPage.value;
  usersLoading.value = true;
  try {
    const res = await listUsers({ page: usersPage.value, limit: pageSize });
    users.value = res.items || [];
    usersTotal.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    usersLoading.value = false;
  }
}

function openUserDlg(row) {
  userForm.editing = row ? row.username : '';
  userForm.username = row ? row.username : '';
  userForm.password = '';
  userForm.superuser = row ? row.superuser : false;
  dlgUser.value = true;
}

async function saveUser() {
  try {
    if (userForm.editing) {
      await updateUser(userForm.editing, {
        password: userForm.password || undefined,
        superuser: userForm.superuser,
      });
      ElMessage.success('用户已更新');
    } else {
      if (!userForm.username || !userForm.password) {
        ElMessage.warning('请输入用户名和密码');
        return;
      }
      await createUser(userForm.username, userForm.password, userForm.superuser);
      ElMessage.success('用户已创建');
    }
    dlgUser.value = false;
    loadUsers();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

async function removeUser(row) {
  try {
    await deleteUser(row.username);
    ElMessage.success(`已删除 ${row.username}`);
    loadUsers();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

/* ---------- ACL 规则 ---------- */
const acls = ref([]);
const aclsTotal = ref(0);
const aclsPage = ref(1);
const aclsLoading = ref(false);
const dlgAcl = ref(false);
const aclForm = reactive({ editing: 0, subjectType: 'all', subject: '', action: 'allow', permission: 'pubsub', topic: '', seq: 0 });

async function loadAcls(p) {
  aclsPage.value = p || aclsPage.value;
  aclsLoading.value = true;
  try {
    const res = await listAcls({ page: aclsPage.value, limit: pageSize });
    acls.value = (res.items || []).slice().sort((a, b) => a.seq - b.seq);
    aclsTotal.value = res.total || 0;
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    aclsLoading.value = false;
  }
}

function openAclDlg(row) {
  aclForm.editing = row ? row.id : 0;
  aclForm.seq = row ? row.seq : 0;
  aclForm.subjectType = row ? (row.username ? 'user' : row.clientid ? 'cid' : 'all') : 'all';
  aclForm.subject = row ? (row.username || row.clientid || '') : '';
  aclForm.action = row ? row.action : 'allow';
  aclForm.permission = row ? row.permission : 'pubsub';
  aclForm.topic = row ? row.topic : '';
  dlgAcl.value = true;
}

function aclBody() {
  return {
    username: aclForm.subjectType === 'user' ? aclForm.subject : '',
    clientid: aclForm.subjectType === 'cid' ? aclForm.subject : '',
    action: aclForm.action,
    permission: aclForm.permission,
    topic: aclForm.topic,
    seq: aclForm.seq,
  };
}

async function saveAcl() {
  if (aclForm.subjectType !== 'all' && !aclForm.subject) {
    ElMessage.warning('请输入主体');
    return;
  }
  if (!aclForm.topic) {
    ElMessage.warning('请输入 Topic Filter');
    return;
  }
  try {
    if (aclForm.editing) {
      await updateAcl(aclForm.editing, aclBody());
      ElMessage.success('规则已更新');
    } else {
      await createAcl(aclBody());
      ElMessage.success('规则已添加');
    }
    dlgAcl.value = false;
    loadAcls();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

async function moveAcl(row, d) {
  const idx = acls.value.findIndex((r) => r.id === row.id);
  const other = acls.value[idx + d];
  if (!other) return;
  try {
    await updateAcl(row.id, { ...row, seq: other.seq });
    await updateAcl(other.id, { ...other, seq: row.seq });
    loadAcls();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

async function removeAcl(row) {
  try {
    await deleteAcl(row.id);
    ElMessage.success('规则已删除');
    loadAcls();
  } catch (e) {
    ElMessage.error(e.message);
  }
}

/* ---------- 规则演练 ---------- */
const pgSrcTab = ref('yaml');
const pgForm = reactive({ username: 'sensor1', clientid: 'sensor-hub-01', op: 'publish', topic: 'up/sensor1' });
const pgEval = computed(() => evalRules(playgroundRules, pgForm));

onMounted(() => {
  loadChains();
  loadUsers(1);
  loadAcls(1);
});
</script>
