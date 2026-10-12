<template>
  <div class="login-wrap">
    <div class="login-card wide">
      <div class="login-logo"><span class="brand-dot">M</span><b>MAYFLY</b></div>
      <div class="login-sub">首次运行安装向导 · 所有数据保存在 ./data(相对工作目录)</div>
      <el-steps :active="wiz.step" align-center finish-status="success" style="margin-bottom:24px">
        <el-step title="主数据库"></el-step>
        <el-step title="管理员密码"></el-step>
        <el-step title="主密钥"></el-step>
      </el-steps>

      <div v-show="wiz.step === 0">
        <div class="db-cards">
          <div class="db-card" :class="{sel: wiz.db==='sqlite'}" @click="wiz.db='sqlite'">
            <b>SQLite(内置)</b>
            <p>零配置,数据存 ./data/mayfly.db。适合单节点轻量部署,推荐默认。</p>
          </div>
          <div class="db-card" :class="{sel: wiz.db==='mysql'}" @click="wiz.db='mysql'">
            <b>MySQL(外部)</b>
            <p>连接已有 MySQL 实例。适合认证/授权数据需要集中管理或多实例共享的场景。</p>
          </div>
        </div>
        <template v-if="wiz.db==='mysql'">
          <el-form label-width="80px" style="margin-top:6px">
            <el-form-item label="主机"><el-input v-model="wiz.dsn.host" class="mono"></el-input></el-form-item>
            <el-form-item label="端口"><el-input-number v-model="wiz.dsn.port" :min="1" :max="65535"></el-input-number></el-form-item>
            <el-form-item label="账号"><el-input v-model="wiz.dsn.user" class="mono"></el-input></el-form-item>
            <el-form-item label="密码"><el-input v-model="wiz.dsn.pass" type="password" show-password></el-input></el-form-item>
            <el-form-item label="数据库"><el-input v-model="wiz.dsn.name" class="mono"></el-input></el-form-item>
          </el-form>
          <el-button :loading="wiz.testing" @click="testDb">测试连接</el-button>
          <el-tag v-if="wiz.testOk === true" type="success" style="margin-left:8px">连接成功</el-tag>
          <el-tag v-else-if="wiz.testOk === false" type="danger" style="margin-left:8px">连接失败</el-tag>
        </template>
        <el-alert v-else type="info" :closable="false"
          title="将创建 ./data/mayfly.yaml(主配置)与 ./data/mayfly.db(SQLite)"></el-alert>
      </div>

      <div v-show="wiz.step === 1">
        <el-form label-width="90px">
          <el-form-item label="管理员账号"><el-input model-value="admin" disabled></el-input></el-form-item>
          <el-form-item label="密码"><el-input v-model="wiz.pass1" type="password" show-password placeholder="Console 登录密码(至少 8 位)"></el-input></el-form-item>
          <el-form-item label="确认密码"><el-input v-model="wiz.pass2" type="password" show-password></el-input></el-form-item>
        </el-form>
        <el-alert type="info" :closable="false" title="此为 Console 管理员,与 MQTT 设备用户完全隔离"></el-alert>
      </div>

      <div v-show="wiz.step === 2">
        <el-alert type="warning" :closable="false" style="margin-bottom:12px"
          title="主密钥用于 Console 会话(JWT)签名与配置中敏感项(如 MySQL 密码)的加密存储。初始化时将自动生成 32 字节随机密钥并写入 ./data/master.key(权限 0600),请妥善备份,丢失将无法解密配置中的敏感项。"></el-alert>
        <div class="payload-view">主密钥由服务端自动生成,不落页面、不经网络传输;
完成初始化后请备份 ./data/master.key。</div>
      </div>

      <div style="display:flex; justify-content:space-between; margin-top:22px; align-items:center">
        <el-button :disabled="wiz.step===0" @click="wiz.step--">上一步</el-button>
        <el-link type="info" :underline="false" style="font-size:12px" @click="goLogin">已有安装?返回登录</el-link>
        <el-button v-if="wiz.step < 2" type="primary" @click="wizNext">下一步</el-button>
        <el-button v-else type="primary" :loading="wiz.finishing" @click="wizFinish">完成并进入控制台</el-button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { reactive } from 'vue';
import { useRouter } from 'vue-router';
import { ElMessage } from 'element-plus';
import { setup, setupTestDb } from '../api/index.js';
import { useAuthStore } from '../stores/auth.js';

const router = useRouter();
const auth = useAuthStore();

const wiz = reactive({
  step: 0,
  db: 'sqlite',
  dsn: { host: '127.0.0.1', port: 3306, user: 'mayfly', pass: '', name: 'mayfly' },
  pass1: '',
  pass2: '',
  testing: false,
  testOk: null,
  finishing: false,
});

function mysqlDsn() {
  const d = wiz.dsn;
  return `${d.user}:${d.pass}@tcp(${d.host}:${d.port})/${d.name}`;
}

async function testDb() {
  wiz.testing = true;
  wiz.testOk = null;
  try {
    await setupTestDb(mysqlDsn());
    wiz.testOk = true;
  } catch (e) {
    wiz.testOk = false;
    ElMessage.error(e.message || '连接失败');
  } finally {
    wiz.testing = false;
  }
}

function wizNext() {
  if (wiz.step === 0 && wiz.db === 'mysql' && wiz.testOk !== true) {
    ElMessage.warning('请先测试连接');
    return;
  }
  if (wiz.step === 1) {
    if (wiz.pass1.length < 8) {
      ElMessage.warning('密码至少 8 位');
      return;
    }
    if (wiz.pass1 !== wiz.pass2) {
      ElMessage.warning('两次密码输入不一致');
      return;
    }
  }
  wiz.step++;
}

async function wizFinish() {
  wiz.finishing = true;
  try {
    const dsn = wiz.db === 'mysql' ? mysqlDsn() : '';
    await setup(wiz.db, dsn, wiz.pass1);
    auth.markInitialized();
    ElMessage.success('初始化完成:配置已写入 ./data,MQTT 监听器需重启进程后启动');
    router.push({ name: 'login' });
  } catch (e) {
    if (e.status === 410) {
      ElMessage.warning('mayfly 已完成初始化,请直接登录');
      auth.markInitialized();
      router.push({ name: 'login' });
    } else {
      ElMessage.error(e.message || '初始化失败');
    }
  } finally {
    wiz.finishing = false;
  }
}

function goLogin() {
  router.push({ name: 'login' });
}
</script>
