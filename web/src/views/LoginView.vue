<template>
  <div class="login-wrap">
    <div class="login-card">
      <div class="login-logo"><span class="brand-dot">M</span><b>MAYFLY</b></div>
      <div class="login-sub">轻量级 MQTT Broker Console</div>
      <el-form @submit.prevent="doLogin">
        <el-form-item>
          <el-input v-model="form.user" size="large" placeholder="用户名" :prefix-icon="User"></el-input>
        </el-form-item>
        <el-form-item>
          <el-input v-model="form.pass" type="password" size="large" show-password placeholder="密码"
                    :prefix-icon="Lock" @keyup.enter="doLogin"></el-input>
        </el-form-item>
        <el-button type="primary" size="large" style="width:100%" :loading="loading" @click="doLogin">登 录</el-button>
      </el-form>
      <div class="login-tip">首次部署将自动进入安装向导 · 下方可切换主题</div>
      <div style="text-align:center; margin-top:16px">
        <button class="theme-btn" title="切换主题" @click="theme.toggle()">
          <el-icon v-if="theme.dark"><Sunny /></el-icon><el-icon v-else><Moon /></el-icon>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { ElMessage } from 'element-plus';
import { User, Lock, Sunny, Moon } from '@element-plus/icons-vue';
import { useAuthStore } from '../stores/auth.js';
import { useThemeStore } from '../stores/theme.js';

const route = useRoute();
const router = useRouter();
const auth = useAuthStore();
const theme = useThemeStore();

const form = reactive({ user: '', pass: '' });
const loading = ref(false);

async function doLogin() {
  if (!form.user || !form.pass) {
    ElMessage.warning('请输入用户名和密码');
    return;
  }
  loading.value = true;
  try {
    await auth.login(form.user, form.pass);
    const redirect = typeof route.query.redirect === 'string' ? route.query.redirect : '/';
    router.push(redirect);
  } catch (e) {
    ElMessage.error(e.message || '登录失败');
  } finally {
    loading.value = false;
  }
}
</script>
