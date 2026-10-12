<template>
  <div class="layout">
    <aside class="aside">
      <div class="brand">
        <span class="brand-dot">M</span><b>MAYFLY</b>
        <span class="ver" v-if="version">{{ version }}</span>
      </div>
      <el-menu :default-active="$route.path" router>
        <el-menu-item index="/overview"><el-icon><DataLine /></el-icon>概览</el-menu-item>
        <el-menu-item index="/connections"><el-icon><Connection /></el-icon>连接</el-menu-item>
        <el-menu-item index="/sessions"><el-icon><Tickets /></el-icon>会话</el-menu-item>
        <el-menu-item index="/subscriptions"><el-icon><Share /></el-icon>订阅</el-menu-item>
        <el-menu-item index="/retained"><el-icon><Collection /></el-icon>Retained</el-menu-item>
        <el-menu-item index="/auth"><el-icon><User /></el-icon>认证与授权</el-menu-item>
        <el-menu-item index="/publish"><el-icon><Promotion /></el-icon>测试发布</el-menu-item>
        <el-menu-item index="/apikeys"><el-icon><Key /></el-icon>API Key</el-menu-item>
      </el-menu>
      <div class="foot">
        <div class="foot-status"><span class="sdot"></span>{{ statusText }}</div>
        <div>MQTT 3.1.1 / 5.0 · 单节点</div>
        <div>{{ footLine }}</div>
      </div>
    </aside>

    <div class="main-col">
      <header class="topbar">
        <span class="page-title">{{ $route.meta.title || '' }}</span>
        <span class="spacer"></span>
        <el-dropdown @command="onUserCmd">
          <span style="cursor:pointer; display:flex; align-items:center; gap:8px; color:var(--mf-text);">
            <el-avatar :size="30" style="background:#d97706; font-size:13px; border-radius:4px;">
              {{ avatarLetter }}
            </el-avatar>
            {{ auth.username }}
            <el-icon><ArrowDown /></el-icon>
          </span>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="theme">
                <el-icon style="margin-right:6px"><Sunny v-if="theme.dark" /><Moon v-else /></el-icon>
                {{ theme.dark ? '切换为浅色' : '切换为深色' }}
              </el-dropdown-item>
              <el-dropdown-item command="logout" divided>退出登录</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </header>

      <main class="content">
        <router-view />
      </main>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted } from 'vue';
import { useRouter } from 'vue-router';
import { useAuthStore } from '../stores/auth.js';
import { useThemeStore } from '../stores/theme.js';
import { useStatusStore } from '../stores/status.js';
import { fmtUptime } from '../utils/format.js';

const router = useRouter();
const auth = useAuthStore();
const theme = useThemeStore();
const status = useStatusStore();

const version = computed(() => (status.latest && status.latest.version) || '');
const statusText = computed(() => (status.online ? '运行中' : '连接中断'));
const footLine = computed(() => {
  const v = version.value ? 'v' + version.value.replace(/^v/, '') : '—';
  const up = status.latest ? fmtUptime(status.latest.uptime_seconds) : '—';
  return `${v} · 已运行 ${up}`;
});
const avatarLetter = computed(() => (auth.username || '?').slice(0, 1).toUpperCase());

function onUserCmd(cmd) {
  if (cmd === 'theme') {
    theme.toggle();
  } else if (cmd === 'logout') {
    auth.logout();
    status.stop();
    router.push({ name: 'login' });
  }
}

onMounted(() => status.start());
onUnmounted(() => status.stop());
</script>
