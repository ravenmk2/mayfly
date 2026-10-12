import { createRouter, createWebHistory } from 'vue-router';
import { useAuthStore } from '../stores/auth.js';
import { ElMessage } from 'element-plus';

const routes = [
  { path: '/login', name: 'login', component: () => import('../views/LoginView.vue'), meta: { public: true } },
  { path: '/setup', name: 'setup', component: () => import('../views/SetupWizard.vue'), meta: { public: true } },
  {
    path: '/',
    component: () => import('../layout/ConsoleLayout.vue'),
    children: [
      { path: '', redirect: '/overview' },
      { path: 'overview', name: 'overview', component: () => import('../views/OverviewView.vue'), meta: { title: '概览' } },
      { path: 'connections', name: 'connections', component: () => import('../views/ConnectionsView.vue'), meta: { title: '连接管理' } },
      { path: 'sessions', name: 'sessions', component: () => import('../views/SessionsView.vue'), meta: { title: '会话管理' } },
      { path: 'subscriptions', name: 'subscriptions', component: () => import('../views/SubscriptionsView.vue'), meta: { title: '订阅管理' } },
      { path: 'retained', name: 'retained', component: () => import('../views/RetainedView.vue'), meta: { title: 'Retained 消息' } },
      { path: 'auth', name: 'auth', component: () => import('../views/AuthView.vue'), meta: { title: '认证与授权' } },
      { path: 'publish', name: 'publish', component: () => import('../views/PublishView.vue'), meta: { title: '测试发布' } },
      { path: 'apikeys', name: 'apikeys', component: () => import('../views/ApiKeysView.vue'), meta: { title: 'API Key 管理' } },
    ],
  },
  { path: '/:pathMatch(.*)*', redirect: '/' },
];

export const router = createRouter({
  history: createWebHistory(),
  routes,
});

// 路由守卫:未初始化强制进入安装向导;已初始化访问 /setup 回登录页;
// 其余页面要求已登录。
router.beforeEach(async (to) => {
  const auth = useAuthStore();
  try {
    await auth.checkSetup();
  } catch {
    // 状态探测失败不阻塞导航(登录页会再次暴露网络错误)
  }

  if (auth.initialized === false && to.name !== 'setup') {
    return { name: 'setup' };
  }
  if (auth.initialized === true && to.name === 'setup') {
    return { name: 'login' };
  }
  if (to.meta.public) {
    if (to.name === 'login' && auth.loggedIn) return { path: '/' };
    return true;
  }
  if (!auth.loggedIn) {
    return { name: 'login', query: to.fullPath !== '/' ? { redirect: to.fullPath } : {} };
  }
  return true;
});

// API 层 401:清理会话并回登录页。
export function bindUnauthorized(auth) {
  return () => {
    const wasLoggedIn = auth.loggedIn;
    auth.logout();
    if (router.currentRoute.value.name !== 'login') {
      if (wasLoggedIn) ElMessage.warning('登录已过期,请重新登录');
      router.push({ name: 'login' });
    }
  };
}
