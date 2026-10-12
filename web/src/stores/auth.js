import { defineStore } from 'pinia';
import {
  login as apiLogin,
  setupStatus,
  storeSession,
  clearSession,
  getStoredToken,
  getStoredUser,
} from '../api/index.js';

// 控制台会话与初始化状态。token 持久化在 localStorage(JWT,12h)。
export const useAuthStore = defineStore('auth', {
  state: () => ({
    token: getStoredToken(),
    username: getStoredUser(),
    // null = 尚未探测; true/false = setup/status 结果
    initialized: null,
  }),
  getters: {
    loggedIn: (s) => !!s.token,
  },
  actions: {
    async checkSetup(force = false) {
      if (this.initialized !== null && !force) return this.initialized;
      const res = await setupStatus();
      this.initialized = !!res.initialized;
      return this.initialized;
    },
    markInitialized() {
      this.initialized = true;
    },
    async login(username, password) {
      const res = await apiLogin(username, password);
      this.token = res.token;
      this.username = username;
      storeSession(res.token, username);
    },
    logout() {
      this.token = '';
      this.username = '';
      clearSession();
    },
  },
});
