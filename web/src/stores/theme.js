import { defineStore } from 'pinia';

const THEME_KEY = 'mf-theme';

function apply(dark) {
  document.documentElement.classList.toggle('dark', dark);
}

// 主题:默认深色,localStorage 持久化(键 mf-theme)。
export const useThemeStore = defineStore('theme', {
  state: () => ({ dark: localStorage.getItem(THEME_KEY) !== 'light' }),
  actions: {
    init() {
      apply(this.dark);
    },
    toggle() {
      this.dark = !this.dark;
      localStorage.setItem(THEME_KEY, this.dark ? 'dark' : 'light');
      apply(this.dark);
    },
  },
});
