import { createApp } from 'vue';
import { createPinia } from 'pinia';
import ElementPlus from 'element-plus';
import zhCn from 'element-plus/es/locale/lang/zh-cn';
import 'element-plus/dist/index.css';
import 'element-plus/theme-chalk/dark/css-vars.css';
import * as ElementPlusIconsVue from '@element-plus/icons-vue';

import App from './App.vue';
import { router, bindUnauthorized } from './router/index.js';
import { setUnauthorizedHandler } from './api/index.js';
import { useThemeStore } from './stores/theme.js';
import { useAuthStore } from './stores/auth.js';
import './styles/index.css';

const app = createApp(App);
const pinia = createPinia();
app.use(pinia);

const theme = useThemeStore();
theme.init();
const auth = useAuthStore();
setUnauthorizedHandler(bindUnauthorized(auth));

app.use(router);
app.use(ElementPlus, { locale: zhCn });
for (const [name, comp] of Object.entries(ElementPlusIconsVue)) {
  app.component(name, comp);
}
app.mount('#app');
