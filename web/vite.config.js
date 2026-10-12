import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 1200,
    rollupOptions: {
      output: {
        // Vite 8 (rolldown) only accepts the function form.
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return undefined;
          }
          if (id.includes('echarts') || id.includes('zrender')) {
            return 'vendor-echarts';
          }
          if (id.includes('element-plus')) {
            return 'vendor-element';
          }
          if (id.includes('/vue/') || id.includes('vue-router') || id.includes('pinia')) {
            return 'vendor-vue';
          }
          return undefined;
        },
      },
    },
  },
  test: {
    environment: 'node',
    include: ['tests/**/*.test.js'],
  },
});
