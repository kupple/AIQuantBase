export default defineNuxtConfig({
  ssr: true,
  devtools: { enabled: true },
  css: [
    'element-plus/dist/index.css',
    '@vue-flow/core/dist/style.css',
    '@vue-flow/core/dist/theme-default.css',
    '@vue-flow/controls/dist/style.css',
    '@vue-flow/minimap/dist/style.css',
    '~/assets/main.css',
  ],
  app: {
    head: {
      title: 'AIQuantBase Workbench',
      meta: [
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
        { name: 'apple-mobile-web-app-capable', content: 'yes' },
      ],
    },
  },
  runtimeConfig: {
    backendBase: process.env.NUXT_BACKEND_BASE || process.env.NUXT_PUBLIC_BACKEND_BASE || 'http://127.0.0.1:8000',
    syncBackendBase: process.env.NUXT_SYNC_BACKEND_BASE || process.env.NUXT_PUBLIC_SYNC_BACKEND_BASE || 'http://127.0.0.1:18080',
    public: {
      appName: 'AIQuantBase Workbench',
    },
  },
  future: {
    compatibilityVersion: 4,
  },
  compatibilityDate: '2026-04-10',
})
