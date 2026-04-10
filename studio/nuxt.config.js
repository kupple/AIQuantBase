export default defineNuxtConfig({
  ssr: true,
  devtools: { enabled: true },
  modules: ["@element-plus/nuxt"],
  elementPlus: {
    defaultLocale: "zh-cn",
    installMethods: ["ElMessage"],
    globalConfig: {
      size: "default",
    },
  },
  css: [
    "@vue-flow/core/dist/style.css",
    "@vue-flow/core/dist/theme-default.css",
    "@vue-flow/controls/dist/style.css",
    "@vue-flow/minimap/dist/style.css",
    "~/assets/main.css",
  ],
  app: {
    head: {
      title: "AIQuantBase Workbench",
      meta: [
        { name: "viewport", content: "width=device-width, initial-scale=1" },
      ],
    },
  },
  runtimeConfig: {
    public: {
      backendBase: process.env.NUXT_PUBLIC_BACKEND_BASE || "http://127.0.0.1:8011",
    },
  },
  future: {
    compatibilityVersion: 4,
  },
  compatibilityDate: "2026-04-10",
});
