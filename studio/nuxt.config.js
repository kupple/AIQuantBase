import fs from 'node:fs'
import path from 'node:path'

const configJsonPath = path.resolve(process.cwd(), 'config.json')
let localConfig = {}
if (fs.existsSync(configJsonPath)) {
  try {
    localConfig = JSON.parse(fs.readFileSync(configJsonPath, 'utf8'))
  } catch {
    localConfig = {}
  }
}

const resolvedBackendBase = process.env.NUXT_BACKEND_BASE || process.env.NUXT_PUBLIC_BACKEND_BASE || localConfig.backendBase || 'http://127.0.0.1:8000'

export default defineNuxtConfig({
  ssr: true,
  devtools: { enabled: true },
  css: [
    'element-plus/dist/index.css',
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
    backendBase: resolvedBackendBase,
    public: {
      appName: 'AIQuantBase Workbench',
    },
  },
  future: {
    compatibilityVersion: 4,
  },
  routeRules: {
    '/capabilities/provider': { redirect: '/capabilities' },
  },
  compatibilityDate: '2026-04-10',
})
