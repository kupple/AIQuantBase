import ElementPlus from 'element-plus'
import zhCn from 'element-plus/es/locale/lang/zh-cn'
import { ID_INJECTION_KEY } from 'element-plus/es/hooks/use-id/index.mjs'
import { ZINDEX_INJECTION_KEY } from 'element-plus/es/hooks/use-z-index/index.mjs'

export default defineNuxtPlugin((nuxtApp) => {
  nuxtApp.vueApp.provide(ID_INJECTION_KEY, {
    prefix: 1024,
    current: 0,
  })
  nuxtApp.vueApp.provide(ZINDEX_INJECTION_KEY, {
    current: 0,
  })

  nuxtApp.vueApp.use(ElementPlus, {
    locale: zhCn,
    size: 'default',
    zIndex: 3000,
  })
})
