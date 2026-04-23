import {
  defineEventHandler,
} from 'h3'
import { buildQueryString, proxyToBackend, resolveBackendBase } from '../../utils/backendProxy.js'

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const backendBase = resolveBackendBase(config)
  const path = Array.isArray(event.context.params?.path)
    ? event.context.params.path.join('/')
    : String(event.context.params?.path || '')
  const target = `${backendBase}/api/sync/${path}${buildQueryString(event)}`
  return proxyToBackend(event, target, 'Sync backend proxy failed')
})
