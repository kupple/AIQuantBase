import { defineEventHandler } from 'h3'
import { proxyToBackend, resolveBackendBase } from '../../utils/backendProxy.js'

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const backendBase = resolveBackendBase(config)
  const name = Array.isArray(event.context.params?.name)
    ? event.context.params.name.join('/')
    : String(event.context.params?.name || '')
  return proxyToBackend(
    event,
    `${backendBase}/api/sync-configs/${encodeURIComponent(name)}`,
    'Sync config proxy failed',
  )
})
