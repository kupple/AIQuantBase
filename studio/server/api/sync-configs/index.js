import {
  defineEventHandler,
} from 'h3'
import { proxyToBackend, resolveBackendBase } from '../../utils/backendProxy.js'

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const backendBase = resolveBackendBase(config)
  return proxyToBackend(event, `${backendBase}/api/sync-configs`, 'Sync config proxy failed')
})
