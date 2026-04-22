import { defineEventHandler, createError } from 'h3'

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options)
  const contentType = response.headers.get('content-type') || ''
  const payload = contentType.includes('application/json') ? await response.json() : await response.text()
  if (!response.ok) {
    const message = typeof payload === 'string'
      ? payload
      : payload?.detail || payload?.message || payload?.error || `request failed: ${response.status}`
    throw new Error(message)
  }
  return payload
}

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const syncBase = String(config.syncBackendBase || 'http://172.16.0.68:18080').replace(/\/$/, '')
  try {
    return await fetchJson(`${syncBase}/api/sync-table-status`)
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Bad Gateway',
      message: error instanceof Error ? error.message : 'sync table status failed',
    })
  }
})
