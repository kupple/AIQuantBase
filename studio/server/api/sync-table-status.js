import { createError, defineEventHandler } from 'h3'

function buildCandidateBases(config) {
  const backendBase = String(config.backendBase || '').trim()
  const defaults = [
    backendBase,
    'http://127.0.0.1:8011',
    'http://127.0.0.1:8000',
    'http://172.16.0.68:18080',
  ]
  return [...new Set(defaults.filter(Boolean).map((item) => item.replace(/\/$/, '')))]
}

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
  const errors = []
  for (const base of buildCandidateBases(config)) {
    try {
      return await fetchJson(`${base}/api/sync-table-status`)
    } catch (error) {
      errors.push(`${base}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }
  throw createError({
    statusCode: 502,
    statusMessage: 'Bad Gateway',
    message: errors.join(' | ') || 'sync table status failed',
  })
})
