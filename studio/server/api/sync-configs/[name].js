import { createError, defineEventHandler } from 'h3'

function buildCandidateBases(config) {
  const backendBase = String(config.backendBase || '').trim()
  const defaults = [
    backendBase,
    'http://127.0.0.1:8011',
    'http://127.0.0.1:8000',
  ]
  return [...new Set(defaults.filter(Boolean).map((item) => item.replace(/\/$/, '')))]
}

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const name = Array.isArray(event.context.params?.name)
    ? event.context.params.name.join('/')
    : String(event.context.params?.name || '')

  const errors = []
  for (const base of buildCandidateBases(config)) {
    const target = `${base}/api/sync-configs/${encodeURIComponent(name)}`
    try {
      const response = await fetch(target)
      const contentType = response.headers.get('content-type') || ''
      const text = await response.text()
      if (!response.ok) {
        errors.push(`${base}: ${response.status} ${response.statusText} ${text}`.trim())
        continue
      }
      return contentType.includes('application/json') ? JSON.parse(text) : text
    } catch (error) {
      errors.push(`${base}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  throw createError({
    statusCode: 502,
    statusMessage: 'Bad Gateway',
    message: `Sync config proxy failed. Tried: ${errors.join(' | ')}`,
  })
})
