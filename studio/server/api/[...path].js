import {
  defineEventHandler,
  getMethod,
  getQuery,
  getRequestHeaders,
  readRawBody,
  setHeader,
  setResponseStatus,
  createError,
} from 'h3'

function buildCandidateBases(config) {
  const configured = String(config.backendBase || '').trim()
  const defaults = [
    configured,
    'http://127.0.0.1:8011',
    'http://127.0.0.1:8000',
  ]

  return [...new Set(defaults.filter(Boolean).map((item) => item.replace(/\/$/, '')))]
}

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const path = Array.isArray(event.context.params?.path)
    ? event.context.params.path.join('/')
    : String(event.context.params?.path || '')

  const query = getQuery(event)
  const search = new URLSearchParams()
  Object.entries(query).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => search.append(key, String(item)))
      return
    }
    if (value !== undefined && value !== null) {
      search.append(key, String(value))
    }
  })

  const method = getMethod(event)
  const body = ['GET', 'HEAD'].includes(method) ? undefined : await readRawBody(event, false)
  const requestHeaders = { ...getRequestHeaders(event) }
  delete requestHeaders.host
  if (body === undefined) {
    delete requestHeaders['content-length']
  }

  const candidates = buildCandidateBases(config)
  const errors = []

  for (const base of candidates) {
    const target = `${base}/api/${path}${search.toString() ? `?${search.toString()}` : ''}`
    try {
      const response = await fetch(target, {
        method,
        headers: requestHeaders,
        body,
      })

      const contentType = response.headers.get('content-type') || ''
      setResponseStatus(event, response.status, response.statusText)
      if (contentType) {
        setHeader(event, 'content-type', contentType)
      }

      const text = await response.text()
      if (!response.ok) {
        errors.push(`${base}: ${response.status} ${response.statusText}`)
        continue
      }

      if (contentType.includes('application/json')) {
        return JSON.parse(text)
      }

      return text
    } catch (error) {
      errors.push(`${base}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  throw createError({
    statusCode: 502,
    statusMessage: 'Bad Gateway',
    message: `Backend proxy failed. Tried: ${errors.join(' | ')}`,
  })
})
