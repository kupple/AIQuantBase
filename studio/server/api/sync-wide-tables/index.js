import {
  createError,
  defineEventHandler,
  getMethod,
  getRequestHeaders,
  readRawBody,
  setHeader,
  setResponseStatus,
} from 'h3'

function buildCandidateBases(config) {
  const backendBase = String(config.backendBase || '').trim()
  return backendBase ? [backendBase.replace(/\/$/, '')] : []
}

export default defineEventHandler(async (event) => {
  const method = getMethod(event)
  const body = ['GET', 'HEAD'].includes(method) ? undefined : await readRawBody(event, false)
  const requestHeaders = { ...getRequestHeaders(event) }
  delete requestHeaders.host
  if (body === undefined) {
    delete requestHeaders['content-length']
  }

  const config = useRuntimeConfig(event)
  const errors = []
  for (const base of buildCandidateBases(config)) {
    const target = `${base}/api/sync-wide-tables`
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
    message: `Sync wide table proxy failed. Tried: ${errors.join(' | ')}`,
  })
})
