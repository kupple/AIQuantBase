import {
  createError,
  getMethod,
  getQuery,
  getRequestHeaders,
  readRawBody,
  setHeader,
  setResponseStatus,
} from 'h3'

export function resolveBackendBase(config) {
  const backendBase = String(config.backendBase || '').trim().replace(/\/$/, '')
  if (!backendBase) {
    throw createError({
      statusCode: 500,
      statusMessage: 'Server Error',
      message: 'backendBase is not configured',
    })
  }
  return backendBase
}

export function buildQueryString(event) {
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
  const queryString = search.toString()
  return queryString ? `?${queryString}` : ''
}

export async function buildProxyInit(event) {
  const method = getMethod(event)
  const body = ['GET', 'HEAD'].includes(method) ? undefined : await readRawBody(event, false)
  const headers = { ...getRequestHeaders(event) }
  delete headers.host
  if (body === undefined) {
    delete headers['content-length']
  }
  return { method, body, headers }
}

export async function proxyToBackend(event, target, errorPrefix) {
  const init = await buildProxyInit(event)
  try {
    const response = await fetch(target, init)
    const contentType = response.headers.get('content-type') || ''
    const text = await response.text()

    if (!response.ok) {
      throw createError({
        statusCode: 502,
        statusMessage: 'Bad Gateway',
        message: `${errorPrefix}: ${response.status} ${response.statusText} ${text}`.trim(),
      })
    }

    setResponseStatus(event, response.status, response.statusText)
    if (contentType) {
      setHeader(event, 'content-type', contentType)
    }
    return contentType.includes('application/json') ? JSON.parse(text) : text
  } catch (error) {
    if (error && typeof error === 'object' && 'statusCode' in error) {
      throw error
    }
    throw createError({
      statusCode: 502,
      statusMessage: 'Bad Gateway',
      message: `${errorPrefix}: ${error instanceof Error ? error.message : String(error)}`,
    })
  }
}
