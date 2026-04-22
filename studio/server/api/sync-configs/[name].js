import { defineEventHandler, getMethod, createError } from 'h3'
import { promises as fs } from 'node:fs'
import path from 'node:path'

function resolveRoot(event) {
  const config = useRuntimeConfig(event)
  return String(config.syncProjectRoot || '').trim()
}

function resolveConfigPath(root, name) {
  const fileName = String(name || '').trim()
  if (!/^run_sync.*\.toml$/i.test(fileName)) {
    throw createError({ statusCode: 400, statusMessage: 'Bad Request', message: 'invalid config file name' })
  }
  return {
    name: fileName,
    filePath: path.join(root, fileName),
  }
}

export default defineEventHandler(async (event) => {
  const method = getMethod(event)
  const root = resolveRoot(event)
  if (!root) {
    throw createError({ statusCode: 500, statusMessage: 'Server Error', message: 'syncProjectRoot is not configured' })
  }

  const { name, filePath } = resolveConfigPath(root, event.context.params?.name || '')
  if (method === 'GET') {
    const content = await fs.readFile(filePath, 'utf8')
    return { name, content }
  }

  throw createError({ statusCode: 405, statusMessage: 'Method Not Allowed', message: 'unsupported method' })
})
