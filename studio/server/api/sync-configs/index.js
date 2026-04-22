import { defineEventHandler, getMethod, readBody, createError } from 'h3'
import { promises as fs } from 'node:fs'
import path from 'node:path'

async function listConfigFiles(root) {
  const entries = await fs.readdir(root, { withFileTypes: true })
  return entries
    .filter((entry) => entry.isFile() && /^run_sync.*\.toml$/i.test(entry.name))
    .map((entry) => entry.name)
    .sort()
}

function resolveRoot(event) {
  const config = useRuntimeConfig(event)
  return String(config.syncProjectRoot || '').trim()
}

function resolveConfigPath(root, name) {
  const fileName = String(name || '').trim()
  if (!/^run_sync.*\.toml$/i.test(fileName)) {
    throw createError({ statusCode: 400, statusMessage: 'Bad Request', message: 'invalid config file name' })
  }
  return path.join(root, fileName)
}

export default defineEventHandler(async (event) => {
  const method = getMethod(event)
  const root = resolveRoot(event)
  if (!root) {
    throw createError({ statusCode: 500, statusMessage: 'Server Error', message: 'syncProjectRoot is not configured' })
  }

  if (method === 'GET') {
    const items = await listConfigFiles(root)
    return { items }
  }

  if (method === 'POST') {
    const body = await readBody(event)
    const fileName = String(body?.name || '').trim()
    const content = String(body?.content || '')
    if (!fileName) {
      throw createError({ statusCode: 400, statusMessage: 'Bad Request', message: 'name is required' })
    }
    const filePath = resolveConfigPath(root, fileName)
    await fs.writeFile(filePath, content, 'utf8')
    return { ok: true, name: fileName }
  }

  throw createError({ statusCode: 405, statusMessage: 'Method Not Allowed', message: 'unsupported method' })
})
