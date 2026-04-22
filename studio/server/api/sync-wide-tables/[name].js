import { defineEventHandler, createError } from 'h3'
import { promises as fs } from 'node:fs'
import path from 'node:path'

function resolveSyncSpecDir(config) {
  const root = String(config.syncProjectRoot || '').trim()
  if (!root) {
    throw createError({ statusCode: 500, statusMessage: 'Server Error', message: 'syncProjectRoot is not configured' })
  }
  return path.join(root, 'wide_table_specs')
}

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const specDir = resolveSyncSpecDir(config)
  const fileName = String(event.context.params?.name || '').trim()
  if (!/^[\w.-]+$/.test(fileName)) {
    throw createError({ statusCode: 400, statusMessage: 'Bad Request', message: 'invalid file name' })
  }
  const filePath = path.join(specDir, fileName.endsWith('.yaml') ? fileName : `${fileName}.yaml`)
  try {
    const content = await fs.readFile(filePath, 'utf8')
    const stat = await fs.stat(filePath)
    return {
      name: path.basename(filePath),
      content,
      exported_path: filePath,
      exported_at: stat.mtime.toISOString(),
    }
  } catch {
    throw createError({ statusCode: 404, statusMessage: 'Not Found', message: 'wide table sync spec not found' })
  }
})
