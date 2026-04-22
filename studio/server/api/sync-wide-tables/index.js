import { defineEventHandler, getMethod, readBody, createError } from 'h3'
import { promises as fs } from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { parse as parseYaml } from 'yaml'

const execFileAsync = promisify(execFile)

function resolveSyncSpecDir(config) {
  const root = String(config.syncProjectRoot || '').trim()
  if (!root) {
    throw createError({ statusCode: 500, statusMessage: 'Server Error', message: 'syncProjectRoot is not configured' })
  }
  return path.join(root, 'wide_table_specs')
}

function resolveProjectRoot() {
  return path.resolve(process.cwd(), '..')
}

async function loadWideTableDesigns(projectRoot) {
  const wideTablePath = path.join(projectRoot, 'config', 'wide_tables.yaml')
  const raw = await fs.readFile(wideTablePath, 'utf8')
  const payload = parseYaml(raw) || {}
  const items = Array.isArray(payload.wide_tables) ? payload.wide_tables : []
  return items
}

async function exportWideTableYaml(projectRoot, designId) {
  const script = `
import sys
from aiquantbase.wide_table import export_wide_table_yaml
print(export_wide_table_yaml(${JSON.stringify(designId)}, 'config/wide_tables.yaml', graph_path='config/graph.yaml', fields_path='config/fields.yaml'))
`
  const env = {
    ...process.env,
    PYTHONPATH: path.join(projectRoot, 'src'),
  }
  const { stdout } = await execFileAsync('python3', ['-c', script], {
    cwd: projectRoot,
    env,
    maxBuffer: 10 * 1024 * 1024,
  })
  return stdout
}

async function safeStat(filePath) {
  try {
    return await fs.stat(filePath)
  } catch {
    return null
  }
}

export default defineEventHandler(async (event) => {
  const config = useRuntimeConfig(event)
  const projectRoot = resolveProjectRoot()
  const specDir = resolveSyncSpecDir(config)
  await fs.mkdir(specDir, { recursive: true })

  if (getMethod(event) === 'GET') {
    const items = await loadWideTableDesigns(projectRoot)
    const result = []
    for (const item of items) {
      const filePath = path.join(specDir, `${item.name}.yaml`)
      const stat = await safeStat(filePath)
      result.push({
        ...item,
        exported: Boolean(stat),
        exported_path: stat ? filePath : '',
        exported_at: stat ? stat.mtime.toISOString() : '',
      })
    }
    return {
      items: result,
      sync_spec_dir: specDir,
    }
  }

  if (getMethod(event) === 'POST') {
    const body = await readBody(event)
    const designId = String(body?.id || '').trim()
    const name = String(body?.name || '').trim()
    if (!designId || !name) {
      throw createError({ statusCode: 400, statusMessage: 'Bad Request', message: 'id and name are required' })
    }
    const yamlText = await exportWideTableYaml(projectRoot, designId)
    const filePath = path.join(specDir, `${name}.yaml`)
    await fs.writeFile(filePath, String(yamlText || ''), 'utf8')
    return {
      ok: true,
      exported_path: filePath,
    }
  }

  throw createError({ statusCode: 405, statusMessage: 'Method Not Allowed', message: 'unsupported method' })
})
