import { readFile, readdir } from 'node:fs/promises'
import { basename, resolve } from 'node:path'
import { createError, defineEventHandler, getQuery } from 'h3'

export default defineEventHandler(async (event) => {
  const docsDir = resolve(process.cwd(), '..', 'docs')
  const query = getQuery(event)
  const requested = basename(String(query.doc || 'README-usage.md'))
  const availableDocs = (await readdir(docsDir)).filter((name) => name.endsWith('.md')).sort()
  if (!availableDocs.includes(requested)) {
    throw createError({
      statusCode: 404,
      statusMessage: 'Not Found',
      message: `Document not found: ${requested}`,
    })
  }
  const docPath = resolve(docsDir, requested)
  const markdown = await readFile(docPath, 'utf8')
  return {
    ok: true,
    doc: requested,
    path: docPath,
    availableDocs,
    markdown,
  }
})
