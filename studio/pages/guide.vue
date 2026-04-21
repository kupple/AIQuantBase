<script setup>
import { computed } from 'vue'
import { navigateTo, useRoute } from '#imports'

const route = useRoute()
const currentDoc = computed(() => String(route.query.doc || 'README-usage.md'))
const { data, error, pending } = await useFetch('/api/project-doc', {
  query: computed(() => ({ doc: currentDoc.value })),
})

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

function toGuideHref(href) {
  const value = String(href || '').trim()
  if (!value) return '#'
  if (value.endsWith('.md')) {
    return `/guide?doc=${encodeURIComponent(basenameFromHref(value))}`
  }
  return value
}

function basenameFromHref(href) {
  const normalized = String(href || '').replace(/\\/g, '/')
  const parts = normalized.split('/')
  return parts[parts.length - 1] || normalized
}

function renderInline(text) {
  return escapeHtml(text)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, href) => {
      const target = toGuideHref(href)
      const isInternalGuide = target.startsWith('/guide?doc=')
      return isInternalGuide
        ? `<a href="${target}">${label}</a>`
        : `<a href="${target}" target="_blank" rel="noreferrer">${label}</a>`
    })
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
}

function renderMarkdown(markdown) {
  const source = String(markdown || '').replace(/\r\n/g, '\n')
  const lines = source.split('\n')
  const html = []
  let index = 0

  while (index < lines.length) {
    const line = lines[index]
    const trimmed = line.trim()

    if (!trimmed) {
      index += 1
      continue
    }

    if (trimmed.startsWith('```')) {
      const codeLines = []
      index += 1
      while (index < lines.length && !lines[index].trim().startsWith('```')) {
        codeLines.push(lines[index])
        index += 1
      }
      html.push(`<pre><code>${escapeHtml(codeLines.join('\n'))}</code></pre>`)
      index += 1
      continue
    }

    const headingMatch = trimmed.match(/^(#{1,6})\s+(.*)$/)
    if (headingMatch) {
      const level = headingMatch[1].length
      html.push(`<h${level}>${renderInline(headingMatch[2])}</h${level}>`)
      index += 1
      continue
    }

    const orderedMatch = trimmed.match(/^(\d+)\.\s+(.*)$/)
    if (orderedMatch) {
      const items = []
      while (index < lines.length) {
        const candidate = lines[index].trim()
        const match = candidate.match(/^\d+\.\s+(.*)$/)
        if (!match) break
        items.push(`<li>${renderInline(match[1])}</li>`)
        index += 1
      }
      html.push(`<ol>${items.join('')}</ol>`)
      continue
    }

    const bulletMatch = trimmed.match(/^[-*]\s+(.*)$/)
    if (bulletMatch) {
      const items = []
      while (index < lines.length) {
        const candidate = lines[index].trim()
        const match = candidate.match(/^[-*]\s+(.*)$/)
        if (!match) break
        items.push(`<li>${renderInline(match[1])}</li>`)
        index += 1
      }
      html.push(`<ul>${items.join('')}</ul>`)
      continue
    }

    const paragraph = []
    while (index < lines.length && lines[index].trim()) {
      paragraph.push(lines[index].trim())
      index += 1
    }
    html.push(`<p>${renderInline(paragraph.join(' '))}</p>`)
  }

  return html.join('\n')
}

const renderedHtml = computed(() => renderMarkdown(data.value?.markdown || ''))

async function openDoc(doc) {
  await navigateTo({
    path: '/guide',
    query: { doc },
  })
}
</script>

<template>
  <div class="page-stack">
    <section class="workbench-grid workbench-grid-guide">
      <el-card shadow="never" class="surface-card surface-card-rail surface-card-rail-slim">
        <template #header>
          <div class="panel-heading">
            <div>
              <span class="panel-title">文档导航</span>
            </div>
          </div>
        </template>

        <div v-if="data?.availableItems?.length" class="guide-doc-list">
          <el-button
            v-for="item in data.availableItems"
            :key="item.doc"
            :type="item.doc === data.doc ? 'primary' : 'default'"
            plain
            class="guide-doc-button"
            @click="openDoc(item.doc)"
          >
            {{ item.title }}
          </el-button>
        </div>
        <el-empty v-else description="当前没有可选文档" :image-size="56" />
      </el-card>

      <el-card shadow="never" class="surface-card surface-card-strong">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">项目说明</span>
              <p class="panel-subtitle">当前页面直接读取项目里的 Markdown 文档并做格式化展示。</p>
            </div>
            <el-tag v-if="data?.doc" effect="plain" round>{{ data.doc }}</el-tag>
          </div>
        </template>

        <el-skeleton v-if="pending" :rows="12" animated />
        <el-alert
          v-else-if="error"
          title="项目说明读取失败"
          type="error"
          :closable="false"
          show-icon
        />
        <article v-else class="markdown-doc" v-html="renderedHtml"></article>
      </el-card>
    </section>
  </div>
</template>
