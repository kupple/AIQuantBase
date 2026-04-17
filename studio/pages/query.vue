<script setup>
import { navigateTo } from '#imports'
import { computed, ref } from 'vue'
import { useWorkbench } from '~/composables/useWorkbench'

const {
  queryLab,
  runNaturalLanguageQuery,
  runSql,
  notifyAction,
  hasLlmConfigured,
} = useWorkbench()
const nlRunning = ref(false)
const sqlRunning = ref(false)

const friendlyQueryError = computed(() => {
  const message = String(queryLab.value.error || '').trim()
  if (!message) return ''

  if (message.includes('CERTIFICATE_VERIFY_FAILED')) {
    return 'LLM 证书校验失败。请到设置页关闭 verify_ssl，或修复本机 Python 证书环境后再试。'
  }

  if (message.includes('urlopen error')) {
    return 'LLM 请求失败。请检查 Base URL、API Key、网络连通性以及 SSL 配置。'
  }

  if (message.includes('LLM config is disabled')) {
    return 'LLM 当前处于停用状态，请到设置页启用后再试。'
  }

  if (message.includes('nodename nor servname provided')) {
    return 'LLM 地址无法解析。请检查设置页中的 Base URL 是否正确。'
  }

  return message
})

const friendlySqlError = computed(() => {
  const message = String(queryLab.value.error || '').trim()
  if (!message) return ''

  if (message.includes('ASOF') || message.includes('JOIN')) {
    return 'SQL 执行失败，可能是关联方式或 Join 条件不被当前 ClickHouse 接受。请检查返回体里的原始 SQL 和错误详情。'
  }

  if (message.includes('Syntax error')) {
    return 'SQL 语法错误。请检查 SQL 预览区中的语句。'
  }

  if (message.includes('Bad Request')) {
    return 'SQL 已发到数据库，但数据库拒绝执行。请查看下方原始结果里的详细错误信息。'
  }

  return message
})

async function handleRunNaturalLanguageQuery() {
  nlRunning.value = true
  try {
    await notifyAction('自然语言查询已完成', runNaturalLanguageQuery)
  } finally {
    nlRunning.value = false
  }
}

async function handleRunSql() {
  sqlRunning.value = true
  try {
    await notifyAction('SQL 执行完成', runSql)
  } finally {
    sqlRunning.value = false
  }
}
</script>

<template>
  <div class="page-stack">
    <section class="workbench-grid workbench-grid-query">
      <el-card shadow="never" class="surface-card query-card query-card-input">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">自然语言输入</span>
              <p class="panel-subtitle">先验证 Query Intent 是否符合预期，再决定要不要执行 SQL。</p>
            </div>
            <el-button type="primary" :loading="nlRunning" @click="handleRunNaturalLanguageQuery">
              生成并执行
            </el-button>
          </div>
        </template>

        <div class="form-stack">
          <el-alert
            v-if="!hasLlmConfigured"
            title="当前 LLM 配置不完整，请先到设置页补全 Provider / Base URL / API Key / Model。"
            type="warning"
            :closable="false"
            show-icon
            class="inline-alert"
          />

          <el-form label-position="top">
            <el-form-item label="问题描述">
              <el-input v-model="queryLab.naturalQuery" type="textarea" :rows="5" resize="none" />
            </el-form-item>
          </el-form>

          <div class="query-helper-list query-helper-list-compact">
            <div class="helper-item">
              <strong>建议 1</strong>
              <span>尽量明确时间范围、标的和字段名，让自然语言入口更容易稳定落到 Query Intent。</span>
            </div>
            <div class="helper-item">
              <strong>建议 2</strong>
              <span>如果你只是想测试 SQL 执行层，可以跳过自然语言生成，直接修改右侧 SQL。</span>
            </div>
            <div v-if="!hasLlmConfigured" class="helper-item">
              <strong>设置</strong>
              <span>
                <el-button link type="primary" @click="navigateTo('/settings')">前往设置页配置 LLM</el-button>
              </span>
            </div>
          </div>
        </div>
      </el-card>

      <div class="stack-block query-card query-card-middle">
        <el-card shadow="never" class="surface-card">
          <template #header>
            <div class="panel-heading">
              <div>
                <span class="panel-title">Query Intent</span>
                <p class="panel-subtitle">这里展示模型生成的中间协议，方便判断字段、过滤和排序是否正确。</p>
              </div>
            </div>
          </template>

          <pre class="code-frame code-frame-medium">{{ queryLab.queryIntent ? JSON.stringify(queryLab.queryIntent, null, 2) : '等待生成 Intent' }}</pre>
        </el-card>

        <el-card shadow="never" class="surface-card">
          <template #header>
            <div class="panel-heading panel-heading-space">
              <div>
                <span class="panel-title">SQL 预览</span>
                <p class="panel-subtitle">可以直接在这里二次改写，再发起执行测试。</p>
              </div>
              <el-button :loading="sqlRunning" @click="handleRunSql">执行当前 SQL</el-button>
            </div>
          </template>

          <el-input v-model="queryLab.sql" type="textarea" :rows="11" resize="none" />
        </el-card>
      </div>

      <el-card shadow="never" class="surface-card query-card query-card-result">
        <template #header>
          <div class="panel-heading">
            <div>
              <span class="panel-title">执行结果</span>
              <p class="panel-subtitle">统一显示接口返回、错误提示和原始结果 JSON。</p>
            </div>
          </div>
        </template>

        <el-alert
          v-if="friendlyQueryError"
          :title="friendlyQueryError"
          type="error"
          :closable="false"
          show-icon
          class="inline-alert"
        />
        <div v-if="queryLab.error && queryLab.error.includes('CERTIFICATE_VERIFY_FAILED')" class="panel-actions panel-actions-compact">
          <el-button type="primary" plain @click="navigateTo('/settings')">
            前往设置页处理
          </el-button>
        </div>
        <el-alert
          v-if="queryLab.result && queryLab.result.ok === false && queryLab.result.sql"
          :title="friendlySqlError"
          type="warning"
          :closable="false"
          show-icon
          class="inline-alert"
        />
        <el-skeleton v-if="nlRunning || sqlRunning" :rows="12" animated />
        <pre v-else class="code-frame code-frame-tall">{{ queryLab.result ? JSON.stringify(queryLab.result, null, 2) : '等待结果' }}</pre>
      </el-card>
    </section>
  </div>
</template>
