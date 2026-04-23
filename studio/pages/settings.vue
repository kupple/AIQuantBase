<script setup>
import { computed, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useWorkbench } from '~/composables/useWorkbench'

const {
  runtime,
  saveWorkspace,
  testDatasourceConnection,
  loading,
  workspace,
} = useWorkbench()

const settingsForm = reactive({
  db_type: '',
  host: '',
  port: 8123,
  database: '',
  username: '',
  password: '',
  secure: false,
  llm_provider_name: '',
  llm_base_url: '',
  llm_api_key: '',
  llm_model_name: '',
  llm_temperature: 0.1,
  llm_max_tokens: 4096,
  llm_enabled: true,
  llm_verify_ssl: true,
  allow_databases_text: '',
  allow_tables_text: '',
})
const settingsHydrated = ref(false)
const calendarHydrating = ref(false)
const calendarLoading = ref(false)
const calendarDatabases = ref([])
const calendarTables = ref([])
const calendarState = reactive({
  database: '',
  table: '',
})
let autoSaveTimer = null

const canBrowseCalendarTables = computed(() =>
  Boolean(settingsForm.db_type && settingsForm.host && settingsForm.port && settingsForm.username)
)
const persistedCalendarTable = computed(() =>
  String(runtime.value.discovery?.trading_calendar_table || '').trim()
)

watch(
  () => runtime.value,
  async () => {
    settingsHydrated.value = false
    syncSettingsFormFromRuntime()
    settingsHydrated.value = true
    await hydrateCalendarSelectionFromRuntime()
  },
  { deep: true, immediate: true }
)

watch(
  settingsForm,
  () => {
    if (!settingsHydrated.value) return
    scheduleAutoSave()
  },
  { deep: true }
)

function syncSettingsFormFromRuntime() {
  const datasource = runtime.value.datasource || {}
  const llm = runtime.value.llm || {}
  const discovery = runtime.value.discovery || {}
  settingsForm.db_type = datasource.db_type || 'clickhouse'
  settingsForm.host = datasource.host || ''
  settingsForm.port = datasource.port || 8123
  settingsForm.database = datasource.database || ''
  settingsForm.username = datasource.username || ''
  settingsForm.password = datasource.password || ''
  settingsForm.secure = Boolean(datasource.secure)
  settingsForm.llm_provider_name = llm.provider_name || 'deepseek'
  settingsForm.llm_base_url = llm.base_url || ''
  settingsForm.llm_api_key = llm.api_key || ''
  settingsForm.llm_model_name = llm.model_name || 'deepseek-chat'
  settingsForm.llm_temperature = llm.temperature ?? 0.1
  settingsForm.llm_max_tokens = llm.max_tokens ?? 4096
  settingsForm.llm_enabled = llm.enabled ?? true
  settingsForm.llm_verify_ssl = llm.verify_ssl ?? true
  settingsForm.allow_databases_text = (discovery.allow_databases || []).join('\n')
  settingsForm.allow_tables_text = (discovery.allow_tables || []).join('\n')

  const { database, table } = splitQualifiedTable(discovery.trading_calendar_table || '')
  calendarState.database = database
  calendarState.table = table
}

function applySettingsFormToRuntime() {
  runtime.value.datasource = {
    ...runtime.value.datasource,
    db_type: settingsForm.db_type,
    host: settingsForm.host,
    port: Number(settingsForm.port) || 8123,
    database: settingsForm.database,
    username: settingsForm.username,
    password: settingsForm.password,
    secure: Boolean(settingsForm.secure),
    extra_params: runtime.value.datasource?.extra_params || {},
  }
  runtime.value.llm = {
    ...runtime.value.llm,
    provider_name: settingsForm.llm_provider_name,
    base_url: settingsForm.llm_base_url,
    api_key: settingsForm.llm_api_key,
    model_name: settingsForm.llm_model_name,
    temperature: Number(settingsForm.llm_temperature) || 0.1,
    max_tokens: Number(settingsForm.llm_max_tokens) || 4096,
    enabled: Boolean(settingsForm.llm_enabled),
    verify_ssl: Boolean(settingsForm.llm_verify_ssl),
  }
  runtime.value.discovery = {
    ...runtime.value.discovery,
    trading_calendar_table: buildQualifiedTable(calendarState.database, calendarState.table),
    allow_databases: splitLines(settingsForm.allow_databases_text),
    allow_tables: splitLines(settingsForm.allow_tables_text),
  }
}

async function handleTestConnection() {
  try {
    applySettingsFormToRuntime()
    await saveWorkspace()
    const result = await testDatasourceConnection()
    await loadCalendarDatabases()
    ElMessage.success({
      message: `连接成功，可见库 ${result.items?.length || 0} 个`,
      duration: 1800,
      grouping: true,
    })
  } catch (error) {
    ElMessage.error({
      message: error instanceof Error ? error.message : '连接失败',
      duration: 2600,
      grouping: true,
    })
  }
}

async function handleSaveSettings() {
  try {
    applySettingsFormToRuntime()
    await saveWorkspace()
    await reloadRuntimeFromWorkspace()
    ElMessage.success({
      message: '配置已保存',
      duration: 1800,
      grouping: true,
    })
  } catch (error) {
    ElMessage.error({
      message: error instanceof Error ? error.message : '保存失败',
      duration: 2600,
      grouping: true,
    })
  }
}

async function handleSaveCalendarSettings() {
  try {
    if (autoSaveTimer) {
      clearTimeout(autoSaveTimer)
      autoSaveTimer = null
    }
    if (!calendarState.database || !calendarState.table) {
      throw new Error('请先完整选择交易日历库和表')
    }
    applySettingsFormToRuntime()
    const expectedTable = buildQualifiedTable(calendarState.database, calendarState.table)
    await saveWorkspace()
    const persisted = await reloadRuntimeFromWorkspace()
    if ((persisted.discovery?.trading_calendar_table || '') !== expectedTable) {
      throw new Error('交易日历配置未成功写入，请重试')
    }
    ElMessage.success({
      message: '交易日历配置已保存',
      duration: 1800,
      grouping: true,
    })
  } catch (error) {
    ElMessage.error({
      message: error instanceof Error ? error.message : '交易日历配置保存失败',
      duration: 2600,
      grouping: true,
    })
  }
}

watch(
  () => calendarState.database,
  async (value, previous) => {
    if (!settingsHydrated.value || calendarHydrating.value) return
    if (!value) {
      calendarTables.value = []
      calendarState.table = ''
      return
    }
    if (value !== previous) {
      calendarState.table = ''
    }
    await loadCalendarTables(value)
  }
)

watch(
  () => calendarState.table,
  () => {
    if (!settingsHydrated.value || calendarHydrating.value) return
  }
)

async function fetchJson(path) {
  const response = await fetch(path)
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(payload?.detail || payload?.message || payload?.error || '请求失败')
  }
  return payload
}

async function reloadRuntimeFromWorkspace() {
  const payload = await fetchJson(
    `/api/workspace?graph_path=${encodeURIComponent(workspace.value.graphPath)}&fields_path=${encodeURIComponent(workspace.value.fieldsPath)}&runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`
  )
  settingsHydrated.value = false
  runtime.value.datasource = payload.runtime?.datasource || {}
  runtime.value.llm = payload.runtime?.llm || {}
  runtime.value.discovery = payload.runtime?.discovery || {}
  syncSettingsFormFromRuntime()
  settingsHydrated.value = true
  await hydrateCalendarSelectionFromRuntime()
  return payload.runtime || {}
}

async function loadCalendarDatabases() {
  if (!canBrowseCalendarTables.value) {
    calendarDatabases.value = []
    calendarTables.value = []
    return []
  }
  calendarLoading.value = true
  try {
    const payload = await fetchJson(`/api/schema/databases?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`)
    calendarDatabases.value = payload.items || []
    return calendarDatabases.value
  } finally {
    calendarLoading.value = false
  }
}

async function loadCalendarTables(database) {
  if (!database) {
    calendarTables.value = []
    return []
  }
  calendarLoading.value = true
  try {
    const payload = await fetchJson(
      `/api/schema/tables?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(database)}`
    )
    calendarTables.value = payload.items || []
    return calendarTables.value
  } finally {
    calendarLoading.value = false
  }
}

async function hydrateCalendarSelectionFromRuntime() {
  calendarHydrating.value = true
  try {
    const { database, table } = splitQualifiedTable(runtime.value.discovery?.trading_calendar_table || '')

    if (!canBrowseCalendarTables.value) {
      calendarDatabases.value = []
      calendarTables.value = []
      calendarState.database = database
      calendarState.table = table
      return
    }

    await loadCalendarDatabases()

    const databaseExists = database && calendarDatabases.value.some((item) => item.name === database)
    calendarState.database = databaseExists ? database : ''

    if (!databaseExists) {
      calendarTables.value = []
      calendarState.table = ''
      return
    }

    await loadCalendarTables(database)

    const tableExists = table && calendarTables.value.some((item) => item.name === table)
    calendarState.table = tableExists ? table : ''
  } finally {
    calendarHydrating.value = false
  }
}

function scheduleAutoSave() {
  if (autoSaveTimer) clearTimeout(autoSaveTimer)
  autoSaveTimer = setTimeout(async () => {
    try {
      applySettingsFormToRuntime()
      await saveWorkspace()
    } catch (error) {
      ElMessage.error({
        message: error instanceof Error ? error.message : '自动保存失败',
        duration: 2600,
        grouping: true,
      })
    }
  }, 500)
}

function splitLines(value) {
  return [...new Set(String(value || '').split('\n').map((item) => item.trim()).filter(Boolean))]
}

function splitQualifiedTable(value) {
  const text = String(value || '').trim()
  const dot = text.indexOf('.')
  if (dot < 0) {
    return { database: '', table: text }
  }
  return {
    database: text.slice(0, dot),
    table: text.slice(dot + 1),
  }
}

function buildQualifiedTable(database, table) {
  if (!database || !table) return ''
  return `${database}.${table}`
}
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="surface-card surface-card-strong">
      <template #header>
        <div class="panel-heading panel-heading-space">
          <div>
            <span class="panel-title">数据库连接</span>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-button :loading="loading" @click="handleSaveSettings">保存配置</el-button>
            <el-button :loading="loading" @click="handleTestConnection">测试连接</el-button>
          </div>
        </div>
      </template>

      <div class="form-stack">
        <div class="three-col-form">
          <el-form-item label="数据库类型">
            <el-select v-model="settingsForm.db_type" style="width: 100%">
              <el-option label="ClickHouse" value="clickhouse" />
              <el-option label="PostgreSQL" value="postgresql" />
            </el-select>
          </el-form-item>
          <el-form-item label="Host">
            <el-input v-model="settingsForm.host" placeholder="127.0.0.1" />
          </el-form-item>
          <el-form-item label="Port">
            <el-input v-model="settingsForm.port" placeholder="8123" />
          </el-form-item>
        </div>

        <div class="three-col-form">
          <el-form-item label="Database">
            <el-input v-model="settingsForm.database" placeholder="可为空" />
          </el-form-item>
          <el-form-item label="Username">
            <el-input v-model="settingsForm.username" placeholder="default" />
          </el-form-item>
          <el-form-item label="Password">
            <el-input v-model="settingsForm.password" type="password" show-password />
          </el-form-item>
        </div>

        <div class="two-col-form">
          <el-form-item label="允许数据库 allow_databases">
            <el-input v-model="settingsForm.allow_databases_text" type="textarea" :rows="6" placeholder="一行一个数据库名" />
          </el-form-item>
          <el-form-item label="允许表 allow_tables">
            <el-input v-model="settingsForm.allow_tables_text" type="textarea" :rows="6" placeholder="一行一个表名，可为空" />
          </el-form-item>
        </div>

        <el-switch v-model="settingsForm.secure" active-text="启用安全连接" inactive-text="普通连接" />
      </div>
    </el-card>

    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading panel-heading-space">
          <div>
            <span class="panel-title">LLM 配置</span>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-button :loading="loading" @click="handleSaveSettings">保存配置</el-button>
          </div>
        </div>
      </template>

      <div class="form-stack">
        <div class="three-col-form">
          <el-form-item label="Provider">
            <el-input v-model="settingsForm.llm_provider_name" placeholder="deepseek" />
          </el-form-item>
          <el-form-item label="Model">
            <el-input v-model="settingsForm.llm_model_name" placeholder="deepseek-chat" />
          </el-form-item>
          <el-form-item label="Base URL">
            <el-input v-model="settingsForm.llm_base_url" placeholder="https://api.deepseek.com" />
          </el-form-item>
        </div>

        <div class="three-col-form">
          <el-form-item label="API Key">
            <el-input v-model="settingsForm.llm_api_key" type="password" show-password placeholder="填写 LLM API Key" />
          </el-form-item>
          <el-form-item label="Temperature">
            <el-input v-model="settingsForm.llm_temperature" placeholder="0.1" />
          </el-form-item>
          <el-form-item label="Max Tokens">
            <el-input v-model="settingsForm.llm_max_tokens" placeholder="4096" />
          </el-form-item>
        </div>

        <div class="two-col-form">
          <el-switch v-model="settingsForm.llm_enabled" active-text="启用 LLM" inactive-text="停用 LLM" />
          <el-switch v-model="settingsForm.llm_verify_ssl" active-text="校验 SSL" inactive-text="忽略 SSL" />
        </div>
      </div>
    </el-card>

    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading panel-heading-space">
          <div>
            <span class="panel-title">交易日历配置</span>
            <p class="panel-subtitle">连接数据库后，在这里选择项目默认使用的交易日历表。</p>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-button :loading="loading" @click="handleSaveCalendarSettings">保存配置</el-button>
          </div>
        </div>
      </template>

      <div class="form-stack">
        <el-alert
          v-if="persistedCalendarTable"
          :title="`当前已保存交易日历表: ${persistedCalendarTable}`"
          type="success"
          :closable="false"
          show-icon
        />

        <el-alert
          v-if="!canBrowseCalendarTables"
          title="请先填写并测试数据库连接，下面才会出现可选的数据库和表。"
          type="info"
          :closable="false"
          show-icon
        />

        <div v-else class="two-col-form">
          <el-form-item label="日历库">
            <el-select
              v-model="calendarState.database"
              filterable
              clearable
              style="width: 100%"
              placeholder="选择数据库"
              :loading="calendarLoading"
            >
              <el-option v-for="item in calendarDatabases" :key="item.name" :label="item.name" :value="item.name" />
            </el-select>
          </el-form-item>
          <el-form-item label="交易日历表">
            <el-select
              v-model="calendarState.table"
              filterable
              clearable
              style="width: 100%"
              placeholder="选择交易日历表"
              :loading="calendarLoading"
              :disabled="!calendarState.database"
            >
              <el-option v-for="item in calendarTables" :key="item.name" :label="item.name" :value="item.name" />
            </el-select>
          </el-form-item>
        </div>
      </div>
    </el-card>
  </div>
</template>
