<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { formatDateTime } from '~/composables/useDateTimeFormat'

const loading = ref(false)
const specs = ref([])
const states = ref([])
const syncSpecDir = ref('')
const search = ref('')
const stateDatabase = ref('default')
const planDialogVisible = ref(false)
const specDialogVisible = ref(false)
const stateDialogVisible = ref(false)
const yamlDialogVisible = ref(false)
const selectedPlan = ref(null)
const selectedSpec = ref(null)
const selectedState = ref(null)
const yamlTitle = ref('')
const yamlContent = ref('')

const filteredItems = computed(() => {
  const keyword = search.value.trim().toLowerCase()
  const stateByName = new Map(states.value.map((item) => [item.wide_table_name, item]))
  return specs.value
    .map((spec) => ({
      ...spec,
      state: stateByName.get(spec.spec_name) || null,
    }))
    .filter((item) => {
      if (!keyword) return true
      return [
        item.spec_name,
        item.source_node,
        item.target?.database,
        item.target?.table,
        item.target?.engine,
        item.state?.last_status,
        item.state?.last_action,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(keyword)
    })
})

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  })
  const contentType = response.headers.get('content-type') || ''
  const payload = contentType.includes('application/json') ? await response.json() : await response.text()
  if (!response.ok) {
    const message = typeof payload === 'string'
      ? payload
      : payload?.detail || payload?.message || payload?.error || '请求失败'
    throw new Error(message)
  }
  return payload
}

async function loadSyncExportStatus() {
  const payload = await api('/api/sync-wide-tables')
  syncSpecDir.value = payload.sync_spec_dir || ''
  const exportedMap = new Map((payload.items || []).map((item) => [item.name, item]))
  specs.value = specs.value.map((spec) => {
    const exported = exportedMap.get(spec.spec_name)
    return {
      ...spec,
      exported: Boolean(exported?.exported),
      exported_at: exported?.exported_at || '',
      exported_path: exported?.exported_path || '',
    }
  })
}

async function loadSpecs() {
  const payload = await api('/api/sync/wide-tables/specs')
  specs.value = payload.specs || []
  await loadSyncExportStatus()
}

async function loadStates() {
  const search = new URLSearchParams()
  if (stateDatabase.value) search.set('state_database', stateDatabase.value)
  try {
    const payload = await api(`/api/sync/wide-tables/states${search.toString() ? `?${search.toString()}` : ''}`)
    states.value = payload.states || []
  } catch (error) {
    states.value = []
    throw error
  }
}

async function refreshPage() {
  loading.value = true
  try {
    await Promise.all([loadSpecs(), loadStates()])
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '同步宽表页加载失败')
  } finally {
    loading.value = false
  }
}

async function handleExport(row) {
  try {
    const payload = await api('/api/sync-wide-tables', {
      method: 'POST',
      body: JSON.stringify({
        id: row.wide_table_id,
        name: row.spec_name,
      }),
    })
    ElMessage.success(`已导出到 ${payload.exported_path}`)
    await loadSpecs()
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '导出宽表同步配置失败')
  }
}

async function exportWideTableSpec(row) {
  const payload = await api('/api/sync-wide-tables', {
    method: 'POST',
    body: JSON.stringify({
      id: row.wide_table_id,
      name: row.spec_name,
    }),
  })
  return payload
}

async function handleViewYaml(row) {
  try {
    const payload = await api(`/api/sync-wide-tables/${encodeURIComponent(row.spec_name)}`)
    yamlTitle.value = row.spec_name
    yamlContent.value = payload.content || ''
    yamlDialogVisible.value = true
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '读取宽表同步配置失败')
  }
}

async function handleViewSpec(row) {
  try {
    const payload = await api(`/api/sync/wide-tables/specs/${encodeURIComponent(row.spec_name)}`)
    selectedSpec.value = payload
    specDialogVisible.value = true
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '读取宽表 spec 失败')
  }
}

async function handleViewState(row) {
  try {
    const search = new URLSearchParams()
    if (stateDatabase.value) search.set('state_database', stateDatabase.value)
    const payload = await api(`/api/sync/wide-tables/states/${encodeURIComponent(row.spec_name)}${search.toString() ? `?${search.toString()}` : ''}`)
    selectedState.value = payload
    stateDialogVisible.value = true
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '读取宽表状态失败')
  }
}

async function handlePlan(row) {
  try {
    const payload = await api('/api/sync/wide-tables/plan', {
      method: 'POST',
      body: JSON.stringify({
        clickhouse_live: true,
        write_state: false,
        state_database: stateDatabase.value || null,
      }),
    })
    const targetPlan = (payload.plans || []).find((item) => item.wide_table_name === row.spec_name) || null
    selectedPlan.value = targetPlan
    planDialogVisible.value = true
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '宽表规划失败')
  }
}

async function handleRun(row) {
  try {
    await exportWideTableSpec(row)
    const payload = await api(`/api/sync/wide-tables/run/${encodeURIComponent(row.spec_name)}?state_database=${encodeURIComponent(stateDatabase.value || 'default')}`, {
      method: 'POST',
    })
    ElMessage.success(`宽表作业已启动: ${payload.job_id}`)
    await refreshPage()
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '触发宽表作业失败')
  }
}

onMounted(async () => {
  await refreshPage()
})
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading panel-heading-space">
          <div>
            <span class="panel-title">同步宽表</span>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-input
              v-model="search"
              clearable
              placeholder="搜索宽表节点 / 来源节点 / 目标表"
              class="panel-search panel-search-wide"
            />
            <el-input v-model="stateDatabase" placeholder="状态库，默认 default" style="width: 180px" />
            <el-button :loading="loading" @click="refreshPage">刷新</el-button>
          </div>
        </div>
      </template>

      <div class="mini-description">
        当前同步目录：{{ syncSpecDir || '-' }}
      </div>

      <el-table :data="filteredItems" row-key="spec_name" empty-text="暂无宽表节点">
        <el-table-column prop="spec_name" label="宽表节点" min-width="180" />
        <el-table-column prop="source_node" label="来源节点" min-width="180" />
        <el-table-column label="目标表" min-width="220">
          <template #default="{ row }">
            {{ row.target?.database }}.{{ row.target?.table }}
          </template>
        </el-table-column>
        <el-table-column label="引擎" width="150">
          <template #default="{ row }">{{ row.target?.engine || '-' }}</template>
        </el-table-column>
        <el-table-column label="已导出" width="100">
          <template #default="{ row }">
            <el-tag size="small" effect="plain" :type="row.exported ? 'success' : 'warning'">
              {{ row.exported ? '是' : '否' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="最近状态" width="120">
          <template #default="{ row }">
            {{ row.state?.last_status || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="最近动作" width="140">
          <template #default="{ row }">
            {{ row.state?.last_action || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="更新时间" min-width="180">
          <template #default="{ row }">
            {{ formatDateTime(row.state?.updated_at || row.exported_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="280" fixed="right">
          <template #default="{ row }">
            <div class="binding-action-group">
              <el-button link type="primary" @click="handleExport(row)">导出</el-button>
              <el-button link @click="handleViewYaml(row)" :disabled="!row.exported">YAML</el-button>
              <el-button link @click="handleViewSpec(row)">Spec</el-button>
              <el-button link @click="handlePlan(row)">规划</el-button>
              <el-button link @click="handleViewState(row)">状态</el-button>
              <el-button link type="success" @click="handleRun(row)">触发作业</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="yamlDialogVisible" :title="`同步宽表配置：${yamlTitle}`" width="920px" destroy-on-close>
      <div class="form-stack">
        <el-input :model-value="yamlContent" type="textarea" :rows="28" readonly />
      </div>
      <template #footer>
        <el-button @click="yamlDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="specDialogVisible" :title="selectedSpec ? `宽表 Spec：${selectedSpec.spec_name}` : '宽表 Spec'" width="860px" destroy-on-close>
      <div v-if="selectedSpec" class="form-stack">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="Spec 名称">{{ selectedSpec.spec_name }}</el-descriptions-item>
          <el-descriptions-item label="Spec 路径">{{ selectedSpec.spec_path }}</el-descriptions-item>
          <el-descriptions-item label="宽表 ID">{{ selectedSpec.wide_table_id }}</el-descriptions-item>
          <el-descriptions-item label="来源节点">{{ selectedSpec.source_node }}</el-descriptions-item>
          <el-descriptions-item label="目标表">{{ selectedSpec.target?.database }}.{{ selectedSpec.target?.table }}</el-descriptions-item>
          <el-descriptions-item label="字段">
            {{ Array.isArray(selectedSpec.fields) ? selectedSpec.fields.join(', ') : '-' }}
          </el-descriptions-item>
        </el-descriptions>
      </div>
      <template #footer>
        <el-button @click="specDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="stateDialogVisible" :title="selectedState ? `宽表状态：${selectedState.wide_table_name}` : '宽表状态'" width="860px" destroy-on-close>
      <div v-if="selectedState" class="form-stack">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="宽表名称">{{ selectedState.wide_table_name }}</el-descriptions-item>
          <el-descriptions-item label="来源节点">{{ selectedState.source_node }}</el-descriptions-item>
          <el-descriptions-item label="目标表">{{ selectedState.target_database }}.{{ selectedState.target_table }}</el-descriptions-item>
          <el-descriptions-item label="最近状态">{{ selectedState.last_status || '-' }}</el-descriptions-item>
          <el-descriptions-item label="最近动作">{{ selectedState.last_action || '-' }}</el-descriptions-item>
          <el-descriptions-item label="最近消息">{{ selectedState.last_message || '-' }}</el-descriptions-item>
          <el-descriptions-item label="开始时间">{{ formatDateTime(selectedState.last_started_at) }}</el-descriptions-item>
          <el-descriptions-item label="结束时间">{{ formatDateTime(selectedState.last_finished_at) }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ formatDateTime(selectedState.updated_at) }}</el-descriptions-item>
        </el-descriptions>
      </div>
      <template #footer>
        <el-button @click="stateDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="planDialogVisible" :title="selectedPlan ? `宽表规划：${selectedPlan.wide_table_name}` : '宽表规划'" width="860px" destroy-on-close>
      <div v-if="selectedPlan" class="form-stack">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="宽表名称">{{ selectedPlan.wide_table_name }}</el-descriptions-item>
          <el-descriptions-item label="目标表">{{ selectedPlan.target_database }}.{{ selectedPlan.target_table }}</el-descriptions-item>
          <el-descriptions-item label="动作">{{ selectedPlan.action }}</el-descriptions-item>
          <el-descriptions-item label="原因">{{ selectedPlan.reason || '-' }}</el-descriptions-item>
          <el-descriptions-item label="校验消息">
            {{ Array.isArray(selectedPlan.validation?.messages) && selectedPlan.validation.messages.length ? selectedPlan.validation.messages.join(' | ') : '-' }}
          </el-descriptions-item>
        </el-descriptions>
      </div>
      <template #footer>
        <el-button @click="planDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>
