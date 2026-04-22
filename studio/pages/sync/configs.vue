<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useSyncService } from '~/composables/useSyncService'

const {
  configs,
  runConfig,
  loadMeta,
} = useSyncService()

const configSearch = ref('')
const logLevel = ref('INFO')
const configDialogVisible = ref(false)
const configEditorMode = ref('create')
const configForm = ref({
  name: '',
  content: '',
})

const filteredConfigs = computed(() => {
  const keyword = configSearch.value.trim().toLowerCase()
  return configs.value.filter((item) => {
    if (!keyword) return true
    return String(item || '').toLowerCase().includes(keyword)
  })
})

async function handleRunConfig(config) {
  try {
    const result = await runConfig({
      config,
      log_level: logLevel.value,
    })
    ElMessage.success(`配置任务已启动: ${result.job_id}`)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '启动配置任务失败')
  }
}

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

async function openConfigDetail(name) {
  try {
    const payload = await api(`/api/sync-configs/${encodeURIComponent(name)}`)
    configEditorMode.value = 'edit'
    configForm.value = {
      name: payload.name,
      content: payload.content,
    }
    configDialogVisible.value = true
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '读取配置失败')
  }
}

function openCreateConfig() {
  configEditorMode.value = 'create'
  configForm.value = {
    name: 'run_sync.custom.toml',
    content: '# 新建同步配置\nsource = "amazingdata"\nlog_level = "INFO"\n\n[[tasks]]\ntask = "daily_kline"\n',
  }
  configDialogVisible.value = true
}

async function handleSaveConfig() {
  try {
    await api('/api/sync-configs', {
      method: 'POST',
      body: JSON.stringify({
        name: configForm.value.name,
        content: configForm.value.content,
      }),
    })
    ElMessage.success(configEditorMode.value === 'create' ? '配置已新增' : '配置已保存')
    configDialogVisible.value = false
    await loadMeta(true)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存配置失败')
  }
}

onMounted(async () => {
  await loadMeta()
})
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading panel-heading-space">
          <div>
            <span class="panel-title">配置同步</span>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-select v-model="logLevel" style="width: 150px">
              <el-option label="INFO" value="INFO" />
              <el-option label="DEBUG" value="DEBUG" />
              <el-option label="WARNING" value="WARNING" />
            </el-select>
            <el-input
              v-model="configSearch"
              clearable
              placeholder="搜索配置文件"
              class="panel-search panel-search-wide"
            />
            <el-button type="primary" @click="openCreateConfig">新增配置</el-button>
            <el-button @click="loadMeta(true)">刷新配置</el-button>
          </div>
        </div>
      </template>

      <el-table :data="filteredConfigs.map((item) => ({ name: item }))" row-key="name" empty-text="暂无配置文件">
        <el-table-column prop="name" label="配置文件" min-width="320" />
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <div class="binding-action-group">
              <el-button link @click="openConfigDetail(row.name)">查看</el-button>
              <el-button link type="primary" @click="handleRunConfig(row.name)">执行</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="configDialogVisible" :title="configEditorMode === 'create' ? '新增配置' : '查看 / 编辑配置'" width="860px" destroy-on-close>
      <div class="form-stack">
        <el-form-item label="配置文件名">
          <el-input v-model="configForm.name" placeholder="例如 run_sync.custom.toml" />
        </el-form-item>
        <el-form-item label="内容">
          <el-input v-model="configForm.content" type="textarea" :rows="24" />
        </el-form-item>
      </div>
      <template #footer>
        <el-button @click="configDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="handleSaveConfig">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>
