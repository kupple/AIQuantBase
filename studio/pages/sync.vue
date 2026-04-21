<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'

const loading = ref(false)
const tasks = ref([])
const configs = ref([])
const jobs = ref([])
const selectedJob = ref(null)
const jobLogs = ref('')
const jobLoading = ref(false)

const taskForm = ref({
  name: '',
  codesText: '',
  begin_date: '',
  end_date: '',
  limit: null,
  force: false,
  resume: false,
  log_level: 'INFO',
})

const configForm = ref({
  config: '',
  log_level: 'INFO',
})

const jobFilters = ref({
  status: '',
  task: '',
  kind: '',
})

const taskOptions = computed(() => tasks.value.map((item) => item.name))

const selectedTaskMeta = computed(() =>
  tasks.value.find((item) => item.name === taskForm.value.name) || null
)

const filteredJobs = computed(() =>
  jobs.value.filter((job) => {
    if (jobFilters.value.status && job.status !== jobFilters.value.status) return false
    if (jobFilters.value.task && job.task !== jobFilters.value.task) return false
    if (jobFilters.value.kind && job.kind !== jobFilters.value.kind) return false
    return true
  })
)

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

async function loadMeta() {
  const [taskPayload, configPayload] = await Promise.all([
    api('/api/sync/meta/tasks'),
    api('/api/sync/meta/configs'),
  ])
  tasks.value = taskPayload.registered_tasks || []
  configs.value = taskPayload.configs || configPayload.configs || []
  if (!configForm.value.config && configPayload.configs?.length) {
    configForm.value.config = configPayload.configs[0]
  }
}

async function loadJobs() {
  const search = new URLSearchParams()
  if (jobFilters.value.status) search.set('status', jobFilters.value.status)
  if (jobFilters.value.task) search.set('task', jobFilters.value.task)
  if (jobFilters.value.kind) search.set('kind', jobFilters.value.kind)
  const payload = await api(`/api/sync/jobs${search.toString() ? `?${search.toString()}` : ''}`)
  jobs.value = payload.jobs || []
}

async function loadJobDetail(jobId) {
  if (!jobId) return
  jobLoading.value = true
  try {
    const payload = await api(`/api/sync/jobs/${encodeURIComponent(jobId)}?tail_lines=200`)
    selectedJob.value = payload
    jobLogs.value = payload.logs_tail || ''
  } finally {
    jobLoading.value = false
  }
}

async function handleRunTask() {
  if (!taskForm.value.name) {
    ElMessage.warning('请先选择任务')
    return
  }
  try {
    const payload = {
      name: taskForm.value.name,
      log_level: taskForm.value.log_level,
      force: taskForm.value.force,
      resume: taskForm.value.resume,
    }
    const codes = taskForm.value.codesText
      .split(/[\s,]+/)
      .map((item) => item.trim())
      .filter(Boolean)
    if (codes.length) payload.codes = codes
    if (taskForm.value.begin_date) payload.begin_date = Number(taskForm.value.begin_date)
    if (taskForm.value.end_date) payload.end_date = Number(taskForm.value.end_date)
    if (taskForm.value.limit) payload.limit = Number(taskForm.value.limit)

    const result = await api('/api/sync/jobs/run-task', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
    ElMessage.success(`任务已启动: ${result.job_id}`)
    await loadJobs()
    await loadJobDetail(result.job_id)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '启动任务失败')
  }
}

async function handleRunConfig() {
  if (!configForm.value.config) {
    ElMessage.warning('请先选择配置')
    return
  }
  try {
    const result = await api('/api/sync/jobs/run-config', {
      method: 'POST',
      body: JSON.stringify({
        config: configForm.value.config,
        log_level: configForm.value.log_level,
      }),
    })
    ElMessage.success(`配置任务已启动: ${result.job_id}`)
    await loadJobs()
    await loadJobDetail(result.job_id)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '启动配置任务失败')
  }
}

async function handleCancelJob(job) {
  try {
    const result = await api(`/api/sync/jobs/${encodeURIComponent(job.job_id)}/cancel`, {
      method: 'POST',
    })
    ElMessage.success(`任务取消中: ${result.job_id}`)
    await loadJobs()
    await loadJobDetail(result.job_id)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '取消任务失败')
  }
}

onMounted(async () => {
  loading.value = true
  try {
    await Promise.all([loadMeta(), loadJobs()])
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '同步管理页加载失败')
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <div class="page-stack">
    <section class="query-grid query-grid-wide">
      <el-card shadow="never" class="surface-card">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">启动同步任务</span>
            </div>
          </div>
        </template>
        <div class="form-stack">
          <el-form-item label="任务名">
            <el-select v-model="taskForm.name" filterable clearable placeholder="选择注册任务" style="width: 100%">
              <el-option v-for="item in taskOptions" :key="item" :label="item" :value="item" />
            </el-select>
          </el-form-item>
          <el-form-item label="代码列表">
            <el-input v-model="taskForm.codesText" type="textarea" :rows="3" placeholder="可选，逗号或空格分隔" />
          </el-form-item>
          <div class="three-col-form">
            <el-form-item label="开始日期">
              <el-input v-model="taskForm.begin_date" placeholder="例如 20240101" />
            </el-form-item>
            <el-form-item label="结束日期">
              <el-input v-model="taskForm.end_date" placeholder="例如 20240131" />
            </el-form-item>
            <el-form-item label="限制条数">
              <el-input v-model="taskForm.limit" placeholder="可选" />
            </el-form-item>
          </div>
          <div class="three-col-form">
            <el-form-item label="日志级别">
              <el-select v-model="taskForm.log_level" style="width: 100%">
                <el-option label="INFO" value="INFO" />
                <el-option label="DEBUG" value="DEBUG" />
                <el-option label="WARNING" value="WARNING" />
              </el-select>
            </el-form-item>
            <el-form-item label="Force">
              <el-switch v-model="taskForm.force" />
            </el-form-item>
            <el-form-item label="Resume">
              <el-switch v-model="taskForm.resume" />
            </el-form-item>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-button type="primary" @click="handleRunTask">启动任务</el-button>
          </div>
          <div v-if="selectedTaskMeta" class="mini-description">
            目标表：{{ selectedTaskMeta.target }} ｜ 数据源：{{ selectedTaskMeta.source }} ｜ 输入解析器：{{ selectedTaskMeta.input_resolver }}
          </div>
        </div>
      </el-card>

      <el-card shadow="never" class="surface-card">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">启动配置同步</span>
            </div>
          </div>
        </template>
        <div class="form-stack">
          <el-form-item label="配置文件">
            <el-select v-model="configForm.config" filterable clearable placeholder="选择配置文件" style="width: 100%">
              <el-option v-for="item in configs" :key="item" :label="item" :value="item" />
            </el-select>
          </el-form-item>
          <el-form-item label="日志级别">
            <el-select v-model="configForm.log_level" style="width: 100%">
              <el-option label="INFO" value="INFO" />
              <el-option label="DEBUG" value="DEBUG" />
              <el-option label="WARNING" value="WARNING" />
            </el-select>
          </el-form-item>
          <div class="panel-actions panel-actions-compact">
            <el-button type="primary" @click="handleRunConfig">启动配置任务</el-button>
          </div>
        </div>
      </el-card>
    </section>

    <section class="workbench-grid">
      <el-card shadow="never" class="surface-card">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">任务列表</span>
            </div>
            <div class="panel-actions panel-actions-compact">
              <el-select v-model="jobFilters.status" clearable placeholder="状态" style="width: 150px" @change="loadJobs">
                <el-option label="running" value="running" />
                <el-option label="success" value="success" />
                <el-option label="failed" value="failed" />
                <el-option label="cancelled" value="cancelled" />
              </el-select>
              <el-select v-model="jobFilters.task" clearable filterable placeholder="任务" style="width: 180px" @change="loadJobs">
                <el-option v-for="item in taskOptions" :key="item" :label="item" :value="item" />
              </el-select>
              <el-select v-model="jobFilters.kind" clearable placeholder="类型" style="width: 160px" @change="loadJobs">
                <el-option label="config" value="config" />
                <el-option label="task" value="task" />
                <el-option label="registered_task" value="registered_task" />
              </el-select>
              <el-button @click="loadJobs">刷新</el-button>
            </div>
          </div>
        </template>

        <el-table :data="filteredJobs" row-key="job_id" height="560" @row-click="(row) => loadJobDetail(row.job_id)">
          <el-table-column prop="job_id" label="Job ID" min-width="160" show-overflow-tooltip />
          <el-table-column prop="task" label="任务" min-width="140" />
          <el-table-column prop="kind" label="类型" width="140" />
          <el-table-column prop="status" label="状态" width="120" />
          <el-table-column prop="target" label="目标表" min-width="160" />
          <el-table-column prop="created_at" label="创建时间" min-width="180" />
          <el-table-column label="操作" width="120" fixed="right">
            <template #default="{ row }">
              <div class="binding-action-group">
                <el-button v-if="row.status === 'running'" link type="danger" @click.stop="handleCancelJob(row)">取消</el-button>
                <el-button v-else link @click.stop="loadJobDetail(row.job_id)">查看</el-button>
              </div>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <el-card shadow="never" class="surface-card">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">任务详情 / 日志</span>
            </div>
          </div>
        </template>
        <div v-if="selectedJob" class="form-stack">
          <el-descriptions :column="2" border>
            <el-descriptions-item label="Job ID">{{ selectedJob.job_id }}</el-descriptions-item>
            <el-descriptions-item label="状态">{{ selectedJob.status }}</el-descriptions-item>
            <el-descriptions-item label="类型">{{ selectedJob.kind }}</el-descriptions-item>
            <el-descriptions-item label="任务">{{ selectedJob.task || '-' }}</el-descriptions-item>
            <el-descriptions-item label="来源">{{ selectedJob.source || '-' }}</el-descriptions-item>
            <el-descriptions-item label="目标">{{ selectedJob.target || '-' }}</el-descriptions-item>
            <el-descriptions-item label="配置">{{ selectedJob.config_path || '-' }}</el-descriptions-item>
            <el-descriptions-item label="PID">{{ selectedJob.pid || '-' }}</el-descriptions-item>
            <el-descriptions-item label="开始时间">{{ selectedJob.started_at || '-' }}</el-descriptions-item>
            <el-descriptions-item label="结束时间">{{ selectedJob.finished_at || '-' }}</el-descriptions-item>
            <el-descriptions-item label="错误" :span="2">{{ selectedJob.error || '-' }}</el-descriptions-item>
          </el-descriptions>
          <el-input :model-value="jobLogs" type="textarea" :rows="20" readonly />
        </div>
        <el-empty v-else description="点击左侧任务查看详情和日志" />
      </el-card>
    </section>
  </div>
</template>
