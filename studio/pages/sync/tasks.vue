<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useSyncService } from '~/composables/useSyncService'

const {
  tasks,
  loadMeta,
  runTask,
  blankTaskPayload,
} = useSyncService()

const taskSearch = ref('')
const selectedTask = ref(null)
const taskDialogVisible = ref(false)
const taskDetailDialogVisible = ref(false)
const taskForm = ref(blankTaskPayload())

const filteredTasks = computed(() => {
  const keyword = taskSearch.value.trim().toLowerCase()
  return tasks.value.filter((item) => {
    if (!keyword) return true
    return [
      item.name,
      item.source,
      item.target,
      item.input_resolver,
      ...(item.request_fields || []),
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .includes(keyword)
  })
})

const selectedTaskRequestFields = computed(() =>
  Array.isArray(selectedTask.value?.request_fields) ? selectedTask.value.request_fields : []
)

function hasRequestField(field) {
  return selectedTaskRequestFields.value.includes(field)
}

function openTaskDialog(task) {
  selectedTask.value = task
  taskForm.value = {
    ...blankTaskPayload(),
    name: task.name,
  }
  taskDialogVisible.value = true
}

function openTaskDetailDialog(task) {
  selectedTask.value = task
  taskDetailDialogVisible.value = true
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
    }
    if (hasRequestField('codes')) {
      const codes = taskForm.value.codesText
        .split(/[\s,]+/)
        .map((item) => item.trim())
        .filter(Boolean)
      if (codes.length) payload.codes = codes
    }
    if (hasRequestField('begin_date') && taskForm.value.begin_date) payload.begin_date = Number(taskForm.value.begin_date)
    if (hasRequestField('end_date') && taskForm.value.end_date) payload.end_date = Number(taskForm.value.end_date)
    if (hasRequestField('limit') && taskForm.value.limit !== '') payload.limit = Number(taskForm.value.limit)
    if (hasRequestField('force')) payload.force = Boolean(taskForm.value.force)
    if (hasRequestField('resume')) payload.resume = Boolean(taskForm.value.resume)
    const result = await runTask(payload)
    ElMessage.success(`任务已启动: ${result.job_id}`)
    taskDialogVisible.value = false
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '启动任务失败')
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
            <span class="panel-title">同步任务列表</span>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-input
              v-model="taskSearch"
              clearable
              placeholder="搜索任务 / 目标表 / 数据源"
              class="panel-search panel-search-wide"
            />
            <el-button @click="loadMeta(true)">刷新任务</el-button>
          </div>
        </div>
      </template>

      <el-table :data="filteredTasks" row-key="name" empty-text="没有可用同步任务">
        <el-table-column prop="name" label="任务名" min-width="180" />
        <el-table-column prop="source" label="数据源" min-width="130" />
        <el-table-column prop="target" label="目标表" min-width="180" />
        <el-table-column prop="input_resolver" label="输入解析器" min-width="180" />
        <el-table-column label="请求参数" min-width="240">
          <template #default="{ row }">
            {{ Array.isArray(row.request_fields) && row.request_fields.length ? row.request_fields.join(', ') : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="140" fixed="right">
          <template #default="{ row }">
            <div class="binding-action-group">
              <el-button link type="primary" @click="openTaskDetailDialog(row)">查看</el-button>
              <el-button link @click="openTaskDialog(row)">同步</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="taskDialogVisible" :title="selectedTask ? `同步任务：${selectedTask.name}` : '同步任务'" width="760px" destroy-on-close>
      <div class="form-stack">
        <div class="mini-description">
          {{ selectedTask ? `目标表：${selectedTask.target} ｜ 数据源：${selectedTask.source}` : '请填写任务参数' }}
        </div>
        <el-form-item label="任务名">
          <el-input :model-value="taskForm.name" disabled />
        </el-form-item>
        <el-form-item v-if="hasRequestField('codes')" label="代码列表">
          <el-input v-model="taskForm.codesText" type="textarea" :rows="4" placeholder="逗号或空格分隔，例如 000001.SZ 510300.SH" />
        </el-form-item>
        <div class="three-col-form">
          <el-form-item v-if="hasRequestField('begin_date')" label="开始日期">
            <el-input v-model="taskForm.begin_date" placeholder="例如 20240101" />
          </el-form-item>
          <el-form-item v-if="hasRequestField('end_date')" label="结束日期">
            <el-input v-model="taskForm.end_date" placeholder="例如 20240131" />
          </el-form-item>
          <el-form-item v-if="hasRequestField('limit')" label="限制条数">
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
          <el-form-item v-if="hasRequestField('force')" label="Force">
            <el-switch v-model="taskForm.force" />
          </el-form-item>
          <el-form-item v-if="hasRequestField('resume')" label="Resume">
            <el-switch v-model="taskForm.resume" />
          </el-form-item>
        </div>
      </div>
      <template #footer>
        <el-button @click="taskDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="handleRunTask">开始同步</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="taskDetailDialogVisible" :title="selectedTask ? `任务说明：${selectedTask.name}` : '任务说明'" width="760px" destroy-on-close>
      <div v-if="selectedTask" class="form-stack">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="任务名">{{ selectedTask.name }}</el-descriptions-item>
          <el-descriptions-item label="数据源">{{ selectedTask.source }}</el-descriptions-item>
          <el-descriptions-item label="目标表">{{ selectedTask.target }}</el-descriptions-item>
          <el-descriptions-item label="输入解析器">{{ selectedTask.input_resolver }}</el-descriptions-item>
          <el-descriptions-item label="请求字段">
            {{ Array.isArray(selectedTask.request_fields) && selectedTask.request_fields.length ? selectedTask.request_fields.join(', ') : '-' }}
          </el-descriptions-item>
          <el-descriptions-item label="回传字段">
            {{ Array.isArray(selectedTask.probe_fields) && selectedTask.probe_fields.length ? selectedTask.probe_fields.join(', ') : '-' }}
          </el-descriptions-item>
        </el-descriptions>
      </div>
      <template #footer>
        <el-button @click="taskDetailDialogVisible = false">关闭</el-button>
        <el-button v-if="selectedTask" type="primary" @click="taskDetailDialogVisible = false; openTaskDialog(selectedTask)">同步这个任务</el-button>
      </template>
    </el-dialog>
  </div>
</template>
