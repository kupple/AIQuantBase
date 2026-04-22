<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { formatDateTime } from '~/composables/useDateTimeFormat'
import { useSyncService } from '~/composables/useSyncService'

const {
  tasks,
  jobs,
  selectedJob,
  jobLogs,
  loadMeta,
  loadJobs,
  loadJobDetail,
  cancelJob,
} = useSyncService()

const jobFilters = ref({
  status: '',
  task: '',
  kind: '',
})
const jobDetailDialogVisible = ref(false)
const jobLoading = ref(false)

async function refreshJobs() {
  await loadJobs(jobFilters.value, true)
}

async function handleCancelJob(job) {
  try {
    await ElMessageBox.confirm(`确定取消任务 ${job.job_id} 吗？`, '取消任务', {
      type: 'warning',
      confirmButtonText: '取消任务',
      cancelButtonText: '返回',
    })
    const result = await cancelJob(job.job_id)
    ElMessage.success(`任务取消中: ${result.job_id}`)
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error instanceof Error ? error.message : '取消任务失败')
  }
}

async function openJobDetailDialog(job) {
  jobLoading.value = true
  try {
    await loadJobDetail(job.job_id)
    jobDetailDialogVisible.value = true
  } finally {
    jobLoading.value = false
  }
}

onMounted(async () => {
  await Promise.all([loadMeta(), refreshJobs()])
})
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading panel-heading-space">
          <div>
            <span class="panel-title">同步记录</span>
          </div>
          <div class="panel-actions panel-actions-compact">
            <el-select v-model="jobFilters.status" clearable placeholder="状态" style="width: 150px" @change="refreshJobs">
              <el-option label="running" value="running" />
              <el-option label="success" value="success" />
              <el-option label="failed" value="failed" />
              <el-option label="cancelled" value="cancelled" />
            </el-select>
            <el-select v-model="jobFilters.task" clearable filterable placeholder="任务" style="width: 180px" @change="refreshJobs">
              <el-option v-for="item in tasks" :key="item.name" :label="item.name" :value="item.name" />
            </el-select>
            <el-select v-model="jobFilters.kind" clearable placeholder="类型" style="width: 160px" @change="refreshJobs">
              <el-option label="config" value="config" />
              <el-option label="task" value="task" />
              <el-option label="registered_task" value="registered_task" />
            </el-select>
            <el-button @click="refreshJobs">刷新</el-button>
          </div>
        </div>
      </template>

      <el-table :data="jobs" row-key="job_id" height="680">
        <el-table-column prop="job_id" label="Job ID" min-width="160" show-overflow-tooltip />
        <el-table-column prop="task" label="任务" min-width="140" />
        <el-table-column prop="kind" label="类型" width="140" />
        <el-table-column prop="status" label="状态" width="120" />
        <el-table-column prop="target" label="目标表" min-width="160" />
        <el-table-column label="创建时间" min-width="180">
          <template #default="{ row }">
            {{ formatDateTime(row.created_at) }}
          </template>
        </el-table-column>
          <el-table-column label="操作" width="120" fixed="right">
            <template #default="{ row }">
              <div class="binding-action-group">
                <el-button link @click.stop="openJobDetailDialog(row)">查看</el-button>
                <el-button v-if="row.status === 'running'" link type="danger" @click.stop="handleCancelJob(row)">取消</el-button>
              </div>
            </template>
          </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="jobDetailDialogVisible" :title="selectedJob ? `同步记录：${selectedJob.job_id}` : '同步记录'" width="920px" destroy-on-close>
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
          <el-descriptions-item label="开始时间">{{ formatDateTime(selectedJob.started_at) }}</el-descriptions-item>
          <el-descriptions-item label="结束时间">{{ formatDateTime(selectedJob.finished_at) }}</el-descriptions-item>
          <el-descriptions-item label="错误" :span="2">{{ selectedJob.error || '-' }}</el-descriptions-item>
        </el-descriptions>
        <el-input :model-value="jobLogs" type="textarea" :rows="22" readonly :loading="jobLoading" />
      </div>
      <template #footer>
        <el-button @click="jobDetailDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>
