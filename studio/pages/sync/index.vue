<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useSyncService } from '~/composables/useSyncService'

const {
  tasks,
  configs,
  runningJobs,
  failedJobs,
  recentJobs,
  loadMeta,
  loadJobs,
} = useSyncService()

const tableStatuses = ref([])

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

async function loadTableStatuses() {
  const payload = await api('/api/sync-table-status')
  tableStatuses.value = payload.items || []
}

onMounted(async () => {
  try {
    await Promise.all([loadMeta(), loadJobs({}, true), loadTableStatuses()])
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '同步总览加载失败')
  }
})
</script>

<template>
  <div class="page-stack">
    <section class="query-grid query-grid-wide">
      <el-card shadow="never" class="surface-card">
        <template #header>
          <div class="panel-heading">
            <span class="panel-title">同步总览</span>
          </div>
        </template>
        <div class="hero-metrics">
          <div class="hero-metric">
            <span>注册任务</span>
            <strong>{{ tasks.length }}</strong>
          </div>
          <div class="hero-metric">
            <span>配置文件</span>
            <strong>{{ configs.length }}</strong>
          </div>
          <div class="hero-metric">
            <span>运行中</span>
            <strong>{{ runningJobs.length }}</strong>
          </div>
          <div class="hero-metric">
            <span>失败任务</span>
            <strong>{{ failedJobs.length }}</strong>
          </div>
        </div>
      </el-card>
    </section>

    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading">
          <span class="panel-title">各表同步情况</span>
        </div>
      </template>
      <el-table :data="tableStatuses" row-key="target" empty-text="暂无同步表状态">
        <el-table-column prop="target" label="目标表" min-width="200" />
        <el-table-column prop="database" label="数据库" min-width="140" />
        <el-table-column prop="latest_date" label="最新日期" min-width="160" />
        <el-table-column prop="row_count" label="数量" min-width="120" />
        <el-table-column prop="last_update_time" label="最近更新日期" min-width="190" />
        <el-table-column prop="status" label="状态" width="120" />
        <el-table-column label="挂载任务" min-width="220">
          <template #default="{ row }">
            {{ Array.isArray(row.tasks) && row.tasks.length ? row.tasks.join(', ') : '-' }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card shadow="never" class="surface-card">
      <template #header>
        <div class="panel-heading">
          <span class="panel-title">正在运行的任务</span>
        </div>
      </template>
      <el-table v-if="runningJobs.length" :data="runningJobs" row-key="job_id">
        <el-table-column prop="job_id" label="Job ID" min-width="160" show-overflow-tooltip />
        <el-table-column prop="task" label="任务" min-width="160" />
        <el-table-column prop="kind" label="类型" width="140" />
        <el-table-column prop="status" label="状态" width="120" />
        <el-table-column prop="target" label="目标表" min-width="180" />
        <el-table-column prop="started_at" label="开始时间" min-width="180" />
      </el-table>
      <el-empty v-else description="当前没任务正在运行" />
    </el-card>
  </div>
</template>
