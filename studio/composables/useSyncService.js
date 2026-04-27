import { computed } from 'vue'
import { useState } from '#imports'

function blankTaskPayload() {
  return {
    name: '',
    codesText: '',
    day: '',
    begin_date: '',
    end_date: '',
    year: '',
    quarter: '',
    year_type: '',
    limit: '',
    force: false,
    resume: false,
    adjustflag: '3',
    frequency: 'd',
    log_level: 'INFO',
  }
}

export function useSyncService() {
  const tasks = useState('sync-tasks', () => [])
  const configs = useState('sync-configs', () => [])
  const jobs = useState('sync-jobs', () => [])
  const selectedJob = useState('sync-selected-job', () => null)
  const jobLogs = useState('sync-job-logs', () => '')
  const loading = useState('sync-loading', () => false)
  const metaLoaded = useState('sync-meta-loaded', () => false)
  const jobsLoaded = useState('sync-jobs-loaded', () => false)

  const runningJobs = computed(() => jobs.value.filter((item) => item.status === 'running'))
  const failedJobs = computed(() => jobs.value.filter((item) => item.status === 'failed'))
  const recentJobs = computed(() => jobs.value.slice(0, 10))

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

  async function loadMeta(force = false) {
    if (metaLoaded.value && !force) return
    const [taskPayload, configPayload] = await Promise.all([
      api('/api/sync/meta/tasks'),
      api('/api/sync/meta/configs'),
    ])
    tasks.value = taskPayload.registered_tasks || []
    configs.value = configPayload.configs || []
    metaLoaded.value = true
  }

  async function loadJobs(filters = {}, force = false) {
    if (jobsLoaded.value && !force && !Object.keys(filters).length) return
    const search = new URLSearchParams()
    Object.entries(filters || {}).forEach(([key, value]) => {
      if (value !== undefined && value !== null && String(value).trim() !== '') {
        search.set(key, String(value))
      }
    })
    const payload = await api(`/api/sync/jobs${search.toString() ? `?${search.toString()}` : ''}`)
    jobs.value = payload.jobs || []
    jobsLoaded.value = true
  }

  async function loadJobDetail(jobId, tailLines = 200) {
    if (!jobId) return null
    const payload = await api(`/api/sync/jobs/${encodeURIComponent(jobId)}?tail_lines=${tailLines}`)
    selectedJob.value = payload
    jobLogs.value = payload.logs_tail || ''
    return payload
  }

  async function runTask(payload) {
    const result = await api('/api/sync/jobs/run-task', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
    await loadJobs({}, true)
    await loadJobDetail(result.job_id)
    return result
  }

  async function runConfig(payload) {
    const result = await api('/api/sync/jobs/run-config', {
      method: 'POST',
      body: JSON.stringify(payload),
    })
    await loadJobs({}, true)
    await loadJobDetail(result.job_id)
    return result
  }

  async function cancelJob(jobId) {
    const result = await api(`/api/sync/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    })
    await loadJobs({}, true)
    await loadJobDetail(result.job_id)
    return result
  }

  return {
    tasks,
    configs,
    jobs,
    selectedJob,
    jobLogs,
    loading,
    runningJobs,
    failedJobs,
    recentJobs,
    blankTaskPayload,
    loadMeta,
    loadJobs,
    loadJobDetail,
    runTask,
    runConfig,
    cancelJob,
  }
}
