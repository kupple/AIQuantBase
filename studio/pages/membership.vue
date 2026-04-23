<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

const runtimePath = ref('config/runtime.local.yaml')
const membershipPath = ref('config/membership.yaml')
const loading = ref(false)
const databases = ref([])
const tables = ref([])
const columns = ref([])
const lookupTables = ref([])
const lookupColumns = ref([])
const sources = ref([])
const domains = ref([])
const sourceSearch = ref('')
const sourceDomainFilter = ref('')
const sourceDialogVisible = ref(false)
const previewLoading = ref(false)
const previewResult = ref({
  sql: '',
  taxonomy_preview: null,
  member_preview: [],
  relation_preview: [],
  raw_rows: [],
})

const sourceForm = ref(blankSource())
const selectedSourceId = ref('')

const filteredSources = computed(() => {
  const keyword = sourceSearch.value.trim().toLowerCase()
  return sources.value.filter((item) => {
    if (sourceDomainFilter.value && item.domain !== sourceDomainFilter.value) return false
    if (!keyword) return true
    return [
      item.source_name,
      item.database,
      item.table,
      item.domain,
      item.taxonomy,
      item.description,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .includes(keyword)
  })
})

const selectedSource = computed(() =>
  sources.value.find((item) => item.id === selectedSourceId.value) || null
)

const mappingRows = computed(() => (
  sourceForm.value.source_kind === 'member_dimension'
    ? [
        {
          label: '归属代码',
          field: sourceForm.value.member_code_field || '-',
          fallback: sourceForm.value.member_code_value || '-',
          desc: '例如 industry_code',
        },
        {
          label: '归属名称',
          field: sourceForm.value.member_name_field || '-',
          fallback: sourceForm.value.member_name_value || '-',
          desc: '例如 level3_name',
        },
        {
          label: '附加信息表',
          field: sourceForm.value.lookup_database && sourceForm.value.lookup_table ? `${sourceForm.value.lookup_database}.${sourceForm.value.lookup_table}` : '-',
          fallback: '-',
          desc: '可选，从另一张表补充名字和附加字段',
        },
        {
          label: '当前表 code 映射字段',
          field: sourceForm.value.lookup_source_field || '-',
          fallback: '-',
          desc: '例如 con_code，用来和附加信息表关联',
        },
        {
          label: '附加表 code 映射字段',
          field: sourceForm.value.lookup_target_field || '-',
          fallback: '-',
          desc: '例如 con_code / code',
        },
        {
          label: '来源系统',
          field: sourceForm.value.source_system_field || '-',
          fallback: sourceForm.value.source_system_value || '-',
          desc: '用于记录这批成员信息从哪里来',
        },
        ...sourceForm.value.attribute_mappings.map((item) => ({
          label: item.label || '附加字段',
          field: item.field || '-',
          fallback: '-',
          desc: '成员信息附加字段',
        })),
      ]
    : [
        {
          label: '股票代码',
          field: sourceForm.value.security_code_field || '-',
          fallback: '-',
          desc: '一般选择 code / security_code',
        },
        {
          label: '归属代码',
          field: sourceForm.value.member_code_field || '-',
          fallback: sourceForm.value.member_code_value || '-',
          desc: '例如 index_code / industry_code',
        },
        {
          label: '归属名称',
          field: sourceForm.value.member_name_field || '-',
          fallback: sourceForm.value.member_name_value || '-',
          desc: '例如 index_name / industry_name',
        },
        {
          label: '生效日期',
          field: sourceForm.value.effective_from_field || '-',
          fallback: '-',
          desc: '一般选择 trade_date',
        },
        {
          label: '结束日期',
          field: sourceForm.value.effective_to_field || '-',
          fallback: '-',
          desc: '如果没有可以留空',
        },
        {
          label: '来源系统',
          field: sourceForm.value.source_system_field || '-',
          fallback: sourceForm.value.source_system_value || '-',
          desc: '用于记录这批关系从哪里来',
        },
      ]
))

const sourceKindLabel = computed(() =>
  sourceForm.value.source_kind === 'member_dimension' ? '成员信息表' : '绑定关系表'
)

const columnOptions = computed(() => columns.value.map((item) => item.name))
const lookupColumnOptions = computed(() => lookupColumns.value.map((item) => item.name))
const previewAttributeColumns = computed(() => previewResult.value.attribute_columns || [])
const canPreview = computed(() =>
  Boolean(
    sourceForm.value.database &&
    sourceForm.value.table &&
    sourceForm.value.domain &&
    sourceForm.value.taxonomy &&
    (
      sourceForm.value.source_kind === 'member_dimension'
        ? (sourceForm.value.member_code_field || sourceForm.value.member_code_value)
        : (
            sourceForm.value.security_code_field &&
            (sourceForm.value.member_code_field || sourceForm.value.member_code_value)
          )
    )
  )
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
    const message = typeof payload === 'string' ? payload : payload?.detail || payload?.message || payload?.error || '请求失败'
    throw new Error(message)
  }
  return payload
}

async function loadWorkspace() {
  loading.value = true
  try {
    const payload = await api(`/api/membership/workspace?membership_path=${encodeURIComponent(membershipPath.value)}`)
    membershipPath.value = payload.workspace?.membership_path || membershipPath.value
    sources.value = payload.sources || []
    domains.value = payload.domains || []
    if (!selectedSourceId.value && sources.value.length) {
      await selectSource(sources.value[0])
    }
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : 'membership 工作区载入失败')
  } finally {
    loading.value = false
  }
}

async function loadDatabases() {
  try {
    const payload = await api(`/api/schema/databases?runtime_path=${encodeURIComponent(runtimePath.value)}`)
    databases.value = payload.items || []
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '数据库列表加载失败')
  }
}

async function loadTables(database) {
  if (!database) {
    tables.value = []
    columns.value = []
    return
  }
  const payload = await api(
    `/api/schema/tables?runtime_path=${encodeURIComponent(runtimePath.value)}&database=${encodeURIComponent(database)}`
  )
  tables.value = payload.items || []
}

async function loadColumns(database, table) {
  if (!database || !table) {
    columns.value = []
    return
  }
  const payload = await api(
    `/api/schema/columns?runtime_path=${encodeURIComponent(runtimePath.value)}&database=${encodeURIComponent(database)}&table=${encodeURIComponent(table)}`
  )
  columns.value = payload.items || []
}

async function loadLookupTables(database) {
  if (!database) {
    lookupTables.value = []
    lookupColumns.value = []
    return
  }
  const payload = await api(
    `/api/schema/tables?runtime_path=${encodeURIComponent(runtimePath.value)}&database=${encodeURIComponent(database)}`
  )
  lookupTables.value = payload.items || []
}

async function loadLookupColumns(database, table) {
  if (!database || !table) {
    lookupColumns.value = []
    return
  }
  const payload = await api(
    `/api/schema/columns?runtime_path=${encodeURIComponent(runtimePath.value)}&database=${encodeURIComponent(database)}&table=${encodeURIComponent(table)}`
  )
  lookupColumns.value = payload.items || []
}

async function selectSource(source) {
  selectedSourceId.value = source.id
  sourceForm.value = {
    ...blankSource(),
    ...source,
    attribute_mappings: Array.isArray(source.attribute_mappings) ? source.attribute_mappings.map((item) => ({ ...item })) : [],
  }
  await loadTables(source.database)
  await loadColumns(source.database, source.table)
  await loadLookupTables(source.lookup_database)
  await loadLookupColumns(source.lookup_database, source.lookup_table)
  previewResult.value = {
    sql: '',
    taxonomy_preview: null,
    member_preview: [],
    relation_preview: [],
    raw_rows: [],
    lookup_rows: [],
    lookup_sql: '',
    attribute_columns: [],
  }
}

function openCreateSource() {
  selectedSourceId.value = ''
  sourceForm.value = blankSource()
  tables.value = []
  columns.value = []
  lookupTables.value = []
  lookupColumns.value = []
  previewResult.value = {
    sql: '',
    taxonomy_preview: null,
    member_preview: [],
    relation_preview: [],
    raw_rows: [],
    lookup_rows: [],
    lookup_sql: '',
    attribute_columns: [],
  }
  sourceDialogVisible.value = true
}

async function openEditSource(source) {
  await selectSource(source)
  sourceDialogVisible.value = true
}

async function saveSource() {
  try {
    await api('/api/membership/sources', {
      method: 'POST',
      body: JSON.stringify({
        membership_path: membershipPath.value,
        source: sourceForm.value,
      }),
    })
    ElMessage.success('source 配置已保存')
    sourceDialogVisible.value = false
    await loadWorkspace()
    const saved = sources.value.find((item) => item.source_name === sourceForm.value.source_name)
    if (saved) {
      await selectSource(saved)
    }
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存 source 配置失败')
  }
}

async function removeSource(source) {
  try {
    await ElMessageBox.confirm(`确认删除配置“${source.source_name}”？`, '删除配置', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    })
    await api(
      `/api/membership/sources?membership_path=${encodeURIComponent(membershipPath.value)}&id=${encodeURIComponent(source.id)}`,
      {
        method: 'DELETE',
      }
    )
    ElMessage.success('配置已删除')
    if (selectedSourceId.value === source.id) {
      selectedSourceId.value = ''
      sourceForm.value = blankSource()
      lookupTables.value = []
      lookupColumns.value = []
      previewResult.value = {
        sql: '',
        taxonomy_preview: null,
        member_preview: [],
        relation_preview: [],
        raw_rows: [],
        lookup_rows: [],
        lookup_sql: '',
        attribute_columns: [],
      }
    }
    await loadWorkspace()
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error instanceof Error ? error.message : '删除配置失败')
  }
}

async function runPreview() {
  if (!canPreview.value) {
    ElMessage.warning('请先选择表并完成关键字段映射')
    return
  }
  previewLoading.value = true
  try {
    const payload = await api('/api/membership/source-preview', {
      method: 'POST',
      body: JSON.stringify({
        runtime_path: runtimePath.value,
        source: sourceForm.value,
        limit: 100,
      }),
    })
    previewResult.value = payload
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '标准化预览失败')
  } finally {
    previewLoading.value = false
  }
}

function blankSource() {
  return {
    id: '',
    source_name: '',
    source_kind: 'relation',
    database: '',
    table: '',
    domain: '',
    taxonomy: '',
    security_type: 'stock',
    status: 'enabled',
    description: '',
    lookup_database: '',
    lookup_table: '',
    lookup_source_field: '',
    lookup_target_field: '',
    lookup_member_name_field: '',
    security_code_field: '',
    member_code_field: '',
    member_name_field: '',
    effective_from_field: '',
    effective_to_field: '',
    source_system_field: '',
    member_code_value: '',
    member_name_value: '',
    source_system_value: 'source_mapping',
    attribute_mappings: [],
  }
}

function addAttributeMapping() {
  sourceForm.value.attribute_mappings.push({
    key: `attr_${sourceForm.value.attribute_mappings.length + 1}`,
    label: '',
    field: '',
  })
}

function removeAttributeMapping(index) {
  sourceForm.value.attribute_mappings.splice(index, 1)
}

watch(
  () => sourceForm.value.database,
  async (value, oldValue) => {
    if (value === oldValue) return
    sourceForm.value.table = ''
    columns.value = []
    await loadTables(value)
  }
)

watch(
  () => sourceForm.value.table,
  async (value, oldValue) => {
    if (value === oldValue) return
    await loadColumns(sourceForm.value.database, value)
  }
)

watch(
  () => sourceForm.value.lookup_database,
  async (value, oldValue) => {
    if (value === oldValue) return
    sourceForm.value.lookup_table = ''
    sourceForm.value.lookup_target_field = ''
    sourceForm.value.lookup_member_name_field = ''
    lookupColumns.value = []
    await loadLookupTables(value)
  }
)

watch(
  () => sourceForm.value.lookup_table,
  async (value, oldValue) => {
    if (value === oldValue) return
    await loadLookupColumns(sourceForm.value.lookup_database, value)
  }
)

onMounted(async () => {
  await Promise.all([loadWorkspace(), loadDatabases()])
})
</script>

<template>
  <div class="membership-page">
    <section class="panel">
      <div class="table-toolbar">
        <div class="toolbar-left">
          <el-input v-model="sourceSearch" placeholder="搜索配置名 / 表名 / 分类体系" clearable />
          <el-select v-model="sourceDomainFilter" placeholder="归属类型" clearable>
            <el-option v-for="item in domains" :key="item.domain" :label="item.domain" :value="item.domain" />
          </el-select>
        </div>
        <div class="toolbar-right">
          <el-button :loading="loading" @click="loadWorkspace">重新载入</el-button>
          <el-button type="primary" @click="openCreateSource">新增配置</el-button>
        </div>
      </div>

      <el-table
        :data="filteredSources"
        row-key="id"
        highlight-current-row
        class="main-table"
        @current-change="(row) => row && selectSource(row)"
        @row-click="selectSource"
      >
        <el-table-column prop="source_name" label="配置名" min-width="180" />
        <el-table-column label="来源类型" min-width="120">
          <template #default="{ row }">
            {{ row.source_kind === 'member_dimension' ? '成员信息表' : '绑定关系表' }}
          </template>
        </el-table-column>
        <el-table-column label="来源表" min-width="220">
          <template #default="{ row }">
            {{ row.database }}.{{ row.table }}
          </template>
        </el-table-column>
        <el-table-column prop="domain" label="归属类型" min-width="120" />
        <el-table-column prop="taxonomy" label="分类体系" min-width="180" />
        <el-table-column label="股票代码字段" min-width="130">
          <template #default="{ row }">
            {{ row.source_kind === 'member_dimension' ? '-' : (row.security_code_field || '-') }}
          </template>
        </el-table-column>
        <el-table-column prop="member_code_field" label="归属代码字段" min-width="130" />
        <el-table-column prop="member_name_field" label="归属名称字段" min-width="130" />
        <el-table-column label="日期字段" min-width="120">
          <template #default="{ row }">
            {{ row.source_kind === 'member_dimension' ? '-' : (row.effective_from_field || '-') }}
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag size="small" effect="plain" :type="row.status === 'enabled' ? 'success' : 'info'">
              {{ row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="170" fixed="right">
          <template #default="{ row }">
            <div class="row-actions">
              <el-button link type="primary" @click.stop="openEditSource(row)">编辑</el-button>
              <el-button link @click.stop="selectSource(row); runPreview()">预览</el-button>
              <el-button link type="danger" @click.stop="removeSource(row)">删除</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <section v-if="selectedSource" class="detail-grid">
      <article class="panel">
        <div class="section-header">
          <h3>当前配置</h3>
          <p>这里用更直白的方式展示这张表要怎么映射。</p>
        </div>
        <div class="selected-summary">
          <div class="summary-card">
            <span>来源类型</span>
            <strong>{{ sourceKindLabel }}</strong>
          </div>
          <div class="summary-card">
            <span>来源表</span>
            <strong>{{ sourceForm.database }}.{{ sourceForm.table }}</strong>
          </div>
          <div class="summary-card">
            <span>分类体系</span>
            <strong>{{ sourceForm.taxonomy || '-' }}</strong>
          </div>
          <div class="summary-card">
            <span>归属类型</span>
            <strong>{{ sourceForm.domain || '-' }}</strong>
          </div>
          <div class="summary-card">
            <span>证券类型</span>
            <strong>{{ sourceForm.source_kind === 'member_dimension' ? '-' : (sourceForm.security_type || '-') }}</strong>
          </div>
        </div>
        <el-table :data="mappingRows" class="mapping-table">
          <el-table-column prop="label" label="标准字段" min-width="120" />
          <el-table-column prop="field" label="选择的表字段" min-width="140" />
          <el-table-column prop="fallback" label="固定值" min-width="120" />
          <el-table-column prop="desc" label="说明" min-width="180" />
        </el-table>
        <div class="detail-actions">
          <el-button @click="openEditSource(sourceForm)">编辑当前配置</el-button>
          <el-button type="primary" :loading="previewLoading" @click="runPreview">生成预览</el-button>
          <el-button type="danger" plain @click="removeSource(sourceForm)">删除当前配置</el-button>
        </div>
      </article>

      <article class="panel">
        <div class="section-header">
          <h3>预览结果</h3>
          <p>{{ sourceForm.source_kind === 'member_dimension' ? '确认这套映射能不能正确生成成员名称和层级信息。' : '确认这套映射能不能正确生成股票归属关系。' }}</p>
        </div>
        <el-tabs class="preview-tabs">
          <el-tab-pane v-if="sourceForm.source_kind === 'relation'" label="归属结果">
            <el-table :data="previewResult.relation_preview || []" height="320">
              <el-table-column prop="security_code" label="股票代码" min-width="120" />
              <el-table-column prop="member_code" label="归属代码" min-width="130" />
              <el-table-column prop="member_name" label="归属名称" min-width="160" />
              <el-table-column prop="effective_from" label="生效日期" min-width="120" />
              <el-table-column prop="effective_to" label="结束日期" min-width="120" />
              <el-table-column prop="source_system" label="来源系统" min-width="140" />
            </el-table>
          </el-tab-pane>
          <el-tab-pane :label="sourceForm.source_kind === 'member_dimension' ? '成员层级' : '归属成员'">
            <el-table :data="previewResult.member_preview || []" height="320">
              <el-table-column prop="member_code" label="归属代码" min-width="140" />
              <el-table-column prop="member_name" label="归属名称" min-width="180" />
              <el-table-column prop="taxonomy" label="分类体系" min-width="160" />
              <el-table-column
                v-for="item in previewAttributeColumns"
                :key="item.key"
                :prop="item.key"
                :label="item.label"
                min-width="140"
              />
            </el-table>
          </el-tab-pane>
          <el-tab-pane label="原始样例">
            <el-table :data="previewResult.raw_rows || []" height="320">
              <el-table-column
                v-for="item in columns.slice(0, 8)"
                :key="item.name"
                :prop="item.name"
                :label="item.name"
                min-width="140"
              />
            </el-table>
          </el-tab-pane>
        </el-tabs>
        <div v-if="previewResult.sql" class="preview-sql">
          <span>预览 SQL</span>
          <code>{{ previewResult.sql }}</code>
        </div>
      </article>
    </section>

    <el-dialog
      v-model="sourceDialogVisible"
      :title="sourceForm.id ? '编辑来源配置' : '新增来源配置'"
      width="860px"
      center
      align-center
      class="source-config-dialog"
    >
      <div class="dialog-scroll-body">
        <div class="dialog-section">
          <h4>基础信息</h4>
          <div class="form-grid dialog-form-grid">
            <div class="form-item span-2">
              <label>配置名</label>
              <el-input v-model="sourceForm.source_name" placeholder="例如 csi_index_members" />
            </div>
            <div class="form-item span-2">
              <label>描述</label>
              <el-input v-model="sourceForm.description" placeholder="说明这张表映射的归属关系，可选" />
            </div>
            <div class="form-item">
              <label>数据库</label>
              <el-select v-model="sourceForm.database" placeholder="选择数据库" clearable filterable>
                <el-option v-for="item in databases" :key="item.name" :label="item.name" :value="item.name" />
              </el-select>
            </div>
            <div class="form-item">
              <label>表</label>
              <el-select v-model="sourceForm.table" placeholder="选择表" clearable filterable>
                <el-option v-for="item in tables" :key="item.name" :label="item.name" :value="item.name" />
              </el-select>
            </div>
          <div class="form-item">
            <label>来源类型</label>
            <el-select v-model="sourceForm.source_kind" placeholder="选择来源类型">
              <el-option label="绑定关系表" value="relation" />
              <el-option label="成员信息表" value="member_dimension" />
            </el-select>
          </div>
          <div class="form-item">
            <label>归属类型</label>
            <el-input v-model="sourceForm.domain" placeholder="例如 index / industry / board" />
          </div>
            <div class="form-item">
              <label>分类体系</label>
              <el-input v-model="sourceForm.taxonomy" placeholder="例如 csi_index / sw2021_l1" />
            </div>
          <div class="form-item">
            <label>证券类型</label>
            <el-select v-model="sourceForm.security_type" placeholder="选择证券类型" :disabled="sourceForm.source_kind === 'member_dimension'">
              <el-option label="stock" value="stock" />
              <el-option label="index" value="index" />
              <el-option label="etf" value="etf" />
                <el-option label="fund" value="fund" />
              </el-select>
            </div>
            <div class="form-item">
              <label>状态</label>
              <el-select v-model="sourceForm.status" placeholder="选择状态">
                <el-option label="enabled" value="enabled" />
                <el-option label="disabled" value="disabled" />
              </el-select>
            </div>
          </div>
        </div>

        <div class="dialog-section">
          <h4>字段映射</h4>
          <div class="mapping-grid dialog-form-grid">
            <div v-if="sourceForm.source_kind === 'relation'" class="mapping-cell">
              <label>股票代码字段</label>
              <el-select v-model="sourceForm.security_code_field" placeholder="例如 code" clearable filterable>
                <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
              </el-select>
            </div>
            <div class="mapping-cell">
              <label>归属代码字段</label>
              <el-select v-model="sourceForm.member_code_field" placeholder="例如 index_code / industry_code" clearable filterable>
                <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
              </el-select>
              <el-input v-model="sourceForm.member_code_value" placeholder="没有字段时可填固定值，例如 000300.SH" />
            </div>
          <div class="mapping-cell">
            <label>归属名称字段</label>
            <el-select v-model="sourceForm.member_name_field" placeholder="例如 index_name / industry_name" clearable filterable>
              <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
            </el-select>
            <el-input v-model="sourceForm.member_name_value" placeholder="没有字段时可填固定值，例如 沪深300" />
          </div>
          <div v-if="sourceForm.source_kind === 'member_dimension'" class="mapping-cell">
            <label>附加信息表数据库</label>
            <el-select v-model="sourceForm.lookup_database" placeholder="可选，选择数据库" clearable filterable>
              <el-option v-for="item in databases" :key="item.name" :label="item.name" :value="item.name" />
            </el-select>
          </div>
          <div v-if="sourceForm.source_kind === 'member_dimension'" class="mapping-cell">
            <label>附加信息表</label>
            <el-select v-model="sourceForm.lookup_table" placeholder="可选，选择表" clearable filterable>
              <el-option v-for="item in lookupTables" :key="item.name" :label="item.name" :value="item.name" />
            </el-select>
          </div>
            <div v-if="sourceForm.source_kind === 'member_dimension' && sourceForm.lookup_table" class="mapping-cell">
              <label>当前表 code 映射字段</label>
              <el-select v-model="sourceForm.lookup_source_field" placeholder="默认用归属代码字段" clearable filterable>
                <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
              </el-select>
            </div>
            <div v-if="sourceForm.source_kind === 'member_dimension' && sourceForm.lookup_table" class="mapping-cell">
              <label>附加表 code 映射字段</label>
              <el-select v-model="sourceForm.lookup_target_field" placeholder="例如 con_code / code / industry_code" clearable filterable>
                <el-option v-for="item in lookupColumnOptions" :key="item" :label="item" :value="item" />
              </el-select>
            </div>
          <div v-if="sourceForm.source_kind === 'member_dimension' && sourceForm.lookup_table" class="mapping-cell span-2">
            <label>附加表默认名称字段</label>
            <el-select v-model="sourceForm.lookup_member_name_field" placeholder="例如 industry_name / index_name" clearable filterable>
              <el-option v-for="item in lookupColumnOptions" :key="item" :label="item" :value="item" />
            </el-select>
          </div>
          <div v-if="sourceForm.source_kind === 'relation'" class="mapping-cell">
            <label>生效日期字段</label>
            <el-select v-model="sourceForm.effective_from_field" placeholder="例如 trade_date" clearable filterable>
              <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
            </el-select>
            </div>
            <div v-if="sourceForm.source_kind === 'relation'" class="mapping-cell">
              <label>结束日期字段</label>
              <el-select v-model="sourceForm.effective_to_field" placeholder="没有可留空" clearable filterable>
                <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
              </el-select>
            </div>
            <div class="mapping-cell">
              <label>来源系统字段</label>
              <el-select v-model="sourceForm.source_system_field" placeholder="可选" clearable filterable>
                <el-option v-for="item in columnOptions" :key="item" :label="item" :value="item" />
              </el-select>
              <el-input v-model="sourceForm.source_system_value" placeholder="或填写固定来源" />
            </div>
          </div>
        </div>

        <div v-if="sourceForm.source_kind === 'member_dimension'" class="dialog-section">
          <div class="attribute-header">
            <h4>附加字段</h4>
            <el-button type="primary" plain @click="addAttributeMapping">新增字段</el-button>
          </div>
          <div v-if="sourceForm.attribute_mappings.length" class="attribute-list">
            <div v-for="(item, index) in sourceForm.attribute_mappings" :key="item.key || index" class="attribute-row">
              <el-input v-model="item.label" placeholder="显示名称，例如 一级行业名称" />
              <el-select
                v-model="item.field"
                :placeholder="sourceForm.source_kind === 'member_dimension' && sourceForm.lookup_table ? '选择附加信息表字段' : '选择当前表字段'"
                clearable
                filterable
              >
                <el-option
                  v-for="fieldName in (sourceForm.source_kind === 'member_dimension' && sourceForm.lookup_table ? lookupColumnOptions : columnOptions)"
                  :key="fieldName"
                  :label="fieldName"
                  :value="fieldName"
                />
              </el-select>
              <el-button type="danger" plain @click="removeAttributeMapping(index)">删除</el-button>
            </div>
          </div>
          <el-empty v-else description="暂未添加附加字段" />
        </div>
      </div>
      <template #footer>
        <el-button @click="sourceDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveSource">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.membership-page {
  display: grid;
  gap: 18px;
  min-width: 0;
}

.panel {
  border: 1px solid var(--line);
  border-radius: 22px;
  background: rgba(255, 255, 255, 0.84);
  box-shadow: var(--shadow-md);
  backdrop-filter: blur(18px);
  padding: 18px;
  min-width: 0;
  overflow: hidden;
}

.table-toolbar,
.toolbar-left,
.toolbar-right,
.detail-grid,
.detail-actions,
.selected-summary,
.row-actions {
  display: flex;
  gap: 12px;
}

.table-toolbar,
.detail-actions {
  justify-content: space-between;
  align-items: center;
  min-width: 0;
}

.toolbar-left {
  flex: 1;
  flex-wrap: nowrap;
  min-width: 0;
  align-items: center;
}

.toolbar-left :deep(.el-input) {
  flex: 1 1 auto;
  min-width: 0;
}

.toolbar-left :deep(.el-select) {
  flex: 0 0 180px;
  min-width: 180px;
}

.toolbar-right,
.row-actions {
  align-items: center;
  justify-content: flex-end;
  flex-wrap: nowrap;
  min-width: 0;
}

.toolbar-right {
  flex: 0 1 auto;
  max-width: 100%;
}

.main-table {
  margin-top: 16px;
  width: 100%;
  min-width: 0;
  --el-table-border-color: rgba(15, 118, 110, 0.08);
  --el-table-header-bg-color: rgba(241, 248, 246, 0.96);
  --el-table-row-hover-bg-color: rgba(15, 118, 110, 0.04);
}

.main-table :deep(.el-table__inner-wrapper),
.main-table :deep(.el-scrollbar),
.main-table :deep(.el-scrollbar__wrap),
.main-table :deep(.el-table__body-wrapper) {
  min-width: 0;
}

.main-table :deep(th.el-table__cell) {
  background: linear-gradient(180deg, rgba(245, 250, 248, 0.98), rgba(238, 246, 243, 0.94));
  color: var(--text);
  font-weight: 700;
  font-size: 13px;
  border-bottom: 1px solid rgba(15, 118, 110, 0.08);
}

.main-table :deep(th.el-table__cell .cell) {
  line-height: 1.25;
  padding-top: 4px;
  padding-bottom: 4px;
}

.main-table :deep(td.el-table__cell .cell) {
  line-height: 1.45;
}

.detail-grid {
  display: grid;
  grid-template-columns: minmax(0, 0.92fr) minmax(0, 1.08fr);
  align-items: start;
  min-width: 0;
}

.section-header,
.dialog-section {
  display: grid;
  gap: 12px;
}

.section-header h3,
.dialog-section h4 {
  margin: 0;
  font-size: 18px;
}

.section-header p {
  margin: 0;
  color: var(--text-soft);
  line-height: 1.6;
}

.selected-summary {
  margin: 14px 0 16px;
  flex-wrap: wrap;
  min-width: 0;
}

.summary-card {
  display: grid;
  gap: 8px;
  min-width: 160px;
  padding: 14px 16px;
  border-radius: 16px;
  border: 1px solid rgba(15, 118, 110, 0.1);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.94), rgba(240, 247, 245, 0.96));
}

.summary-card span {
  color: var(--text-soft);
  font-size: 12px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.summary-card strong {
  font-size: 15px;
  overflow-wrap: anywhere;
}

.mapping-table {
  margin-bottom: 14px;
  width: 100%;
  min-width: 0;
}

.preview-tabs {
  margin-top: 12px;
  min-width: 0;
}

.dialog-section + .dialog-section {
  margin-top: 18px;
}

.attribute-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.attribute-list {
  display: grid;
  gap: 12px;
}

.attribute-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
  gap: 12px;
  align-items: center;
}

.form-item {
  display: grid;
  gap: 8px;
}

.form-item label {
  color: var(--text-soft);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.form-grid,
.mapping-grid {
  display: grid;
  gap: 12px;
}

.dialog-form-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.mapping-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.mapping-cell {
  display: grid;
  gap: 8px;
}

.mapping-cell label {
  color: var(--text-soft);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.span-2 {
  grid-column: span 2;
}

.preview-sql {
  display: grid;
  gap: 8px;
  margin-top: 12px;
}

.preview-sql span {
  color: var(--text-soft);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.preview-sql code {
  display: block;
  padding: 12px;
  border-radius: 14px;
  background: rgba(15, 23, 31, 0.92);
  color: #d5f6f3;
  overflow: auto;
}

.dialog-scroll-body {
  max-height: min(68vh, 720px);
  overflow-y: auto;
  padding-right: 6px;
}

.source-config-dialog :deep(.el-dialog) {
  max-height: 84vh;
  display: flex;
  flex-direction: column;
}

.source-config-dialog :deep(.el-dialog__body) {
  overflow: hidden;
  padding-top: 8px;
}

@media (max-width: 1280px) {
  .detail-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 860px) {
  .mapping-grid,
  .dialog-form-grid {
    grid-template-columns: 1fr;
  }

  .attribute-row {
    grid-template-columns: 1fr;
  }

  .span-2 {
    grid-column: span 1;
  }

  .table-toolbar {
    flex-wrap: wrap;
  }

  .toolbar-left,
  .toolbar-right {
    width: 100%;
    flex-wrap: wrap;
  }

  .toolbar-left :deep(.el-input),
  .toolbar-left :deep(.el-select) {
    min-width: 100%;
  }

  .toolbar-right {
    justify-content: flex-start;
  }
}
</style>
