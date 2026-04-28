<script setup>
import { computed, h, onMounted, ref, watch } from 'vue'
import { ElButton, ElCheckbox, ElEmpty, ElMessageBox, ElSwitch, ElTag } from 'element-plus'
import { useRoute } from '#imports'
import { formatDateTime } from '~/composables/useDateTimeFormat'
import { useWorkbench } from '~/composables/useWorkbench'

const route = useRoute()
const nodeDialogVisible = ref(false)
const nodeDialogMode = ref('create')
const fieldDialogVisible = ref(false)
const fieldCreateMode = ref('base')
const nodeSearch = ref('')
const nodeStatusFilter = ref('all')
const bindingSearch = ref('')
const bindingFilter = ref('all')
const showAdvancedBinding = ref(false)
const selectedFieldKeys = ref([])
const aiNotesLoading = ref(false)
const wideTableFieldSearch = ref('')
const selectedWorkbenchType = ref('node')
const wideTableDialogVisible = ref(false)
const wideTableSyncStates = ref([])
const wideTableForm = ref(blankWideTable())
const wideTableTargetTables = ref([])
const selectedWideTableId = ref('')
const wideTableSyncLoading = ref(false)
const wideTableRunningNames = ref([])
const wideTableRunningStartedAt = ref({})
const wideTableStateLoading = ref(false)
const wideTableSyncStateDatabase = ref('default')

const {
  workspace,
  graph,
  fields,
  schema,
  attach,
  nodeForm,
  bindingForm,
  visibleNodes,
  currentTableProfile,
  currentNodeFieldBindings,
  getNodeFieldBindings,
  loading,
  hasDatasourceConfigured,
  ensureWorkspaceLoaded,
  saveWorkspace,
  selectDatabase,
  selectTable,
  selectAttachDatabase,
  selectAttachTable,
  inferNodeTemplate,
  startNewNode,
  resetBindingForm,
  editNode,
  saveNode,
  addBaseFieldBinding,
  saveDerivedFieldBinding,
  saveRelatedFieldBinding,
  duplicateNode,
  deleteNode,
  getNodeDeleteImpact,
  removeFieldBinding,
  notifyAction,
} = useWorkbench()

const nodeTableOptions = computed(() => schema.value.tables)
const baseColumnOptions = computed(() => schema.value.columns)
const attachTableOptions = computed(() => attach.value.tables)
const attachColumnOptions = computed(() => attach.value.columns)
const sourceNodeOptions = computed(() =>
  visibleNodes.value.filter((node) => node.name && node.name !== nodeForm.value.name)
)

const wideTableDesigns = computed(() =>
  visibleNodes.value
    .map((node) => buildWideTableDesignFromNode(node))
    .filter(Boolean)
)

const wideTableByName = computed(() =>
  new Map(wideTableDesigns.value.map((item) => [item.name, item]))
)

const currentNodeWideTable = computed(() =>
  nodeForm.value.name ? (wideTableByName.value.get(nodeForm.value.name) || null) : null
)

const wideTableStateByName = computed(() => {
  const map = new Map()
  for (const item of wideTableSyncStates.value || []) {
    const name = String(item?.wide_table_name || '').trim()
    if (name && !map.has(name)) {
      map.set(name, item)
    }
  }
  return map
})

const currentNodeWideTableSyncState = computed(() => {
  const item = currentNodeWideTable.value
  if (!item) return null
  return wideTableStateByName.value.get(item.name) || null
})

const currentNodeWideTableStatus = computed(() =>
  buildWideTableStatus(currentNodeWideTable.value, currentNodeWideTableSyncState.value)
)

const nodeListFilterOptions = computed(() => {
  const options = [
    { label: '全部', value: 'all' },
    { label: '启用', value: 'enabled' },
  ]
  if (visibleNodes.value.some((node) => node.status === 'disabled')) {
    options.push({ label: '未启用', value: 'disabled' })
  }
  return options
})

const currentNodeSummary = computed(() => {
  const wideTable = currentNodeWideTable.value
  const syncStatus = currentNodeWideTableStatus.value
  return [
    { label: '主表', value: currentTableProfile.value.table ? `${currentTableProfile.value.database}.${currentTableProfile.value.table}` : '-' },
    { label: '粒度', value: nodeForm.value.grain || '-' },
    { label: '主键', value: (nodeForm.value.entity_keys || []).join(', ') || '-' },
    { label: '时间键', value: nodeForm.value.time_key || '-' },
    { label: '状态', value: nodeForm.value.status === 'enabled' ? '启用' : '停用' },
    { label: '资产类型', value: nodeForm.value.asset_type || '-' },
    { label: '查询频率', value: nodeForm.value.query_freq || '-' },
    ...(wideTable
      ? [
          { label: '宽表目标', value: `${wideTable.target_database}.${wideTable.target_table}` },
          { label: '宽表状态', value: wideTable.status === 'enabled' ? '可用' : '不可用' },
          { label: '同步状态', value: syncStatus.label },
          { label: syncStatus.timeLabel, value: syncStatus.timeValue },
          { label: '同步信息', value: syncStatus.message || '-' },
        ]
      : []),
  ]
})

const currentNodeBindingHint = computed(() => {
  if (!nodeForm.value.name) return ''
  if (currentNodeWideTable.value) {
    return '当前节点是宽表节点：按普通节点维护字段，同时保留宽表同步配置。'
  }
  return '当前节点直接展示自身主表字段与字段绑定。'
})

const canSaveNode = computed(() => Boolean(nodeForm.value.name && nodeForm.value.database && nodeForm.value.tableName))
const canAddBaseField = computed(() => Boolean(nodeForm.value.name && bindingForm.value.base_source_field))
const canAddDerivedField = computed(() =>
  Boolean(
    nodeForm.value.name &&
    bindingForm.value.standard_field &&
    bindingForm.value.formula &&
    Array.isArray(bindingForm.value.depends_on) &&
    bindingForm.value.depends_on.length
  )
)
const canBindField = computed(() => {
  if (bindingForm.value.binding_mode === 'source_node') {
    return Boolean(
      nodeForm.value.name &&
      bindingForm.value.standard_field &&
      bindingForm.value.source_node &&
      bindingForm.value.source_field
    )
  }
  return Boolean(
    nodeForm.value.name &&
    attach.value.selectedDatabase &&
    attach.value.selectedTable &&
    bindingForm.value.standard_field &&
    bindingForm.value.source_field &&
    bindingForm.value.base_join_field &&
    bindingForm.value.source_join_field
  )
})
const selectedFieldKeySet = computed(() => new Set(selectedFieldKeys.value))

const filteredFieldBindings = computed(() => {
  const keyword = bindingSearch.value.trim().toLowerCase()

  return currentNodeFieldBindings.value
    .filter((item) => {
      if (bindingFilter.value !== 'all' && item.binding_type !== bindingFilter.value) {
        return false
      }

      if (!keyword) return true

      const haystack = [
        item.standard_field,
        item.source_table,
        item.source_field,
        item.source_node,
        item.field_role,
        item.relation_mode,
        item.join_keys_text,
        ...(item.notes || []),
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()

      return haystack.includes(keyword)
    })
    .map((item) => ({
      ...item,
      __rowKey: `${item.base_node}__${item.standard_field}__${item.source_node}__${item.source_table}__${item.source_field}`,
      __bindingOrder: item.binding_type === 'base' ? 0 : (item.binding_type === 'derived' ? 1 : 2)
    }))
    .sort((a, b) => {
      if (a.__bindingOrder !== b.__bindingOrder) {
        return a.__bindingOrder - b.__bindingOrder
      }
      return String(a.standard_field || '').localeCompare(String(b.standard_field || ''))
    })
})
const selectableFieldRows = computed(() => filteredFieldBindings.value)
const selectedFieldRows = computed(() =>
  filteredFieldBindings.value.filter((item) => selectedFieldKeySet.value.has(item.__rowKey))
)
const allSelectableRowsChecked = computed(() =>
  selectableFieldRows.value.length > 0 &&
  selectableFieldRows.value.every((item) => selectedFieldKeySet.value.has(item.__rowKey))
)

const wideTableFieldOptions = computed(() => {
  const sourceNodeName = String(
    wideTableForm.value.source_node
    || selectedWideTable.value?.source_node
    || nodeForm.value.name
    || ''
  ).trim()
  const seen = new Set()
  return getNodeFieldBindings(sourceNodeName)
    .map((item) => String(item.standard_field || '').trim())
    .filter(Boolean)
    .filter((value) => {
      if (seen.has(value)) return false
      seen.add(value)
      return true
    })
})

const selectedWideTable = computed(() =>
  wideTableDesigns.value.find((item) => item.id === selectedWideTableId.value)
  || null
)

const selectedWideTableSyncState = computed(() => {
  const item = selectedWideTable.value
  if (!item) return null
  return wideTableStateByName.value.get(item.name) || null
})

const selectedWideTableStatus = computed(() =>
  buildWideTableStatus(selectedWideTable.value, selectedWideTableSyncState.value)
)

const workbenchRows = computed(() => {
  const keyword = nodeSearch.value.trim().toLowerCase()
  const wideDesignByName = wideTableByName.value
  const nodeNameSet = new Set(visibleNodes.value.map((node) => String(node.name || '')))
  const nodeRows = visibleNodes.value
    .filter((node) => {
      const bindingFields = getNodeFieldBindings(node.name)
      const wideTable = wideDesignByName.get(node.name)
      const fieldHaystack = bindingFields
        .flatMap((item) => [
          item.standard_field,
          item.source_field,
          item.source_table,
          item.source_node,
          item.field_role,
          ...(item.notes || []),
        ])
        .filter(Boolean)
        .join(' ')

      if (nodeStatusFilter.value === 'enabled' && node.status !== 'enabled') return false
      if (nodeStatusFilter.value === 'disabled' && node.status !== 'disabled') return false
      if (!keyword) return true
      const haystack = [
        node.name,
        node.table,
        node.status,
        node.description,
        node.description_zh,
        node.grain,
        node.asset_type,
        node.query_freq,
        ...(node.fields || []),
        wideTable?.target_database,
        wideTable?.target_table,
        wideTable?.engine,
        ...(Array.isArray(wideTable?.fields) ? wideTable.fields : []),
        ...(Array.isArray(wideTable?.key_fields) ? wideTable.key_fields : []),
        fieldHaystack,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(keyword)
    })
    .map((node) => ({
      ...node,
      __rowType: 'node',
      __rowKey: `node:${node.name}`,
      __wideTable: wideDesignByName.get(node.name) || null,
      __summary: node.table || '-',
    }))

  const wideRows = wideTableDesigns.value
    .filter((item) => {
      if (nodeNameSet.has(String(item.name || ''))) return false
      const sourceBindingFields = item.source_node
        ? getNodeFieldBindings(item.source_node)
            .flatMap((row) => [
              row.standard_field,
              row.source_field,
              row.source_table,
              ...(row.notes || []),
            ])
            .filter(Boolean)
            .join(' ')
        : ''

      if (nodeStatusFilter.value === 'enabled' && item.status !== 'enabled') return false
      if (nodeStatusFilter.value === 'disabled' && item.status !== 'disabled') return false
      if (!keyword) return true
      const haystack = [
        item.name,
        item.description,
        item.source_node,
        item.target_database,
        item.target_table,
        item.engine,
        item.status,
        ...(Array.isArray(item.fields) ? item.fields : []),
        ...(Array.isArray(item.key_fields) ? item.key_fields : []),
        sourceBindingFields,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(keyword)
    })
    .map((item) => ({
      ...item,
      __rowType: 'wide_table',
      __rowKey: `wide:${item.id}`,
      __summary: item.target_database && item.target_table ? `${item.target_database}.${item.target_table}` : '-',
    }))

  return [...nodeRows, ...wideRows].sort((a, b) => {
    const typeOrder = a.__rowType === b.__rowType ? 0 : (a.__rowType === 'node' ? -1 : 1)
    if (typeOrder !== 0) return typeOrder
    return String(a.name || '').localeCompare(String(b.name || ''))
  })
})

const currentWorkbenchRowKey = computed(() =>
  selectedWorkbenchType.value === 'wide_table'
    ? `wide:${selectedWideTableId.value || ''}`
    : `node:${nodeForm.value.name || ''}`
)

const currentWideTableSummary = computed(() => {
  const item = selectedWideTable.value
  if (!item) return []
  const syncState = selectedWideTableSyncState.value
  const syncStatus = selectedWideTableStatus.value
  return [
    { label: '来源节点', value: item.source_node || '-' },
    { label: '目标表', value: item.target_database && item.target_table ? `${item.target_database}.${item.target_table}` : '-' },
    { label: '引擎', value: item.engine || '-' },
    { label: '状态', value: item.status === 'enabled' ? '可用' : '不可用' },
    { label: '同步状态', value: syncStatus.label },
    { label: '最近同步动作', value: syncState?.last_action || '-' },
    { label: syncStatus.timeLabel, value: syncStatus.timeValue },
    { label: '同步信息', value: syncStatus.message || '-' },
    { label: '主键字段', value: Array.isArray(item.key_fields) && item.key_fields.length ? item.key_fields.join(', ') : '-' },
    { label: '字段数量', value: String(currentWideTableFieldRows.value.length) },
  ]
})

const currentWideTableFieldRows = computed(() => {
  const item = selectedWideTable.value
  if (!item) return []
  const selectedFields = new Set((Array.isArray(item.fields) ? item.fields : []).map((fieldName) => String(fieldName || '').trim()))
  return getNodeFieldBindings(item.source_node)
    .filter((row) => selectedFields.has(String(row.standard_field || '').trim()))
    .map((row) => ({
      ...row,
      __rowKey: `${row.__rowKey}__wide`,
    }))
})

const filteredCurrentWideTableFieldRows = computed(() => {
  const keyword = wideTableFieldSearch.value.trim().toLowerCase()
  return currentWideTableFieldRows.value.filter((item) => {
    if (!keyword) return true
    return [
      item.standard_field,
      item.source_table,
      item.source_field,
      item.source_node,
      item.field_role,
      item.relation_mode,
      item.join_keys_text,
      ...(item.notes || []),
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
      .includes(keyword)
  })
})

const wideTableBindingRows = computed(() =>
  filteredCurrentWideTableFieldRows.value
)

const wideTableBindingDisplayColumns = computed(() =>
  [
    ...bindingTableColumns.filter((item) => !['selected', 'action'].includes(item.key)),
    {
      key: 'action',
      dataKey: 'action',
      title: '操作',
      width: 110,
      align: 'center',
      cellRenderer: ({ rowData }) =>
        h('div', { class: 'binding-action-group' }, [
          h(
            ElButton,
            {
              link: true,
              type: 'primary',
              onClick: () => handleEditWideTableField(rowData),
            },
            () => '编辑'
          ),
          h(
            ElButton,
            {
              link: true,
              type: 'danger',
              onClick: () => handleRemoveWideTableField(rowData),
            },
            () => '移除'
          ),
        ]),
    },
  ]
)

const wideTableCanSave = computed(() =>
  Boolean(
    wideTableForm.value.name &&
    wideTableForm.value.source_node &&
    wideTableForm.value.target_database &&
    wideTableForm.value.target_table &&
    Array.isArray(wideTableForm.value.fields) &&
    wideTableForm.value.fields.length &&
    Array.isArray(wideTableForm.value.key_fields) &&
    wideTableForm.value.key_fields.length
  )
)

const fieldDialogTitle = computed(() => {
  if (bindingForm.value.edit_mode) {
    if (fieldCreateMode.value === 'base') return '编辑主表字段'
    if (fieldCreateMode.value === 'derived') return '编辑派生字段'
    return '编辑关联字段'
  }
  if (fieldCreateMode.value === 'derived') return '添加派生字段'
  return '添加字段绑定'
})

const fieldDialogConfirmText = computed(() => {
  if (bindingForm.value.edit_mode) {
    return '保存修改'
  }
  if (fieldCreateMode.value === 'base') return '添加主表字段'
  if (fieldCreateMode.value === 'derived') return '添加派生字段'
  return '添加关联字段'
})

const bindingTableColumns = [
  {
    key: 'selected',
    dataKey: 'selected',
    title: '选择',
    width: 72,
    headerCellRenderer: () =>
      h(ElCheckbox, {
        modelValue: allSelectableRowsChecked.value,
        indeterminate: selectedFieldRows.value.length > 0 && !allSelectableRowsChecked.value,
        onChange: (value) => toggleSelectAllRows(Boolean(value)),
      }),
    cellRenderer: ({ rowData }) =>
      h(ElCheckbox, {
        modelValue: selectedFieldKeySet.value.has(rowData.__rowKey),
        onChange: (value) => toggleSelectedFieldRow(rowData.__rowKey, Boolean(value)),
      }),
  },
  {
    key: 'standard_field',
    dataKey: 'standard_field',
    title: '标准字段',
    width: 220,
  },
  {
    key: 'binding_type',
    dataKey: 'binding_type',
    title: '来源类型',
    width: 110,
    cellRenderer: ({ cellData }) =>
      h(
        ElTag,
        {
          size: 'small',
          round: true,
          effect: 'plain',
          type: cellData === 'base' ? 'success' : (cellData === 'wide_table' ? 'info' : 'warning'),
          disableTransitions: true,
        },
        () => (cellData === 'base' ? '主表' : (cellData === 'wide_table' ? '宽表' : '关联'))
      ),
  },
  {
    key: 'binding_mode',
    dataKey: 'binding_mode',
    title: '绑定模式',
    width: 120,
    cellRenderer: ({ cellData }) =>
      h(
        ElTag,
        {
          size: 'small',
          round: true,
          effect: 'plain',
          type: cellData === 'source_node' ? 'warning' : (cellData === 'derived' ? 'info' : (cellData === 'materialized' ? 'info' : 'success')),
          disableTransitions: true,
        },
        () => (cellData === 'source_node' ? '来源节点' : (cellData === 'derived' ? '派生' : (cellData === 'materialized' ? '物化' : '来源表')))
      ),
  },
  {
    key: 'source_ref',
    dataKey: 'source_table',
    title: '来源表 / 节点',
    width: 260,
    cellRenderer: ({ rowData }) => {
      if (rowData.binding_type === 'wide_table') {
        return rowData.source_node || '-'
      }
      const isSourceNode = rowData.binding_mode === 'source_node'
      return isSourceNode ? (rowData.source_node || '-') : (rowData.source_table || '-')
    },
  },
  {
    key: 'source_field',
    dataKey: 'source_field',
    title: '来源字段',
    width: 180,
  },
  {
    key: 'field_role',
    dataKey: 'field_role',
    title: '字段角色',
    width: 150,
  },
  {
    key: 'relation_mode',
    dataKey: 'relation_mode',
    title: '时间关系',
    width: 140,
    cellRenderer: ({ rowData }) => rowData.relation_mode || (rowData.binding_type === 'base' ? '-' : '未配置'),
  },
  {
    key: 'derived_definition',
    dataKey: 'formula',
    title: '派生定义',
    width: 280,
    cellRenderer: ({ rowData }) => {
      if (rowData.binding_type !== 'derived') return '-'
      const depends = Array.isArray(rowData.depends_on) && rowData.depends_on.length
        ? rowData.depends_on.join(', ')
        : '-'
      const formula = rowData.formula || '-'
      return `依赖: ${depends} | 公式: ${formula}`
    },
  },
  {
    key: 'join_keys_text',
    dataKey: 'join_keys_text',
    title: 'Join Keys',
    width: 200,
    cellRenderer: ({ cellData }) => cellData || '-',
  },
  {
    key: 'notes',
    dataKey: 'notes',
    title: '备注',
    width: 260,
    cellRenderer: ({ rowData }) => {
      const notes = Array.isArray(rowData.notes) ? rowData.notes.filter(Boolean) : []
      return notes.length ? notes.join(' | ') : '-'
    },
  },
  {
    key: 'action',
    dataKey: 'action',
    title: '操作',
    width: 96,
    align: 'center',
    cellRenderer: ({ rowData }) =>
      h('div', { class: 'binding-action-group' }, [
        h(
          ElButton,
          {
            link: true,
            type: 'primary',
            onClick: () => handleEditField(rowData),
          },
          () => '编辑'
        ),
        h(
          ElButton,
          {
            link: true,
            type: 'danger',
            onClick: () => handleRemoveField(rowData),
          },
          () => '移除'
        ),
      ]),
  },
]

function editableNotes(notes) {
  return (notes || [])
    .filter((item) => {
      const text = String(item || '')
      return !text.startsWith('挂载自 ') && !text.startsWith('挂载自主表字段 ')
    })
    .join('\n')
}

function splitTableRef(tableRef) {
  const text = String(tableRef || '')
  const dot = text.indexOf('.')
  if (dot < 0) {
    return { database: '', tableName: text }
  }
  return {
    database: text.slice(0, dot),
    tableName: text.slice(dot + 1),
  }
}

async function callJson(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  })
  const payload = await response.json()
  if (!response.ok) {
    throw new Error(payload?.detail || payload?.message || payload?.error || '请求失败')
  }
  return payload
}

function blankWideTable() {
  return {
    id: '',
    name: '',
    description: '',
    source_node: '',
    target_database: '',
    target_table: '',
    engine: 'Memory',
    status: 'disabled',
    fields: [],
    key_fields: [],
    partition_by_text: '',
    order_by_text: '',
    version_field: '',
  }
}

function buildWideTableDesignFromNode(node) {
  const meta = node?.wide_table
  if (!meta || typeof meta !== 'object') return null

  const { database, tableName } = splitTableRef(node.table)
  const keyFields = Array.isArray(meta.key_fields) && meta.key_fields.length
    ? [...meta.key_fields]
    : [...new Set([...(node.entity_keys || []), node.time_key || ''].filter(Boolean))]

  return {
    id: meta.id || `wide::${node.name}`,
    name: node.name,
    description: meta.description || node.description || node.description_zh || '',
    source_node: meta.source_node || node.name,
    target_database: meta.target_database || database,
    target_table: meta.target_table || tableName,
    engine: meta.engine || 'Memory',
    status: meta.status || node.status || 'enabled',
    fields: Array.isArray(meta.fields) && meta.fields.length ? [...meta.fields] : [...(node.fields || [])],
    key_fields: keyFields,
    partition_by: Array.isArray(meta.partition_by) ? [...meta.partition_by] : [],
    order_by: Array.isArray(meta.order_by) && meta.order_by.length ? [...meta.order_by] : [...keyFields],
    version_field: meta.version_field || '',
    created_at: meta.created_at || '',
    updated_at: meta.updated_at || '',
  }
}

function syncSelectedWideTableId() {
  const items = wideTableDesigns.value
  if (!items.length) {
    selectedWideTableId.value = ''
    return items
  }
  if (!items.some((item) => item.id === selectedWideTableId.value)) {
    selectedWideTableId.value = items[0].id
  }
  return items
}

function nowIsoSecond() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, '')
}

function isWideTableRunning(item) {
  const name = String(item?.name || '').trim()
  return Boolean(name && wideTableRunningNames.value.includes(name))
}

function buildWideTableStatus(item, state) {
  if (!item) {
    return {
      status: 'none',
      label: '-',
      tagType: 'info',
      timeLabel: '同步时间',
      timeValue: '-',
      message: '',
    }
  }

  const rawStatus = String(state?.last_status || '').trim().toLowerCase()
  const running = isWideTableRunning(item) || rawStatus === 'running' || rawStatus === 'pending'
  const localStartedAt = wideTableRunningStartedAt.value[String(item.name || '').trim()] || ''

  if (running) {
    return {
      status: 'running',
      label: '同步中',
      tagType: 'warning',
      timeLabel: '开始时间',
      timeValue: formatDateTime(state?.last_started_at || localStartedAt || state?.updated_at),
      message: state?.last_message || '宽表同步正在执行',
    }
  }

  if (rawStatus === 'success') {
    return {
      status: 'success',
      label: '同步成功',
      tagType: 'success',
      timeLabel: '最新同步时间',
      timeValue: formatDateTime(state?.last_finished_at || state?.updated_at),
      message: state?.last_message || '',
    }
  }

  if (rawStatus === 'failed') {
    return {
      status: 'failed',
      label: '同步失败',
      tagType: 'danger',
      timeLabel: '失败时间',
      timeValue: formatDateTime(state?.last_finished_at || state?.updated_at),
      message: state?.last_message || '',
    }
  }

  return {
    status: 'not_synced',
    label: '要同步',
    tagType: 'info',
    timeLabel: '同步时间',
    timeValue: '-',
    message: '当前宽表还没有同步记录',
  }
}

function normalizeWideTablePayload(payload) {
  const keyFields = [...new Set((payload.key_fields || []).map((item) => String(item || '').trim()).filter(Boolean))]
  const fields = [...new Set((payload.fields || []).map((item) => String(item || '').trim()).filter(Boolean))]
  const partitionBy = Array.isArray(payload.partition_by) ? payload.partition_by : normalizeCsvList(payload.partition_by_text)
  const orderBy = Array.isArray(payload.order_by) ? payload.order_by : normalizeCsvList(payload.order_by_text)
  const name = String(payload.name || payload.source_node || '').trim()
  return {
    ...payload,
    id: String(payload.id || `wide::${name}`).trim(),
    name,
    source_node: String(payload.source_node || name).trim(),
    target_database: String(payload.target_database || '').trim(),
    target_table: String(payload.target_table || name).trim(),
    engine: String(payload.engine || 'Memory').trim() || 'Memory',
    status: String(payload.status || 'enabled').trim() || 'enabled',
    fields,
    key_fields: keyFields,
    partition_by: partitionBy,
    order_by: orderBy.length ? orderBy : keyFields,
    version_field: String(payload.version_field || '').trim(),
    created_at: payload.created_at || nowIsoSecond(),
    updated_at: nowIsoSecond(),
  }
}

function applyWideTableDesignToGraph(payload) {
  const design = normalizeWideTablePayload(payload)
  const index = graph.value.nodes.findIndex((node) => node.name === design.name || node.name === design.source_node)
  if (index < 0) {
    throw new Error(`找不到宽表节点：${design.name}`)
  }

  const node = graph.value.nodes[index]
  graph.value.nodes[index] = {
    ...node,
    name: design.name,
    description: design.description || node.description || null,
    status: design.status,
    wide_table: {
      id: design.id,
      description: design.description,
      source_node: design.source_node,
      target_database: design.target_database,
      target_table: design.target_table,
      engine: design.engine,
      fields: [...design.fields],
      key_fields: [...design.key_fields],
      partition_by: [...design.partition_by],
      order_by: [...design.order_by],
      version_field: design.version_field,
      status: design.status,
      created_at: design.created_at,
      updated_at: design.updated_at,
    },
  }
  return design
}

function removeWideTableDesignFromGraph(row) {
  const index = graph.value.nodes.findIndex((node) => node.name === row.name || node.wide_table?.id === row.id)
  if (index < 0) return
  const nextNode = { ...graph.value.nodes[index] }
  delete nextNode.wide_table
  graph.value.nodes[index] = nextNode
}

function normalizeCsvList(value) {
  return String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

async function syncWideTableSelectionFromGraph() {
  return syncSelectedWideTableId()
}

async function loadWideTableSyncStates() {
  if (!hasDatasourceConfigured.value) {
    wideTableSyncStates.value = []
    return
  }
  wideTableStateLoading.value = true
  try {
    const search = new URLSearchParams()
    if (wideTableSyncStateDatabase.value) {
      search.set('state_database', wideTableSyncStateDatabase.value)
    }
    const payload = await callJson(`/api/sync/wide-tables/states${search.toString() ? `?${search.toString()}` : ''}`)
    wideTableSyncStates.value = payload.states || []
  } catch {
    wideTableSyncStates.value = []
  } finally {
    wideTableStateLoading.value = false
  }
}

async function loadWideTableTargetTables(database) {
  wideTableTargetTables.value = []
  if (!database || !hasDatasourceConfigured.value) return []
  const payload = await callJson(
    `/api/schema/tables?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(database)}`
  )
  wideTableTargetTables.value = payload.items || []
  return wideTableTargetTables.value
}

function openWideTableCreateDialog() {
  const sourceNodeName = nodeForm.value.name || ''
  const targetDatabase = currentTableProfile.value.database || nodeForm.value.database || ''
  const targetTable = currentTableProfile.value.table || nodeForm.value.tableName || ''
  const copiedFields = [...wideTableFieldOptions.value]
  const copiedKeyFields = [
    ...new Set(
      [
        ...(Array.isArray(nodeForm.value.entity_keys) ? nodeForm.value.entity_keys : []),
        nodeForm.value.time_key || '',
      ].filter(Boolean)
    ),
  ]
  wideTableForm.value = {
    ...blankWideTable(),
    name: sourceNodeName || '',
    description: sourceNodeName ? `由普通节点 ${sourceNodeName} 转换生成` : '',
    source_node: sourceNodeName,
    target_database: targetDatabase,
    target_table: sourceNodeName || targetTable,
    fields: copiedFields,
    key_fields: copiedKeyFields.length ? copiedKeyFields : wideTableFieldOptions.value.filter((item) => ['code', 'trade_time', 'trade_date'].includes(item)).slice(0, 2),
  }
  if (wideTableForm.value.engine === 'ReplacingMergeTree' && !wideTableForm.value.version_field) {
    wideTableForm.value.version_field = 'updated_at'
  }
  wideTableTargetTables.value = []
  wideTableDialogVisible.value = true
}

function handleConvertCurrentNodeToWideTable() {
  openWideTableCreateDialog()
}

async function openWideTableEditDialog(row) {
  selectedWideTableId.value = row.id
  wideTableForm.value = {
    ...blankWideTable(),
    ...row,
    partition_by_text: Array.isArray(row.partition_by) ? row.partition_by.join(', ') : '',
    order_by_text: Array.isArray(row.order_by) ? row.order_by.join(', ') : '',
    fields: Array.isArray(row.fields) ? [...row.fields] : [],
    key_fields: Array.isArray(row.key_fields) ? [...row.key_fields] : [],
  }
  await loadWideTableTargetTables(wideTableForm.value.target_database)
  wideTableDialogVisible.value = true
}

async function handleAddWideTableField() {
  if (!selectedWideTable.value) return
  await openWideTableEditDialog(selectedWideTable.value)
}

function selectWideTable(row) {
  selectedWorkbenchType.value = 'wide_table'
  selectedWideTableId.value = row?.id || ''
}

async function handleSaveWideTable() {
  const payload = {
    ...wideTableForm.value,
    source_node: nodeForm.value.name || wideTableForm.value.source_node,
    partition_by: normalizeCsvList(wideTableForm.value.partition_by_text),
    order_by: normalizeCsvList(wideTableForm.value.order_by_text),
  }
  await notifyAction('宽表节点已保存', async () => {
    const saved = applyWideTableDesignToGraph(payload)
    selectedWideTableId.value = saved.id
    await saveWorkspace()
    syncSelectedWideTableId()
    await loadWideTableSyncStates()
  })
  wideTableDialogVisible.value = false
}

async function handleDeleteWideTable(row) {
  await ElMessageBox.confirm(`确定删除宽表节点 ${row.name} 吗？`, '删除宽表节点', {
    type: 'warning',
    confirmButtonText: '删除',
    cancelButtonText: '取消',
  })
  await notifyAction('宽表节点已删除', async () => {
    removeWideTableDesignFromGraph(row)
    await saveWorkspace()
    syncSelectedWideTableId()
    await loadWideTableSyncStates()
  })
}

async function handleRunWideTableSync(row) {
  if (!hasDatasourceConfigured.value) {
    throw new Error('ClickHouse 数据源未配置，无法执行宽表同步')
  }
  wideTableSyncLoading.value = true
  const runningName = String(row?.name || '').trim()
  if (runningName) {
    wideTableRunningNames.value = [...new Set([...wideTableRunningNames.value, runningName])]
    wideTableRunningStartedAt.value = {
      ...wideTableRunningStartedAt.value,
      [runningName]: new Date().toISOString(),
    }
  }
  try {
    const payload = await callJson('/api/sync/wide-tables/run-inline', {
      method: 'POST',
      body: JSON.stringify({
        id: row.id,
        graph_path: workspace.value.graphPath,
        fields_path: workspace.value.fieldsPath,
        state_database: wideTableSyncStateDatabase.value || 'default',
      }),
    })
    await loadWideTableSyncStates()
    const firstResult = Array.isArray(payload.results) ? payload.results[0] : null
    const actionMessage = firstResult?.message || (payload.ok ? '宽表同步完成' : '宽表同步失败')
    await notifyAction(actionMessage, async () => {})
  } finally {
    if (runningName) {
      wideTableRunningNames.value = wideTableRunningNames.value.filter((name) => name !== runningName)
      const nextStartedAt = { ...wideTableRunningStartedAt.value }
      delete nextStartedAt[runningName]
      wideTableRunningStartedAt.value = nextStartedAt
    }
    wideTableSyncLoading.value = false
  }
}

async function handleEditWideTableField(row) {
  const sourceNode = selectedWideTable.value?.source_node
  if (!sourceNode) return
  await notifyAction('', () => editNode(sourceNode))
  selectedWorkbenchType.value = 'node'
  await handleEditField(row)
}

async function handleRemoveWideTableField(row) {
  const design = selectedWideTable.value
  if (!design) return
  await ElMessageBox.confirm(`确定从宽表节点 ${design.name} 中移除字段 ${row.standard_field} 吗？`, '移除宽表字段', {
    type: 'warning',
    confirmButtonText: '移除',
    cancelButtonText: '取消',
  })
  const nextFields = (Array.isArray(design.fields) ? design.fields : []).filter((field) => field !== row.standard_field)
  const nextKeyFields = (Array.isArray(design.key_fields) ? design.key_fields : []).filter((field) => field !== row.standard_field)
  const nextPartitionBy = (Array.isArray(design.partition_by) ? design.partition_by : []).filter((field) => field !== row.standard_field)
  const nextOrderBy = (Array.isArray(design.order_by) ? design.order_by : []).filter((field) => field !== row.standard_field)
  const nextVersionField = design.version_field === row.standard_field ? '' : design.version_field

  await notifyAction('宽表字段已移除', async () => {
    applyWideTableDesignToGraph({
      ...design,
      fields: nextFields,
      key_fields: nextKeyFields,
      partition_by: nextPartitionBy,
      order_by: nextOrderBy,
      version_field: nextVersionField,
    })
    await saveWorkspace()
    syncSelectedWideTableId()
  })
}

function toggleSelectedFieldRow(rowKey, checked) {
  const set = new Set(selectedFieldKeys.value)
  if (checked) {
    set.add(rowKey)
  } else {
    set.delete(rowKey)
  }
  selectedFieldKeys.value = [...set]
}

function toggleSelectAllRows(checked) {
  selectedFieldKeys.value = checked ? selectableFieldRows.value.map((item) => item.__rowKey) : []
}

async function handleEditField(row) {
  if (!row) return

  const isDerived = row.binding_mode === 'derived' || row.resolver_type === 'derived'
  const isBase = !isDerived && row.binding_type === 'base'
  fieldCreateMode.value = isDerived ? 'derived' : (isBase ? 'base' : 'relation')
  resetBindingForm()
  showAdvancedBinding.value = row.binding_mode === 'source_node'

  if (isDerived) {
    Object.assign(bindingForm.value, {
      binding_mode: 'derived',
      standard_field: row.standard_field || '',
      field_role: row.field_role || 'derived_field',
      resolver_type: 'derived',
      depends_on: Array.isArray(row.depends_on) ? [...row.depends_on] : [],
      formula: row.formula || '',
      notes: editableNotes(row.notes),
      edit_mode: true,
      original_base_node: row.base_node || '',
      original_source_table: row.source_table || '',
      original_standard_field: row.standard_field || '',
      original_source_node: row.source_node || '',
      original_source_field: row.source_field || '',
      original_binding_type: row.binding_type || '',
    })
    fieldDialogVisible.value = true
    return
  }

  if (isBase) {
    Object.assign(bindingForm.value, {
      binding_mode: row.binding_mode || 'source_table',
      base_source_field: row.source_field || '',
      standard_field: row.standard_field || '',
      field_role: row.field_role === 'base_field' ? 'direct_field' : (row.field_role || 'direct_field'),
      resolver_type: row.resolver_type || 'direct',
      notes: editableNotes(row.notes),
      edit_mode: true,
      original_base_node: row.base_node || '',
      original_source_table: row.source_table || '',
      original_standard_field: row.standard_field || '',
      original_source_node: row.source_node || '',
      original_source_field: row.source_field || '',
      original_binding_type: row.binding_type || '',
    })
    fieldDialogVisible.value = true
    return
  }

  const bindingMode = row.binding_mode || 'source_table'
  const { database, tableName } = splitTableRef(row.source_table)
  if (bindingMode === 'source_table') {
    if (database) {
      await notifyAction('', () => selectAttachDatabase(database))
    }
    if (tableName) {
      await notifyAction('', () => selectAttachTable(tableName))
    }
  }

  Object.assign(bindingForm.value, {
    binding_mode: bindingMode,
    source_node: bindingMode === 'source_node' ? (row.source_node || '') : '',
    standard_field: row.standard_field || '',
    source_field: row.source_field || '',
    field_role: row.field_role || 'direct_field',
    resolver_type: row.resolver_type || 'direct',
    base_join_field: row.base_join_field || '',
    source_join_field: row.source_join_field || '',
    time_mode: row.relation_mode || bindingForm.value.time_mode || 'same_date',
    base_time_field: row.base_time_field || '',
    source_time_field: row.source_time_field || '',
    source_start_field: row.source_start_field || '',
    source_end_field: row.source_end_field || '',
    notes: editableNotes(row.notes),
    edit_mode: true,
    original_base_node: row.base_node || '',
    original_source_table: row.source_table || '',
    original_standard_field: row.standard_field || '',
    original_source_node: row.source_node || '',
    original_source_field: row.source_field || '',
    original_binding_type: row.binding_type || '',
  })
  fieldDialogVisible.value = true
}

function openFieldDialog(mode) {
  fieldCreateMode.value = mode
  resetBindingForm()
  showAdvancedBinding.value = false
  if (mode === 'derived') {
    bindingForm.value.binding_mode = 'derived'
    bindingForm.value.field_role = 'derived_field'
    bindingForm.value.resolver_type = 'derived'
  } else {
    bindingForm.value.binding_mode = 'source_table'
  }
  fieldDialogVisible.value = true
}

const derivedDependencyOptions = computed(() => {
  const seen = new Set()
  return currentNodeFieldBindings.value
    .map((item) => String(item.standard_field || '').trim())
    .filter(Boolean)
    .filter((value) => {
      if (value === String(bindingForm.value.standard_field || '').trim()) return false
      if (seen.has(value)) return false
      seen.add(value)
      return true
    })
    .sort((a, b) => a.localeCompare(b))
    .map((value) => ({ label: value, value }))
})

const baseFieldOptions = computed(() => {
  return baseColumnOptions.value.map((column) => ({
    ...column,
    disabled: false,
  }))
})

const grainOptions = [
  { label: '日线 daily', value: 'daily' },
  { label: '分钟 minute', value: 'minute' },
  { label: '日频因子 daily_factor', value: 'daily_factor' },
]

const timeModeOptions = [
  { label: '不配置时间绑定', value: 'none' },
  { label: '同日 same_date', value: 'same_date' },
  { label: '同刻 same_timestamp', value: 'same_timestamp' },
  { label: '向前对齐 asof', value: 'asof' },
  { label: '生效区间 effective_range', value: 'effective_range' },
]

const assetTypeOptions = [
  { label: '股票 stock', value: 'stock' },
  { label: 'ETF etf', value: 'etf' },
  { label: '指数 index', value: 'index' },
  { label: '基金 fund', value: 'fund' },
  { label: '宏观 macro', value: 'macro' },
  { label: '可转债 kzz', value: 'kzz' },
]

const queryFreqOptions = [
  { label: '日线 1d', value: '1d' },
  { label: '分钟 1m', value: '1m' },
]

const nodeStatusOptions = [
  { label: '启用', value: 'enabled' },
  { label: '停用', value: 'disabled' },
]

const baseFilterOpOptions = [
  { label: '=', value: '=' },
  { label: '!=', value: '!=' },
  { label: 'in', value: 'in' },
  { label: 'not_in', value: 'not_in' },
  { label: 'like', value: 'like' },
]

const bindingModeOptions = [
  { label: '来源表', value: 'source_table' },
  { label: '来源节点', value: 'source_node' },
]

const visibleBindingModeOptions = computed(() => {
  if (showAdvancedBinding.value || bindingForm.value.binding_mode === 'source_node') {
    return bindingModeOptions
  }
  return bindingModeOptions.filter((item) => item.value === 'source_table')
})

watch(
  () => bindingForm.value.base_source_field,
  (value) => {
    if (fieldCreateMode.value !== 'base') return
    if (!value) return
    if (!bindingForm.value.standard_field || bindingForm.value.standard_field === bindingForm.value.source_field) {
      bindingForm.value.standard_field = value
    }
  }
)

async function selectNodeFromRoute() {
  const routeNode = normalizeRouteQueryValue(route.query.node)
  if (!routeNode) return
  if (routeNode === nodeForm.value.name) return

  const target = visibleNodes.value.find((node) => node.name === routeNode)
  if (!target) return
  await notifyAction('', () => editNode(target))
  selectedWorkbenchType.value = 'node'
  bindingFilter.value = 'all'
  bindingSearch.value = ''
}

watch(
  () => route.query.node,
  () => {
    selectNodeFromRoute().catch(() => {})
  }
)

watch(
  () => visibleNodes.value.length,
  () => {
    selectNodeFromRoute().catch(() => {})
  }
)

watch(
  nodeListFilterOptions,
  (options) => {
    if (!options.some((item) => item.value === nodeStatusFilter.value)) {
      nodeStatusFilter.value = 'all'
    }
  },
  { immediate: true }
)

onMounted(async () => {
  await ensureWorkspaceLoaded()
  await syncWideTableSelectionFromGraph()
  await loadWideTableSyncStates()
  await selectNodeFromRoute()
})

watch(
  () => bindingForm.value.source_field,
  (value) => {
    if (fieldCreateMode.value !== 'relation') return
    if (!value) return
    if (!bindingForm.value.standard_field || bindingForm.value.standard_field === bindingForm.value.source_field) {
      bindingForm.value.standard_field = value
    }
    if (!bindingForm.value.source_join_field) {
      const sameName = attachColumnOptions.value.find((item) => item.name === bindingForm.value.base_join_field)
      if (sameName) {
        bindingForm.value.source_join_field = sameName.name
      }
    }
  }
)

watch(
  showAdvancedBinding,
  (enabled) => {
    if (!enabled && bindingForm.value.binding_mode === 'source_node') {
      bindingForm.value.binding_mode = 'source_table'
    }
  }
)

watch(
  () => nodeForm.value.name,
  async (value, oldValue) => {
    if (value === oldValue) return
    await syncWideTableSelectionFromGraph()
  }
)

watch(
  () => wideTableForm.value.target_database,
  async (value, oldValue) => {
    if (value === oldValue) return
    wideTableForm.value.target_table = ''
    await loadWideTableTargetTables(value)
  }
)

watch(
  wideTableDesigns,
  (items) => {
    if (!items.length) {
      selectedWideTableId.value = ''
      return
    }
    if (!items.some((item) => item.id === selectedWideTableId.value)) {
      selectedWideTableId.value = items[0].id
    }
  },
  { immediate: true }
)

function openCreateDialog() {
  nodeDialogMode.value = 'create'
  startNewNode()
  nodeDialogVisible.value = true
}

function openEditDialog() {
  nodeDialogMode.value = 'edit'
  nodeDialogVisible.value = true
}

function addBaseFilterRow() {
  nodeForm.value.base_filters.push({
    field: '',
    op: '=',
    value: '',
  })
}

function removeBaseFilterRow(index) {
  nodeForm.value.base_filters.splice(index, 1)
}

async function handleNodeDatabaseChange(value) {
  await notifyAction('', () => selectDatabase(value))
}

async function handleNodeTableChange(value) {
  await notifyAction('', () => selectTable(value))
  if (!nodeForm.value.name) {
    inferNodeTemplate()
  }
}

async function handleAttachDatabaseChange(value) {
  await notifyAction('', () => selectAttachDatabase(value))
}

async function handleAttachTableChange(value) {
  await notifyAction('', () => selectAttachTable(value))
}

async function handleEditNode(node) {
  selectedWorkbenchType.value = 'node'
  bindingFilter.value = 'all'
  bindingSearch.value = ''
  await notifyAction('', () => editNode(node))
}

async function handleSelectWorkbenchRow(row) {
  if (!row) return
  if (row.__rowType === 'wide_table') {
    selectWideTable(row)
    return
  }
  await handleEditNode(row)
}

async function handleSaveNodeDialog() {
  const message = nodeDialogMode.value === 'create' ? '节点已保存' : '节点已更新'
  await notifyAction(message, async () => {
    saveNode()
    await saveWorkspace()
  })
  nodeDialogVisible.value = false
}

async function handleAddBaseField() {
  await notifyAction('字段已添加', async () => {
    addBaseFieldBinding()
    await saveWorkspace()
  })
  fieldDialogVisible.value = false
}

async function handleAddDerivedField() {
  await notifyAction('派生字段已添加', async () => {
    saveDerivedFieldBinding()
    await saveWorkspace()
  })
  fieldDialogVisible.value = false
}

async function handleAddRelatedField() {
  await notifyAction('字段已添加', async () => {
    saveRelatedFieldBinding()
    await saveWorkspace()
  })
  fieldDialogVisible.value = false
}

async function handleAiWriteNotes() {
  if (!selectedFieldRows.value.length) return
  aiNotesLoading.value = true
  try {
    const payload = await callJson('/api/fields/ai-notes', {
      method: 'POST',
      body: JSON.stringify({
        runtime_path: workspace.value.runtimePath,
        node_name: nodeForm.value.name,
        items: selectedFieldRows.value.map((item) => ({
          row_key: item.__rowKey,
          standard_field: item.standard_field,
          binding_type: item.binding_type,
          binding_mode: item.binding_mode,
          source_table: item.source_table,
          source_field: item.source_field,
          relation_mode: item.relation_mode,
          depends_on: item.depends_on || [],
          formula: item.formula || '',
          notes: item.notes || [],
        })),
      }),
    })
    if (payload.ok === false) {
      throw new Error(payload.error || 'AI 备注生成失败')
    }

    const notesByKey = new Map(
      (payload.items || []).map((item) => [String(item.row_key || ''), Array.isArray(item.notes) ? item.notes : []])
    )
    for (const row of selectedFieldRows.value) {
      const nextNotes = notesByKey.get(row.__rowKey)
      if (!nextNotes || !nextNotes.length) continue
      const fieldIndex = fields.value.findIndex((item) =>
        String(item.base_node || '') === String(row.base_node || '') &&
        String(item.standard_field || '') === String(row.standard_field || '') &&
        String(item.binding_mode || '') === String(row.binding_mode || '') &&
        String(item.source_table || '') === String(row.source_table || '') &&
        String(item.source_node || '') === String(row.source_node || '') &&
        String(item.source_field || '') === String(row.source_field || '')
      )
      if (fieldIndex >= 0) {
        fields.value[fieldIndex] = {
          ...fields.value[fieldIndex],
          notes: nextNotes,
        }
      } else {
        fields.value.push({
          standard_field: row.standard_field,
          source_node: row.binding_mode === 'source_node' ? (row.source_node || null) : null,
          source_field: row.source_field || null,
          field_role: row.binding_type === 'derived' ? 'derived_field' : 'direct_field',
          base_node: row.base_node || nodeForm.value.name,
          binding_mode: row.binding_mode || 'source_table',
          source_table: row.source_table || null,
          relation_type: row.binding_type === 'derived' ? null : (row.relation_type || null),
          join_keys: [],
          time_binding: null,
          bridge_steps: [],
          resolver_type: row.binding_type === 'derived' ? 'derived' : 'direct',
          depends_on: Array.isArray(row.depends_on) ? row.depends_on : [],
          formula: row.formula || null,
          applies_to_grain: nodeForm.value.grain || null,
          path_domain: null,
          path_group: null,
          via_node: null,
          time_semantics: null,
          lookahead_category: null,
          description_zh: null,
          notes: nextNotes,
        })
      }
    }
    await saveWorkspace()
    selectedFieldKeys.value = []
    await notifyAction('AI 备注已写入', async () => {})
  } finally {
    aiNotesLoading.value = false
  }
}

async function handleDeleteNode() {
  if (!nodeForm.value.name) return
  const impact = getNodeDeleteImpact(nodeForm.value.name)

  if (!impact.canDelete) {
    const lines = []
    if (impact.referencedBySourceFields.length) {
      lines.push(`被 ${impact.referencedBySourceFields.length} 个字段作为 source_node 引用：${impact.referencedBySourceFields.slice(0, 12).map((item) => item.standard_field).join('、')}${impact.referencedBySourceFields.length > 12 ? '...' : ''}`)
    }
    if (impact.referencedByLegacyTableFields.length) {
      lines.push(`仍有 ${impact.referencedByLegacyTableFields.length} 个旧版 source_table 字段还依赖这个节点：${impact.referencedByLegacyTableFields.slice(0, 12).map((item) => item.standard_field).join('、')}${impact.referencedByLegacyTableFields.length > 12 ? '...' : ''}`)
    }

    await ElMessageBox.alert(lines.join('<br/><br/>'), '节点仍被引用', {
      confirmButtonText: '知道了',
      dangerouslyUseHTMLString: true,
    })
    return
  }

  const cleanupLines = []
  if (impact.referencedBySourceFields.length) {
    cleanupLines.push(`会同时删除 ${impact.referencedBySourceFields.length} 个 source_node 字段绑定：${impact.referencedBySourceFields.slice(0, 12).map((item) => item.standard_field).join('、')}${impact.referencedBySourceFields.length > 12 ? '...' : ''}`)
  }
  if (impact.referencedByLegacyTableFields.length) {
    cleanupLines.push(`会同时删除 ${impact.referencedByLegacyTableFields.length} 个旧版 source_table 字段绑定：${impact.referencedByLegacyTableFields.slice(0, 12).map((item) => item.standard_field).join('、')}${impact.referencedByLegacyTableFields.length > 12 ? '...' : ''}`)
  }
  const confirmMessage = cleanupLines.length
    ? `确定删除节点 ${nodeForm.value.name} 吗？<br/><br/>${cleanupLines.join('<br/>')}`
    : `确定删除节点 ${nodeForm.value.name} 吗？`

  await ElMessageBox.confirm(confirmMessage, '删除节点', {
    type: 'warning',
    confirmButtonText: '删除',
    cancelButtonText: '取消',
    dangerouslyUseHTMLString: cleanupLines.length > 0,
  })
  await notifyAction('节点已删除', async () => {
    deleteNode(nodeForm.value.name)
    await saveWorkspace()
  })
}

async function handleDuplicateNode() {
  if (!nodeForm.value.name) return
  await notifyAction('节点已复制', async () => {
    await duplicateNode(nodeForm.value.name)
    await saveWorkspace()
  })
}

async function handleRemoveField(row) {
  await notifyAction('字段已删除', async () => {
    removeFieldBinding(row)
    await saveWorkspace()
  })
}

function normalizeRouteQueryValue(value) {
  if (Array.isArray(value)) return String(value[0] || '')
  return String(value || '')
}
</script>

<template>
  <div class="page-stack">
    <section class="workbench-grid workbench-grid-nodehub-focused">
      <el-card shadow="never" class="surface-card surface-card-rail surface-card-rail-slim">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div></div>
            <div class="panel-actions panel-actions-compact database-rail-actions">
              <el-segmented v-model="nodeStatusFilter" :options="nodeListFilterOptions" class="database-rail-filter" />
              <el-input
                v-model="nodeSearch"
                clearable
                placeholder="搜索节点 / 主表 / 目标表 / 字段"
                class="panel-search database-rail-search"
              />
              <el-button type="primary" plain class="database-rail-create" @click="openCreateDialog">新建节点</el-button>
            </div>
          </div>
        </template>

        <el-table
          :data="workbenchRows"
          row-key="__rowKey"
          height="720"
          class="node-list-table"
          table-layout="fixed"
          highlight-current-row
          :current-row-key="currentWorkbenchRowKey"
          empty-text="当前没有节点或宽表节点"
          @row-click="handleSelectWorkbenchRow"
        >
          <el-table-column label="节点 / 目标表">
            <template #default="{ row }">
              <div class="node-list-primary">
                <strong>{{ row.name }}</strong>
                <span>{{ row.__summary }}</span>
                <div class="node-tag-cloud node-tag-cloud-rail">
                  <el-tag
                    size="small"
                    effect="plain"
                    :type="row.__rowType === 'wide_table' || row.__wideTable ? 'warning' : 'primary'"
                    disable-transitions
                  >
                    {{ row.__rowType === 'wide_table' || row.__wideTable ? '宽表' : '普通' }}
                  </el-tag>
                  <el-tag size="small" effect="plain" :type="row.status === 'enabled' ? 'success' : 'info'" disable-transitions>
                    {{ row.status === 'enabled' ? (row.__rowType === 'wide_table' || row.__wideTable ? '可用' : '启用') : (row.__rowType === 'wide_table' || row.__wideTable ? '不可用' : '停用') }}
                  </el-tag>
                  <el-tag v-if="row.__rowType === 'wide_table' && row.engine" size="small" effect="plain" type="success" disable-transitions>
                    {{ row.engine }}
                  </el-tag>
                  <el-tag v-if="row.__wideTable?.engine" size="small" effect="plain" type="success" disable-transitions>
                    {{ row.__wideTable.engine }}
                  </el-tag>
                  <el-tag v-if="row.__rowType === 'node' && row.asset_type" size="small" effect="plain" type="success" disable-transitions>{{ row.asset_type }}</el-tag>
                  <el-tag v-if="row.__rowType === 'node' && row.query_freq" size="small" effect="plain" disable-transitions>{{ row.query_freq }}</el-tag>
                </div>
              </div>
            </template>
          </el-table-column>
        </el-table>
      </el-card>

      <div class="stack-block node-detail-stack">
        <template v-if="selectedWorkbenchType === 'wide_table'">
          <el-card shadow="never" class="surface-card">
            <template #header>
              <div class="panel-heading panel-heading-space">
                <div>
                  <span class="panel-title">当前宽表节点</span>
                </div>
                <div class="panel-actions panel-actions-compact">
                  <el-button :loading="wideTableStateLoading" :disabled="!selectedWideTable" @click="loadWideTableSyncStates">刷新同步状态</el-button>
                  <el-button type="success" :loading="wideTableSyncLoading" :disabled="!selectedWideTable" @click="selectedWideTable && notifyAction('', () => handleRunWideTableSync(selectedWideTable))">同步宽表</el-button>
                  <el-button :disabled="!selectedWideTable" @click="selectedWideTable && openWideTableEditDialog(selectedWideTable)">编辑宽表节点</el-button>
                  <el-button type="danger" plain :disabled="!selectedWideTable" @click="selectedWideTable && handleDeleteWideTable(selectedWideTable)">删除宽表节点</el-button>
                </div>
              </div>
            </template>

            <div v-if="selectedWideTable" class="stack-block">
              <div class="table-hero-strip">
                <div class="table-hero-meta">
                  <h3>{{ selectedWideTable.name }}</h3>
                  <p>{{ selectedWideTable.description || '当前宽表节点暂无描述' }}</p>
                </div>
                <div class="panel-actions panel-actions-compact">
                  <el-tag round effect="plain">字段 {{ currentWideTableFieldRows.length }}</el-tag>
                  <el-tag round effect="plain" :type="selectedWideTableStatus.tagType">
                    {{ selectedWideTableStatus.label }}
                  </el-tag>
                </div>
              </div>

              <el-descriptions :column="3" border class="node-summary-descriptions">
                <el-descriptions-item v-for="item in currentWideTableSummary" :key="item.label" :label="item.label">
                  {{ item.value }}
                </el-descriptions-item>
              </el-descriptions>
            </div>
            <el-empty v-else description="请先从左侧选择一个宽表节点" :image-size="84" />
          </el-card>

          <el-card shadow="never" class="surface-card surface-card-strong">
            <template #header>
              <div class="panel-heading panel-heading-space">
                <div>
                  <span class="panel-title">绑定字段表</span>
                </div>
                <div class="panel-actions panel-actions-compact binding-toolbar-meta">
                  <el-button
                    type="primary"
                    :disabled="!selectedWideTable"
                    @click="handleAddWideTableField"
                  >
                    新增字段
                  </el-button>
                  <el-tag type="success" effect="plain" round>
                    绑定数量 {{ wideTableBindingRows.length }}
                  </el-tag>
                  <el-input
                    v-model="wideTableFieldSearch"
                    clearable
                    placeholder="搜索字段名 / 主键 / 排序"
                    class="panel-search panel-search-wide panel-search-xl"
                  />
                </div>
              </div>
            </template>

            <div v-if="wideTableBindingRows.length" class="binding-table-v2-shell binding-table-v2-shell-wide">
              <el-auto-resizer>
                <template #default="{ width, height }">
                  <el-table-v2
                    :columns="wideTableBindingDisplayColumns"
                    :data="wideTableBindingRows"
                    :width="width"
                    :height="height"
                    :row-height="52"
                    :header-height="44"
                    row-key="__rowKey"
                    fixed
                    class="binding-table-v2"
                  />
                </template>
              </el-auto-resizer>
            </div>
            <el-empty v-else description="当前宽表节点还没有字段结构" :image-size="56" />
          </el-card>
        </template>

        <template v-else>
          <el-card shadow="never" class="surface-card">
            <template #header>
              <div class="panel-heading panel-heading-space">
                <div>
                  <span class="panel-title">{{ currentNodeWideTable ? '当前宽表节点' : '当前节点' }}</span>
                </div>
                <div class="panel-actions panel-actions-compact">
                  <el-button v-if="currentNodeWideTable" :loading="wideTableStateLoading" :disabled="!nodeForm.name" @click="loadWideTableSyncStates">刷新同步状态</el-button>
                  <el-button v-if="currentNodeWideTable" type="success" :loading="wideTableSyncLoading" :disabled="!nodeForm.name" @click="notifyAction('', () => handleRunWideTableSync(currentNodeWideTable))">同步宽表</el-button>
                  <el-button v-if="currentNodeWideTable" :disabled="!nodeForm.name" @click="openWideTableEditDialog(currentNodeWideTable)">编辑宽表配置</el-button>
                  <el-button v-else type="primary" plain :disabled="!nodeForm.name" @click="handleConvertCurrentNodeToWideTable">转换宽表</el-button>
                  <el-button plain :disabled="!nodeForm.name" @click="handleDuplicateNode">复制节点</el-button>
                  <el-button :disabled="!nodeForm.name" @click="openEditDialog">编辑节点</el-button>
                  <el-button type="danger" plain :disabled="!nodeForm.name" @click="handleDeleteNode">删除节点</el-button>
                </div>
              </div>
            </template>

            <div v-if="nodeForm.name" class="stack-block">
              <div class="table-hero-strip">
                <div class="table-hero-meta">
                  <h3>{{ nodeForm.name }}</h3>
                  <p>{{ nodeForm.description || '当前节点暂无描述' }}</p>
                </div>
                <div class="panel-actions panel-actions-compact">
                  <el-tag v-if="currentNodeWideTable" round effect="plain" type="warning">宽表</el-tag>
                  <el-tag v-if="currentNodeWideTable?.engine" round effect="plain" type="success">{{ currentNodeWideTable.engine }}</el-tag>
                  <el-tag round effect="plain">绑定字段 {{ currentNodeFieldBindings.length }}</el-tag>
                  <el-tag v-if="currentNodeWideTable" round effect="plain" :type="currentNodeWideTableStatus.tagType">
                    {{ currentNodeWideTableStatus.label }}
                  </el-tag>
                </div>
              </div>

              <div class="mini-description">
                {{ currentNodeBindingHint }}
              </div>

              <el-descriptions :column="4" border class="node-summary-descriptions">
                <el-descriptions-item v-for="item in currentNodeSummary" :key="item.label" :label="item.label">
                  {{ item.value }}
                </el-descriptions-item>
              </el-descriptions>
            </div>
            <el-empty v-else description="请先从左侧选择一个节点" :image-size="84" />
          </el-card>

          <el-card shadow="never" class="surface-card surface-card-strong">
            <template #header>
              <div class="panel-heading panel-heading-space">
                <div>
                  <span class="panel-title">绑定字段表</span>
                </div>
                <div class="panel-actions panel-actions-compact binding-toolbar-meta">
                  <el-button type="primary" :disabled="!nodeForm.name" @click="openFieldDialog('base')">添加字段</el-button>
                  <el-tag type="success" effect="plain" round>
                    绑定数量 {{ filteredFieldBindings.length }}
                  </el-tag>
                  <el-input
                    v-model="bindingSearch"
                    clearable
                    placeholder="搜索标准字段 / 来源表 / Join Keys"
                    class="panel-search panel-search-wide panel-search-xl"
                  />
                  <el-segmented
                    v-model="bindingFilter"
                    :options="[
                      { label: '全部', value: 'all' },
                      { label: '主表字段', value: 'base' },
                      { label: '派生字段', value: 'derived' },
                      { label: '关联字段', value: 'relation' }
                    ]"
                  />
                </div>
              </div>
            </template>

            <div v-if="filteredFieldBindings.length" class="binding-table-v2-shell binding-table-v2-shell-wide">
              <el-auto-resizer>
                <template #default="{ width, height }">
                  <el-table-v2
                    :columns="bindingTableColumns"
                    :data="filteredFieldBindings"
                    :width="width"
                    :height="height"
                    :row-height="52"
                    :header-height="44"
                    row-key="__rowKey"
                    fixed
                    class="binding-table-v2"
                  />
                </template>
              </el-auto-resizer>
            </div>
            <el-empty v-else description="当前节点还没有字段绑定" :image-size="56" />

            <div v-if="selectedFieldRows.length" class="binding-table-footer">
              <div class="binding-table-footer-copy">
                <span>已选 {{ selectedFieldRows.length }} 项</span>
                <span>AI 会按当前字段定义批量生成备注并写回配置。</span>
              </div>
              <div class="panel-actions panel-actions-compact">
                <el-button :loading="aiNotesLoading" type="primary" @click="handleAiWriteNotes">AI 写备注</el-button>
              </div>
            </div>
          </el-card>
        </template>
      </div>
    </section>

    <el-dialog v-model="nodeDialogVisible" :title="nodeDialogMode === 'create' ? '新建节点' : '编辑节点'" width="920px" destroy-on-close>
      <div class="form-stack">
        <div class="mini-description">在弹窗里维护节点主表、键和粒度。保存后，主页面继续负责字段绑定。</div>
        <div class="three-col-form">
          <el-form-item label="节点名">
            <el-input v-model="nodeForm.name" placeholder="例如 stock_daily_real" />
          </el-form-item>
          <el-form-item label="数据库">
            <el-select
              :model-value="nodeForm.database"
              placeholder="选择主表数据库"
              filterable
              style="width: 100%"
              :loading="loading"
              @change="handleNodeDatabaseChange"
            >
              <el-option v-for="db in schema.databases" :key="db.name" :label="db.name" :value="db.name" />
            </el-select>
          </el-form-item>
          <el-form-item label="主表">
            <el-select
              :model-value="nodeForm.tableName"
              placeholder="选择主表"
              filterable
              style="width: 100%"
              @change="handleNodeTableChange"
            >
              <el-option v-for="table in nodeTableOptions" :key="table.name" :label="table.name" :value="table.name" />
            </el-select>
          </el-form-item>
        </div>

        <div class="three-col-form">
          <el-form-item label="主键字段 entity_keys">
            <el-select v-model="nodeForm.entity_keys" multiple collapse-tags style="width: 100%" placeholder="选择主键字段">
              <el-option v-for="column in baseColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
            </el-select>
          </el-form-item>
          <el-form-item label="时间键字段 time_key">
            <el-select v-model="nodeForm.time_key" style="width: 100%" placeholder="选择时间键字段" clearable>
              <el-option v-for="column in baseColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
            </el-select>
          </el-form-item>
          <el-form-item label="粒度 grain">
            <el-select v-model="nodeForm.grain" style="width: 100%">
              <el-option v-for="option in grainOptions" :key="option.value" :label="option.label" :value="option.value" />
            </el-select>
          </el-form-item>
        </div>

        <el-form-item label="描述">
          <el-input v-model="nodeForm.description" type="textarea" :rows="3" placeholder="描述这个节点的用途" />
        </el-form-item>

        <div class="node-config-block">
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">逻辑入口配置</span>
              <p class="panel-subtitle">这里决定这个节点是否作为逻辑入口暴露，以及是否复用别的物理节点和过滤条件。</p>
            </div>
          </div>

          <div class="three-col-form">
            <el-form-item label="资产类型 asset_type">
              <el-select v-model="nodeForm.asset_type" clearable style="width: 100%" placeholder="例如 stock / etf / index">
                <el-option v-for="option in assetTypeOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
            <el-form-item label="查询频率 query_freq">
              <el-select v-model="nodeForm.query_freq" clearable style="width: 100%" placeholder="例如 1d / 1m">
                <el-option v-for="option in queryFreqOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
            <el-form-item label="状态管理">
              <el-segmented v-model="nodeForm.status" :options="nodeStatusOptions" />
            </el-form-item>
          </div>

          <div class="stack-block">
            <div class="panel-heading panel-heading-space">
              <div>
                <span class="panel-title">基础过滤条件 base_filters</span>
                <p class="panel-subtitle">这些条件会在逻辑入口查询时自动追加到 where，例如 `security_type = EXTRA_ETF`。</p>
              </div>
              <el-button plain @click="addBaseFilterRow">添加过滤条件</el-button>
            </div>

            <div v-if="nodeForm.base_filters.length" class="filter-editor-list">
              <div v-for="(item, index) in nodeForm.base_filters" :key="`${index}-${item.field}-${item.op}`" class="filter-editor-row">
                <el-input v-model="item.field" placeholder="字段名，如 security_type" />
                <el-select v-model="item.op" style="width: 120px">
                  <el-option v-for="option in baseFilterOpOptions" :key="option.value" :label="option.label" :value="option.value" />
                </el-select>
                <el-input v-model="item.value" placeholder="值，如 EXTRA_ETF" />
                <el-button link type="danger" @click="removeBaseFilterRow(index)">删除</el-button>
              </div>
            </div>
            <el-empty v-else description="当前没有基础过滤条件" :image-size="56" />
          </div>
        </div>
      </div>
      <template #footer>
        <div class="panel-actions panel-actions-compact">
          <el-button @click="nodeDialogVisible = false">取消</el-button>
          <el-button type="primary" :disabled="!canSaveNode" @click="handleSaveNodeDialog">保存节点</el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog v-model="fieldDialogVisible" :title="fieldDialogTitle" width="960px" destroy-on-close>
      <div class="stack-block">
        <el-segmented
          v-model="fieldCreateMode"
          :options="[
            { label: '主表字段', value: 'base' },
            { label: '派生字段', value: 'derived' },
            { label: '关联字段', value: 'relation' }
          ]"
        />

        <div v-if="fieldCreateMode === 'base'" class="form-stack">
          <div class="mini-description">从当前节点主表选择字段，并将它绑定成标准字段。</div>
          <div class="three-col-form">
            <el-form-item label="主表字段">
              <el-select v-model="bindingForm.base_source_field" filterable style="width: 100%" placeholder="选择主表字段">
                <el-option
                  v-for="column in baseFieldOptions"
                  :key="column.name"
                  :label="`${column.name} (${column.type})${column.disabled ? ' · 已绑定' : ''}`"
                  :value="column.name"
                  :disabled="column.disabled"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="标准字段名">
              <el-input v-model="bindingForm.standard_field" placeholder="默认等于主表字段名" />
            </el-form-item>
          </div>
          <div class="two-col-form">
            <el-form-item label="备注">
              <el-input v-model="bindingForm.notes" placeholder="给这个字段留一条备注" />
            </el-form-item>
          </div>
        </div>

        <div v-else-if="fieldCreateMode === 'derived'" class="form-stack">
          <div class="mini-description">为当前节点新增派生字段，适合 `turnover_rate` 这类按已有标准字段计算得到的字段。</div>
          <div class="three-col-form">
            <el-form-item label="标准字段名">
              <el-input v-model="bindingForm.standard_field" placeholder="例如 turnover_rate / market_cap" />
            </el-form-item>
            <div></div>
          </div>

          <el-form-item label="依赖字段">
            <el-select
              v-model="bindingForm.depends_on"
              multiple
              filterable
              clearable
              style="width: 100%"
              placeholder="选择派生依赖字段"
            >
              <el-option
                v-for="option in derivedDependencyOptions"
                :key="option.value"
                :label="option.label"
                :value="option.value"
              />
            </el-select>
          </el-form-item>

          <el-form-item label="派生公式">
            <el-input
              v-model="bindingForm.formula"
              type="textarea"
              :rows="3"
              placeholder="例如 {volume} / nullIf(({float_share} * 10000), 0)"
            />
          </el-form-item>

          <el-form-item label="备注">
            <el-input v-model="bindingForm.notes" type="textarea" :rows="2" placeholder="记录这个派生字段的口径说明" />
          </el-form-item>
        </div>

        <div v-else class="form-stack">
          <div class="mini-description">从其他表选择字段、Join Key 和时间关系，系统会自动生成底层关系。</div>
          <div class="two-col-form">
            <el-form-item label="高级绑定">
              <div class="advanced-binding-toggle">
                <el-switch v-model="showAdvancedBinding" />
                <span class="mini-description">
                  默认只展示来源表配置，需要手工引用其他入口节点时再打开来源节点模式。
                </span>
              </div>
            </el-form-item>
            <el-form-item v-if="showAdvancedBinding || bindingForm.binding_mode === 'source_node'" label="绑定模式">
              <el-segmented v-model="bindingForm.binding_mode" :options="visibleBindingModeOptions" />
            </el-form-item>
          </div>

          <div v-if="bindingForm.binding_mode === 'source_table'" class="three-col-form">
            <el-form-item label="来源数据库">
              <el-select
                :model-value="attach.selectedDatabase"
                placeholder="选择来源数据库"
                filterable
                style="width: 100%"
                @change="handleAttachDatabaseChange"
              >
                <el-option v-for="db in attach.databases" :key="db.name" :label="db.name" :value="db.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源表">
              <el-select
                :model-value="attach.selectedTable"
                placeholder="选择来源表"
                filterable
                style="width: 100%"
                @change="handleAttachTableChange"
              >
                <el-option v-for="table in attachTableOptions" :key="table.name" :label="table.name" :value="table.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源字段">
              <el-select v-model="bindingForm.source_field" placeholder="选择来源字段" filterable style="width: 100%">
                <el-option v-for="column in attachColumnOptions" :key="column.name" :label="`${column.name} (${column.type})`" :value="column.name" />
              </el-select>
            </el-form-item>
          </div>

          <div v-else class="three-col-form">
            <el-form-item label="来源节点">
              <el-select v-model="bindingForm.source_node" filterable clearable style="width: 100%" placeholder="选择来源节点">
                <el-option v-for="node in sourceNodeOptions" :key="node.name" :label="`${node.name} · ${node.table}`" :value="node.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源字段">
              <el-input v-model="bindingForm.source_field" placeholder="填写来源节点字段名" />
            </el-form-item>
            <el-form-item label="标准字段名">
              <el-input v-model="bindingForm.standard_field" placeholder="例如 is_st / market_cap" />
            </el-form-item>
          </div>

          <div class="three-col-form">
            <el-form-item v-if="bindingForm.binding_mode === 'source_table'" label="标准字段名">
              <el-input v-model="bindingForm.standard_field" placeholder="例如 is_st / market_cap" />
            </el-form-item>
          </div>

          <div v-if="bindingForm.binding_mode === 'source_table'" class="three-col-form">
            <el-form-item label="当前节点主键字段">
              <el-select v-model="bindingForm.base_join_field" style="width: 100%" placeholder="选择主表键">
                <el-option v-for="column in baseColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源表关联字段">
              <el-select v-model="bindingForm.source_join_field" style="width: 100%" placeholder="选择来源表键">
                <el-option v-for="column in attachColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="时间绑定模式">
              <el-select v-model="bindingForm.time_mode" style="width: 100%">
                <el-option v-for="option in timeModeOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </el-form-item>
          </div>

          <div class="three-col-form" v-if="bindingForm.binding_mode === 'source_table' && bindingForm.time_mode !== 'none' && bindingForm.time_mode !== 'effective_range'">
            <el-form-item label="当前节点时间字段">
              <el-select v-model="bindingForm.base_time_field" style="width: 100%" placeholder="选择主表时间字段">
                <el-option v-for="column in baseColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源表时间字段">
              <el-select v-model="bindingForm.source_time_field" style="width: 100%" placeholder="选择来源表时间字段">
                <el-option v-for="column in attachColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
          </div>

          <div class="three-col-form" v-if="bindingForm.binding_mode === 'source_table' && bindingForm.time_mode === 'effective_range'">
            <el-form-item label="当前节点时间字段">
              <el-select v-model="bindingForm.base_time_field" style="width: 100%" placeholder="选择主表时间字段">
                <el-option v-for="column in baseColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源开始时间字段">
              <el-select v-model="bindingForm.source_start_field" style="width: 100%" placeholder="选择开始字段">
                <el-option v-for="column in attachColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
            <el-form-item label="来源结束时间字段">
              <el-select v-model="bindingForm.source_end_field" style="width: 100%" placeholder="选择结束字段">
                <el-option v-for="column in attachColumnOptions" :key="column.name" :label="column.name" :value="column.name" />
              </el-select>
            </el-form-item>
          </div>

          <el-form-item label="备注">
            <el-input v-model="bindingForm.notes" type="textarea" :rows="2" placeholder="记录这个字段的关系说明" />
          </el-form-item>
        </div>
      </div>

      <template #footer>
        <div class="panel-actions panel-actions-compact">
          <el-button @click="fieldDialogVisible = false">取消</el-button>
          <el-button v-if="fieldCreateMode === 'base'" type="primary" :disabled="!canAddBaseField" @click="handleAddBaseField">
            {{ fieldDialogConfirmText }}
          </el-button>
          <el-button v-else-if="fieldCreateMode === 'derived'" type="primary" :disabled="!canAddDerivedField" @click="handleAddDerivedField">
            {{ fieldDialogConfirmText }}
          </el-button>
          <el-button v-else type="primary" :disabled="!canBindField" @click="handleAddRelatedField">
            {{ fieldDialogConfirmText }}
          </el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog v-model="wideTableDialogVisible" :title="wideTableForm.id ? '编辑宽表节点' : '从普通节点转换宽表节点'" width="880px" destroy-on-close>
      <div class="form-stack">
        <div class="mini-description">这里配置的不是查询节点，而是一个由当前节点字段生成的宽表节点定义。</div>
        <div class="three-col-form">
          <el-form-item label="设计名">
            <el-input v-model="wideTableForm.name" placeholder="例如 stock_daily_real" disabled />
          </el-form-item>
          <el-form-item label="目标库">
            <el-select
              v-model="wideTableForm.target_database"
              filterable
              clearable
              placeholder="选择目标库"
              style="width: 100%"
              disabled
            >
              <el-option
                v-for="db in schema.databases"
                :key="db.name"
                :label="db.name"
                :value="db.name"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="目标表">
            <el-select
              v-model="wideTableForm.target_table"
              filterable
              clearable
              allow-create
              default-first-option
              placeholder="选择或输入目标表"
              style="width: 100%"
              disabled
            >
              <el-option
                v-for="table in wideTableTargetTables"
                :key="table.name"
                :label="table.name"
                :value="table.name"
              />
            </el-select>
          </el-form-item>
          <el-form-item label="引擎">
            <el-select v-model="wideTableForm.engine" style="width: 100%">
              <el-option label="Memory" value="Memory" />
              <el-option label="ReplacingMergeTree" value="ReplacingMergeTree" />
            </el-select>
          </el-form-item>
          <el-form-item label="状态">
            <el-select v-model="wideTableForm.status" style="width: 100%">
              <el-option label="可用" value="enabled" />
              <el-option label="不可用" value="disabled" />
            </el-select>
          </el-form-item>
          <el-form-item label="版本字段">
            <el-select v-model="wideTableForm.version_field" clearable placeholder="ReplacingMergeTree 可选" style="width: 100%">
              <el-option v-for="item in wideTableFieldOptions" :key="item" :label="item" :value="item" />
            </el-select>
          </el-form-item>
        </div>

        <el-form-item label="描述">
          <el-input v-model="wideTableForm.description" type="textarea" :rows="2" placeholder="描述宽表节点用途，可选" />
        </el-form-item>

        <el-form-item label="字段">
          <el-select v-model="wideTableForm.fields" multiple filterable collapse-tags collapse-tags-tooltip style="width: 100%" placeholder="选择需要落宽表节点的字段">
            <el-option v-for="item in wideTableFieldOptions" :key="item" :label="item" :value="item" />
          </el-select>
        </el-form-item>

        <el-form-item label="主键字段">
          <el-select v-model="wideTableForm.key_fields" multiple filterable collapse-tags collapse-tags-tooltip style="width: 100%" placeholder="选择宽表节点主键字段">
            <el-option v-for="item in wideTableFieldOptions" :key="item" :label="item" :value="item" />
          </el-select>
        </el-form-item>

        <div class="three-col-form">
          <el-form-item label="Partition By">
            <el-input v-model="wideTableForm.partition_by_text" placeholder="逗号分隔，例如 toYYYYMM(trade_time)" />
          </el-form-item>
          <el-form-item label="Order By">
            <el-input v-model="wideTableForm.order_by_text" placeholder="逗号分隔，默认用主键字段" />
          </el-form-item>
          <el-form-item label="来源节点">
            <el-input :model-value="nodeForm.name || wideTableForm.source_node" disabled />
          </el-form-item>
        </div>
      </div>
      <template #footer>
        <el-button @click="wideTableDialogVisible = false">取消</el-button>
        <el-button type="primary" :disabled="!wideTableCanSave" @click="handleSaveWideTable">保存</el-button>
      </template>
    </el-dialog>

  </div>
</template>
