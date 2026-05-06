<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useCapabilityAccess } from '~/composables/useCapabilityAccess'

const {
  loading,
  saving,
  workspacePayload,
  providerNodes,
  dataAssets,
  loadWorkspace,
  loadDataAssets,
  saveDataAsset,
  deleteDataAsset,
} = useCapabilityAccess()

const ENTITY_PRESETS = [
  {
    id: 'stock',
    name: '股票',
    role: 'center',
    description: 'discrete_stock 的中心实体，所有交易、持仓和下单最终都落到股票。',
  },
  {
    id: 'index',
    name: '指数',
    role: 'external',
    description: '指数日线、指数成分、指数信号等能力可通过成分关系映射到股票。',
  },
  {
    id: 'industry',
    name: '行业',
    role: 'external',
    description: '行业日线、行业分类、行业成分等能力可用于行业研究和股票映射。',
  },
  {
    id: 'concept',
    name: '概念',
    role: 'external',
    description: '概念热度、概念成分、主题池等能力可作为股票池或信号来源。',
  },
  {
    id: 'etf',
    name: 'ETF',
    role: 'external',
    description: 'ETF 日线、ETF 成分和持仓关系可用于基金类策略研究。',
  },
  {
    id: 'fund',
    name: '基金',
    role: 'external',
    description: '基金净值、基金持仓和基金披露数据可作为扩展研究对象。',
  },
]

const DATA_SHAPES = {
  time_series: {
    label: '时间序列',
    brief: '实体 + 时间 + 字段',
    tone: 'green',
  },
  relation: {
    label: '区间关系',
    brief: '集合实体 + 成员实体 + 生效区间',
    tone: 'blue',
  },
  disclosure: {
    label: '披露生效',
    brief: '报告期 + 披露日 + 字段',
    tone: 'orange',
  },
  event: {
    label: '事件记录',
    brief: '实体 + 事件时间 + 事件字段',
    tone: 'red',
  },
  static_profile: {
    label: '静态资料',
    brief: '实体 + 基础属性',
    tone: 'gray',
  },
}

const ACCESS_TYPES = {
  entity_data: {
    label: '实体自身数据',
    brief: '给当前实体补充日线、分钟、披露、事件或静态字段。',
  },
  relation_data: {
    label: '实体关系数据',
    brief: '建立当前实体和另一个实体之间的成分、归属或映射关系。',
  },
}

const MARKET_GRAIN_OPTIONS = [
  { value: 'daily', label: '日线' },
  { value: 'minute', label: '分钟' },
]

const ASSET_BINDING_SCHEMAS = {
  time_series: [
    { key: 'entity', label: '实体字段', required: true, placeholder: '选择股票/实体代码字段' },
    { key: 'time', label: '时间字段', required: true, placeholder: '选择交易日或时间字段' },
  ],
  relation: [
    { key: 'group', label: '实体标识 code', required: true, placeholder: '选择实体代码字段' },
    { key: 'member', label: '股票字段标识 code', required: true, placeholder: '选择股票代码字段' },
    { key: 'start', label: '生效开始', required: true, placeholder: '选择开始日期字段' },
    { key: 'end', label: '生效结束', required: true, placeholder: '选择结束日期字段' },
  ],
  disclosure: [
    { key: 'entity', label: '实体字段', required: true, placeholder: '选择股票/实体代码字段' },
    { key: 'announce_date', label: '披露日字段', required: true, placeholder: '选择披露日期字段' },
    { key: 'report_period', label: '报告期字段', required: false, placeholder: '选择报告期字段' },
  ],
  event: [
    { key: 'entity', label: '实体字段', required: true, placeholder: '选择股票/实体代码字段' },
    { key: 'event_time', label: '事件时间字段', required: true, placeholder: '选择事件时间字段' },
  ],
  static_profile: [
    { key: 'entity', label: '实体字段', required: true, placeholder: '选择实体代码字段' },
  ],
}

const FIELD_HELP = {
  market_cap: '总市值',
  float_market_cap: '流通市值',
  turnover_rate: '换手率',
  industry_code: '行业代码',
  industry_level1_name: '一级行业',
  industry_level2_name: '二级行业',
  industry_level3_name: '三级行业',
  industry_name: '行业名称',
  index_code: '指数代码',
  index_name: '指数名称',
  index_constituent_code: '成分归属代码',
  index_constituent_name: '成分归属名称',
  buy_amount: '买入额',
  sell_amount: '卖出额',
  change_range: '涨跌幅',
}

const ENTITY_CODE_FIELD_CANDIDATES = {
  stock: ['code', 'stock_code', 'symbol', 'con_code', 'ts_code', 'sec_code', 'market_code'],
  industry: ['industry_code', 'index_code'],
  index: ['index_code'],
  concept: ['concept_code', 'index_code'],
  etf: ['etf_code', 'fund_code', 'code'],
  fund: ['fund_code', 'code'],
}

const ENTITY_STATE_STORAGE_KEY = 'aiquantbase.extensions_lab.entities'

const selectedEntityId = ref('stock')
const entityDialogVisible = ref(false)
const marketDataDialogVisible = ref(false)
const extensionDataDialogVisible = ref(false)
const editingEntityId = ref('')
const assetDialogKind = ref('market')
const assetNodeKeyword = ref('')
const bindingFieldKeyword = ref('')
const fieldTableKeyword = ref('')
const fieldRealTableFilter = ref('')
const fieldStockFilter = ref('all')
const editingAssetId = ref('')
const hydratingAssetForm = ref(false)
const customEntities = ref([])
const hiddenEntityIds = ref([])
const disabledEntityIds = ref([])
const entityForm = reactive({
  id: '',
  name: '',
  description: '',
})
const assetForm = reactive({
  capability: '',
  name: '',
  description: '',
  entity_id: 'stock',
  target_entity_id: 'stock',
  access_type: 'entity_data',
  asset_group: 'market',
  market_grain: 'daily',
  data_shape: 'time_series',
  node_name: '',
  fields: [],
  bindings: {
    entity: '',
    time: '',
    group: '',
    member: '',
    start: '',
    end: '',
    event_time: '',
    announce_date: '',
    report_period: '',
  },
})

const workspaceReady = computed(() => Boolean(workspacePayload.value))

const extensionRows = computed(() =>
  dataAssets.value.map((item) => hydrateDataAsset(item))
)

const entities = computed(() => {
  const map = new Map()
  for (const item of ENTITY_PRESETS) {
    if (hiddenEntityIds.value.includes(item.id)) continue
    map.set(item.id, {
      ...item,
      enabled: isEntityEnabled(item),
      ability_count: 0,
      field_count: 0,
      node_count: 0,
      nodes: new Set(),
      shapes: new Set(),
    })
  }
  for (const item of customEntities.value) {
    map.set(item.id, {
      ...item,
      role: 'external',
      enabled: isEntityEnabled(item),
      ability_count: 0,
      field_count: 0,
      node_count: 0,
      nodes: new Set(),
      shapes: new Set(),
    })
  }
  for (const capability of extensionRows.value) {
    if (hiddenEntityIds.value.includes(capability.entity_id)) continue
    if (!map.has(capability.entity_id)) {
      map.set(capability.entity_id, {
        id: capability.entity_id,
        name: entityLabel(capability.entity_id),
        role: 'external',
        description: '从能力或节点名称自动识别出的扩展实体。',
        enabled: isEntityEnabled({ id: capability.entity_id }),
        ability_count: 0,
        field_count: 0,
        node_count: 0,
        nodes: new Set(),
        shapes: new Set(),
      })
    }
    const row = map.get(capability.entity_id)
    row.ability_count += 1
    row.field_count += capability.field_count
    for (const node of capability.provider_nodes) row.nodes.add(node)
    row.shapes.add(capability.data_shape)
  }
  return [...map.values()]
    .map((item) => ({
      ...item,
      node_count: item.nodes.size,
      shape_count: item.shapes.size,
      nodes: [...item.nodes],
      shapes: [...item.shapes],
    }))
    .sort((a, b) => {
      if (a.id === 'stock') return -1
      if (b.id === 'stock') return 1
      if (b.ability_count !== a.ability_count) return b.ability_count - a.ability_count
      return a.name.localeCompare(b.name, 'zh-Hans-CN')
    })
})

const selectedEntity = computed(() =>
  entities.value.find((item) => item.id === selectedEntityId.value) || entities.value[0] || null
)

const selectedCapabilities = computed(() =>
  extensionRows.value.filter((item) => item.entity_id === selectedEntity.value?.id)
)

const selectedEntityDeleteAssets = computed(() => {
  const entityId = selectedEntity.value?.id
  if (!entityId) return []
  return extensionRows.value.filter((item) =>
    item.entity_id === entityId || item.target_entity_id === entityId
  )
})

const marketPanelCapabilities = computed(() =>
  selectedCapabilities.value.filter((item) => isMarketPanelCapability(item))
)

const extensionSetupCapabilities = computed(() =>
  selectedCapabilities.value.filter((item) => !isMarketPanelCapability(item))
)

const selectedEntityIsLocal = computed(() =>
  customEntities.value.some((item) => item.id === selectedEntity.value?.id)
)

const canDeleteSelectedEntity = computed(() =>
  Boolean(selectedEntity.value && selectedEntity.value.id !== 'stock')
)

const entityDeleteConfirmTitle = computed(() => {
  const count = selectedEntityDeleteAssets.value.length
  return count
    ? `确认删除这个实体，并删除 ${count} 个数据资产？`
    : '确认删除这个实体？'
})

const selectedAssetNode = computed(() =>
  providerNodes.value.find((item) => item.name === assetForm.node_name) || null
)

const assetNodeOptions = computed(() =>
  providerNodes.value.filter((item) => !providerQueryProfiles(item).includes('virtual_runtime_rule'))
)

const visibleAssetNodeOptions = computed(() => {
  const keyword = assetNodeKeyword.value.trim().toLowerCase()
  const options = assetDialogKind.value === 'market'
    ? assetNodeOptions.value.filter((node) => isMarketDataNode(node))
    : sortExtensionNodeOptions(assetNodeOptions.value)
  if (!keyword) return options
  return options.filter((node) => {
    const text = [
      node.name,
      node.description_zh,
      node.description,
      nodeShapeLabel(node),
      ...nodeSourceFields(node),
    ].map((item) => String(item || '').toLowerCase()).join(' ')
    return text.includes(keyword)
  })
})

function sortExtensionNodeOptions(nodes) {
  return [...nodes].sort((a, b) => extensionNodeRank(a) - extensionNodeRank(b))
}

function extensionNodeRank(node) {
  const shape = nodeDataShape(node)
  const role = String(node?.node_role || '').toLowerCase()
  if (shape === 'relation') return 0
  if (role.includes('relation')) return 1
  if (shape === 'event' || shape === 'disclosure') return 2
  if (shape === 'static_profile') return 3
  return 4
}

const assetFieldOptions = computed(() => {
  const options = new Map()
  const structuralFields = new Set(assetStructuralFields.value)
  const add = (value) => {
    const text = String(value || '').trim()
    if (!text || options.has(text)) return
    if (structuralFields.has(text) && !assetForm.fields.includes(text)) return
    options.set(text, {
      value: text,
      label: FIELD_HELP[text] || text,
    })
  }
  for (const field of nodeSourceFields(selectedAssetNode.value)) add(field)
  for (const field of Object.values(selectedAssetNode.value?.keys || {})) add(field)
  for (const field of assetForm.fields) add(field)
  return [...options.values()]
})

const assetBindingOptions = computed(() => {
  const options = new Map()
  const add = (value) => {
    const text = String(value || '').trim()
    if (!text || options.has(text)) return
    options.set(text, {
      value: text,
      label: text,
      help: FIELD_HELP[text] || '',
    })
  }
  const node = selectedAssetNode.value
  for (const field of nodeSourceFields(node)) add(field)
  for (const field of Object.values(node?.keys || {})) add(field)
  for (const field of node?.entity_keys || []) add(field)
  for (const field of Object.values(node?.interval_keys || {})) add(field)
  add(node?.time_key)
  for (const field of Object.values(assetForm.bindings)) add(field)
  return [...options.values()]
})

const visibleAssetBindingOptions = computed(() => {
  const keyword = bindingFieldKeyword.value.trim().toLowerCase()
  if (!keyword) return assetBindingOptions.value
  return assetBindingOptions.value.filter((field) => {
    const text = [
      field.label,
      field.value,
      field.help,
      bindingFieldRole(field.value),
    ].map((item) => String(item || '').toLowerCase()).join(' ')
    return text.includes(keyword)
  })
})

const assetStructuralFields = computed(() =>
  uniqueList(Object.values(assetForm.bindings))
)

const assetBindingSchema = computed(() => {
  const schema = ASSET_BINDING_SCHEMAS[assetForm.data_shape] || ASSET_BINDING_SCHEMAS.time_series
  if (assetForm.data_shape !== 'relation') return schema
  const entityName = entityLabel(assetForm.entity_id)
  return schema.map((binding) => {
    if (binding.key === 'group') {
      return {
        ...binding,
        label: `${entityName}标识 code`,
        placeholder: `选择${entityName}代码字段`,
      }
    }
    if (binding.key === 'member') {
      return {
        ...binding,
        label: '股票字段标识 code',
        placeholder: '选择股票代码字段',
      }
    }
    return binding
  })
})

const assetBindingSummary = computed(() =>
  buildManualBindingSummary(assetForm.data_shape, assetForm.bindings)
)

const assetTargetMode = computed(() =>
  assetForm.access_type === 'relation_data' ? 'stock_relation' : 'entity_fields'
)

const stockRelationDisabled = computed(() =>
  !selectedEntity.value || selectedEntity.value.id === 'stock'
)

const showStockBindingColumn = computed(() =>
  selectedEntity.value?.id !== 'stock'
)

const selectedEntityEnabled = computed(() =>
  isEntityEnabled(selectedEntity.value)
)

const connectedFieldRows = computed(() =>
  selectedCapabilities.value.flatMap((capability) => {
    const binding = capability.provider_bindings?.[0]?.field_bindings || {}
    const bindingFields = new Set(uniqueList(Object.values(binding)))
    const fieldNames = uniqueList(
      capability.fields.map((field) => field.name || field.source || field)
    ).filter((fieldName) => !bindingFields.has(fieldName))
    return fieldNames.map((fieldName) => ({
      id: `${capability.capability}-${fieldName}`,
      fieldName,
      realTable: capabilityRealTableTitle(capability),
      timeRule: capabilityTimeRule(capability),
      stockBound: capabilityStockBound(capability) ? '是' : '否',
    }))
  })
)

const fieldRealTableOptions = computed(() =>
  uniqueList(connectedFieldRows.value.map((row) => row.realTable))
    .map((table) => ({ label: table, value: table }))
)

const filteredConnectedFieldRows = computed(() => {
  const keyword = fieldTableKeyword.value.trim().toLowerCase()
  return connectedFieldRows.value.filter((row) => {
    if (keyword) {
      const text = [row.fieldName, row.realTable, row.timeRule]
        .map((item) => String(item || '').toLowerCase())
        .join(' ')
      if (!text.includes(keyword)) return false
    }
    if (fieldRealTableFilter.value && row.realTable !== fieldRealTableFilter.value) return false
    if (showStockBindingColumn.value && fieldStockFilter.value !== 'all' && row.stockBound !== fieldStockFilter.value) return false
    return true
  })
})

const stockRelationRows = computed(() => {
  if (selectedEntity.value?.id !== 'stock') return []
  return extensionRows.value
    .filter((capability) =>
      capability.entity_id !== 'stock'
      && !hiddenEntityIds.value.includes(capability.entity_id)
      && isEntityEnabled({ id: capability.entity_id })
      && capability.target_entity_id === 'stock'
      && (capability.data_shape === 'relation' || capability.access_type === 'relation_data')
    )
    .flatMap((capability) => {
      const binding = capability.provider_bindings?.[0]?.field_bindings || {}
      const structuralFields = new Set(uniqueList(Object.values(binding)))
      return capability.fields
        .map((field) => field.name || field.source || field)
        .filter((fieldName) => String(fieldName || '').trim() && !structuralFields.has(fieldName))
        .map((fieldName) => ({
          id: `${capability.capability}-${fieldName}`,
          fieldName,
          entityName: entityLabel(capability.entity_id),
          realTable: capabilityRealTableTitle(capability),
          fieldRole: '附加字段',
          timeRule: capabilityTimeRule(capability),
        }))
    })
})

onMounted(async () => {
  try {
    loadEntityState()
    await Promise.all([loadWorkspace(), loadDataAssets()])
    if (!entities.value.some((item) => item.id === selectedEntityId.value)) {
      selectedEntityId.value = entities.value[0]?.id || 'stock'
    }
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '实体能力测试页加载失败')
  }
})

watch(() => assetForm.node_name, () => {
  if (hydratingAssetForm.value) return
  const node = selectedAssetNode.value
  assetForm.fields = []
  bindingFieldKeyword.value = ''
  if (node) {
    assetForm.data_shape = assetDialogKind.value === 'market' ? 'time_series' : nodeDataShape(node)
    assetForm.access_type = assetDialogKind.value === 'market' ? 'entity_data' : inferAssetAccessType(node)
    if (assetDialogKind.value === 'market') {
      assetForm.market_grain = inferMarketGrain(node)
    }
    syncAssetAccessEntity(node)
    applyNodeBindingDefaults(node)
  } else {
    resetAssetBindings()
  }
})

watch(() => assetForm.data_shape, () => {
  bindingFieldKeyword.value = ''
  if (assetDialogKind.value === 'market') {
    assetForm.access_type = 'entity_data'
  } else if (assetForm.data_shape === 'relation') {
    assetForm.access_type = 'relation_data'
  } else {
    assetForm.access_type = 'entity_data'
  }
  if (selectedAssetNode.value) {
    applyNodeBindingDefaults(selectedAssetNode.value)
    syncAssetAccessEntity(selectedAssetNode.value)
  } else {
    resetAssetBindings()
    syncAssetAccessEntity(null)
  }
})

watch(() => assetForm.access_type, () => {
  syncAssetAccessEntity(selectedAssetNode.value)
})

watch(selectedEntityId, () => {
  fieldTableKeyword.value = ''
  fieldRealTableFilter.value = ''
  fieldStockFilter.value = 'all'
})

watch([customEntities, hiddenEntityIds, disabledEntityIds], () => {
  persistEntityState()
}, { deep: true })

function loadEntityState() {
  if (typeof window === 'undefined') return
  try {
    const payload = JSON.parse(window.localStorage.getItem(ENTITY_STATE_STORAGE_KEY) || '{}')
    customEntities.value = Array.isArray(payload.custom_entities) ? payload.custom_entities : []
    hiddenEntityIds.value = Array.isArray(payload.hidden_entity_ids)
      ? payload.hidden_entity_ids.filter((item) => item !== 'stock')
      : []
    disabledEntityIds.value = Array.isArray(payload.disabled_entity_ids)
      ? payload.disabled_entity_ids.filter((item) => item !== 'stock')
      : []
  } catch {
    customEntities.value = []
    hiddenEntityIds.value = []
    disabledEntityIds.value = []
  }
}

function persistEntityState() {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(ENTITY_STATE_STORAGE_KEY, JSON.stringify({
    custom_entities: customEntities.value,
    hidden_entity_ids: hiddenEntityIds.value,
    disabled_entity_ids: disabledEntityIds.value.filter((item) => item !== 'stock'),
  }))
}

function isEntityEnabled(entity) {
  if (!entity || entity.id === 'stock') return true
  return !disabledEntityIds.value.includes(entity.id)
}

function setSelectedEntityEnabled(value) {
  const entity = selectedEntity.value
  if (!entity) return
  if (entity.id === 'stock') {
    disabledEntityIds.value = disabledEntityIds.value.filter((item) => item !== 'stock')
    return
  }
  if (value) {
    disabledEntityIds.value = disabledEntityIds.value.filter((item) => item !== entity.id)
  } else if (!disabledEntityIds.value.includes(entity.id)) {
    disabledEntityIds.value = [...disabledEntityIds.value, entity.id]
  }
}

function nodeByName(name) {
  return providerNodes.value.find((item) => item.name === name) || null
}

function providerQueryProfiles(value) {
  const profiles = value?.query_profiles || value?.access_patterns || []
  return Array.isArray(profiles)
    ? profiles.map((item) => String(item || '').trim()).filter(Boolean)
    : []
}

function inferDataShape({ outputScope, queryProfiles, node }) {
  const profileSet = new Set(queryProfiles)
  const timeKeyMode = String(node?.time_key_mode || '').toLowerCase()
  const intervalKeys = node?.interval_keys && typeof node.interval_keys === 'object' ? node.interval_keys : {}
  const hasIntervalKeys = Boolean(intervalKeys.start || intervalKeys.end || intervalKeys.start_time || intervalKeys.end_time)
  if (profileSet.has('event_stream')) return 'event'
  if (profileSet.has('point_in_time_report') || profileSet.has('asof_report')) return 'disclosure'
  if (profileSet.has('dimension_lookup')) return 'static_profile'
  if (outputScope.scope_type === 'event_stream') return 'event'
  if (profileSet.has('interval_membership') || timeKeyMode === 'range' || hasIntervalKeys) return 'relation'
  if (timeKeyMode === 'single') return 'time_series'
  if (!node?.time_key && !node?.keys?.time && !node?.keys?.datetime) return 'static_profile'
  return 'time_series'
}

function buildManualBindingSummary(dataShape, bindings) {
  const value = (key, fallback = '待选择') => String(bindings?.[key] || '').trim() || fallback
  if (dataShape === 'relation') {
    return `集合：${value('group')} / 成员：${value('member')} / 区间：${value('start')} -> ${value('end')}`
  }
  if (dataShape === 'event') {
    return `实体：${value('entity')} / 事件时间：${value('event_time')}`
  }
  if (dataShape === 'disclosure') {
    const report = value('report_period', '')
    return `实体：${value('entity')} / 披露日：${value('announce_date')}${report ? ` / 报告期：${report}` : ''}`
  }
  if (dataShape === 'static_profile') {
    return `实体：${value('entity')} / 静态资料`
  }
  return `实体：${value('entity')} / 时间：${value('time')}`
}

function resetAssetBindings() {
  for (const key of Object.keys(assetForm.bindings)) {
    assetForm.bindings[key] = ''
  }
}

function applyNodeBindingDefaults(node) {
  resetAssetBindings()
  if (!node) return
  const intervalKeys = normalizeIntervalKeys(node.interval_keys)
  if (assetForm.data_shape === 'relation') {
    assetForm.bindings.group = relationGroupField(node)
    assetForm.bindings.member = relationMemberField(node)
    assetForm.bindings.start = findNodeFieldByRole(node, ['interval_start']) || intervalKeys.start || node.time_key || ''
    assetForm.bindings.end = findNodeFieldByRole(node, ['interval_end']) || intervalKeys.end || ''
    return
  }
  if (assetForm.data_shape === 'event') {
    assetForm.bindings.entity = primaryEntityField(node)
    assetForm.bindings.event_time = findNodeFieldByRole(node, ['event_time_key']) || node.keys?.event_time || node.keys?.time || node.time_key || ''
    return
  }
  if (assetForm.data_shape === 'disclosure') {
    assetForm.bindings.entity = primaryEntityField(node)
    assetForm.bindings.announce_date = findNodeFieldByRole(node, ['announce_time_key']) || node.keys?.announce_date || node.keys?.publish_time || node.time_key || ''
    assetForm.bindings.report_period = reportPeriodField(node)
    return
  }
  assetForm.bindings.entity = primaryEntityField(node)
  if (assetForm.data_shape === 'time_series') {
    assetForm.bindings.time = primaryTimeField(node)
  }
}

function inferAssetAccessType(node) {
  const shape = nodeDataShape(node)
  if (shape === 'relation') return 'relation_data'
  return 'entity_data'
}

function syncAssetAccessEntity(node) {
  const currentEntityId = selectedEntity.value?.id || 'stock'
  if (assetForm.access_type === 'relation_data') {
    assetForm.entity_id = currentEntityId === 'stock' ? inferRelationSourceEntityId(node) : currentEntityId
    assetForm.target_entity_id = 'stock'
    return
  }
  assetForm.entity_id = assetDialogKind.value === 'market'
    ? inferMarketEntityId(node)
    : currentEntityId
  assetForm.target_entity_id = assetForm.entity_id
}

function entityDataShapeFromNode(node) {
  const shape = node ? nodeDataShape(node) : assetForm.data_shape
  return shape === 'relation' ? 'time_series' : (shape || 'time_series')
}

function setAssetTargetMode(mode) {
  if (mode === 'stock_relation') {
    if (stockRelationDisabled.value) {
      ElMessage.warning('股票实体不需要再建立股票关系')
      return
    }
    assetForm.access_type = 'relation_data'
    assetForm.data_shape = 'relation'
  } else {
    assetForm.access_type = 'entity_data'
    assetForm.data_shape = entityDataShapeFromNode(selectedAssetNode.value)
  }
  syncAssetAccessEntity(selectedAssetNode.value)
  applyNodeBindingDefaults(selectedAssetNode.value)
}

function validateAssetBindings() {
  const missing = assetBindingSchema.value
    .filter((item) => item.required && !String(assetForm.bindings[item.key] || '').trim())
    .map((item) => item.label)
  if (missing.length) {
    ElMessage.error(`请完成字段绑定：${missing.join('、')}`)
    return false
  }
  return true
}

function nodeSourceFields(node) {
  return uniqueList([
    ...(Array.isArray(node?.source_fields) ? node.source_fields : []),
    ...(Array.isArray(node?.fields) ? node.fields : []),
    ...(Array.isArray(node?.wide_table?.fields) ? node.wide_table.fields : []),
  ])
}

function nodeFieldCount(node) {
  return nodeSourceFields(node).length
}

function normalizeIntervalKeys(value) {
  const source = value && typeof value === 'object' ? value : {}
  return {
    start: String(source.start || source.start_time || '').trim(),
    end: String(source.end || source.end_time || '').trim(),
  }
}

function normalizedEntityKeys(node) {
  const keys = node?.keys || {}
  return uniqueList([
    ...(Array.isArray(node?.entity_keys) ? node.entity_keys : []),
    keys.symbol,
    keys.entity,
    keys.member,
  ])
}

function primaryEntityField(node) {
  return findNodeFieldByRole(node, ['entity_key'])
    || preferredEntityCodeField(node, assetForm.entity_id)
    || normalizedEntityKeys(node)[0]
    || stockLikeField(node)
    || ''
}

function primaryTimeField(node) {
  const keys = node?.keys || {}
  return findNodeFieldByRole(node, ['time_key']) || keys.time || keys.datetime || node?.time_key || ''
}

function relationMemberField(node) {
  const keys = node?.keys || {}
  return findNodeFieldByRole(node, ['member_key', 'relation_entity_key'])
    || keys.member
    || keys.symbol
    || normalizedEntityKeys(node)[0]
    || stockLikeField(node)
    || ''
}

function relationGroupField(node) {
  const keys = node?.keys || {}
  const explicitGroup = findNodeFieldByRole(node, ['group_key'])
  if (explicitGroup) return explicitGroup
  const member = relationMemberField(node)
  const intervalKeys = normalizeIntervalKeys(node?.interval_keys)
  const blocked = new Set(uniqueList([member, node?.time_key, intervalKeys.start, intervalKeys.end]))
  const fields = nodeSourceFields(node)
  const preferred = uniqueList([
    ...(ENTITY_CODE_FIELD_CANDIDATES[assetForm.entity_id] || []),
    'industry_code',
    'concept_code',
    'index_code',
    'fund_code',
    'etf_code',
    'group_code',
  ])
  return keys.group
    || keys.parent
    || preferred.find((field) => fields.includes(field) && !blocked.has(field))
    || fields.find((field) => field.includes('code') && !blocked.has(field))
    || ''
}

function reportPeriodField(node) {
  const explicitReportPeriod = findNodeFieldByRole(node, ['report_period_key'])
  if (explicitReportPeriod) return explicitReportPeriod
  const fields = nodeSourceFields(node)
  return ['report_period', 'end_date', 'period', 'fiscal_period'].find((field) => fields.includes(field)) || ''
}

function stockLikeField(node) {
  const fields = nodeSourceFields(node)
  return ['stock_code', 'code', 'symbol', 'con_code', 'ts_code', 'sec_code', 'market_code']
    .find((field) => fields.includes(field)) || ''
}

function preferredEntityCodeField(node, entityId) {
  const fields = new Set(nodeSourceFields(node))
  const candidates = ENTITY_CODE_FIELD_CANDIDATES[entityId] || []
  return candidates.find((field) => fields.has(field)) || ''
}

function inferNodeEntityId(node) {
  const text = `${node?.name || ''} ${nodeSourceFields(node).join(' ')}`.toLowerCase()
  if (text.includes('industry')) return 'industry'
  if (text.includes('concept')) return 'concept'
  if (text.includes('etf')) return 'etf'
  if (text.includes('fund')) return 'fund'
  if (text.includes('index')) return 'index'
  if (stockLikeField(node)) return 'stock'
  return selectedEntity.value?.id || 'stock'
}

function inferMarketEntityId(node) {
  return inferNodeEntityId(node) || selectedEntity.value?.id || 'stock'
}

function inferRelationSourceEntityId(node) {
  if (selectedEntity.value?.id && selectedEntity.value.id !== 'stock') {
    return selectedEntity.value.id
  }
  const group = relationGroupField(node).toLowerCase()
  const text = `${node?.name || ''} ${group}`.toLowerCase()
  if (text.includes('industry')) return 'industry'
  if (text.includes('concept')) return 'concept'
  if (text.includes('etf')) return 'etf'
  if (text.includes('fund')) return 'fund'
  if (text.includes('index')) return 'index'
  return selectedEntity.value?.id || inferNodeEntityId(node)
}

function nodeDisplayDescription(node) {
  return node?.description_zh || node?.description || '暂无节点说明'
}

function nodeTimeKeyModeLabel(node) {
  const mode = String(node?.time_key_mode || '').toLowerCase()
  if (mode === 'range') return '区间时间键'
  if (mode === 'single') return '单点时间键'
  if (normalizeIntervalKeys(node?.interval_keys).start || normalizeIntervalKeys(node?.interval_keys).end) return '区间时间键'
  return node?.time_key ? '单点时间键' : '无时间键'
}

function nodeDataShape(node) {
  return inferDataShape({
    outputScope: {},
    queryProfiles: providerQueryProfiles(node),
    node,
  })
}

function nodeShapeLabel(node) {
  const shape = nodeDataShape(node)
  return DATA_SHAPES[shape]?.label || shape
}

function nodeKeyPreview(node) {
  const entityKeys = Array.isArray(node?.entity_keys) ? node.entity_keys : []
  const keys = uniqueList([
    node?.keys?.symbol,
    node?.keys?.entity,
    node?.keys?.time,
    node?.keys?.datetime,
    node?.time_key,
    ...entityKeys,
  ])
  return keys.slice(0, 2).join(' / ')
}

function bindingFieldRole(field) {
  const text = String(field || '').trim()
  const node = selectedAssetNode.value
  if (!text || !node) return '字段'
  const keys = node.keys || {}
  const intervalKeys = node.interval_keys || {}
  const entityKeys = Array.isArray(node.entity_keys) ? node.entity_keys : []
  if (uniqueList([keys.symbol, keys.entity, ...entityKeys]).includes(text)) return '实体键'
  if (uniqueList([keys.time, keys.datetime, node.time_key]).includes(text)) return '时间键'
  if (uniqueList([keys.event_time]).includes(text)) return '事件时间'
  if (uniqueList([keys.announce_date, keys.publish_time]).includes(text)) return '披露日'
  if (Object.values(intervalKeys).map((item) => String(item || '').trim()).includes(text)) return '区间键'
  if (nodeSourceFields(node).includes(text)) return '来源字段'
  return '手动字段'
}

function filterAssetNodeOptions(keyword) {
  assetNodeKeyword.value = String(keyword || '')
}

function handleAssetNodeVisibleChange(visible) {
  if (visible) assetNodeKeyword.value = ''
}

function filterBindingFieldOptions(keyword) {
  bindingFieldKeyword.value = String(keyword || '')
}

function handleBindingFieldVisibleChange(visible) {
  if (visible) bindingFieldKeyword.value = ''
}

function uniqueList(values) {
  return [...new Set(values.map((item) => String(item || '').trim()).filter(Boolean))]
}

function normalizeFieldFacts(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  return Object.fromEntries(
    Object.entries(value)
      .map(([fieldName, fact]) => {
        const field = String(fieldName || '').trim()
        if (!field || !fact || typeof fact !== 'object' || Array.isArray(fact)) return null
        return [field, { ...fact }]
      })
      .filter(Boolean)
  )
}

function findNodeFieldByRole(node, roles) {
  const roleSet = new Set((roles || []).map((item) => String(item || '').trim()).filter(Boolean))
  if (!roleSet.size) return ''
  const facts = normalizeFieldFacts(node?.field_facts)
  for (const [fieldName, fact] of Object.entries(facts)) {
    if (roleSet.has(String(fact?.role || fact?.field_role || '').trim())) {
      return fieldName
    }
  }
  return ''
}

function hydrateDataAsset(asset) {
  const providerNodes = Array.isArray(asset.provider_nodes) ? asset.provider_nodes : []
  const fields = Array.isArray(asset.fields) ? asset.fields : []
  return {
    ...asset,
    fields,
    field_count: asset.field_count || fields.length,
    provider_nodes: providerNodes,
    provider_bindings: Array.isArray(asset.provider_bindings) ? asset.provider_bindings : [],
    query_profiles: Array.isArray(asset.query_profiles) ? asset.query_profiles : [],
    output_scope: asset.output_scope || {},
    primary_node: nodeByName(providerNodes[0]),
    draft: false,
  }
}

function capabilityPrimaryNode(capability) {
  return capability?.primary_node || nodeByName(capability?.provider_nodes?.[0]) || null
}

function capabilityRealTableTitle(capability) {
  const node = capabilityPrimaryNode(capability)
  return node?.name || capability?.provider_nodes?.[0] || capability?.capability || '-'
}

function capabilityNodeDescriptionTitle(capability) {
  const node = capabilityPrimaryNode(capability)
  return node?.description_zh || node?.description || capabilityRealTableTitle(capability)
}

function capabilityTimeRule(capability) {
  const binding = capability?.provider_bindings?.[0]?.field_bindings || {}
  if (capability?.data_shape === 'relation') {
    return `区间 ${binding.start || '-'} -> ${binding.end || '-'}`
  }
  if (capability?.data_shape === 'event') {
    return `事件时间 ${binding.event_time || '-'}`
  }
  if (capability?.data_shape === 'disclosure') {
    return `披露日 ${binding.announce_date || '-'}`
  }
  if (capability?.data_shape === 'static_profile') {
    return '静态'
  }
  return `${marketGrainLabel(capability?.market_grain)} ${binding.time || '-'}`
}

function capabilityStockBound(capability) {
  if (capability?.data_shape === 'relation' || capability?.access_type === 'relation_data') {
    return capability?.entity_id === 'stock' || capability?.target_entity_id === 'stock'
  }
  return capability?.entity_id === 'stock'
}

function isMarketPanelCapability(capability) {
  if (capability?.asset_group === 'market') {
    return capability?.data_shape === 'time_series' && capability?.access_type !== 'relation_data'
  }
  if (capability?.asset_group === 'extension') return false
  if (capability?.access_type === 'relation_data') return false
  if (capability?.data_shape !== 'time_series') return false
  const node = capabilityPrimaryNode(capability)
  return isMarketDataNode(node)
}

function isMarketDataNode(node) {
  if (!node || nodeDataShape(node) !== 'time_series') return false
  const text = [
    node.name,
    node.description_zh,
    node.description,
    node.table,
  ].map((item) => String(item || '').toLowerCase()).join(' ')
  const profiles = providerQueryProfiles(node)
  return profiles.includes('anchored_intraday_window')
    || profiles.includes('intraday_panel')
    || profiles.includes('daily_panel')
    || /daily|minute|intraday|kline|行情|日线|分钟/.test(text)
}

function inferMarketGrain(node) {
  const text = [
    node?.name,
    node?.description_zh,
    node?.description,
    node?.table,
    node?.grain,
    node?.query_freq,
    ...providerQueryProfiles(node),
  ].map((item) => String(item || '').toLowerCase()).join(' ')
  return /minute|intraday|1m|1min|分钟/.test(text) ? 'minute' : 'daily'
}

function marketGrainLabel(value) {
  return MARKET_GRAIN_OPTIONS.find((item) => item.value === value)?.label || '日线'
}

function capabilityShapeLabel(capability) {
  if (capability?.asset_group === 'market') {
    return marketGrainLabel(capability.market_grain)
  }
  return DATA_SHAPES[capability?.data_shape]?.label || capability?.data_shape
}

function capabilityShapeTone(capability) {
  if (capability?.asset_group === 'market') {
    return capability.market_grain === 'minute' ? 'blue' : 'green'
  }
  return DATA_SHAPES[capability?.data_shape]?.tone || 'green'
}

function capabilityAccessLabel(capability) {
  return ACCESS_TYPES[capability?.access_type]?.label
    || (capability?.data_shape === 'relation' ? ACCESS_TYPES.relation_data.label : ACCESS_TYPES.entity_data.label)
}

function capabilityRouteLabel(capability) {
  if (capability?.data_shape === 'relation' || capability?.access_type === 'relation_data') {
    return `${entityLabel(capability?.entity_id || selectedEntity.value?.id)} -> ${entityLabel(capability?.target_entity_id || 'stock')}`
  }
  return `归到${entityLabel(capability?.entity_id || selectedEntity.value?.id)}实体`
}

function entityLabel(value) {
  return customEntities.value.find((item) => item.id === value)?.name
    || ENTITY_PRESETS.find((item) => item.id === value)?.name
    || value
    || '-'
}

function entityTypeLabel(entity) {
  if (!entity) return '-'
  if (customEntities.value.some((item) => item.id === entity.id)) return '本地实体'
  return entity.role === 'center' ? '中心实体' : '预设实体'
}

function selectEntity(id) {
  selectedEntityId.value = id
}

function openEntityDialog() {
  editingEntityId.value = ''
  entityForm.id = ''
  entityForm.name = ''
  entityForm.description = ''
  entityDialogVisible.value = true
}

function openEditEntityDialog() {
  if (!selectedEntity.value) return
  editingEntityId.value = selectedEntity.value.id
  entityForm.id = selectedEntity.value.id
  entityForm.name = selectedEntity.value.name
  entityForm.description = selectedEntity.value.description || ''
  entityDialogVisible.value = true
}

function resetAssetForm(shape = 'time_series', accessType = 'entity_data') {
  const entity = selectedEntity.value
  const currentEntityId = entity?.id || 'stock'
  editingAssetId.value = ''
  assetForm.entity_id = currentEntityId
  assetForm.target_entity_id = accessType === 'relation_data' ? 'stock' : currentEntityId
  assetForm.access_type = accessType
  assetForm.asset_group = assetDialogKind.value
  assetForm.market_grain = 'daily'
  assetForm.data_shape = shape
  assetForm.node_name = ''
  assetForm.fields = []
  assetForm.capability = ''
  assetForm.name = ''
  assetForm.description = ''
  assetNodeKeyword.value = ''
  bindingFieldKeyword.value = ''
  resetAssetBindings()
}

async function refreshWorkspaceForAssetDialog() {
  try {
    await loadWorkspace()
  } catch (error) {
    ElMessage.warning(error instanceof Error ? error.message : '节点字段刷新失败，将使用当前缓存')
  }
}

async function openMarketDataDialog() {
  await refreshWorkspaceForAssetDialog()
  assetDialogKind.value = 'market'
  resetAssetForm('time_series', 'entity_data')
  marketDataDialogVisible.value = true
}

async function openExtensionDataDialog() {
  await refreshWorkspaceForAssetDialog()
  assetDialogKind.value = 'extension'
  resetAssetForm(selectedEntity.value?.id === 'stock' ? 'time_series' : 'relation', selectedEntity.value?.id === 'stock' ? 'entity_data' : 'relation_data')
  extensionDataDialogVisible.value = true
}

async function openEditDataAsset(capability) {
  await refreshWorkspaceForAssetDialog()
  const assetGroup = capability?.asset_group === 'market' ? 'market' : 'extension'
  hydratingAssetForm.value = true
  assetDialogKind.value = assetGroup
  resetAssetForm(capability?.data_shape || 'time_series', capability?.access_type || 'entity_data')
  editingAssetId.value = capability?.capability || ''
  assetForm.capability = capability?.capability || ''
  assetForm.name = capability?.name || ''
  assetForm.description = capability?.description || ''
  assetForm.entity_id = capability?.entity_id || selectedEntity.value?.id || 'stock'
  assetForm.access_type = capability?.access_type || 'entity_data'
  assetForm.target_entity_id = assetForm.access_type === 'relation_data'
    ? (capability?.target_entity_id || 'stock')
    : assetForm.entity_id
  assetForm.asset_group = assetGroup
  assetForm.market_grain = capability?.market_grain || 'daily'
  assetForm.data_shape = capability?.data_shape || 'time_series'
  assetForm.node_name = capability?.provider_nodes?.[0] || ''
  assetForm.fields = (capability?.fields || []).map((field) => field.name || field.source || field).filter(Boolean)
  resetAssetBindings()
  const binding = capability?.provider_bindings?.[0] || {}
  Object.assign(assetForm.bindings, {
    ...assetForm.bindings,
    ...(binding.field_bindings || {}),
  })
  if (assetGroup === 'market') {
    marketDataDialogVisible.value = true
  } else {
    extensionDataDialogVisible.value = true
  }
  nextTick(() => {
    hydratingAssetForm.value = false
  })
}

async function removeDataAsset(capability) {
  const assetId = capability?.capability
  if (!assetId) return
  try {
    await deleteDataAsset(assetId)
    ElMessage.success('已删除数据资产')
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '数据资产删除失败')
  }
}

function toggleAssetField(field) {
  const text = String(field || '').trim()
  if (!text) return
  if (assetForm.fields.includes(text)) {
    assetForm.fields = assetForm.fields.filter((item) => item !== text)
  } else {
    assetForm.fields = [...assetForm.fields, text]
  }
}

function selectAllAssetFields() {
  assetForm.fields = assetFieldOptions.value.map((item) => item.value)
}

function clearAssetFields() {
  assetForm.fields = []
}

function sanitizeCapabilityPart(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
}

function buildMarketCapabilityId() {
  return uniqueList([
    'asset',
    assetForm.entity_id,
    assetForm.market_grain,
    sanitizeCapabilityPart(assetForm.node_name),
  ]).join('_')
}

function buildAssetCapabilityId(isMarketAsset) {
  if (isMarketAsset) return buildMarketCapabilityId()
  const relationPart = assetForm.access_type === 'relation_data'
    ? [assetForm.entity_id, assetForm.target_entity_id, 'relation']
    : [assetForm.entity_id, assetForm.data_shape]
  return uniqueList([
    'asset',
    ...relationPart,
    sanitizeCapabilityPart(assetForm.node_name),
  ]).join('_')
}

function buildAssetName(isMarketAsset) {
  if (isMarketAsset) {
    return `${entityLabel(assetForm.entity_id)}${marketGrainLabel(assetForm.market_grain)}数据`
  }
  if (assetForm.access_type === 'relation_data') {
    return `${entityLabel(assetForm.entity_id)} -> ${entityLabel(assetForm.target_entity_id)}关系`
  }
  return `${entityLabel(assetForm.entity_id)}${DATA_SHAPES[assetForm.data_shape]?.label || '数据'}`
}

function normalizedAssetBindings(node) {
  const bindings = { ...assetForm.bindings }
  if (!node) return bindings
  if (!bindings.entity) {
    bindings.entity = primaryEntityField(node)
  }
  if (assetForm.data_shape === 'time_series' && !bindings.time) {
    bindings.time = primaryTimeField(node)
  }
  if (assetForm.data_shape === 'relation') {
    if (!bindings.group) bindings.group = relationGroupField(node)
    if (!bindings.member) bindings.member = relationMemberField(node)
    if (!bindings.start) bindings.start = findNodeFieldByRole(node, ['interval_start']) || normalizeIntervalKeys(node?.interval_keys).start || node.time_key || ''
    if (!bindings.end) bindings.end = findNodeFieldByRole(node, ['interval_end']) || normalizeIntervalKeys(node?.interval_keys).end || ''
  }
  if (assetForm.data_shape === 'event' || assetForm.data_shape === 'disclosure') {
    if (!bindings.event_time) {
      bindings.event_time = findNodeFieldByRole(node, ['event_time_key']) || node.keys?.event_time || node.keys?.time || node.time_key || ''
    }
    if (!bindings.announce_date) {
      bindings.announce_date = findNodeFieldByRole(node, ['announce_time_key']) || node.keys?.announce_date || node.keys?.publish_time || ''
    }
    if (!bindings.report_period) {
      bindings.report_period = reportPeriodField(node)
    }
  }
  return bindings
}

async function saveDraftAsset() {
  const isMarketAsset = assetDialogKind.value === 'market'
  const marketLabel = marketGrainLabel(assetForm.market_grain)
  const capability = buildAssetCapabilityId(isMarketAsset)
  const name = buildAssetName(isMarketAsset)
  if (!assetForm.node_name) {
    ElMessage.error('请选择数据节点')
    return
  }
  if (!isMarketAsset && !validateAssetBindings()) {
    return
  }
  if (assetForm.access_type !== 'relation_data' && !assetForm.fields.length) {
    ElMessage.error('请选择至少一个暴露字段')
    return
  }
  if (extensionRows.value.some((item) => item.capability === capability && item.capability !== editingAssetId.value)) {
    ElMessage.error(isMarketAsset ? '该日线/分钟数据节点已添加' : '能力 ID 已存在')
    return
  }
  const node = selectedAssetNode.value
  const bindings = normalizedAssetBindings(node)
  const asset = {
    capability,
    name,
    description: assetForm.description || (isMarketAsset ? `${marketLabel}数据节点：${assetForm.node_name}` : '本地测试能力，尚未写入后端注册表。'),
    enabled: true,
    entity_id: assetForm.entity_id,
    target_entity_id: assetForm.target_entity_id,
    access_type: assetForm.access_type,
    asset_group: assetForm.asset_group,
    market_grain: assetForm.market_grain,
    data_shape: assetForm.data_shape,
    fields: assetForm.fields.map((field) => ({
      name: field,
      source: field,
      label: FIELD_HELP[field] || field,
    })),
    field_count: assetForm.fields.length,
    provider_nodes: [assetForm.node_name],
    provider_bindings: [{
      provider_node: assetForm.node_name,
      field_bindings: bindings,
      access_type: assetForm.access_type,
      source_entity_id: assetForm.entity_id,
      target_entity_id: assetForm.target_entity_id,
      market_grain: assetForm.market_grain,
    }],
    query_profiles: providerQueryProfiles(node),
    output_scope: {},
    binding_summary: isMarketAsset ? `周期：${marketLabel}` : assetBindingSummary.value,
    draft: false,
  }
  try {
    await saveDataAsset(asset, { replaceAssetId: editingAssetId.value || undefined })
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '数据资产保存失败')
    return
  }
  marketDataDialogVisible.value = false
  extensionDataDialogVisible.value = false
  editingAssetId.value = ''
  ElMessage.success('已保存数据资产')
}

function saveEntity() {
  const id = String(entityForm.id || '').trim()
  const name = String(entityForm.name || '').trim()
  if (!/^[a-z][a-z0-9_]*$/.test(id)) {
    ElMessage.error('实体 ID 只能使用小写英文、数字和下划线')
    return
  }
  if (!name) {
    ElMessage.error('请填写实体中文名')
    return
  }
  if (!editingEntityId.value && entities.value.some((item) => item.id === id)) {
    ElMessage.error('实体 ID 已存在')
    return
  }
  const targetId = editingEntityId.value || id
  const nextEntity = {
    id: targetId,
    name,
    description: entityForm.description || '本地测试实体，尚未写入后端注册表。',
  }
  const index = customEntities.value.findIndex((item) => item.id === targetId)
  if (index >= 0) {
    customEntities.value[index] = nextEntity
  } else {
    customEntities.value.push(nextEntity)
  }
  hiddenEntityIds.value = hiddenEntityIds.value.filter((item) => item !== targetId)
  disabledEntityIds.value = disabledEntityIds.value.filter((item) => item !== targetId)
  selectedEntityId.value = targetId
  editingEntityId.value = ''
  entityDialogVisible.value = false
}

async function deleteSelectedEntity() {
  const entity = selectedEntity.value
  if (!entity) return
  if (entity.id === 'stock') {
    ElMessage.warning('stock 是中心实体，不能删除')
    return
  }
  const relatedAssets = [...selectedEntityDeleteAssets.value]
  try {
    for (const asset of relatedAssets) {
      await deleteDataAsset(asset.capability)
    }
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '实体关联数据资产删除失败')
    return
  }
  customEntities.value = customEntities.value.filter((item) => item.id !== entity.id)
  disabledEntityIds.value = disabledEntityIds.value.filter((item) => item !== entity.id)
  if (ENTITY_PRESETS.some((item) => item.id === entity.id) && !hiddenEntityIds.value.includes(entity.id)) {
    hiddenEntityIds.value = [...hiddenEntityIds.value, entity.id]
  }
  selectedEntityId.value = entities.value.find((item) => item.id !== entity.id)?.id || 'stock'
  ElMessage.success(relatedAssets.length ? `已删除实体和 ${relatedAssets.length} 个数据资产` : '已删除实体')
}

function closeEntityDialog() {
  editingEntityId.value = ''
  entityDialogVisible.value = false
}
</script>

<template>
  <section class="entity-lab-page">
    <el-skeleton v-if="loading && !workspaceReady" :rows="10" animated />

    <template v-else>
      <aside class="entity-rail">
        <div class="rail-head">
          <div>
            <p>Entity Lab</p>
            <h2>实体</h2>
          </div>
          <el-button size="small" type="primary" plain @click="openEntityDialog">添加实体</el-button>
        </div>

        <button
          v-for="entity in entities"
          :key="entity.id"
          type="button"
          class="entity-row"
          :class="{ active: selectedEntity?.id === entity.id, disabled: !isEntityEnabled(entity) }"
          @click="selectEntity(entity.id)"
        >
          <span class="entity-avatar">{{ entity.name.slice(0, 1) }}</span>
          <span class="entity-copy">
            <strong>{{ entity.name }}</strong>
            <code>{{ entity.id }}</code>
          </span>
          <em>{{ entity.ability_count }}</em>
        </button>
      </aside>

      <main v-if="selectedEntity" class="entity-main">
        <section class="entity-hero">
          <div class="entity-profile">
            <p>{{ entityTypeLabel(selectedEntity) }}</p>
            <div class="entity-profile-title">
              <h1>{{ selectedEntity.name }}</h1>
              <code>{{ selectedEntity.id }}</code>
            </div>
            <span>{{ selectedEntity.description }}</span>
          </div>
          <div class="entity-hero-side">
            <div class="entity-toolbar">
              <label class="entity-enable-control">
                <span>启用</span>
                <el-switch
                  :model-value="selectedEntityEnabled"
                  :disabled="selectedEntity.id === 'stock'"
                  @change="setSelectedEntityEnabled"
                />
              </label>
              <el-button size="small" type="primary" plain @click="openEditEntityDialog">编辑</el-button>
              <el-popconfirm
                v-if="canDeleteSelectedEntity"
                :title="entityDeleteConfirmTitle"
                confirm-button-text="删除"
                cancel-button-text="取消"
                width="260"
                @confirm="deleteSelectedEntity"
              >
                <template #reference>
                  <el-button size="small" type="danger" plain>删除</el-button>
                </template>
              </el-popconfirm>
              <el-button v-else size="small" type="danger" plain disabled>删除</el-button>
            </div>
            <div class="hero-metrics">
              <div>
                <strong>{{ selectedCapabilities.length }}</strong>
                <span>已接入能力</span>
              </div>
              <div>
                <strong>{{ selectedEntity.node_count || 0 }}</strong>
                <span>数据节点</span>
              </div>
              <div>
                <strong>{{ selectedEntity.field_count || 0 }}</strong>
                <span>暴露字段</span>
              </div>
            </div>
          </div>
        </section>

        <section class="ability-board">
          <div class="section-heading">
            <div>
              <p>日线 / 分钟数据</p>
            </div>
            <el-button type="primary" @click="openMarketDataDialog">
              新增日线/分钟数据
            </el-button>
          </div>

          <div v-if="marketPanelCapabilities.length" class="business-list market-list">
            <article
              v-for="capability in marketPanelCapabilities"
              :key="capability.capability"
              class="business-card market-card"
            >
              <div class="business-main">
                <div class="ability-title">
                  <strong>{{ capability.name || capability.capability }}</strong>
                </div>
                <p>{{ capability.provider_nodes.join(' / ') || '未绑定节点' }} · {{ capability.fields.length }} 字段</p>
                <div class="ability-meta">
                  <span>周期：{{ marketGrainLabel(capability.market_grain) }}</span>
                  <span>{{ capabilityRouteLabel(capability) }}</span>
                </div>
                <div class="field-strip">
                  <el-tag
                    v-for="field in capability.fields.slice(0, 4)"
                    :key="field.name"
                    effect="plain"
                  >
                    {{ field.label }}
                  </el-tag>
                  <el-tag v-if="capability.fields.length > 4" effect="plain">
                    +{{ capability.fields.length - 4 }}
                  </el-tag>
                </div>
              </div>
              <div class="card-actions">
                <div class="shape-pill" :class="`tone-${capabilityShapeTone(capability)}`">
                  {{ capabilityShapeLabel(capability) }}
                </div>
                <div class="card-action-buttons">
                  <el-button size="small" plain type="primary" @click="openEditDataAsset(capability)">编辑</el-button>
                  <el-popconfirm
                    title="确认删除这个数据资产？"
                    confirm-button-text="删除"
                    cancel-button-text="取消"
                    width="190"
                    @confirm="removeDataAsset(capability)"
                  >
                    <template #reference>
                      <el-button size="small" plain type="danger">删除</el-button>
                    </template>
                  </el-popconfirm>
                </div>
              </div>
            </article>
          </div>

          <div v-else class="empty-business">
            当前实体还没有日线/分钟面板数据。股票实体自己的行情数据不需要再关联股票，直接作为 stock 自身数据接入。
            <div class="empty-actions">
              <el-button size="small" type="primary" @click="openMarketDataDialog">新增日线/分钟数据</el-button>
            </div>
          </div>
        </section>

        <section class="connection-board">
          <div class="section-heading">
            <div>
              <p>扩展 / 关系数据</p>
            </div>
            <el-button
              type="primary"
              plain
              @click="openExtensionDataDialog"
            >
              新增扩展/关系数据
            </el-button>
          </div>

          <div v-if="extensionSetupCapabilities.length" class="business-list market-list">
            <article
              v-for="capability in extensionSetupCapabilities"
              :key="capability.capability"
              class="business-card market-card relation-card"
            >
              <div class="business-main">
                <div class="ability-title">
                  <strong>{{ capabilityNodeDescriptionTitle(capability) }}</strong>
                </div>
                <p>{{ capability.provider_nodes.join(' / ') || '未绑定节点' }} · {{ capability.fields.length }} 字段</p>
                <div class="ability-meta">
                  <span>类型：{{ capabilityAccessLabel(capability) }}</span>
                  <span>{{ capabilityRouteLabel(capability) }}</span>
                </div>
                <div class="field-strip">
                  <el-tag
                    v-for="field in capability.fields.slice(0, 4)"
                    :key="field.name"
                    effect="plain"
                  >
                    {{ field.label }}
                  </el-tag>
                  <el-tag v-if="capability.fields.length > 4" effect="plain">
                    +{{ capability.fields.length - 4 }}
                  </el-tag>
                </div>
              </div>
              <div class="card-actions">
                <div class="shape-pill" :class="`tone-${capabilityShapeTone(capability)}`">
                  {{ capabilityShapeLabel(capability) }}
                </div>
                <div class="card-action-buttons">
                  <el-button size="small" plain type="primary" @click="openEditDataAsset(capability)">编辑</el-button>
                  <el-popconfirm
                    title="确认删除这个数据资产？"
                    confirm-button-text="删除"
                    cancel-button-text="取消"
                    width="190"
                    @confirm="removeDataAsset(capability)"
                  >
                    <template #reference>
                      <el-button size="small" plain type="danger">删除</el-button>
                    </template>
                  </el-popconfirm>
                </div>
              </div>
            </article>
          </div>

          <div v-else class="empty-business warning">
            当前实体还没有扩展/关系数据。这里用于接入实体关系、事件记录、披露生效和静态资料。
            <div class="empty-actions">
              <el-button
                size="small"
                type="primary"
                plain
                @click="openExtensionDataDialog"
              >
                新增扩展/关系数据
              </el-button>
            </div>
          </div>
        </section>

        <section class="overview-card">
          <div class="section-heading">
            <div>
              <p>已接入字段</p>
            </div>
          </div>

          <div class="field-panel">
            <div class="field-filter-bar">
              <el-input
                v-model="fieldTableKeyword"
                clearable
                class="field-table-search"
                placeholder="搜索字段名 / real表 / 时间规则"
              />
              <el-select
                v-model="fieldRealTableFilter"
                clearable
                filterable
                class="field-real-filter"
                placeholder="所属real表"
              >
                <el-option
                  v-for="item in fieldRealTableOptions"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value"
                />
              </el-select>
              <el-segmented
                v-if="showStockBindingColumn"
                v-model="fieldStockFilter"
                :options="[
                  { label: '全部', value: 'all' },
                  { label: '绑定stock', value: '是' },
                  { label: '未绑定', value: '否' },
                ]"
              />
            </div>
            <el-table
              :data="filteredConnectedFieldRows"
              row-key="id"
              height="360"
              class="binding-table connected-field-table"
              empty-text="当前实体还没有接入字段"
            >
              <el-table-column prop="fieldName" label="字段名" min-width="170" show-overflow-tooltip />
              <el-table-column prop="realTable" label="所属real表" min-width="220" show-overflow-tooltip />
              <el-table-column prop="timeRule" label="时间规则" min-width="180" show-overflow-tooltip />
              <el-table-column v-if="showStockBindingColumn" prop="stockBound" label="是否绑定stock实体" width="150" align="center">
                <template #default="{ row }">
                  <el-tag size="small" :type="row.stockBound === '是' ? 'success' : 'info'" effect="plain">
                    {{ row.stockBound }}
                  </el-tag>
                </template>
              </el-table-column>
            </el-table>
          </div>
        </section>

        <section v-if="selectedEntity.id === 'stock'" class="overview-card">
          <div class="section-heading">
            <div>
              <p>实体绑定关系</p>
            </div>
          </div>

          <div class="field-panel">
            <el-table
              :data="stockRelationRows"
              row-key="id"
              height="240"
              class="binding-table connected-field-table"
              empty-text="当前还没有其他实体绑定到stock"
            >
              <el-table-column prop="fieldName" label="字段名" min-width="160" show-overflow-tooltip />
              <el-table-column prop="realTable" label="所属real表" min-width="220" show-overflow-tooltip />
              <el-table-column prop="entityName" label="绑定实体" width="120" show-overflow-tooltip />
              <el-table-column prop="fieldRole" label="字段用途" min-width="140" show-overflow-tooltip />
              <el-table-column prop="timeRule" label="时间规则" min-width="180" show-overflow-tooltip />
            </el-table>
          </div>
        </section>
      </main>
    </template>

    <el-dialog
      v-model="entityDialogVisible"
      :title="editingEntityId ? '编辑实体' : '添加测试实体'"
      width="420px"
      append-to-body
      align-center
    >
      <el-form label-position="top">
        <el-form-item label="实体 ID">
          <el-input v-model="entityForm.id" :disabled="Boolean(editingEntityId)" placeholder="例如 concept" />
        </el-form-item>
        <el-form-item label="实体中文名">
          <el-input v-model="entityForm.name" placeholder="例如 概念" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input
            v-model="entityForm.description"
            type="textarea"
            :rows="3"
            placeholder="这个实体在策略中代表什么"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="closeEntityDialog">取消</el-button>
        <el-button type="primary" @click="saveEntity">{{ editingEntityId ? '保存修改' : '添加' }}</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="marketDataDialogVisible"
      :title="editingAssetId ? '编辑日线/分钟数据' : '新增日线/分钟数据'"
      width="min(980px, calc(100vw - 36px))"
      append-to-body
      align-center
      class="asset-dialog"
    >
      <div class="asset-dialog-body">
        <section class="asset-step">
          <div class="asset-step-title">
            <span>01</span>
            <strong>数据节点</strong>
          </div>
          <div class="asset-node-step">
            <label>
              <span>数据节点</span>
              <el-select
                v-model="assetForm.node_name"
                class="asset-node-select"
                :class="{ 'has-value': Boolean(assetForm.node_name) }"
                filterable
                :filter-method="filterAssetNodeOptions"
                @visible-change="handleAssetNodeVisibleChange"
                popper-class="asset-node-popper"
                placeholder="搜索日线/分钟 real 节点"
              >
                <template #empty>
                  <div class="asset-node-empty">
                    没有匹配的数据节点
                  </div>
                </template>
                <el-option
                  v-for="node in visibleAssetNodeOptions"
                  :key="node.name"
                  :label="node.name"
                  :value="node.name"
                >
                  <div class="asset-node-option">
                    <div class="asset-node-option-main">
                      <strong>{{ node.name }}</strong>
                      <span>{{ nodeDisplayDescription(node) }}</span>
                    </div>
                    <div class="asset-node-option-side">
                      <em>{{ nodeShapeLabel(node) }}</em>
                      <code>{{ nodeFieldCount(node) }} 字段</code>
                      <span v-if="nodeKeyPreview(node)">{{ nodeKeyPreview(node) }}</span>
                    </div>
                  </div>
                </el-option>
              </el-select>
            </label>
            <label class="asset-shape-control">
              <span>数据周期</span>
              <el-segmented
                v-model="assetForm.market_grain"
                :options="MARKET_GRAIN_OPTIONS"
              />
              <em>{{ selectedAssetNode ? '已按节点自动预选，可手动调整' : '选择节点后自动预选日线或分钟' }}</em>
            </label>
          </div>
        </section>

        <section class="asset-step">
          <div class="asset-step-title">
            <span>02</span>
            <strong>暴露字段</strong>
          </div>
          <div class="field-picker-toolbar">
            <span>已选择 {{ assetForm.fields.length }} / {{ assetFieldOptions.length }}</span>
            <div>
              <el-button size="small" text type="primary" @click="selectAllAssetFields">全选</el-button>
              <el-button size="small" text @click="clearAssetFields">清空</el-button>
            </div>
          </div>
          <div v-if="assetFieldOptions.length" class="asset-field-list">
            <button
              v-for="field in assetFieldOptions"
              :key="field.value"
              type="button"
              class="asset-field-row"
              :class="{ active: assetForm.fields.includes(field.value) }"
              @click="toggleAssetField(field.value)"
            >
              <strong>{{ field.label }}</strong>
              <code>{{ field.value }}</code>
              <em>{{ assetForm.fields.includes(field.value) ? '已选' : '选择' }}</em>
            </button>
          </div>
          <el-empty v-else description="请选择数据节点后查看字段" :image-size="64" />
        </section>
      </div>

      <template #footer>
        <div class="dialog-actions">
          <el-button @click="marketDataDialogVisible = false">关闭</el-button>
          <el-button type="primary" :loading="saving" @click="saveDraftAsset">
            {{ editingAssetId ? '保存修改' : '保存数据资产' }}
          </el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog
      v-model="extensionDataDialogVisible"
      :title="editingAssetId ? '编辑扩展/关系数据' : '新增扩展/关系数据'"
      width="min(980px, calc(100vw - 36px))"
      append-to-body
      align-center
      class="asset-dialog"
    >
      <div class="asset-dialog-body">
        <section class="asset-step">
          <div class="asset-step-title">
            <span>01</span>
            <strong>数据节点</strong>
          </div>
          <div class="asset-node-step">
            <label>
              <span>数据节点</span>
              <el-select
                v-model="assetForm.node_name"
                class="asset-node-select"
                :class="{ 'has-value': Boolean(assetForm.node_name) }"
                filterable
                :filter-method="filterAssetNodeOptions"
                @visible-change="handleAssetNodeVisibleChange"
                popper-class="asset-node-popper"
                placeholder="搜索关系节点，例如 index_constituent_real"
              >
                <template #empty>
                  <div class="asset-node-empty">
                    没有匹配的数据节点
                  </div>
                </template>
                <el-option
                  v-for="node in visibleAssetNodeOptions"
                  :key="node.name"
                  :label="node.name"
                  :value="node.name"
                >
                  <div class="asset-node-option">
                    <div class="asset-node-option-main">
                      <strong>{{ node.name }}</strong>
                      <span>{{ nodeDisplayDescription(node) }}</span>
                    </div>
                    <div class="asset-node-option-side">
                      <em>{{ nodeShapeLabel(node) }}</em>
                      <code>{{ nodeFieldCount(node) }} 字段</code>
                      <span v-if="nodeKeyPreview(node)">{{ nodeKeyPreview(node) }}</span>
                    </div>
                  </div>
                </el-option>
              </el-select>
            </label>
            <label class="asset-shape-control">
              <span>数据类型</span>
              <el-select
                v-model="assetForm.data_shape"
                class="asset-shape-select"
                :class="{ 'has-value': Boolean(assetForm.data_shape) }"
                popper-class="asset-shape-popper"
                placeholder="选择数据形态"
              >
                <el-option
                  v-for="(shape, key) in DATA_SHAPES"
                  :key="key"
                  :label="shape.label"
                  :value="key"
                >
                  <div class="asset-shape-option">
                    <strong>{{ shape.label }}</strong>
                    <span>{{ shape.brief }}</span>
                  </div>
                </el-option>
              </el-select>
            </label>
            <label class="asset-shape-control">
              <span>数据周期</span>
              <el-segmented
                v-model="assetForm.market_grain"
                :options="MARKET_GRAIN_OPTIONS"
              />
              <em>{{ selectedAssetNode ? '已按节点自动预选，可手动调整' : '选择节点后自动预选日线或分钟' }}</em>
            </label>
          </div>
        </section>

        <section class="asset-step">
          <div class="asset-step-title">
            <span>02</span>
            <strong>目标实体</strong>
          </div>
          <div class="asset-target-panel compact">
            <button
              type="button"
              class="asset-target-card"
              :class="{ active: assetTargetMode === 'entity_fields' }"
              @click="setAssetTargetMode('entity_fields')"
            >
              <strong>扩充当前实体字段</strong>
              <span>{{ entityLabel(selectedEntity?.id) }}</span>
            </button>
            <button
              type="button"
              class="asset-target-card"
              :class="{ active: assetTargetMode === 'stock_relation', disabled: stockRelationDisabled }"
              :disabled="stockRelationDisabled"
              @click="setAssetTargetMode('stock_relation')"
            >
              <strong>和股票建立关系</strong>
              <span>{{ entityLabel(selectedEntity?.id) }} -> 股票</span>
            </button>
          </div>

          <div class="manual-binding-grid compact">
            <label
              v-for="binding in assetBindingSchema"
              :key="binding.key"
            >
              <span>{{ binding.label }}</span>
              <el-select
                v-model="assetForm.bindings[binding.key]"
                class="binding-field-select"
                :class="{ 'has-value': Boolean(assetForm.bindings[binding.key]) }"
                filterable
                clearable
                :filter-method="filterBindingFieldOptions"
                @visible-change="handleBindingFieldVisibleChange"
                popper-class="binding-field-popper"
                :disabled="!selectedAssetNode"
                :placeholder="binding.placeholder"
              >
                <template #empty>
                  <div class="binding-field-empty">
                    没有匹配的字段
                  </div>
                </template>
                <el-option
                  v-for="field in visibleAssetBindingOptions"
                  :key="`${binding.key}-${field.value}`"
                  :label="field.value"
                  :value="field.value"
                >
                  <div class="binding-field-option">
                    <div class="binding-field-option-main">
                      <strong>{{ field.value }}</strong>
                      <span>{{ field.help || bindingFieldRole(field.value) }}</span>
                    </div>
                    <em>{{ bindingFieldRole(field.value) }}</em>
                  </div>
                </el-option>
              </el-select>
            </label>
          </div>
        </section>

        <section class="asset-step">
          <div class="asset-step-title">
            <span>03</span>
            <strong>{{ assetForm.access_type === 'relation_data' ? '附加字段（可选）' : '暴露字段' }}</strong>
          </div>
          <div class="field-picker-toolbar">
            <span>已选择 {{ assetForm.fields.length }} / {{ assetFieldOptions.length }}</span>
            <div>
              <el-button size="small" text type="primary" @click="selectAllAssetFields">全选</el-button>
              <el-button size="small" text @click="clearAssetFields">清空</el-button>
            </div>
          </div>
          <div v-if="assetFieldOptions.length" class="asset-field-list">
            <button
              v-for="field in assetFieldOptions"
              :key="field.value"
              type="button"
              class="asset-field-row"
              :class="{ active: assetForm.fields.includes(field.value) }"
              @click="toggleAssetField(field.value)"
            >
              <strong>{{ field.label }}</strong>
              <code>{{ field.value }}</code>
              <em>{{ assetForm.fields.includes(field.value) ? '已选' : '选择' }}</em>
            </button>
          </div>
          <el-empty v-else description="请选择数据节点后查看字段" :image-size="64" />
        </section>

      </div>

      <template #footer>
        <div class="dialog-actions">
          <el-button @click="extensionDataDialogVisible = false">关闭</el-button>
          <el-button type="primary" :loading="saving" @click="saveDraftAsset">
            {{ editingAssetId ? '保存修改' : '保存数据资产' }}
          </el-button>
        </div>
      </template>
    </el-dialog>
  </section>
</template>

<style scoped>
.entity-lab-page {
  display: grid;
  grid-template-columns: 300px minmax(0, 1fr);
  gap: 18px;
  min-height: calc(100vh - 150px);
}

.entity-rail,
.entity-hero,
.ability-board,
.connection-board,
.overview-card {
  border: 1px solid rgba(24, 91, 68, 0.12);
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 18px 50px rgba(31, 75, 49, 0.08);
}

.entity-rail {
  position: sticky;
  top: 84px;
  align-self: start;
  border-radius: 24px;
  padding: 16px;
}

.rail-head,
.section-heading,
.entity-hero {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
}

.rail-head {
  margin-bottom: 14px;
}

.rail-head p,
.section-heading p,
.entity-hero p {
  margin: 0 0 5px;
  color: #6b7b70;
  font-size: 12px;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.ability-board .section-heading p,
.connection-board .section-heading p,
.overview-card .section-heading p {
  margin: 0;
  color: #17261d;
  font-size: 18px;
  font-weight: 900;
  letter-spacing: 0;
  text-transform: none;
}

.rail-head h2,
.section-heading h2,
.entity-hero h1 {
  margin: 0;
  color: #14261b;
}

.entity-row {
  width: 100%;
  display: grid;
  grid-template-columns: 42px 1fr auto;
  align-items: center;
  gap: 10px;
  border: 1px solid transparent;
  border-radius: 16px;
  margin-bottom: 8px;
  padding: 10px;
  background: transparent;
  color: inherit;
  cursor: pointer;
  text-align: left;
}

.entity-row:hover,
.entity-row.active {
  border-color: rgba(14, 125, 93, 0.24);
  background: linear-gradient(135deg, rgba(229, 247, 239, 0.95), rgba(255, 255, 255, 0.9));
}

.entity-row.disabled {
  opacity: 0.55;
}

.entity-avatar {
  width: 40px;
  height: 40px;
  border-radius: 14px;
  display: grid;
  place-items: center;
  background: #137d62;
  color: #fff;
  font-weight: 900;
}

.entity-copy {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.entity-copy strong {
  color: #1c2b21;
  font-size: 15px;
}

.entity-copy code {
  color: #66756b;
  font-size: 12px;
}

.entity-row em {
  min-width: 28px;
  border-radius: 999px;
  padding: 4px 8px;
  background: rgba(19, 125, 98, 0.1);
  color: #137d62;
  font-style: normal;
  font-weight: 800;
  text-align: center;
}

.entity-main {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.entity-hero {
  border-radius: 28px;
  padding: 24px;
  background:
    radial-gradient(circle at 10% 10%, rgba(39, 164, 122, 0.15), transparent 32%),
    linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(238, 250, 244, 0.9));
}

.entity-profile {
  display: grid;
  gap: 7px;
  min-width: 0;
}

.entity-profile-title {
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.entity-profile-title code {
  border-radius: 999px;
  padding: 4px 8px;
  background: rgba(19, 125, 98, 0.1);
  color: #137d62;
  font-size: 12px;
  font-weight: 900;
}

.entity-hero span {
  color: #587064;
  line-height: 1.7;
}

.entity-hero-side {
  display: grid;
  justify-items: end;
  gap: 12px;
}

.entity-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.entity-enable-control {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  border: 1px solid rgba(19, 125, 98, 0.14);
  border-radius: 999px;
  padding: 4px 8px;
  background: rgba(255, 255, 255, 0.72);
}

.entity-enable-control span {
  color: #405347;
  font-size: 12px;
  font-weight: 900;
}

.hero-metrics {
  display: grid;
  grid-template-columns: repeat(3, 110px);
  gap: 10px;
}

.hero-metrics div {
  border-radius: 20px;
  padding: 14px;
  background: rgba(255, 255, 255, 0.82);
  text-align: center;
}

.hero-metrics strong {
  display: block;
  color: #0f775d;
  font-size: 28px;
}

.hero-metrics span {
  color: #63756b;
  font-size: 12px;
}

.ability-board,
.connection-board,
.overview-card {
  border-radius: 28px;
  padding: 22px;
}

.business-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-top: 16px;
}

.market-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.business-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 16px;
  align-items: flex-start;
  border: 1px solid rgba(28, 67, 48, 0.08);
  border-radius: 20px;
  padding: 16px;
  background: rgba(255, 255, 255, 0.88);
}

.market-card {
  align-items: stretch;
  gap: 10px;
  border-radius: 14px;
  padding: 10px 12px;
  min-width: 0;
}

.market-card .ability-title {
  gap: 8px;
}

.market-card .ability-title strong,
.market-card .ability-title code {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.market-card .ability-title strong {
  font-size: 14px;
}

.market-card .ability-title code {
  max-width: 240px;
  font-size: 11px;
}

.market-card p {
  margin: 4px 0;
  font-size: 12px;
}

.market-card .ability-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 12px;
  font-size: 11px;
}

.market-card .field-strip {
  gap: 4px;
  margin-top: 6px;
}

.market-card :deep(.el-tag) {
  height: 22px;
  padding: 0 7px;
  font-size: 11px;
}

.market-card .shape-pill {
  padding: 5px 9px;
  font-size: 11px;
}

.card-actions {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  justify-content: space-between;
  align-self: stretch;
  gap: 8px;
}

.card-action-buttons {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  margin-top: auto;
  white-space: nowrap;
}

.card-action-buttons :deep(.el-button) {
  height: 26px;
  min-width: 44px;
  border-radius: 999px;
  padding: 0 10px;
  font-size: 12px;
  font-weight: 800;
  box-shadow: none;
}

.card-action-buttons :deep(.el-button--primary.is-plain) {
  color: #137d62;
  border-color: rgba(19, 125, 98, 0.24);
  background: rgba(255, 255, 255, 0.92);
}

.card-action-buttons :deep(.el-button--danger.is-plain) {
  color: #b43a36;
  border-color: rgba(180, 58, 54, 0.24);
  background: rgba(255, 247, 246, 0.92);
}

.ability-title {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
}

.ability-title strong {
  color: #17261d;
}

.ability-title code {
  color: #137d62;
  font-size: 12px;
}

.business-card p {
  margin: 8px 0;
  color: #65756c;
  font-size: 13px;
}

.ability-meta {
  display: grid;
  gap: 4px;
  color: #4f6257;
  font-size: 12px;
}

.field-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 10px;
}

.shape-pill {
  align-self: flex-start;
  border-radius: 999px;
  padding: 7px 12px;
  background: rgba(19, 125, 98, 0.1);
  color: #137d62;
  font-size: 12px;
  font-weight: 900;
  white-space: nowrap;
}

.shape-pill.tone-blue {
  background: rgba(42, 102, 197, 0.1);
  color: #2a66c5;
}

.shape-pill.tone-orange {
  background: rgba(198, 115, 29, 0.12);
  color: #b15d12;
}

.shape-pill.tone-red {
  background: rgba(198, 58, 54, 0.1);
  color: #b53532;
}

.shape-pill.tone-gray {
  background: rgba(93, 105, 99, 0.12);
  color: #5d6963;
}

.empty-business {
  border: 1px dashed rgba(35, 83, 61, 0.18);
  border-radius: 16px;
  padding: 18px;
  color: #75867c;
  text-align: center;
  margin-top: 16px;
}

.empty-business.warning {
  border-color: rgba(181, 93, 18, 0.25);
  background: rgba(255, 247, 237, 0.72);
  color: #9a5b18;
}

.empty-actions {
  margin-top: 12px;
}

.center-entity-note {
  display: grid;
  gap: 8px;
  margin-top: 16px;
  border-radius: 20px;
  padding: 18px;
  background: linear-gradient(135deg, rgba(229, 247, 239, 0.95), rgba(255, 255, 255, 0.9));
}

.center-entity-note strong {
  color: #173426;
  font-size: 18px;
}

.center-entity-note span {
  color: #607268;
  line-height: 1.7;
}

.relation-card {
  background:
    radial-gradient(circle at 100% 0%, rgba(42, 102, 197, 0.08), transparent 26%),
    rgba(255, 255, 255, 0.9);
}

.field-panel {
  margin-top: 12px;
  overflow: hidden;
  border: 1px solid var(--el-border-color-light, #ebeef5);
  border-radius: 12px;
  background: #ffffff;
}

.field-filter-bar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
  padding: 10px;
  border-bottom: 1px solid var(--el-border-color-light, #ebeef5);
  background: #ffffff;
}

.field-table-search {
  flex: 1 1 260px;
  min-width: 220px;
}

.field-real-filter {
  flex: 0 0 240px;
}

.connected-field-table {
  width: 100%;
}

.asset-dialog-body {
  max-height: min(72vh, 760px);
  overflow-y: auto;
  padding-right: 4px;
}

.asset-step {
  border: 1px solid rgba(29, 72, 51, 0.1);
  border-radius: 22px;
  padding: 16px;
  margin-bottom: 14px;
  background: rgba(250, 253, 250, 0.96);
}

.asset-step-title {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 14px;
}

.asset-step-title span {
  width: 34px;
  height: 34px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: #137d62;
  color: #fff;
  font-weight: 900;
}

.asset-step-title strong {
  color: #17261d;
  font-size: 17px;
}

.asset-form-grid {
  display: grid;
  gap: 12px;
}

.asset-form-grid.two {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.asset-form-grid.three {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.asset-node-step {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 280px;
  align-items: start;
  gap: 12px;
}

.asset-node-step label {
  display: grid;
  gap: 8px;
}

.asset-node-step label > span {
  color: #405347;
  font-size: 13px;
  font-weight: 800;
}

.asset-shape-control {
  display: grid;
  gap: 8px;
  min-width: 0;
}

.asset-shape-control > span {
  color: #405347;
  font-size: 13px;
  font-weight: 800;
}

.asset-shape-control em {
  color: #68796e;
  font-size: 12px;
  font-style: normal;
  font-weight: 700;
  line-height: 1.35;
}

.asset-shape-select {
  width: 100%;
}

.asset-shape-select :deep(.el-select__wrapper) {
  min-height: 42px;
  border-radius: 14px;
  padding: 6px 12px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 0 0 1px rgba(29, 72, 51, 0.12) inset;
}

.asset-shape-select :deep(.el-select__wrapper.is-focused) {
  box-shadow: 0 0 0 1px rgba(19, 125, 98, 0.46) inset, 0 0 0 3px rgba(19, 125, 98, 0.08);
}

.asset-shape-select :deep(.el-select__placeholder) {
  color: #98a79e;
  font-size: 13px;
  font-weight: 700;
}

.asset-shape-select.has-value :deep(.el-select__placeholder),
.asset-shape-select.has-value :deep(.el-select__selected-item) {
  color: #17261d;
  font-size: 13px;
  font-weight: 800;
}

.asset-shape-option {
  display: grid;
  gap: 3px;
  min-height: 44px;
  border: 1px solid transparent;
  border-radius: 9px;
  padding: 8px 10px;
}

.asset-shape-option strong {
  color: #17261d;
  font-size: 13px;
  font-weight: 900;
  line-height: 1.15;
}

.asset-shape-option span {
  color: #6d7d72;
  font-size: 12px;
  font-weight: 700;
  line-height: 1.25;
}

:global(.asset-shape-popper) {
  z-index: 10002 !important;
  border: 1px solid rgba(29, 72, 51, 0.1);
  border-radius: 13px;
  overflow: hidden;
  box-shadow: 0 16px 38px rgba(28, 64, 45, 0.14);
}

:global(.asset-shape-popper .el-select-dropdown__wrap) {
  max-height: 260px;
}

:global(.asset-shape-popper .el-select-dropdown__list) {
  padding: 6px;
}

:global(.asset-shape-popper .el-select-dropdown__item) {
  height: auto;
  margin: 2px 0;
  padding: 0;
  border-radius: 9px;
  color: inherit;
  line-height: normal;
}

:global(.asset-shape-popper .el-select-dropdown__item.is-hovering) {
  background: rgba(19, 125, 98, 0.06);
}

:global(.asset-shape-popper .el-select-dropdown__item.is-selected) {
  background: rgba(19, 125, 98, 0.09);
  color: inherit;
  font-weight: 400;
}

:global(.asset-shape-popper .el-select-dropdown__item.is-selected) .asset-shape-option {
  border-color: rgba(19, 125, 98, 0.22);
}

.asset-form-grid label,
.asset-textarea {
  display: grid;
  gap: 8px;
}

.asset-form-grid label > span,
.asset-textarea > span {
  color: #405347;
  font-size: 13px;
  font-weight: 800;
}

.asset-node-select {
  width: 100%;
}

.asset-node-select :deep(.el-select__wrapper) {
  min-height: 42px;
  border-radius: 14px;
  padding: 6px 12px;
  box-shadow: 0 0 0 1px rgba(29, 72, 51, 0.12) inset;
}

.asset-node-select :deep(.el-select__wrapper.is-focused) {
  box-shadow: 0 0 0 1px rgba(19, 125, 98, 0.46) inset, 0 0 0 3px rgba(19, 125, 98, 0.08);
}

.asset-node-select :deep(.el-select__placeholder) {
  color: #98a79e;
  font-size: 13px;
  font-weight: 700;
}

.asset-node-select.has-value :deep(.el-select__placeholder),
.asset-node-select.has-value :deep(.el-select__selected-item) {
  color: #17261d;
  font-size: 13px;
  font-weight: 800;
}

.node-option {
  display: flex;
  flex-direction: column;
  gap: 2px;
  line-height: 1.3;
}

.node-option strong {
  color: #17261d;
}

.node-option span {
  color: #6b7b70;
  font-size: 12px;
}

.asset-node-option {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  min-height: 58px;
  border: 1px solid transparent;
  border-radius: 10px;
  padding: 9px 10px;
}

.asset-node-option-main {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.asset-node-option-main strong {
  overflow: hidden;
  color: #17261d;
  font-size: 13px;
  font-weight: 900;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.asset-node-option-main span {
  overflow: hidden;
  color: #68796e;
  font-size: 12px;
  font-weight: 600;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.asset-node-option-side {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 5px;
  width: 220px;
}

.asset-node-option-side em,
.asset-node-option-side code {
  border-radius: 999px;
  padding: 3px 7px;
  font-size: 11px;
  font-style: normal;
  font-weight: 900;
  line-height: 1.1;
}

.asset-node-option-side em {
  background: rgba(19, 125, 98, 0.1);
  color: #137d62;
}

.asset-node-option-side code {
  background: rgba(43, 74, 57, 0.08);
  color: #465b4f;
}

.asset-node-option-side span {
  flex-basis: 100%;
  overflow: hidden;
  color: #7a8a80;
  font-size: 11px;
  font-weight: 700;
  line-height: 1.2;
  text-align: right;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.asset-node-empty {
  padding: 18px;
  color: #75867c;
  font-size: 13px;
  text-align: center;
}

:global(.asset-node-popper) {
  z-index: 10002 !important;
  border: 1px solid rgba(29, 72, 51, 0.1);
  border-radius: 14px;
  overflow: hidden;
  box-shadow: 0 18px 44px rgba(28, 64, 45, 0.16);
}

:global(.asset-node-popper .el-select-dropdown__wrap) {
  max-height: 310px;
}

:global(.asset-node-popper .el-select-dropdown__list) {
  padding: 6px;
}

:global(.asset-node-popper .el-select-dropdown__item) {
  height: auto;
  margin: 2px 0;
  padding: 0;
  border-radius: 10px;
  color: inherit;
  line-height: normal;
}

:global(.asset-node-popper .el-select-dropdown__item.is-hovering) {
  background: rgba(19, 125, 98, 0.06);
}

:global(.asset-node-popper .el-select-dropdown__item.is-selected) {
  background: rgba(19, 125, 98, 0.09);
  color: inherit;
  font-weight: 400;
}

:global(.asset-node-popper .el-select-dropdown__item.is-selected) .asset-node-option {
  border-color: rgba(19, 125, 98, 0.22);
}

.recognition-panel {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.recognition-panel div {
  border-radius: 16px;
  padding: 12px;
  background: rgba(255, 255, 255, 0.82);
}

.recognition-panel span {
  display: block;
  margin-bottom: 6px;
  color: #66786d;
  font-size: 12px;
  font-weight: 800;
}

.recognition-panel strong {
  color: #17261d;
  font-size: 13px;
  line-height: 1.5;
}

.access-type-panel {
  display: grid;
  grid-template-columns: minmax(0, 1.2fr) minmax(220px, 0.8fr);
  gap: 12px;
  margin-bottom: 12px;
}

.asset-target-panel {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  margin-bottom: 12px;
}

.asset-target-card {
  display: grid;
  align-content: center;
  gap: 4px;
  min-height: 68px;
  border: 1px solid rgba(29, 72, 51, 0.12);
  border-radius: 16px;
  padding: 10px 12px;
  appearance: none;
  background: rgba(255, 255, 255, 0.86);
  color: #17261d;
  font: inherit;
  text-align: left;
  cursor: pointer;
  transition: border-color 0.18s ease, background 0.18s ease, box-shadow 0.18s ease;
}

.asset-target-card:hover {
  border-color: rgba(19, 125, 98, 0.28);
  background: rgba(248, 252, 249, 0.96);
}

.asset-target-card.active {
  border-color: rgba(19, 125, 98, 0.42);
  background: rgba(229, 245, 239, 0.74);
  box-shadow: inset 0 0 0 1px rgba(19, 125, 98, 0.18);
}

.asset-target-card.disabled {
  cursor: not-allowed;
  opacity: 0.52;
}

.asset-target-card.disabled:hover {
  border-color: rgba(29, 72, 51, 0.12);
  background: rgba(255, 255, 255, 0.86);
}

.asset-target-card span {
  color: #52675a;
  font-size: 12px;
  font-weight: 900;
}

.asset-target-card strong {
  color: #17261d;
  font-size: 14px;
  font-weight: 950;
}

.access-type-control,
.target-entity-control {
  display: grid;
  gap: 8px;
  border: 1px solid rgba(29, 72, 51, 0.1);
  border-radius: 16px;
  padding: 12px;
  background: rgba(255, 255, 255, 0.82);
}

.target-entity-control {
  grid-column: 1 / -1;
}

.target-entity-control.compact {
  grid-column: auto;
}

.access-type-control > span,
.target-entity-control > span {
  color: #52675a;
  font-size: 12px;
  font-weight: 900;
}

.access-type-control em {
  color: #68796e;
  font-size: 12px;
  font-style: normal;
  font-weight: 700;
  line-height: 1.45;
}

.access-type-control :deep(.el-segmented) {
  --el-segmented-item-selected-bg-color: #137d62;
  --el-segmented-item-selected-color: #fff;
  --el-segmented-bg-color: rgba(19, 125, 98, 0.08);
  width: 100%;
}

.access-type-control :deep(.el-segmented__item-label) {
  font-size: 12px;
  font-weight: 900;
}

.parsed-structure-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.parsed-structure-item {
  display: grid;
  gap: 6px;
  border: 1px solid rgba(19, 125, 98, 0.11);
  border-radius: 14px;
  padding: 12px;
  background: rgba(229, 247, 239, 0.45);
}

.parsed-structure-item span {
  color: #52675a;
  font-size: 12px;
  font-weight: 900;
}

.parsed-structure-item strong {
  overflow: hidden;
  color: #17261d;
  font-size: 14px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.parsed-structure-item em {
  overflow: hidden;
  color: #68796e;
  font-size: 12px;
  font-style: normal;
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.advanced-binding-panel {
  border-radius: 14px;
  margin-top: 12px;
  background: rgba(255, 255, 255, 0.72);
}

.advanced-binding-panel summary {
  cursor: pointer;
  color: #137d62;
  font-size: 13px;
  font-weight: 900;
  list-style-position: inside;
}

.advanced-binding-panel[open] {
  border: 1px solid rgba(19, 125, 98, 0.12);
  padding: 12px;
}

.advanced-binding-panel[open] summary {
  margin-bottom: 12px;
}

.manual-binding-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.manual-binding-grid.compact {
  gap: 10px;
}

.manual-binding-grid label {
  display: grid;
  gap: 8px;
}

.manual-binding-grid label > span {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #405347;
  font-size: 13px;
  font-weight: 800;
}

.manual-binding-grid em {
  border-radius: 999px;
  padding: 2px 7px;
  background: rgba(181, 93, 18, 0.11);
  color: #a65b14;
  font-size: 11px;
  font-style: normal;
}

.binding-field-select {
  width: 100%;
}

.binding-field-select :deep(.el-select__wrapper) {
  min-height: 40px;
  border-radius: 12px;
  padding: 6px 11px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 0 0 1px rgba(29, 72, 51, 0.11) inset;
}

.binding-field-select :deep(.el-select__wrapper.is-focused) {
  background: #fff;
  box-shadow: 0 0 0 1px rgba(19, 125, 98, 0.46) inset, 0 0 0 3px rgba(19, 125, 98, 0.08);
}

.binding-field-select :deep(.el-select__placeholder) {
  color: #98a79e;
  font-size: 13px;
  font-weight: 700;
}

.binding-field-select.has-value :deep(.el-select__placeholder),
.binding-field-select.has-value :deep(.el-select__selected-item) {
  color: #17261d;
  font-size: 13px;
  font-weight: 800;
}

.binding-field-option {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  min-height: 46px;
  border: 1px solid transparent;
  border-radius: 9px;
  padding: 8px 9px;
}

.binding-field-option-main {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.binding-field-option-main strong {
  overflow: hidden;
  color: #17261d;
  font-size: 13px;
  font-weight: 900;
  line-height: 1.15;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.binding-field-option-main span {
  overflow: hidden;
  color: #6d7d72;
  font-size: 12px;
  font-weight: 700;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.binding-field-option em {
  border-radius: 999px;
  padding: 4px 8px;
  background: rgba(19, 125, 98, 0.1);
  color: #137d62;
  font-size: 11px;
  font-style: normal;
  font-weight: 900;
  white-space: nowrap;
}

.binding-field-empty {
  padding: 16px;
  color: #75867c;
  font-size: 13px;
  text-align: center;
}

:global(.binding-field-popper) {
  z-index: 10002 !important;
  border: 1px solid rgba(29, 72, 51, 0.1);
  border-radius: 13px;
  overflow: hidden;
  box-shadow: 0 16px 38px rgba(28, 64, 45, 0.14);
}

:global(.binding-field-popper .el-select-dropdown__wrap) {
  max-height: 260px;
}

:global(.binding-field-popper .el-select-dropdown__list) {
  padding: 6px;
}

:global(.binding-field-popper .el-select-dropdown__item) {
  height: auto;
  margin: 2px 0;
  padding: 0;
  border-radius: 9px;
  color: inherit;
  line-height: normal;
}

:global(.binding-field-popper .el-select-dropdown__item.is-hovering) {
  background: rgba(19, 125, 98, 0.06);
}

:global(.binding-field-popper .el-select-dropdown__item.is-selected) {
  background: rgba(19, 125, 98, 0.09);
  color: inherit;
  font-weight: 400;
}

:global(.binding-field-popper .el-select-dropdown__item.is-selected) .binding-field-option {
  border-color: rgba(19, 125, 98, 0.22);
}

.manual-binding-summary {
  display: grid;
  gap: 6px;
  border-radius: 14px;
  margin-top: 12px;
  padding: 12px;
  background: rgba(19, 125, 98, 0.07);
}

.manual-binding-summary span {
  color: #52675a;
  font-size: 12px;
  font-weight: 800;
}

.manual-binding-summary strong {
  color: #17261d;
  font-size: 13px;
  line-height: 1.5;
}

.field-picker-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
  color: #43584b;
  font-weight: 800;
}

.asset-field-list {
  display: grid;
  gap: 8px;
}

.asset-field-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(140px, auto) auto;
  align-items: center;
  gap: 12px;
  border: 1px solid rgba(29, 72, 51, 0.1);
  border-radius: 14px;
  padding: 10px 12px;
  background: #fff;
  cursor: pointer;
  text-align: left;
}

.asset-field-row.active {
  border-color: rgba(19, 125, 98, 0.42);
  background: rgba(229, 247, 239, 0.8);
}

.asset-field-row strong {
  color: #17261d;
}

.asset-field-row code {
  color: #137d62;
}

.asset-field-row em {
  border-radius: 999px;
  padding: 4px 9px;
  background: rgba(19, 125, 98, 0.1);
  color: #137d62;
  font-style: normal;
  font-weight: 800;
}

.dialog-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

@media (max-width: 1180px) {
  .entity-lab-page {
    grid-template-columns: 1fr;
  }

  .entity-rail {
    position: static;
  }

  .business-card,
  .asset-node-step,
  .asset-target-panel,
  .access-type-panel,
  .parsed-structure-grid,
  .asset-form-grid.two,
  .asset-form-grid.three,
  .manual-binding-grid,
  .recognition-panel {
    grid-template-columns: 1fr;
  }

  .entity-hero {
    align-items: stretch;
    flex-direction: column;
  }

  .entity-hero-side {
    justify-items: stretch;
  }

  .entity-toolbar {
    justify-content: flex-end;
  }

  .hero-metrics {
    grid-template-columns: repeat(3, 1fr);
  }

  .market-list {
    grid-template-columns: 1fr;
  }
}
</style>
