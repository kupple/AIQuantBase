<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useCapabilityAccess } from '~/composables/useCapabilityAccess'

const {
  loading,
  saving,
  workspacePayload,
  providerNodes,
  capabilityRegistry,
  capabilities,
  modeProfiles,
  diagnostics,
  loadWorkspace,
  saveProviderMapping,
  saveModeCapability,
  deleteModeCapability,
} = useCapabilityAccess()

const selectedModeId = ref('')
const activeCapabilityKey = ref('')
const configDialogVisible = ref(false)
const extensionDialogVisible = ref(false)
const extensionDialogMode = ref('create')
const editingExtensionCapability = ref('')
const suppressExtensionFieldReset = ref(false)
const fieldRows = ref([])

const providerForm = reactive({
  nodeName: '',
  capability: '',
  capabilityName: '',
  capabilityDescription: '',
  defaultSlots: [],
  outputScope: defaultOutputScope(),
  keys: {},
  assetTypes: 'stock',
  queryProfiles: 'panel_time_series',
})

const extensionForm = reactive({
  capability: '',
  capabilityName: '',
  capabilityDescription: '',
  nodeName: '',
  slots: [],
  fields: [],
  outputScope: defaultOutputScope(),
})

const FIELD_HELP = {
  open: ['开盘价', '交易日开盘价格'],
  high: ['最高价', '交易日最高价格'],
  low: ['最低价', '交易日最低价格'],
  close: ['收盘价', '交易日收盘价格'],
  pre_close: ['昨收价', '前一交易日收盘价格'],
  volume: ['成交量', '成交股数'],
  amount: ['成交额', '成交金额'],
  money: ['成交额', '成交金额'],
  backward_adj_factor: ['后复权因子', '用于价格复权计算的因子'],
  high_limited: ['涨停价', '当日涨停限制价格'],
  low_limited: ['跌停价', '当日跌停限制价格'],
  is_st: ['ST 标记', '是否 ST 或风险警示证券'],
  is_suspended: ['停牌标记', '当日是否停牌'],
  is_wd_sec: ['退市风险标记', '是否退市或即将退市证券'],
  is_kcb: ['科创板标记', '是否科创板证券'],
  is_cyb: ['创业板标记', '是否创业板证券'],
  is_bjs: ['北交所标记', '是否北交所证券'],
  is_star_board: ['科创板标记', '模式侧语义字段，对应 is_kcb'],
  is_gem_board: ['创业板标记', '模式侧语义字段，对应 is_cyb'],
  listed_days: ['上市天数', '交易日距离上市日的天数'],
  market_cap: ['总市值', '总股本市值'],
  float_market_cap: ['流通市值', '流通股本市值'],
  turnover_rate: ['换手率', '成交活跃度指标'],
  dividend_cash_pre_tax: ['税前现金分红', '每股税前现金分红'],
  dividend_cash_after_tax: ['税后现金分红', '每股税后现金分红'],
  dividend_stock_per_share: ['送股比例', '每股送股数量'],
  dividend_bonus_rate: ['送股率', '分红送股率'],
  dividend_conversed_rate: ['转增率', '资本公积转增率'],
  ex_dividend_date: ['除权除息日', '权益调整生效日期'],
  dividend_payout_date: ['派息日', '现金分红到账日期'],
  equity_record_date: ['股权登记日', '享有权益登记日期'],
  lot_size: ['最小交易单位', 'A 股通常为 100 股'],
  supports_min_commission: ['最低佣金规则', '是否支持最低佣金约束'],
  supports_order_volume_ratio: ['成交量比例限制', '是否支持按成交量比例限制撮合'],
}

const CAPABILITY_LABELS = {
  'price.daily': '日线行情',
  'benchmark.daily': '基准日线',
  'corporate_action.dividend': '分红送转',
  'tradability.limit_status': '涨跌停状态',
  'universe.tradeable_flags': 'ST/停牌/退市状态',
  'universe.board_flags': '板块标记',
  'order_constraints.cn_stock': 'A 股订单规则',
  'price.intraday': '分钟行情',
  valuation_snapshot: '估值快照',
  custom_factor_daily: '自定义日频因子',
  industry_classification: '行业分类',
  dragon_tiger_daily: '龙虎榜日频',
}

const SLOT_LABELS = {
  universe_fields: '候选池',
  ranking_fields: '排序打分',
  filter_fields: '筛选条件',
  signal_fields: '买卖信号',
  weighting_fields: '仓位权重',
  report_fields: '报告展示',
  factor_inputs: '因子输入',
  groupby_fields: '分组控制',
  neutralization_fields: '中性化',
  risk_fields: '风控判断',
}

const SLOT_UI_META = {
  universe_fields: {
    eyebrow: '选股范围',
    title: '候选池',
    description: '用于决定哪些股票进入后续计算。',
  },
  filter_fields: {
    eyebrow: '条件过滤',
    title: '筛选条件',
    description: '用于过滤股票，例如行业、风险、阈值条件。',
  },
  ranking_fields: {
    eyebrow: '排序选股',
    title: '排序打分',
    description: '用于排名、打分、TopN 选股。',
  },
  signal_fields: {
    eyebrow: '交易信号',
    title: '买卖信号',
    description: '用于 entry / exit 这类买卖判断。',
  },
  weighting_fields: {
    eyebrow: '资金分配',
    title: '仓位权重',
    description: '用于目标仓位或资金权重计算。',
  },
  groupby_fields: {
    eyebrow: '组合约束',
    title: '分组控制',
    description: '用于按行业、板块等维度分组限制。',
  },
  neutralization_fields: {
    eyebrow: '因子处理',
    title: '中性化',
    description: '用于行业、市值等维度的中性化处理。',
  },
  risk_fields: {
    eyebrow: '交易约束',
    title: '风控判断',
    description: '用于风险过滤或下单前约束。',
  },
  report_fields: {
    eyebrow: '结果输出',
    title: '报告展示',
    description: '只进入结果和诊断，不直接影响交易。',
  },
  factor_inputs: {
    eyebrow: '研究输入',
    title: '因子输入',
    description: '用于研究模式或因子计算。',
  },
}

const OUTPUT_SCOPE_OPTIONS = [
  {
    value: 'daily_panel',
    label: '股票日频数据',
    badge: '日',
    summary: '每只股票每天一行',
    description: '适合行情、估值、换手率、自定义日频因子。',
  },
  {
    value: 'linked_daily_panel',
    label: '关联日频数据',
    badge: '联',
    summary: '行业/指数等数据映射回股票',
    description: '适合行业分类、行业日线、板块日线等。',
  },
  {
    value: 'intraday_panel',
    label: '股票盘中数据',
    badge: '分',
    summary: '每只股票每个分钟一行',
    description: '适合分钟行情、盘中指标、盘口衍生数据。',
  },
  {
    value: 'event_stream',
    label: '事件流水数据',
    badge: '事',
    summary: '按事件发生时间记录',
    description: '适合公告、分红、龙虎榜等事件型数据。',
  },
]

const DEFAULT_SLOTS_BY_OUTPUT_SCOPE = {
  daily_panel: ['factor_inputs', 'ranking_fields', 'filter_fields', 'weighting_fields', 'report_fields'],
  linked_daily_panel: ['filter_fields', 'groupby_fields', 'neutralization_fields', 'report_fields'],
  intraday_panel: ['signal_fields', 'filter_fields', 'report_fields'],
  event_stream: ['filter_fields', 'ranking_fields', 'report_fields'],
}

const ENTITY_TYPE_OPTIONS = [
  { value: 'stock', label: '股票' },
  { value: 'index', label: '指数' },
  { value: 'industry', label: '行业' },
  { value: 'etf', label: 'ETF' },
  { value: 'fund', label: '基金' },
  { value: 'custom', label: '自定义实体' },
]

const selectedMode = computed(() =>
  modeProfiles.value.find((item) => item.mode_id === selectedModeId.value)
)

const modeCapabilityRows = computed(() => {
  const mode = selectedMode.value
  if (!mode) return []
  return [
    ...normalizeModeCapabilities(mode.required_capabilities, 'required'),
    ...normalizeModeCapabilities(mode.conditional_capabilities, 'conditional'),
    ...normalizeModeCapabilities(mode.optional_capabilities, 'optional'),
    ...normalizeModeCapabilities(mode.extension_capability_bindings, 'extension'),
  ]
})

const dataCapabilityRows = computed(() =>
  modeCapabilityRows.value.filter((item) => !isRuntimeRuleCapability(item))
)

const selectedCapability = computed(() =>
  modeCapabilityRows.value.find((item) => item.key === activeCapabilityKey.value)
)

const selectedProviderNode = computed(() =>
  providerNodes.value.find((item) => item.name === providerForm.nodeName)
)

const selectedExtensionNode = computed(() =>
  providerNodes.value.find((item) => item.name === extensionForm.nodeName)
)

const isEditingExtension = computed(() => extensionDialogMode.value === 'edit')

const extensionDialogTitle = computed(() =>
  isEditingExtension.value ? '编辑扩展能力' : '新增扩展能力'
)

const selectedProviderBinding = computed(() =>
  capabilities.value.find(
    (item) => item.capability === providerForm.capability && item.provider_node === providerForm.nodeName
  )
)

const activeProviderIsVirtual = computed(() =>
  providerQueryProfiles(selectedProviderNode.value).includes('virtual_runtime_rule')
)

const providerOptions = computed(() => {
  if (!selectedCapability.value) return providerNodes.value
  const names = new Set(
    capabilities.value
      .filter((item) => item.capability === selectedCapability.value.capability)
      .map((item) => item.provider_node)
  )
  const matched = providerNodes.value.filter((item) => names.has(item.name))
  return matched.length ? matched : providerNodes.value
})

const sourceFieldOptions = computed(() =>
  buildSourceFieldOptions(selectedProviderBinding.value, fieldRows.value, selectedProviderNode.value)
)

const extensionSlotOptions = computed(() =>
  (selectedMode.value?.extension_slots || [])
    .map(normalizeExtensionSlot)
    .filter((item) => item.slot)
)

const capabilityMetaMap = computed(() => {
  const rows = new Map()
  for (const item of capabilityRegistry.value || []) {
    const capability = String(item.capability || '').trim()
    if (!capability) continue
    rows.set(capability, {
      capability,
      name: String(item.name || '').trim(),
      description: String(item.description || '').trim(),
      default_slots: item.default_slots || [],
      output_scope: normalizeOutputScope(item.output_scope),
    })
  }
  return rows
})

const extensionNodeOptions = computed(() =>
  providerNodes.value.filter((item) => !providerQueryProfiles(item).includes('virtual_runtime_rule'))
)

const extensionFieldOptions = computed(() => {
  const binding = capabilities.value.find(
    (item) => item.capability === extensionForm.capability && item.provider_node === extensionForm.nodeName
  )
  return buildSourceFieldOptions(
    binding,
    extensionForm.fields.map((field) => ({ source_field: field })),
    selectedExtensionNode.value
  )
})

const scopeKeyFieldOptions = computed(() =>
  buildScopeKeyFieldOptions(selectedExtensionNode.value)
)

const capabilityStats = computed(() => {
  const rows = dataCapabilityRows.value
  return {
    required: rows.filter((item) => item.section === 'required').length,
    conditional: rows.filter((item) => item.section === 'conditional').length,
    optional: rows.filter((item) => item.section === 'optional').length,
    extension: rows.filter((item) => item.section === 'extension').length,
    missing: rows.filter((item) => item.missing_fields.length || !item.provider_bindings.length).length,
  }
})

const visibleCapabilitySections = computed(() =>
  ['required', 'conditional', 'optional', 'extension'].filter((section) =>
    section === 'extension' || dataCapabilityRows.value.some((item) => item.section === section)
  )
)

async function ensureWorkspace() {
  try {
    if (!workspacePayload.value) {
      await loadWorkspace()
    }
    await nextTick()
    setDefaultMode()
    setDefaultCapability()
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '能力工作区加载失败')
  }
}

function setDefaultMode() {
  if (!selectedModeId.value && modeProfiles.value.length) {
    selectedModeId.value = modeProfiles.value[0].mode_id
  }
}

function setDefaultCapability() {
  if (!activeCapabilityKey.value && dataCapabilityRows.value.length) {
    selectCapability(dataCapabilityRows.value[0])
  }
}

function normalizeModeCapabilities(items, section) {
  return (items || []).map((item, index) => {
    const capability = String(item.capability || '').trim()
    const fields = (item.fields || []).map((field) => String(field || '').trim()).filter(Boolean)
    const providerBindings = capabilities.value.filter((row) => row.capability === capability)
    const meta = capabilityMetaMap.value.get(capability)
    const outputScope = firstOutputScope(
      ...providerBindings.map((binding) => binding.output_scope),
      meta?.output_scope
    )
    const mappedFields = new Set()
    for (const binding of providerBindings) {
      for (const field of Object.keys(binding.fields || {})) {
        mappedFields.add(field)
      }
    }
    return {
      ...item,
      key: `${section}:${capability}:${index}`,
      section,
      capability,
      fields,
      slots: item.allowed_slots || item.slots || [],
      output_scope: outputScope,
      provider_bindings: providerBindings,
      mapped_fields: [...mappedFields].sort(),
      missing_fields: fields.filter((field) => !mappedFields.has(field)),
    }
  })
}

function selectCapability(row) {
  activeCapabilityKey.value = row.key

  const binding = row.provider_bindings[0]
  const node = providerNodes.value.find((item) => item.name === binding?.provider_node) || providerOptions.value[0]
  providerForm.nodeName = binding?.provider_node || node?.name || ''
  providerForm.capability = row.capability
  providerForm.capabilityName = capabilityLabel(row.capability)
  providerForm.capabilityDescription = binding?.description || ''
  providerForm.defaultSlots = row.slots || []
  providerForm.outputScope = cloneOutputScope(firstOutputScope(binding?.output_scope, row.output_scope))
  providerForm.keys = binding?.keys || node?.keys || {}
  providerForm.assetTypes = (binding?.asset_types || node?.asset_types || ['stock']).join(', ')
  providerForm.queryProfiles = providerQueryProfiles(binding).length
    ? providerQueryProfiles(binding).join(', ')
    : providerQueryProfiles(node).join(', ') || 'panel_time_series'

  rebuildFieldRows(row, binding)
}

function rebuildFieldRows(row, binding) {
  const sourceFields = binding?.fields || {}
  const semanticFields = row.fields?.length ? row.fields : Object.keys(sourceFields)
  fieldRows.value = semanticFields.map((field) => ({
    semantic_field: field,
    source_field: formatFieldValue(sourceFields[field] ?? field),
  }))
}

function openCapabilityDialog(row) {
  selectCapability(row)
  configDialogVisible.value = true
}

function openCapabilityCard(row) {
  if (row?.section === 'extension') {
    openEditExtensionDialog(row)
    return
  }
  openCapabilityDialog(row)
}

function resetExtensionForm() {
  extensionForm.capability = ''
  extensionForm.capabilityName = ''
  extensionForm.capabilityDescription = ''
  extensionForm.nodeName = ''
  extensionForm.fields = []
  extensionForm.outputScope = defaultOutputScope()
  applyDefaultExtensionSlotsForScope(extensionForm.outputScope.scope_type)
}

function openExtensionDialog() {
  extensionDialogMode.value = 'create'
  editingExtensionCapability.value = ''
  resetExtensionForm()
  extensionDialogVisible.value = true
}

function openEditExtensionDialog(row) {
  extensionDialogMode.value = 'edit'
  editingExtensionCapability.value = row.capability
  suppressExtensionFieldReset.value = true
  extensionForm.capability = row.capability
  extensionForm.capabilityName = capabilityLabel(row.capability)
  extensionForm.capabilityDescription = capabilityMetaMap.value.get(row.capability)?.description || ''
  extensionForm.nodeName = row.provider_bindings[0]?.provider_node || ''
  extensionForm.slots = [...(row.slots || [])]
  extensionForm.fields = [...(row.fields || [])]
  extensionForm.outputScope = applyNodeKeyDefaults(
    firstOutputScope(row.output_scope, capabilityMetaMap.value.get(row.capability)?.output_scope),
    selectedExtensionNode.value
  )
  extensionDialogVisible.value = true
  nextTick(() => {
    suppressExtensionFieldReset.value = false
  })
}

async function handleSaveProviderMapping() {
  if (!selectedCapability.value) return
  if (activeProviderIsVirtual.value) {
    ElMessage.warning('运行时规则不需要绑定 real 表字段')
    return
  }
  try {
    await saveProviderMapping(providerForm, fieldRows.value)
    ElMessage.success('字段映射已保存')
    await nextTick()
    const refreshed = modeCapabilityRows.value.find(
      (item) => item.capability === selectedCapability.value.capability && item.section === selectedCapability.value.section
    )
    if (refreshed) selectCapability(refreshed)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存失败')
  }
}

async function handleSaveExtensionCapability() {
  if (!selectedModeId.value || !extensionForm.capability) return
  const capability = normalizeExtensionCapabilityId(extensionForm.capability)
  if (!capability) {
    ElMessage.error('能力英文 ID 只能使用小写英文、数字和下划线，例如 sentiment_news_daily')
    return
  }
  if (!extensionForm.slots.length) {
    ElMessage.error('请选择允许使用位置')
    return
  }
  if (!extensionForm.nodeName || !extensionForm.fields.length) {
    ElMessage.error('请选择数据节点和要暴露给策略的能力字段')
    return
  }

  const selectedNode = providerNodes.value.find((item) => item.name === extensionForm.nodeName)
  const capabilityName = String(extensionForm.capabilityName || '').trim() || capabilityLabel(capability)
  const capabilityDescription = String(extensionForm.capabilityDescription || '').trim()
  const nodeName = String(extensionForm.nodeName || '').trim()
  try {
    await saveModeCapability({
      modeId: selectedModeId.value,
      section: 'extension',
      capability,
      fieldsText: extensionForm.fields.join(', '),
      allowedSlots: extensionForm.slots,
    })
    await saveProviderMapping(
      {
        nodeName,
        capability,
        capabilityName,
        capabilityDescription,
        defaultSlots: extensionForm.slots,
        outputScope: extensionForm.outputScope,
        keys: selectedNode?.keys || {},
        assetTypes: (selectedNode?.asset_types || ['stock']).join(', '),
        queryProfiles: providerQueryProfiles(selectedNode).join(', ') || 'panel_time_series',
      },
      extensionForm.fields.map((field) => ({
        semantic_field: field,
        source_field: field,
      }))
    )
    ElMessage.success(isEditingExtension.value ? '扩展能力已更新' : '扩展能力已添加')
    extensionDialogVisible.value = false
    await nextTick()
    const refreshed = modeCapabilityRows.value.find(
      (item) => item.capability === capability && item.section === 'extension'
    )
    if (refreshed) selectCapability(refreshed)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存失败')
  }
}

async function handleDeleteExtensionCapability(row) {
  if (!selectedModeId.value || !row?.capability) return
  try {
    await ElMessageBox.confirm(
      `确认彻底删除扩展能力「${capabilityLabel(row.capability)}」？该操作会删除所有模式绑定，并移除 AIQuantBase 全局注册和字段映射。`,
      '删除扩展能力',
      {
        confirmButtonText: '彻底删除',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )
    await deleteModeCapability({
      modeId: selectedModeId.value,
      section: 'extension',
      capability: row.capability,
      deleteProviderRegistration: true,
    })
    if (activeCapabilityKey.value === row.key) {
      activeCapabilityKey.value = ''
    }
    ElMessage.success('扩展能力已删除')
    await nextTick()
    setDefaultCapability()
  } catch (error) {
    if (error === 'cancel' || error === 'close') return
    ElMessage.error(error instanceof Error ? error.message : '删除失败')
  }
}

function capabilityStatus(row) {
  if (!row.provider_bindings.length) return { type: 'danger', label: '未绑定数据节点' }
  if (row.missing_fields.length) return { type: 'warning', label: `缺 ${row.missing_fields.length} 个字段` }
  return { type: 'success', label: '可用' }
}

function isRuntimeRuleCapability(row) {
  const capability = String(row?.capability || '')
  if (capability.startsWith('order_constraints.')) return true
  return (row?.provider_bindings || []).some((binding) =>
    providerQueryProfiles(binding).includes('virtual_runtime_rule')
  )
}

function sectionTitle(section) {
  if (section === 'required') return '必需能力'
  if (section === 'conditional') return '条件能力'
  if (section === 'optional') return '可选能力'
  return '扩展能力'
}

function modeKindLabel(value) {
  const text = String(value || '')
  if (text === 'strategy_modes') return '策略模式'
  if (text === 'research_modes') return '研究模式'
  return text || '-'
}

function defaultOutputScope() {
  return {
    scope_type: 'daily_panel',
    entity_type: 'stock',
    keys: {
      entity: 'code',
      time: 'trade_time',
    },
  }
}

function normalizeOutputScope(value, options = {}) {
  if (!value || typeof value !== 'object') {
    return options.fallback ? defaultOutputScope() : {}
  }
  const scopeType = String(value.scope_type || '').trim()
  if (!scopeType) return options.fallback ? defaultOutputScope() : {}
  const keys = value.keys && typeof value.keys === 'object' ? value.keys : {}
  if (scopeType === 'linked_daily_panel') {
    return {
      scope_type: scopeType,
      base_entity_type: String(value.base_entity_type || 'stock').trim(),
      linked_entity_type: String(value.linked_entity_type || 'industry').trim(),
      output_entity_type: String(value.output_entity_type || 'stock').trim(),
      keys: {
        entity: String(keys.entity || 'code').trim(),
        time: String(keys.time || 'trade_time').trim(),
      },
    }
  }
  if (scopeType === 'event_stream') {
    return {
      scope_type: scopeType,
      entity_type: String(value.entity_type || 'stock').trim(),
      keys: {
        entity: String(keys.entity || 'code').trim(),
        event_time: String(keys.event_time || keys.time || 'event_time').trim(),
      },
    }
  }
  return {
    scope_type: scopeType,
    entity_type: String(value.entity_type || 'stock').trim(),
    keys: {
      entity: String(keys.entity || 'code').trim(),
      time: String(keys.time || 'trade_time').trim(),
    },
  }
}

function cloneOutputScope(value) {
  return normalizeOutputScope(value, { fallback: true })
}

function firstOutputScope(...values) {
  for (const value of values) {
    const scope = normalizeOutputScope(value)
    if (scope.scope_type) return scope
  }
  return {}
}

function outputScopeLabel(value) {
  const scope = normalizeOutputScope(value)
  if (!scope.scope_type) return '未设置输出维度'
  const option = OUTPUT_SCOPE_OPTIONS.find((item) => item.value === scope.scope_type)
  if (scope.scope_type === 'linked_daily_panel') {
    return `${option?.label || scope.scope_type}：${entityTypeLabel(scope.base_entity_type)} -> ${entityTypeLabel(scope.linked_entity_type)} -> ${entityTypeLabel(scope.output_entity_type)}`
  }
  return `${option?.label || scope.scope_type}：${entityTypeLabel(scope.entity_type)}`
}

function entityTypeLabel(value) {
  return ENTITY_TYPE_OPTIONS.find((item) => item.value === value)?.label || value || '-'
}

function providerQueryProfiles(value) {
  const profiles = value?.query_profiles || value?.access_patterns || []
  return Array.isArray(profiles) ? profiles.map((item) => String(item || '').trim()).filter(Boolean) : []
}

function nodeDescription(node) {
  const text = String(node?.description_zh || node?.description || '').trim()
  if (text) return text
  const fieldCount = Array.isArray(node?.source_fields) ? node.source_fields.length : 0
  return fieldCount ? `${fieldCount} 个可用字段` : '暂无中文说明'
}

function isOutputScopeSelected(scopeType) {
  return extensionForm.outputScope.scope_type === scopeType
}

function setOutputScopeType(scopeType) {
  if (!scopeType) return
  extensionForm.outputScope = applyNodeKeyDefaults({
    ...extensionForm.outputScope,
    scope_type: scopeType,
  }, selectedExtensionNode.value)
  applyDefaultExtensionSlotsForScope(scopeType)
}

function defaultExtensionSlotsForScope(scopeType) {
  const allowedSlots = new Set(extensionSlotOptions.value.map((item) => item.slot))
  return (DEFAULT_SLOTS_BY_OUTPUT_SCOPE[scopeType] || []).filter((slot) => allowedSlots.has(slot))
}

function applyDefaultExtensionSlotsForScope(scopeType) {
  const defaults = defaultExtensionSlotsForScope(scopeType)
  if (defaults.length) {
    extensionForm.slots = defaults
  }
}

function buildScopeKeyFieldOptions(node) {
  const options = new Map()
  const addOption = (value) => {
    const text = String(value || '').trim()
    if (!text || options.has(text)) return
    const help = fieldHelp(text)
    options.set(text, {
      value: text,
      label: help.label === text ? text : `${text}｜${help.label}`,
      description: help.description,
    })
  }
  for (const value of Object.values(node?.keys || {})) addOption(value)
  for (const value of node?.source_fields || []) addOption(value)
  return [...options.values()]
}

function applyNodeKeyDefaults(scopeValue, node) {
  const scope = cloneOutputScope(scopeValue)
  if (!node) return scope
  const entityField = inferNodeEntityField(node)
  const timeField = inferNodeTimeField(node, scope.scope_type)
  if (entityField) scope.keys.entity = entityField
  if (scope.scope_type === 'event_stream') {
    if (timeField) scope.keys.event_time = timeField
    delete scope.keys.time
  } else {
    if (timeField) scope.keys.time = timeField
    delete scope.keys.event_time
  }
  return scope
}

function inferNodeEntityField(node) {
  const keys = node?.keys || {}
  return pickNodeField(node, [
    keys.symbol,
    keys.entity,
    keys.code,
    'code',
    'market_code',
    'index_code',
  ])
}

function inferNodeTimeField(node, scopeType) {
  const keys = node?.keys || {}
  const eventCandidates = [keys.event_time, keys.publish_time, keys.payout_time, keys.time, 'event_time', 'ann_date', 'date_ex']
  const panelCandidates = [keys.time, keys.datetime, 'trade_time', 'trade_date', 'datetime', 'date']
  return pickNodeField(node, scopeType === 'event_stream' ? eventCandidates : panelCandidates)
}

function pickNodeField(node, candidates) {
  const sourceFields = new Set((node?.source_fields || []).map((item) => String(item || '').trim()).filter(Boolean))
  const keyFields = new Set(Object.values(node?.keys || {}).map((item) => String(item || '').trim()).filter(Boolean))
  for (const candidate of candidates) {
    const text = String(candidate || '').trim()
    if (text && (sourceFields.has(text) || keyFields.has(text))) return text
  }
  for (const value of keyFields) return value
  for (const value of sourceFields) return value
  return ''
}

function modeDescription(mode) {
  if (mode?.description) return mode.description
  if (mode?.mode_kind === 'research_modes') {
    return '用于数据探索、因子验证和研究分析，侧重读取行情、估值、行业和自定义因子等能力。'
  }
  if (mode?.mode_kind === 'strategy_modes') {
    return '用于策略回测和交易决策，侧重行情、可交易状态、订单规则和策略运行所需字段。'
  }
  return '选择该模式后，右侧会显示它需要的数据能力和字段映射情况。'
}

function capabilityLabel(capability) {
  const meta = capabilityMetaMap.value.get(capability)
  if (meta?.name) return meta.name
  return CAPABILITY_LABELS[capability] || capability
}

function normalizeExtensionSlot(item) {
  if (typeof item === 'string') {
    return {
      slot: item,
      name: slotLabel(item),
      description: slotDescription(item),
    }
  }
  const slot = String(item?.slot || item?.id || item?.name || '').trim()
  if (!slot) return { slot: '' }
  return {
    ...item,
    slot,
    name: item?.name || slotLabel(slot),
    description: item?.description || slotDescription(slot),
  }
}

function isExtensionSlotSelected(slot) {
  return extensionForm.slots.includes(slot)
}

function isExtensionFieldSelected(field) {
  return extensionForm.fields.includes(field)
}

function toggleExtensionSlot(slot) {
  if (!slot) return
  if (isExtensionSlotSelected(slot)) {
    extensionForm.slots = extensionForm.slots.filter((item) => item !== slot)
  } else {
    extensionForm.slots = [...extensionForm.slots, slot]
  }
}

function toggleExtensionField(field) {
  if (!field) return
  if (isExtensionFieldSelected(field)) {
    extensionForm.fields = extensionForm.fields.filter((item) => item !== field)
  } else {
    extensionForm.fields = [...extensionForm.fields, field]
  }
}

function selectAllExtensionFields() {
  extensionForm.fields = extensionFieldOptions.value.map((item) => item.value)
}

function clearExtensionFields() {
  extensionForm.fields = []
}

function normalizeExtensionCapabilityId(value) {
  const text = String(value || '').trim()
  if (!/^[a-z][a-z0-9_]*$/.test(text)) return ''
  return text
}

function fieldHelp(field) {
  const [label, description] = FIELD_HELP[field] || [field, '']
  return { label, description }
}

function fieldPreview(row) {
  if (row.slots?.length && row.fields?.length) {
    return `${row.fields.map((field) => fieldHelp(field).label).join('、')}｜槽位：${row.slots.map(slotLabel).join('、')}`
  }
  if (row.fields?.length) {
    return row.fields.map((field) => fieldHelp(field).label).join('、')
  }
  if (row.slots?.length) {
    return `字段槽位：${row.slots.map(slotLabel).join('、')}`
  }
  return '无需固定字段'
}

function slotLabel(slot) {
  return SLOT_LABELS[slot] || slot
}

function slotUiMeta(slot) {
  return {
    eyebrow: '扩展用途',
    title: slotLabel(slot),
    description: slotDescription(slot),
    ...(SLOT_UI_META[slot] || {}),
  }
}

function slotDescription(slot) {
  const descriptions = {
    universe_fields: '用于构建或缩小候选股票池，例如指数成分、主题标签、自定义股票池标记。',
    filter_fields: '用于过滤候选股票，例如财务过滤、风险过滤、龙虎榜过滤。',
    ranking_fields: '用于排序、打分和选股，通常需要数值字段，例如市值、因子分、资金流。',
    signal_fields: '用于买卖信号表达式，例如技术指标信号、风险预警分数。',
    weighting_fields: '用于目标权重或仓位计算，例如目标仓位、风险权重、得分权重。',
    report_fields: '仅用于结果输出和诊断展示，不直接影响交易决策。',
    factor_inputs: '用于研究或因子计算的输入字段。',
  }
  return descriptions[slot] || '当前模式定义的扩展数据使用位置。'
}

function formatFieldValue(value) {
  if (value && typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value ?? '')
}

function buildSourceFieldOptions(binding, rows, node) {
  const options = new Map()
  const addOption = (value) => {
    const text = formatFieldValue(value).trim()
    if (!text || options.has(text)) return
    const help = fieldHelp(text)
    options.set(text, {
      value: text,
      label: help.label === text ? text : `${text}｜${help.label}`,
      description: help.description,
    })
  }

  for (const value of node?.source_fields || []) addOption(value)
  const fields = binding?.fields || {}
  for (const [semanticField, sourceField] of Object.entries(fields)) {
    addOption(semanticField)
    addOption(sourceField)
    if (sourceField && typeof sourceField === 'object') {
      for (const dependency of sourceField.depends_on || []) addOption(dependency)
    }
  }
  for (const row of rows || []) addOption(row.source_field)
  return [...options.values()]
}

watch(modeProfiles, () => {
  setDefaultMode()
})

watch(selectedModeId, () => {
  activeCapabilityKey.value = ''
  nextTick(setDefaultCapability)
})

watch(
  () => providerForm.nodeName,
  () => {
    if (!selectedCapability.value) return
    rebuildFieldRows(selectedCapability.value, selectedProviderBinding.value)
  }
)

watch(
  () => extensionForm.nodeName,
  () => {
    if (suppressExtensionFieldReset.value) return
    extensionForm.fields = []
    extensionForm.outputScope = applyNodeKeyDefaults(extensionForm.outputScope, selectedExtensionNode.value)
  }
)

watch(
  () => extensionForm.outputScope.scope_type,
  () => {
    extensionForm.outputScope = applyNodeKeyDefaults(extensionForm.outputScope, selectedExtensionNode.value)
  }
)

onMounted(ensureWorkspace)
</script>

<template>
  <div class="cap-page">
    <section class="workbench-grid">
      <aside class="mode-sidebar">
        <el-card class="panel-card" shadow="never">
          <div class="mode-intro">
            <h2>模式工作台</h2>
            <p>选择一个业务模式，右侧会联动显示该模式需要的数据能力、字段映射和待处理项。</p>
          </div>

          <div class="mode-list">
            <button
              v-for="mode in modeProfiles"
              :key="mode.mode_id"
              type="button"
              class="mode-option"
              :class="{ active: selectedModeId === mode.mode_id }"
              @click="selectedModeId = mode.mode_id"
            >
              <strong>{{ mode.mode_name }}</strong>
              <span>{{ modeKindLabel(mode.mode_kind) }}</span>
              <p>{{ modeDescription(mode) }}</p>
            </button>
          </div>
        </el-card>
      </aside>

      <main class="mode-main content-panel">
        <div v-if="selectedMode" class="mode-context">
          <div>
            <h2>{{ selectedMode.mode_name }}</h2>
            <span>{{ modeKindLabel(selectedMode.mode_kind) }}</span>
            <p>{{ modeDescription(selectedMode) }}</p>
          </div>
          <div class="inline-stats">
            <span><strong>{{ capabilityStats.required }}</strong> 必需</span>
            <span><strong>{{ capabilityStats.conditional }}</strong> 条件</span>
            <span><strong>{{ capabilityStats.optional }}</strong> 可选</span>
            <span><strong>{{ capabilityStats.extension }}</strong> 扩展</span>
            <span><strong>{{ capabilityStats.missing }}</strong> 待处理</span>
          </div>
        </div>

        <section v-for="section in visibleCapabilitySections" :key="section" class="cap-section">
          <div class="section-title">
            <h2>{{ sectionTitle(section) }}</h2>
            <el-button
              v-if="section === 'extension'"
              size="large"
              type="primary"
              class="extension-add-button"
              @click="openExtensionDialog"
            >
              新增扩展能力
            </el-button>
          </div>

          <div v-if="dataCapabilityRows.filter((item) => item.section === section).length" class="capability-grid">
            <div
              v-for="row in dataCapabilityRows.filter((item) => item.section === section)"
              :key="row.key"
              role="button"
              tabindex="0"
              class="capability-card"
              :class="{ active: activeCapabilityKey === row.key }"
              @click="openCapabilityCard(row)"
              @keydown.enter="openCapabilityCard(row)"
              @keydown.space.prevent="openCapabilityCard(row)"
            >
              <div class="card-head">
                <div>
                  <strong>{{ capabilityLabel(row.capability) }}</strong>
                  <span>{{ row.capability }}</span>
                </div>
                <el-tag :type="capabilityStatus(row).type" size="small">{{ capabilityStatus(row).label }}</el-tag>
              </div>
              <p>{{ fieldPreview(row) }}</p>
              <div class="provider-tags">
                <el-tag v-for="binding in row.provider_bindings" :key="binding.provider_node" size="small" effect="plain">
                  {{ binding.provider_node }}
                </el-tag>
                <el-tag v-if="row.section === 'extension'" size="small" type="info" effect="plain">
                  {{ outputScopeLabel(row.output_scope) }}
                </el-tag>
                <el-tag v-if="!row.provider_bindings.length" size="small" type="danger" effect="plain">未绑定</el-tag>
              </div>
              <div v-if="row.section === 'extension'" class="extension-card-actions">
                <el-button size="small" plain @click.stop="openEditExtensionDialog(row)">编辑</el-button>
                <el-button size="small" plain type="danger" @click.stop="handleDeleteExtensionCapability(row)">
                  删除
                </el-button>
              </div>
            </div>
          </div>
          <el-empty
            v-else
            class="section-empty"
            :image-size="68"
            :description="section === 'extension' ? '当前模式暂无扩展能力' : '暂无能力'"
          />
        </section>
      </main>
    </section>

    <el-dialog
      v-model="configDialogVisible"
      width="min(980px, 92vw)"
      class="capability-dialog"
      destroy-on-close
    >
      <template #header>
        <div v-if="selectedCapability" class="dialog-title">
          <div>
            <p>{{ selectedMode?.mode_name }} / {{ sectionTitle(selectedCapability.section) }}</p>
            <h2>{{ capabilityLabel(selectedCapability.capability) }}</h2>
            <span>{{ selectedCapability.capability }}</span>
          </div>
          <el-tag :type="capabilityStatus(selectedCapability).type">{{ capabilityStatus(selectedCapability).label }}</el-tag>
        </div>
      </template>

      <section v-if="selectedCapability" class="dialog-body">
        <el-form label-position="top" class="node-form">
          <el-form-item label="数据节点">
            <el-select v-model="providerForm.nodeName" filterable>
              <el-option
                v-for="node in providerOptions"
                :key="node.name"
                :label="`${node.name} (${providerQueryProfiles(node).join(', ') || '-'})`"
                :value="node.name"
              />
            </el-select>
          </el-form-item>
        </el-form>

        <el-alert
          v-if="selectedCapability.missing_fields.length"
          type="warning"
          :closable="false"
          show-icon
          class="mb"
          :title="`缺少字段映射：${selectedCapability.missing_fields.join(', ')}`"
        />

        <el-table v-if="fieldRows.length" :data="fieldRows" size="small" border class="field-table">
          <el-table-column label="模式字段" min-width="260">
            <template #default="{ row }">
              <div class="semantic-cell" :title="fieldHelp(row.semantic_field).description || row.semantic_field">
                <strong>{{ fieldHelp(row.semantic_field).label }}</strong>
                <span>{{ row.semantic_field }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="real 表字段" min-width="360">
            <template #default="{ row }">
              <el-select
                v-model="row.source_field"
                filterable
                class="source-select"
                placeholder="选择字段"
                :disabled="activeProviderIsVirtual"
              >
                <el-option
                  v-for="option in sourceFieldOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value"
                >
                  <div class="source-option">
                    <strong>{{ option.value }}</strong>
                    <span v-if="option.description">{{ option.description }}</span>
                  </div>
                </el-option>
              </el-select>
            </template>
          </el-table-column>
        </el-table>

        <el-empty v-else description="当前能力不需要字段映射" :image-size="72" />
      </section>

      <template #footer>
        <div class="dialog-actions">
          <el-button @click="configDialogVisible = false">关闭</el-button>
          <el-button
            type="primary"
            :loading="saving"
            :disabled="activeProviderIsVirtual || !fieldRows.length || !providerForm.nodeName"
            @click="handleSaveProviderMapping"
          >
            保存字段映射
          </el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog
      v-model="extensionDialogVisible"
      :title="extensionDialogTitle"
      width="min(920px, calc(100vw - 32px))"
      class="extension-capability-dialog"
      align-center
      append-to-body
      destroy-on-close
    >
      <div class="extension-dialog-scroll">
        <el-form label-position="top" class="extension-form">
          <section class="extension-step-card">
            <div class="extension-step-title">
              <span>01</span>
              <h3>基础信息</h3>
            </div>
            <div class="extension-form-grid two">
              <el-form-item label="能力英文名">
                <el-input
                  v-model="extensionForm.capability"
                  clearable
                  :disabled="isEditingExtension"
                  placeholder="例如 sentiment_news_daily"
                />
              </el-form-item>

              <el-form-item label="能力中文名">
                <el-input
                  v-model="extensionForm.capabilityName"
                  clearable
                  placeholder="例如 新闻情绪日频"
                />
              </el-form-item>
            </div>
            <el-form-item label="能力说明">
              <el-input
                v-model="extensionForm.capabilityDescription"
                type="textarea"
                :rows="2"
                placeholder="简要说明这份数据在策略里解决什么问题"
              />
            </el-form-item>
          </section>

          <section class="extension-step-card">
            <div class="extension-step-title">
              <span>02</span>
              <h3>数据来源</h3>
            </div>
            <el-form-item label="数据节点">
              <el-select
                v-model="extensionForm.nodeName"
                filterable
                allow-create
                default-first-option
                class="full-width"
                popper-class="extension-capability-popper"
                placeholder="选择 real 表节点"
              >
                <el-option
                  v-for="node in extensionNodeOptions"
                  :key="node.name"
                  :label="node.name"
                  :value="node.name"
                >
                  <div class="node-option">
                    <strong>{{ node.name }}</strong>
                    <span>{{ nodeDescription(node) }}</span>
                  </div>
                </el-option>
              </el-select>
            </el-form-item>
            <div v-if="selectedExtensionNode" class="selected-node-summary">
              <span>{{ selectedExtensionNode.source_fields?.length || 0 }} 个字段</span>
              <span>{{ providerQueryProfiles(selectedExtensionNode).join(' / ') || '未声明查询方式' }}</span>
            </div>
          </section>

          <section class="extension-step-card">
            <div class="extension-step-title">
              <span>03</span>
              <h3>输出维度</h3>
            </div>
            <div class="scope-editor">
              <div class="output-scope-grid">
                <button
                  v-for="option in OUTPUT_SCOPE_OPTIONS"
                  :key="option.value"
                  type="button"
                  class="output-scope-card"
                  :class="{ active: isOutputScopeSelected(option.value) }"
                  @click="setOutputScopeType(option.value)"
                >
                  <span class="scope-badge">{{ option.badge }}</span>
                  <strong>{{ option.label }}</strong>
                  <em>{{ option.summary }}</em>
                </button>
              </div>

              <div v-if="extensionForm.outputScope.scope_type === 'linked_daily_panel'" class="scope-grid three">
                <div class="scope-field">
                  <label>基础实体</label>
                  <el-select
                    v-model="extensionForm.outputScope.base_entity_type"
                    popper-class="extension-capability-popper"
                    placeholder="选择基础实体"
                  >
                    <el-option
                      v-for="option in ENTITY_TYPE_OPTIONS"
                      :key="`base-${option.value}`"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
                <div class="scope-field">
                  <label>关联实体</label>
                  <el-select
                    v-model="extensionForm.outputScope.linked_entity_type"
                    popper-class="extension-capability-popper"
                    placeholder="选择关联实体"
                  >
                    <el-option
                      v-for="option in ENTITY_TYPE_OPTIONS"
                      :key="`linked-${option.value}`"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
                <div class="scope-field">
                  <label>输出实体</label>
                  <el-select
                    v-model="extensionForm.outputScope.output_entity_type"
                    popper-class="extension-capability-popper"
                    placeholder="选择输出实体"
                  >
                    <el-option
                      v-for="option in ENTITY_TYPE_OPTIONS"
                      :key="`output-${option.value}`"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
              </div>

              <div v-else class="scope-grid">
                <div class="scope-field">
                  <label>实体类型</label>
                  <el-select
                    v-model="extensionForm.outputScope.entity_type"
                    popper-class="extension-capability-popper"
                    placeholder="选择实体类型"
                  >
                    <el-option
                      v-for="option in ENTITY_TYPE_OPTIONS"
                      :key="option.value"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
              </div>

              <div class="scope-grid">
                <div class="scope-field">
                  <label>实体字段</label>
                  <el-select
                    v-model="extensionForm.outputScope.keys.entity"
                    filterable
                    class="full-width"
                    popper-class="extension-capability-popper"
                    :disabled="!scopeKeyFieldOptions.length"
                    placeholder="选择数据节点后自动匹配"
                  >
                    <el-option
                      v-for="option in scopeKeyFieldOptions"
                      :key="`entity-${option.value}`"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
                <div v-if="extensionForm.outputScope.scope_type === 'event_stream'" class="scope-field">
                  <label>事件时间字段</label>
                  <el-select
                    v-model="extensionForm.outputScope.keys.event_time"
                    filterable
                    class="full-width"
                    popper-class="extension-capability-popper"
                    :disabled="!scopeKeyFieldOptions.length"
                    placeholder="选择事件时间字段"
                  >
                    <el-option
                      v-for="option in scopeKeyFieldOptions"
                      :key="`event-time-${option.value}`"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
                <div v-else class="scope-field">
                  <label>时间字段</label>
                  <el-select
                    v-model="extensionForm.outputScope.keys.time"
                    filterable
                    class="full-width"
                    popper-class="extension-capability-popper"
                    :disabled="!scopeKeyFieldOptions.length"
                    placeholder="选择数据节点后自动匹配"
                  >
                    <el-option
                      v-for="option in scopeKeyFieldOptions"
                      :key="`time-${option.value}`"
                      :label="option.label"
                      :value="option.value"
                    />
                  </el-select>
                </div>
              </div>
            </div>
          </section>

          <section class="extension-step-card">
            <div class="extension-step-title">
              <span>04</span>
              <h3>策略用途</h3>
            </div>
            <div v-if="extensionSlotOptions.length" class="usage-choice-grid">
              <button
                v-for="slot in extensionSlotOptions"
                :key="slot.slot"
                type="button"
                class="usage-choice-card"
                :class="{ active: isExtensionSlotSelected(slot.slot) }"
                @click="toggleExtensionSlot(slot.slot)"
              >
                <span>{{ slotUiMeta(slot.slot).eyebrow }}</span>
                <strong>{{ slot.name || slotUiMeta(slot.slot).title }}</strong>
                <em>{{ isExtensionSlotSelected(slot.slot) ? '已选择' : '点击选择' }}</em>
              </button>
            </div>
            <el-empty v-else description="当前模式没有定义扩展用途" :image-size="72" />
          </section>

          <section class="extension-step-card">
            <div class="extension-step-title">
              <span>05</span>
              <h3>策略可用字段</h3>
            </div>
            <div class="field-picker-toolbar">
              <span>已选择 {{ extensionForm.fields.length }} / {{ extensionFieldOptions.length }}</span>
              <div>
                <el-button size="small" text type="primary" @click="selectAllExtensionFields">全选</el-button>
                <el-button size="small" text @click="clearExtensionFields">清空</el-button>
              </div>
            </div>
            <div v-if="extensionFieldOptions.length" class="field-choice-list">
              <button
                v-for="option in extensionFieldOptions"
                :key="option.value"
                type="button"
                class="field-choice-row"
                :class="{ active: isExtensionFieldSelected(option.value) }"
                @click="toggleExtensionField(option.value)"
              >
                <span class="field-choice-main">
                  <strong>{{ fieldHelp(option.value).label }}</strong>
                  <code>{{ option.value }}</code>
                </span>
                <small>{{ option.description || fieldHelp(option.value).description || '来自所选数据节点' }}</small>
                <em>{{ isExtensionFieldSelected(option.value) ? '已选' : '选择' }}</em>
              </button>
            </div>
            <el-empty v-else description="请先选择数据节点" :image-size="64" />
          </section>
        </el-form>
      </div>

      <template #footer>
        <div class="dialog-actions">
          <el-button @click="extensionDialogVisible = false">关闭</el-button>
          <el-button type="primary" :loading="saving" @click="handleSaveExtensionCapability">
            {{ isEditingExtension ? '保存修改' : '添加到当前模式' }}
          </el-button>
        </div>
      </template>
    </el-dialog>

    <el-card v-if="diagnostics.length" class="panel-card" shadow="never">
      <template #header>诊断</template>
      <el-table :data="diagnostics" size="small">
        <el-table-column prop="severity" label="级别" width="100" />
        <el-table-column prop="code" label="Code" width="180" />
        <el-table-column prop="message" label="Message" min-width="360" />
      </el-table>
    </el-card>
  </div>
</template>

<style scoped>
.cap-page {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.workbench-grid {
  display: grid;
  grid-template-columns: 320px minmax(0, 1fr);
  gap: 16px;
  align-items: start;
}

.mode-sidebar,
.mode-main {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.content-panel {
  padding: 16px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: #fff;
}

.panel-card {
  border-radius: 8px;
  border: 1px solid rgba(65, 88, 72, 0.12);
}

.card-header,
.section-title,
.dialog-title,
.dialog-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.extension-add-button {
  min-height: 42px;
  padding: 0 20px;
  border-radius: 8px;
  font-weight: 700;
  letter-spacing: 0.02em;
  box-shadow: 0 8px 18px rgba(24, 120, 86, 0.18);
}

.mode-intro {
  padding: 2px 2px 14px;
  border-bottom: 1px solid rgba(65, 88, 72, 0.1);
}

.mode-intro h2 {
  margin: 0;
  color: #1e3327;
  font-size: 18px;
}

.mode-intro p,
.mode-option p,
.mode-context p {
  margin: 6px 0 0;
  color: #526259;
  font-size: 13px;
  line-height: 1.5;
}

.card-head span,
.dialog-title p,
.dialog-title span {
  color: #6c786f;
  font-size: 12px;
}

.mode-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-top: 14px;
}

.mode-option {
  width: 100%;
  padding: 12px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.86);
  text-align: left;
  cursor: pointer;
  transition: background 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
}

.mode-option:hover,
.mode-option.active {
  border-color: rgba(31, 109, 79, 0.42);
  background: rgba(242, 249, 245, 0.96);
  box-shadow: var(--shadow-sm);
}

.mode-option strong,
.mode-option span {
  display: block;
}

.mode-option strong {
  color: #1e3327;
  font-size: 15px;
}

.mode-option span {
  margin-top: 4px;
  color: #6c786f;
  font-size: 12px;
}

.mode-context {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
  padding: 12px 14px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.72);
}

.mode-context span,
.inline-stats span {
  color: #6c786f;
  font-size: 12px;
}

.inline-stats {
  display: grid;
  grid-template-columns: repeat(2, minmax(74px, 1fr));
  gap: 10px;
  justify-content: flex-end;
  min-width: 300px;
}

.inline-stats span {
  padding: 6px 10px;
  border-radius: 8px;
  background: rgba(57, 102, 74, 0.08);
}

.inline-stats strong {
  color: #1e3327;
}

h2,
h3 {
  margin: 0;
  color: #1e3327;
}

.cap-section {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.capability-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.capability-card {
  display: flex;
  flex-direction: column;
  min-height: 132px;
  padding: 16px;
  border-radius: 8px;
  border: 1px solid rgba(65, 88, 72, 0.14);
  background: rgba(255, 255, 255, 0.9);
  text-align: left;
  cursor: pointer;
  transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
}

.capability-card:hover,
.capability-card.active {
  transform: translateY(-2px);
  border-color: rgba(31, 109, 79, 0.42);
  box-shadow: var(--shadow-md);
}

.card-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: flex-start;
}

.card-head strong,
.card-head span {
  display: block;
}

.capability-card p {
  margin: 12px 0;
  color: #526259;
  font-size: 13px;
  line-height: 1.45;
  overflow-wrap: anywhere;
}

.provider-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.extension-card-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: auto;
  padding-top: 12px;
}

.dialog-title {
  align-items: flex-start;
}

.dialog-title h2 {
  margin: 4px 0 2px;
}

.dialog-title p {
  margin: 0;
}

.dialog-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.node-form {
  max-width: 520px;
}

.extension-form {
  width: 100%;
}

:deep(.extension-form .el-form-item) {
  margin-bottom: 10px;
}

:deep(.extension-form .el-form-item:last-child) {
  margin-bottom: 0;
}

:global(.el-overlay:has(.extension-capability-dialog)) {
  z-index: 9999 !important;
}

:global(.el-overlay-dialog:has(.extension-capability-dialog)) {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 4px;
  overflow: hidden;
}

:global(.extension-capability-dialog) {
  margin: 0;
  display: flex;
  flex-direction: column;
  height: min(700px, calc(100vh - 24px));
  max-height: calc(100vh - 24px);
  border-radius: 14px;
  overflow: hidden;
  background:
    radial-gradient(circle at 16% 0%, rgba(42, 126, 92, 0.12), transparent 34%),
    linear-gradient(180deg, #ffffff 0%, #f8fbf8 100%);
  box-shadow: 0 28px 76px rgba(18, 40, 28, 0.24);
}

:global(.extension-capability-dialog .el-dialog__header) {
  padding: 12px 20px 10px;
  margin: 0;
  border-bottom: 1px solid rgba(65, 88, 72, 0.1);
}

:global(.extension-capability-dialog .el-dialog__title) {
  color: #14281d;
  font-size: 18px;
  font-weight: 800;
  letter-spacing: 0.01em;
}

:global(.extension-capability-dialog .el-dialog__headerbtn) {
  top: 9px;
  right: 14px;
}

:global(.extension-capability-dialog .el-dialog__body) {
  display: flex;
  flex: 1 1 auto;
  min-height: 0;
  padding: 0;
  overflow: hidden;
}

:global(.extension-capability-dialog .el-dialog__footer) {
  padding: 10px 20px 12px;
  border-top: 1px solid rgba(65, 88, 72, 0.1);
  background: rgba(255, 255, 255, 0.92);
}

:global(.extension-capability-popper) {
  z-index: 10001 !important;
}

.extension-dialog-scroll {
  flex: 1 1 auto;
  min-height: 0;
  max-height: none;
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 14px 20px 16px;
}

.extension-dialog-scroll::-webkit-scrollbar {
  width: 8px;
}

.extension-dialog-scroll::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: rgba(46, 91, 65, 0.24);
}

.extension-dialog-scroll::-webkit-scrollbar-track {
  background: transparent;
}

.extension-step-card {
  padding: 12px 14px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 11px;
  background: rgba(255, 255, 255, 0.86);
  box-shadow: 0 10px 28px rgba(31, 70, 48, 0.06);
}

.extension-step-card + .extension-step-card {
  margin-top: 10px;
}

.extension-step-title {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 10px;
}

.extension-step-title span {
  display: inline-grid;
  place-items: center;
  width: 24px;
  height: 24px;
  border-radius: 7px;
  background: #187856;
  color: #fff;
  font-size: 12px;
  font-weight: 800;
}

.extension-step-title h3 {
  color: #14281d;
  font-size: 15px;
  font-weight: 800;
}

.extension-form-grid {
  display: grid;
  gap: 8px;
}

.extension-form-grid.two {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.node-option {
  display: flex;
  flex-direction: column;
  gap: 2px;
  line-height: 1.3;
}

.node-option strong {
  color: #1e3327;
  font-weight: 700;
}

.node-option span {
  color: #6c786f;
  font-size: 12px;
}

.selected-node-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: -6px;
}

.selected-node-summary span {
  padding: 4px 8px;
  border-radius: 999px;
  background: rgba(24, 120, 86, 0.09);
  color: #315342;
  font-size: 12px;
  font-weight: 700;
}

.output-scope-grid,
.usage-choice-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  width: 100%;
}

.output-scope-card,
.usage-choice-card {
  position: relative;
  min-height: 76px;
  padding: 9px 10px;
  border: 1px solid rgba(65, 88, 72, 0.14);
  border-radius: 12px;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(245, 250, 246, 0.94));
  text-align: left;
  cursor: pointer;
  transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease, background 0.16s ease;
}

.output-scope-card:hover,
.output-scope-card.active,
.usage-choice-card:hover,
.usage-choice-card.active {
  transform: translateY(-1px);
  border-color: rgba(24, 120, 86, 0.56);
  background: linear-gradient(180deg, rgba(244, 252, 247, 0.98), rgba(235, 248, 240, 0.96));
  box-shadow: 0 10px 22px rgba(31, 109, 79, 0.12);
}

.scope-badge {
  display: inline-grid;
  place-items: center;
  width: 24px;
  height: 24px;
  margin-bottom: 7px;
  border-radius: 7px;
  background: rgba(24, 120, 86, 0.12);
  color: #187856;
  font-size: 13px;
  font-weight: 800;
}

.output-scope-card.active .scope-badge {
  background: #187856;
  color: #fff;
}

.output-scope-card strong,
.usage-choice-card strong {
  display: block;
  color: #14281d;
  font-size: 13px;
  line-height: 1.25;
}

.output-scope-card em,
.usage-choice-card em {
  display: inline-block;
  margin-top: 4px;
  color: #187856;
  font-size: 11px;
  font-style: normal;
  font-weight: 800;
}

.output-scope-card small,
.usage-choice-card small {
  display: block;
  margin-top: 6px;
  color: #526259;
  font-size: 11px;
  line-height: 1.4;
}

.usage-choice-card {
  min-height: 72px;
}

.usage-choice-card > span {
  display: inline-block;
  margin-bottom: 5px;
  color: #6c786f;
  font-size: 11px;
  font-weight: 700;
}

.usage-choice-card.active::after {
  position: absolute;
  top: 10px;
  right: 10px;
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: #187856;
  content: '';
}

.field-picker-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 8px;
}

.field-picker-toolbar span {
  color: #405248;
  font-size: 12px;
  font-weight: 800;
}

.field-choice-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 260px;
  overflow-y: auto;
  padding-right: 4px;
}

.field-choice-list::-webkit-scrollbar {
  width: 6px;
}

.field-choice-list::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: rgba(46, 91, 65, 0.2);
}

.field-choice-row {
  display: grid;
  grid-template-columns: minmax(180px, 0.9fr) minmax(180px, 1.1fr) auto;
  align-items: center;
  gap: 12px;
  min-height: 42px;
  padding: 7px 10px;
  border: 1px solid rgba(65, 88, 72, 0.14);
  border-radius: 9px;
  background: rgba(255, 255, 255, 0.9);
  text-align: left;
  cursor: pointer;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, background 0.16s ease;
}

.field-choice-row:hover,
.field-choice-row.active {
  border-color: rgba(24, 120, 86, 0.56);
  background: linear-gradient(180deg, rgba(244, 252, 247, 0.98), rgba(235, 248, 240, 0.96));
  box-shadow: 0 6px 16px rgba(31, 109, 79, 0.08);
}

.field-choice-main {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.field-choice-main strong {
  flex: 0 1 auto;
  max-width: 42%;
  color: #14281d;
  font-size: 13px;
  line-height: 1.25;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-choice-main code {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  color: #187856;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: 11px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-choice-row small {
  overflow: hidden;
  color: #526259;
  font-size: 11px;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-choice-row em {
  min-width: 38px;
  padding: 2px 7px;
  border-radius: 999px;
  background: rgba(68, 91, 76, 0.1);
  color: #526259;
  font-size: 11px;
  font-style: normal;
  font-weight: 800;
  text-align: center;
}

.field-choice-row.active em {
  background: #187856;
  color: #fff;
}

.full-width {
  width: 100%;
}

.slot-choice-grid,
.scope-choice-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 6px;
  width: 100%;
}

.slot-choice-card {
  min-height: 46px;
  padding: 6px 8px;
  border: 1px solid rgba(65, 88, 72, 0.16);
  border-radius: 7px;
  background: rgba(248, 251, 248, 0.92);
  text-align: left;
  cursor: pointer;
  transition: border-color 0.18s ease, background 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
}

.slot-choice-card:hover,
.slot-choice-card.active {
  border-color: rgba(23, 128, 91, 0.56);
  background: rgba(237, 248, 242, 0.98);
  box-shadow: 0 4px 10px rgba(31, 109, 79, 0.08);
}

.slot-choice-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
}

.slot-choice-head strong {
  color: #1e3327;
  font-size: 11px;
  line-height: 1.2;
}

.slot-choice-head span {
  flex: 0 0 auto;
  padding: 1px 4px;
  border-radius: 999px;
  background: rgba(68, 91, 76, 0.1);
  color: #526259;
  font-size: 9px;
}

.slot-choice-card.active .slot-choice-head span {
  background: rgba(24, 120, 86, 0.16);
  color: #187856;
  font-weight: 700;
}

.slot-choice-card code {
  display: inline-block;
  margin-top: 2px;
  color: #187856;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: 9px;
}

.slot-choice-card small {
  display: -webkit-box;
  margin-top: 2px;
  overflow: hidden;
  color: #526259;
  font-size: 9px;
  line-height: 1.2;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 1;
}

.scope-choice-card {
  min-height: 50px;
}

.scope-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
}

.scope-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.scope-grid.three {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.scope-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}

.scope-field label {
  color: #405248;
  font-size: 11px;
  font-weight: 700;
  line-height: 1.2;
}

.mb {
  margin-bottom: 2px;
}

.field-table {
  width: 100%;
}

.semantic-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.semantic-cell strong {
  flex: 0 1 auto;
  max-width: 45%;
  color: #1e3327;
  font-size: 13px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.semantic-cell span {
  flex: 1;
  min-width: 0;
  color: #526259;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: 12px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

:deep(.field-table .el-table__cell) {
  padding: 6px 0;
}

:deep(.field-table .cell) {
  line-height: 1.35;
}

.source-select {
  width: 100%;
}

.source-option {
  display: flex;
  flex-direction: column;
  line-height: 1.35;
}

.source-option span {
  color: #6c786f;
  font-size: 12px;
}

@media (max-width: 1180px) {
  .workbench-grid,
  .capability-grid,
  .slot-choice-grid {
    grid-template-columns: 1fr;
  }

  .output-scope-grid,
  .usage-choice-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .field-choice-row {
    grid-template-columns: minmax(160px, 1fr) auto;
  }

  .field-choice-row small {
    display: none;
  }
}

@media (max-width: 760px) {
  :global(.el-overlay-dialog:has(.extension-capability-dialog)) {
    padding: 6px;
  }

  .extension-form-grid.two,
  .scope-grid,
  .scope-grid.three,
  .output-scope-grid,
  .usage-choice-grid {
    grid-template-columns: 1fr;
  }

  .extension-dialog-scroll {
    padding: 12px;
  }

  :global(.extension-capability-dialog .el-dialog__header),
  :global(.extension-capability-dialog .el-dialog__footer) {
    padding-right: 14px;
    padding-left: 14px;
  }
}
</style>
