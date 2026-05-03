<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { navigateTo } from '#imports'
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
  saveModeCapability,
  deleteModeCapability,
  resolveModeExtensionContract,
} = useCapabilityAccess()

const selectedModeId = ref('')
const activeCapabilityKey = ref('')
const configDialogVisible = ref(false)
const extensionBindDialogVisible = ref(false)
const extensionBindDialogMode = ref('create')
const extensionContract = ref(null)
const extensionContractLoading = ref(false)
const extensionToggleCapability = ref('')
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

const extensionBindForm = reactive({
  capability: '',
  fields: [],
  fieldUsages: {},
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

const OUTPUT_SCOPE_LABELS = {
  daily_panel: '股票日频数据',
  linked_daily_panel: '关联日频数据',
  intraday_panel: '股票盘中数据',
  event_stream: '事件流水数据',
}

const ENTITY_TYPE_LABELS = {
  stock: '股票',
  index: '指数',
  industry: '行业',
  etf: 'ETF',
  fund: '基金',
  custom: '自定义实体',
}

const CORE_CAPABILITIES = new Set([
  'price.daily',
  'benchmark.daily',
  'price.intraday',
  'corporate_action.dividend',
  'tradability.limit_status',
  'universe.tradeable_flags',
  'universe.board_flags',
  'order_constraints.cn_stock',
])

const DEFAULT_EXTENSION_SLOTS_BY_OUTPUT_SCOPE = {
  daily_panel: ['ranking_fields', 'filter_fields', 'weighting_fields', 'report_fields'],
  linked_daily_panel: ['filter_fields', 'groupby_fields', 'neutralization_fields', 'report_fields'],
  intraday_panel: ['signal_fields', 'filter_fields', 'report_fields'],
  event_stream: ['filter_fields', 'ranking_fields', 'report_fields'],
}

const selectedMode = computed(() =>
  modeProfiles.value.find((item) => item.mode_id === selectedModeId.value)
)

const selectedModeSlotRows = computed(() =>
  (selectedMode.value?.extension_slots || [])
    .map(normalizeExtensionSlot)
    .filter((item) => item.slot)
)

const selectedModeSlotMap = computed(() => {
  const rows = new Map()
  for (const item of selectedModeSlotRows.value) {
    rows.set(item.slot, item)
  }
  return rows
})

const modeGroups = computed(() => {
  const groups = [
    { key: 'research_modes', title: '研究模式', modes: [] },
    { key: 'strategy_modes', title: '回测模式', modes: [] },
  ]
  const knownGroups = new Map(groups.map((group) => [group.key, group]))
  for (const mode of modeProfiles.value) {
    const group = knownGroups.get(mode.mode_kind)
    if (group) {
      group.modes.push(mode)
    } else {
      let extraGroup = knownGroups.get('other_modes')
      if (!extraGroup) {
        extraGroup = { key: 'other_modes', title: '其他模式', modes: [] }
        knownGroups.set('other_modes', extraGroup)
        groups.push(extraGroup)
      }
      extraGroup.modes.push(mode)
    }
  }
  return groups.filter((group) => group.modes.length)
})

const modeCapabilityRows = computed(() => {
  const mode = selectedMode.value
  if (!mode) return []
  return [
    ...normalizeModeCapabilities(mode.required_capabilities, 'required'),
    ...normalizeModeCapabilities(mode.conditional_capabilities, 'conditional'),
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
      field_usages: normalizeFieldUsages(item.field_usages),
      output_scope: normalizeOutputScope(item.output_scope),
    })
  }
  return rows
})

const publicExtensionCapabilities = computed(() =>
  capabilityRegistry.value
    .filter((item) => !CORE_CAPABILITIES.has(String(item.capability || '')) && item.enabled !== false)
    .map((item) => {
      const capability = String(item.capability || '').trim()
      const providerBindings = capabilities.value.filter((row) => row.capability === capability)
      const outputScope = firstOutputScope(
        ...providerBindings.map((binding) => binding.output_scope),
        item.output_scope
      )
      const defaultFieldUsages = normalizeFieldUsages(item.field_usages)
      return {
        ...item,
        capability,
        output_scope: outputScope,
        field_usages: defaultFieldUsages,
        fields: extensionCapabilityFields(providerBindings, defaultFieldUsages),
        provider_bindings: providerBindings,
        provider_nodes: providerBindings.map((binding) => binding.provider_node),
      }
    })
)

const selectedModeExtensionBindingMap = computed(() => {
  const rows = new Map()
  for (const item of selectedMode.value?.extension_capability_bindings || []) {
    const capability = String(item.capability || '').trim()
    if (capability) rows.set(capability, item)
  }
  return rows
})

const extensionCapabilityRows = computed(() =>
  publicExtensionCapabilities.value.map((item, index) => {
    const binding = selectedModeExtensionBindingMap.value.get(item.capability) || null
    const bindingFieldUsages = normalizeFieldUsages(binding?.field_usages)
    const boundFields = Object.keys(bindingFieldUsages).length
      ? Object.keys(bindingFieldUsages)
      : (binding?.fields || []).map((field) => String(field || '').trim()).filter(Boolean)
    const publicFields = item.fields.map((field) => field.name)
    const fields = boundFields.length ? boundFields : publicFields
    const fallbackSlots = binding?.allowed_slots || binding?.slots || capabilityDefaultSlots(item)
    const fieldUsages = Object.keys(bindingFieldUsages).length
      ? bindingFieldUsages
      : buildFieldUsageMap(fields, item.field_usages, fallbackSlots)
    const mappedFields = new Set()
    for (const providerBinding of item.provider_bindings || []) {
      for (const field of Object.keys(providerBinding.fields || {})) {
        mappedFields.add(field)
      }
    }
    return {
      ...(binding || {}),
      key: `extension:${item.capability}:${index}`,
      section: 'extension',
      capability: item.capability,
      fields,
      field_usages: fieldUsages,
      slots: fieldUsageSlots(fieldUsages),
      output_scope: item.output_scope,
      default_slots: item.default_slots || [],
      provider_bindings: item.provider_bindings || [],
      provider_nodes: item.provider_nodes || [],
      mapped_fields: [...mappedFields].sort(),
      missing_fields: binding ? fields.filter((field) => !mappedFields.has(field)) : [],
      enabled: Boolean(binding),
    }
  })
)

const selectedExtensionCapabilityRow = computed(() =>
  publicExtensionCapabilities.value.find((item) => item.capability === extensionBindForm.capability) || null
)

const existingExtensionModeBinding = computed(() => {
  const capability = extensionBindForm.capability
  if (!capability || !selectedMode.value) return null
  return (selectedMode.value.extension_capability_bindings || [])
    .find((item) => item.capability === capability) || null
})

const extensionFieldOptions = computed(() => selectedExtensionCapabilityRow.value?.fields || [])

const resolvedExtensionSlots = computed(() => extensionContract.value?.slots || [])

const extensionUsageColumns = computed(() => {
  return selectedModeSlotRows.value
})

const extensionFieldUsageRows = computed(() =>
  extensionFieldOptions.value.map((field) => ({
    ...field,
    usages: _stringList(extensionBindForm.fieldUsages[field.name]?.usages),
    enabled: _stringList(extensionBindForm.fieldUsages[field.name]?.usages).length > 0,
  }))
)

const selectedExtensionFieldCount = computed(() =>
  extensionFieldUsageRows.value.filter((item) => item.enabled).length
)

const resolvedExtensionUseRows = computed(() =>
  resolvedExtensionSlots.value.map((slot) => {
    const definition = selectedModeSlotMap.value.get(slot) || {}
    return {
      slot,
      label: definition.name || SLOT_LABELS[slot] || slot,
      description: definition.description || slotDescription(slot),
    }
  })
)

const modeAcceptsExtensionScope = computed(() =>
  extensionContract.value?.mode_accepts_output_scope ?? true
)

const capabilityStats = computed(() => {
  const rows = dataCapabilityRows.value
  return {
    required: rows.filter((item) => item.section === 'required').length,
    conditional: rows.filter((item) => item.section === 'conditional').length,
    extension: extensionCapabilityRows.value.filter((item) => item.enabled).length,
  }
})

const capabilitySectionRows = computed(() => ({
  required: dataCapabilityRows.value.filter((item) => item.section === 'required'),
  conditional: dataCapabilityRows.value.filter((item) => item.section === 'conditional'),
  extension: extensionCapabilityRows.value,
}))

const visibleCapabilitySections = computed(() =>
  ['required', 'conditional', 'extension'].filter((section) =>
    section === 'extension' || capabilitySectionRows.value[section]?.length
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
    const providerBindings = capabilityProviderBindings(capability, item.provider_node)
    const meta = capabilityMetaMap.value.get(capability)
    const outputScope = firstOutputScope(
      ...providerBindings.map((binding) => binding.output_scope),
      meta?.output_scope
    )
    const modeFieldMap = normalizeFieldMap(item.field_map)
    const hasModeFieldMap = Object.prototype.hasOwnProperty.call(item, 'field_map')
    const mappedFields = new Set(Object.keys(modeFieldMap))
    if (!hasModeFieldMap) {
      const mappingBindings = item.provider_node
        ? providerBindings.filter((binding) => binding.provider_node === item.provider_node)
        : providerBindings
      for (const binding of mappingBindings) {
        for (const field of Object.keys(binding.fields || {})) {
          mappedFields.add(field)
        }
      }
    }
    return {
      ...item,
      key: `${section}:${capability}:${index}`,
      section,
      capability,
      fields,
      slots: item.allowed_slots || item.slots || [],
      provider_node: String(item.provider_node || '').trim(),
      field_map: modeFieldMap,
      has_mode_field_map: hasModeFieldMap,
      output_scope: outputScope,
      provider_bindings: providerBindings,
      mapped_fields: [...mappedFields].sort(),
      missing_fields: fields.filter((field) => !mappedFields.has(field)),
    }
  })
}

function selectCapability(row) {
  activeCapabilityKey.value = row.key

  const binding = providerBindingForRow(row)
  const node = providerNodes.value.find((item) => item.name === binding?.provider_node) || providerOptions.value[0]
  providerForm.nodeName = row.provider_node || binding?.provider_node || node?.name || ''
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
  const useModeFieldMap = Boolean(row?.has_mode_field_map && (!row.provider_node || row.provider_node === providerForm.nodeName))
  const sourceFields = useModeFieldMap ? row.field_map || {} : binding?.fields || {}
  const semanticFields = row.fields?.length ? row.fields : Object.keys(sourceFields)
  fieldRows.value = semanticFields.map((field) => ({
    semantic_field: field,
    source_field: formatFieldValue(
      Object.prototype.hasOwnProperty.call(sourceFields, field)
        ? sourceFields[field]
        : useModeFieldMap
        ? ''
        : field
    ),
  }))
}

function openCapabilityDialog(row) {
  selectCapability(row)
  configDialogVisible.value = true
}

function openCapabilityCard(row) {
  if (row?.section === 'extension') {
    openExtensionBindDialog(row)
    return
  }
  openCapabilityDialog(row)
}

function openExtensionBindDialog(row = null) {
  if (!selectedMode.value) {
    ElMessage.warning('请先选择模式')
    return
  }
  const capability = row?.capability || ''
  if (!capability) {
    ElMessage.warning('请先选择一张扩展能力卡片')
    return
  }
  const hasModeBinding = Boolean(row?.enabled || selectedModeExtensionBindingMap.value.get(capability))
  extensionBindDialogMode.value = hasModeBinding ? 'edit' : 'create'
  extensionBindForm.capability = capability
  applyExtensionBindingDefaults(row)
  extensionBindDialogVisible.value = true
  nextTick(refreshExtensionContract)
}

function applyExtensionBindingDefaults(row = null) {
  const capability = extensionBindForm.capability
  const binding = row?.capability === capability ? row : existingExtensionModeBinding.value
  const capabilityRow = publicExtensionCapabilities.value.find((item) => item.capability === capability)
  const fieldNames = (capabilityRow?.fields || []).map((field) => field.name)
  const bindingFieldUsages = normalizeFieldUsages(binding?.field_usages)
  const fallbackSlots = binding?.slots || binding?.allowed_slots || capabilityDefaultSlots(capabilityRow)
  const nextFieldUsages = Object.keys(bindingFieldUsages).length
    ? bindingFieldUsages
    : buildFieldUsageMap(binding?.fields?.length ? binding.fields : fieldNames, capabilityRow?.field_usages, fallbackSlots)
  extensionBindForm.fields = Object.keys(nextFieldUsages)
  extensionBindForm.fieldUsages = nextFieldUsages
}

function resetExtensionFieldUsagesToDefault() {
  const capabilityRow = selectedExtensionCapabilityRow.value
  const fieldNames = (capabilityRow?.fields || []).map((field) => field.name)
  const nextFieldUsages = buildFieldUsageMap(fieldNames, capabilityRow?.field_usages, capabilityDefaultSlots(capabilityRow))
  extensionBindForm.fields = Object.keys(nextFieldUsages)
  extensionBindForm.fieldUsages = nextFieldUsages
}

function isExtensionFieldEnabled(field) {
  return _stringList(extensionBindForm.fieldUsages[field]?.usages).length > 0
}

function toggleExtensionFieldEnabled(field) {
  if (!field) return
  if (isExtensionFieldEnabled(field)) {
    extensionBindForm.fieldUsages = {
      ...extensionBindForm.fieldUsages,
      [field]: { usages: [] },
    }
  } else {
    const defaults = defaultUsagesForExtensionField(selectedExtensionCapabilityRow.value, field)
    extensionBindForm.fieldUsages = {
      ...extensionBindForm.fieldUsages,
      [field]: { usages: defaults },
    }
  }
  syncExtensionFieldsFromUsages()
}

function isAllExtensionFieldsEnabled() {
  return extensionFieldUsageRows.value.length > 0
    && extensionFieldUsageRows.value.every((field) => field.enabled)
}

function isExtensionFieldsIndeterminate() {
  const enabledCount = extensionFieldUsageRows.value.filter((field) => field.enabled).length
  return enabledCount > 0 && enabledCount < extensionFieldUsageRows.value.length
}

function setAllExtensionFieldsEnabled(checked) {
  const next = { ...extensionBindForm.fieldUsages }
  for (const field of extensionFieldUsageRows.value) {
    const current = _stringList(next[field.name]?.usages)
    next[field.name] = {
      usages: checked
        ? (current.length ? current : defaultUsagesForExtensionField(selectedExtensionCapabilityRow.value, field.name))
        : [],
    }
  }
  extensionBindForm.fieldUsages = next
  syncExtensionFieldsFromUsages()
}

function isExtensionUsageSelected(field, slot) {
  return _stringList(extensionBindForm.fieldUsages[field]?.usages).includes(slot)
}

function toggleExtensionUsage(field, slot) {
  if (!field || !slot) return
  const usages = _stringList(extensionBindForm.fieldUsages[field]?.usages)
  const nextUsages = usages.includes(slot)
    ? usages.filter((item) => item !== slot)
    : [...usages, slot]
  extensionBindForm.fieldUsages = {
    ...extensionBindForm.fieldUsages,
    [field]: { usages: nextUsages },
  }
  syncExtensionFieldsFromUsages()
}

function isExtensionUsageColumnAllSelected(slot) {
  return extensionFieldUsageRows.value.length > 0
    && extensionFieldUsageRows.value.every((field) => isExtensionUsageSelected(field.name, slot))
}

function isExtensionUsageColumnIndeterminate(slot) {
  const selectedCount = extensionFieldUsageRows.value
    .filter((field) => isExtensionUsageSelected(field.name, slot)).length
  return selectedCount > 0 && selectedCount < extensionFieldUsageRows.value.length
}

function setExtensionUsageColumn(slot, checked) {
  if (!slot) return
  const next = { ...extensionBindForm.fieldUsages }
  for (const field of extensionFieldUsageRows.value) {
    const current = _stringList(next[field.name]?.usages)
    const usages = checked
      ? [...new Set([...current, slot])]
      : current.filter((item) => item !== slot)
    next[field.name] = { usages }
  }
  extensionBindForm.fieldUsages = next
  syncExtensionFieldsFromUsages()
}

function syncExtensionFieldsFromUsages() {
  extensionBindForm.fields = Object.entries(extensionBindForm.fieldUsages)
    .filter(([, item]) => _stringList(item?.usages).length)
    .map(([field]) => field)
}

async function handleSaveProviderMapping() {
  if (!selectedCapability.value) return
  if (activeProviderIsVirtual.value) {
    ElMessage.warning('运行时规则不需要绑定 real 表字段')
    return
  }
  if (!selectedModeId.value || !providerForm.nodeName) {
    ElMessage.warning('请先选择模式和数据节点')
    return
  }
  const fields = fieldRows.value
    .map((row) => String(row.semantic_field || '').trim())
    .filter(Boolean)
  const fieldMap = {}
  for (const row of fieldRows.value) {
    const semanticField = String(row.semantic_field || '').trim()
    const sourceField = normalizeSourceFieldValue(row.source_field)
    if (semanticField && sourceField !== undefined) {
      fieldMap[semanticField] = sourceField
    }
  }
  try {
    await saveModeCapability({
      modeId: selectedModeId.value,
      section: selectedCapability.value.section,
      capability: selectedCapability.value.capability,
      fields,
      providerNode: providerForm.nodeName,
      fieldMap,
    })
    ElMessage.success('当前模式字段映射已保存')
    await nextTick()
    const refreshed = modeCapabilityRows.value.find(
      (item) => item.capability === selectedCapability.value.capability && item.section === selectedCapability.value.section
    )
    if (refreshed) selectCapability(refreshed)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存失败')
  }
}

async function refreshExtensionContract() {
  if (!extensionBindDialogVisible.value || !selectedModeId.value || !extensionBindForm.capability) {
    extensionContract.value = null
    return
  }
  const capabilityRow = selectedExtensionCapabilityRow.value
  if (!capabilityRow) {
    extensionContract.value = null
    return
  }
  const currentKey = JSON.stringify({
    modeId: selectedModeId.value,
    capability: extensionBindForm.capability,
    outputScope: capabilityRow.output_scope,
    defaultSlots: capabilityRow.default_slots || [],
    fields: extensionFieldOptions.value.map((field) => field.name),
    defaultFieldUsages: capabilityRow.field_usages || {},
    fieldUsages: extensionBindForm.fieldUsages || {},
    slots: existingExtensionModeBinding.value?.slots || [],
  })
  extensionContractLoading.value = true
  try {
    const result = await resolveModeExtensionContract({
      modeId: selectedModeId.value,
      capability: extensionBindForm.capability,
      outputScope: capabilityRow.output_scope,
      defaultSlots: capabilityRow.default_slots || [],
      fields: extensionFieldOptions.value.map((field) => field.name),
      defaultFieldUsages: capabilityRow.field_usages || {},
      fieldUsages: extensionBindForm.fieldUsages || {},
      slots: existingExtensionModeBinding.value?.slots || [],
    })
    const latestKey = JSON.stringify({
      modeId: selectedModeId.value,
      capability: extensionBindForm.capability,
      outputScope: selectedExtensionCapabilityRow.value?.output_scope,
      defaultSlots: selectedExtensionCapabilityRow.value?.default_slots || [],
      fields: extensionFieldOptions.value.map((field) => field.name),
      defaultFieldUsages: selectedExtensionCapabilityRow.value?.field_usages || {},
      fieldUsages: extensionBindForm.fieldUsages || {},
      slots: existingExtensionModeBinding.value?.slots || [],
    })
    if (latestKey === currentKey) {
      extensionContract.value = result
    }
  } catch (error) {
    extensionContract.value = {
      ok: false,
      mode_accepts_output_scope: false,
      slots: [],
      diagnostics: [{ message: error instanceof Error ? error.message : '模式合约解析失败' }],
    }
  } finally {
    extensionContractLoading.value = false
  }
}

async function handleSaveModeExtensionBinding() {
  if (!selectedModeId.value || !extensionBindForm.capability) return
  if (!selectedExtensionCapabilityRow.value) {
    ElMessage.error('请选择公共扩展能力')
    return
  }
  if (!modeAcceptsExtensionScope.value) {
    ElMessage.error('当前模式不接受这个扩展能力的输出维度')
    return
  }
  const selectedFieldUsages = activeExtensionFieldUsages()
  const selectedSlots = fieldUsageSlots(selectedFieldUsages)
  if (!selectedSlots.length) {
    ElMessage.error('当前模式没有可自动承接这个扩展能力的用途')
    return
  }
  if (extensionFieldOptions.value.length && !Object.keys(selectedFieldUsages).length) {
    ElMessage.error('请至少启用一个字段用途')
    return
  }
  try {
    await saveModeCapability({
      modeId: selectedModeId.value,
      section: 'extension',
      capability: extensionBindForm.capability,
      fieldsText: Object.keys(selectedFieldUsages).join(', '),
      allowedSlots: selectedSlots,
      fieldUsages: selectedFieldUsages,
    })
    extensionBindDialogVisible.value = false
    ElMessage.success(extensionBindDialogMode.value === 'edit' ? '扩展能力用法已更新' : '扩展能力已开启')
    await nextTick()
    const refreshed = modeCapabilityRows.value.find(
      (item) => item.capability === extensionBindForm.capability && item.section === 'extension'
    )
    if (refreshed) selectCapability(refreshed)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存失败')
  }
}

function isExtensionToggleLoading(capability) {
  return extensionToggleCapability.value === capability
}

async function handleToggleModeExtension(row, enabled) {
  if (!selectedModeId.value || !row?.capability) return
  if (enabled) {
    await handleEnableModeExtension(row)
  } else {
    await handleDisableModeExtension(row)
  }
}

async function handleEnableModeExtension(row) {
  if (!row.provider_bindings?.length) {
    ElMessage.error('该扩展能力还没有绑定数据节点，请先到公共能力库维护')
    return
  }
  extensionToggleCapability.value = row.capability
  try {
    const contract = await resolveModeExtensionContract({
      modeId: selectedModeId.value,
      capability: row.capability,
      outputScope: row.output_scope,
      defaultSlots: row.default_slots || [],
      fields: row.fields || [],
      defaultFieldUsages: row.field_usages || {},
      slots: row.slots || [],
    })
    if (!contract?.mode_accepts_output_scope) {
      ElMessage.error('当前模式不接受这个扩展能力的输出维度')
      return
    }
    if (!contract?.slots?.length) {
      ElMessage.error('当前模式没有可自动承接这个扩展能力的用途')
      return
    }
    const fieldUsages = contract.field_usages && Object.keys(contract.field_usages).length
      ? normalizeFieldUsages(contract.field_usages)
      : buildFieldUsageMap(row.fields || [], row.field_usages, contract.slots)
    await saveModeCapability({
      modeId: selectedModeId.value,
      section: 'extension',
      capability: row.capability,
      fieldsText: Object.keys(fieldUsages).join(', '),
      allowedSlots: fieldUsageSlots(fieldUsages),
      fieldUsages,
    })
    ElMessage.success('扩展能力已开启')
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '开启失败')
  } finally {
    extensionToggleCapability.value = ''
  }
}

async function handleDisableModeExtension(row) {
  if (!row.enabled) return
  extensionToggleCapability.value = row.capability
  try {
    await deleteModeCapability({
      modeId: selectedModeId.value,
      section: 'extension',
      capability: row.capability,
    })
    if (extensionBindForm.capability === row.capability) {
      extensionBindDialogVisible.value = false
    }
    ElMessage.success('扩展能力已关闭')
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '关闭失败')
  } finally {
    extensionToggleCapability.value = ''
  }
}

async function handleDeleteModeExtensionBinding() {
  if (!selectedModeId.value || !extensionBindForm.capability) return
  try {
    await ElMessageBox.confirm(
      `确认从当前模式解绑「${capabilityLabel(extensionBindForm.capability)}」？公共扩展能力定义会保留。`,
      '解绑扩展能力',
      {
        confirmButtonText: '解绑',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )
    await deleteModeCapability({
      modeId: selectedModeId.value,
      section: 'extension',
      capability: extensionBindForm.capability,
    })
    extensionBindDialogVisible.value = false
    activeCapabilityKey.value = ''
    ElMessage.success('已从当前模式解绑')
    await nextTick()
    setDefaultCapability()
  } catch (error) {
    if (error === 'cancel' || error === 'close') return
    ElMessage.error(error instanceof Error ? error.message : '解绑失败')
  }
}

function capabilityStatus(row) {
  if (row.section === 'extension' && !row.enabled) {
    if (!row.provider_bindings.length) return { type: 'danger', label: '未绑定数据节点' }
    return { type: 'info', label: '未启用' }
  }
  if (row.section === 'extension' && row.enabled) {
    if (!row.provider_bindings.length) return { type: 'danger', label: '未绑定数据节点' }
    if (row.missing_fields.length) return { type: 'warning', label: `缺 ${row.missing_fields.length} 个字段` }
    return { type: 'success', label: '已启用' }
  }
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
  if (section === 'conditional') return '可选能力'
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
  const label = OUTPUT_SCOPE_LABELS[scope.scope_type] || scope.scope_type
  if (scope.scope_type === 'linked_daily_panel') {
    return `${label}：${entityTypeLabel(scope.base_entity_type)} -> ${entityTypeLabel(scope.linked_entity_type)} -> ${entityTypeLabel(scope.output_entity_type)}`
  }
  return `${label}：${entityTypeLabel(scope.entity_type)}`
}

function entityTypeLabel(value) {
  return ENTITY_TYPE_LABELS[value] || value || '-'
}

function providerQueryProfiles(value) {
  const profiles = value?.query_profiles || value?.access_patterns || []
  return Array.isArray(profiles) ? profiles.map((item) => String(item || '').trim()).filter(Boolean) : []
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

function extensionCapabilityFields(providerBindings, defaultFieldUsages = {}) {
  const rows = []
  const seen = new Set()
  const usages = normalizeFieldUsages(defaultFieldUsages)
  for (const binding of providerBindings || []) {
    for (const [field, source] of Object.entries(binding.fields || {})) {
      if (seen.has(field)) continue
      seen.add(field)
      rows.push({
        name: field,
        source: sourceFieldLabel(source),
        usages: _stringList(usages[field]?.usages),
        ...fieldHelp(field),
      })
    }
  }
  return rows
}

function sourceFieldLabel(value) {
  if (typeof value === 'string') return value
  if (value && typeof value === 'object') {
    if (value.derive) return `派生：${value.derive}`
    if (value.field) return value.field
    return '结构映射'
  }
  return '-'
}

function fieldHelp(field) {
  const [label, description] = FIELD_HELP[field] || [field, '']
  return { label, description }
}

function fieldPreview(row) {
  if (row.section === 'extension') {
    return extensionFieldPreview(row)
  }
  if (row.slots?.length && row.fields?.length) {
    return `${row.fields.map((field) => fieldHelp(field).label).join('、')}｜用途：${row.slots.map(slotLabel).join('、')}`
  }
  if (row.fields?.length) {
    return row.fields.map((field) => fieldHelp(field).label).join('、')
  }
  if (row.slots?.length) {
    return `模式用途：${row.slots.map(slotLabel).join('、')}`
  }
  return '无需固定字段'
}

function extensionFieldPreview(row) {
  if (!row.enabled && row.fields?.length) {
    return `可提供字段：${row.fields.map((field) => fieldHelp(field).label).join('、')}`
  }
  if (row.fields?.length) {
    return `字段：${row.fields.map((field) => fieldHelp(field).label).join('、')}`
  }
  if (row.mapped_fields?.length) {
    return `已映射字段：${row.mapped_fields.map((field) => fieldHelp(field).label).join('、')}`
  }
  return '动态字段或无需固定字段'
}

function extensionUsePreview(row) {
  const labels = (row.slots || []).map(slotLabel).filter(Boolean)
  return labels.length ? labels.join('、') : '未声明'
}

function slotLabel(slot) {
  return SLOT_LABELS[slot] || slot
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

function slotDescription(slot) {
  const descriptions = {
    universe_fields: '用于构建或缩小候选股票池。',
    filter_fields: '用于过滤候选股票。',
    ranking_fields: '用于排序、打分和选股。',
    signal_fields: '用于买卖信号表达式。',
    weighting_fields: '用于目标权重或仓位计算。',
    groupby_fields: '用于行业、主题、板块等分组统计或分组约束。',
    neutralization_fields: '用于行业或风格中性化处理。',
    report_fields: '仅用于结果输出和诊断展示。',
    factor_inputs: '用于研究或因子计算的输入字段。',
  }
  return descriptions[slot] || '当前模式定义的扩展数据使用位置。'
}

function _stringList(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || '').trim()).filter(Boolean)
  }
  if (typeof value === 'string') {
    return value.split(',').map((item) => item.trim()).filter(Boolean)
  }
  return []
}

function normalizeFieldUsages(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  const rows = {}
  for (const [field, item] of Object.entries(value)) {
    const fieldName = String(field || '').trim()
    if (!fieldName) continue
    const usages = Array.isArray(item)
      ? _stringList(item)
      : _stringList(item?.usages || item?.slots || item?.allowed_slots)
    if (usages.length) rows[fieldName] = { usages }
  }
  return rows
}

function normalizeFieldMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  const rows = {}
  for (const [field, source] of Object.entries(value)) {
    const fieldName = String(field || '').trim()
    if (!fieldName || source === undefined || source === null || source === '') continue
    rows[fieldName] = source
  }
  return rows
}

function capabilityProviderBindings(capability, preferredProviderNode = '') {
  const rows = capabilities.value.filter((row) => row.capability === capability)
  const preferred = String(preferredProviderNode || '').trim()
  if (!preferred) return rows
  return [
    ...rows.filter((row) => row.provider_node === preferred),
    ...rows.filter((row) => row.provider_node !== preferred),
  ]
}

function providerBindingForRow(row) {
  const providerNode = String(row?.provider_node || '').trim()
  if (providerNode) {
    return (row.provider_bindings || []).find((binding) => binding.provider_node === providerNode) || row.provider_bindings?.[0]
  }
  return row?.provider_bindings?.[0]
}

function fieldUsageSlots(fieldUsages) {
  const slots = new Set()
  for (const item of Object.values(normalizeFieldUsages(fieldUsages))) {
    for (const slot of _stringList(item.usages)) slots.add(slot)
  }
  return [...slots].sort()
}

function buildFieldUsageMap(fields, defaultFieldUsages = {}, fallbackSlots = []) {
  const defaults = normalizeFieldUsages(defaultFieldUsages)
  const fallback = _stringList(fallbackSlots)
  const allowed = new Set(selectedModeSlotRows.value.map((item) => item.slot))
  const rows = {}
  for (const field of _stringList(fields)) {
    const usages = _stringList(defaults[field]?.usages || fallback).filter((slot) => allowed.has(slot))
    if (usages.length) rows[field] = { usages }
  }
  return rows
}

function capabilityDefaultSlots(capabilityRow) {
  const explicit = _stringList(capabilityRow?.default_slots)
  if (explicit.length) return explicit
  const scopeType = normalizeOutputScope(capabilityRow?.output_scope).scope_type
  return DEFAULT_EXTENSION_SLOTS_BY_OUTPUT_SCOPE[scopeType] || []
}

function defaultUsagesForExtensionField(capabilityRow, field) {
  const defaults = normalizeFieldUsages(capabilityRow?.field_usages)
  const usages = _stringList(defaults[field]?.usages)
  return usages.length
    ? usages.filter((slot) => selectedModeSlotMap.value.has(slot))
    : capabilityDefaultSlots(capabilityRow).filter((slot) => selectedModeSlotMap.value.has(slot))
}

function activeExtensionFieldUsages() {
  const rows = {}
  for (const [field, item] of Object.entries(normalizeFieldUsages(extensionBindForm.fieldUsages))) {
    const usages = _stringList(item.usages).filter((slot) => selectedModeSlotMap.value.has(slot))
    if (usages.length) rows[field] = { usages }
  }
  return rows
}

function formatFieldValue(value) {
  if (value && typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value ?? '')
}

function normalizeSourceFieldValue(value) {
  if (value === null || value === undefined) return undefined
  if (typeof value !== 'string') return value
  const text = value.trim()
  if (!text) return undefined
  if (text.startsWith('{') || text.startsWith('[')) {
    try {
      return JSON.parse(text)
    } catch {
      return text
    }
  }
  return text
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
  extensionBindDialogVisible.value = false
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
  () => extensionBindForm.capability,
  () => {
    if (!extensionBindDialogVisible.value) return
    applyExtensionBindingDefaults()
    refreshExtensionContract()
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
            <p>选择一个业务模式，右侧会联动显示该模式需要的数据能力和字段映射。</p>
          </div>

          <div class="mode-list">
            <section v-for="group in modeGroups" :key="group.key" class="mode-group">
              <div class="mode-group-title">
                <span>{{ group.title }}</span>
                <em>{{ group.modes.length }}</em>
              </div>
              <button
                v-for="mode in group.modes"
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
            </section>
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
            <span><strong>{{ capabilityStats.required }}</strong><em>必需</em></span>
            <span><strong>{{ capabilityStats.conditional }}</strong><em>可选</em></span>
            <span><strong>{{ capabilityStats.extension }}</strong><em>扩展</em></span>
          </div>
        </div>

        <section v-for="section in visibleCapabilitySections" :key="section" class="cap-section">
          <div class="section-title">
            <div>
              <h2>{{ sectionTitle(section) }}</h2>
              <p v-if="section === 'extension'" class="section-subtitle">
                已启用 {{ capabilityStats.extension }} / {{ publicExtensionCapabilities.length }} 个。默认展示全部公共扩展能力；开关只影响当前模式是否启用，公共定义在扩展能力库维护。
              </p>
            </div>
            <div v-if="section === 'extension'" class="section-actions">
              <el-button size="large" plain @click="navigateTo('/extensions')">公共能力库</el-button>
            </div>
          </div>

          <div v-if="capabilitySectionRows[section]?.length" class="capability-grid">
            <div
              v-for="row in capabilitySectionRows[section]"
              :key="row.key"
              role="button"
              tabindex="0"
              class="capability-card"
              :class="{
                active: activeCapabilityKey === row.key,
                'extension-card': row.section === 'extension',
                'extension-enabled': row.section === 'extension' && row.enabled,
                'extension-disabled': row.section === 'extension' && !row.enabled,
              }"
              @click="openCapabilityCard(row)"
              @keydown.enter="openCapabilityCard(row)"
              @keydown.space.prevent="openCapabilityCard(row)"
            >
              <div class="card-head">
                <div>
                  <strong>{{ capabilityLabel(row.capability) }}</strong>
                  <span class="capability-id">{{ row.capability }}</span>
                </div>
                <div class="card-controls">
                  <el-tag :type="capabilityStatus(row).type" size="small">{{ capabilityStatus(row).label }}</el-tag>
                  <el-switch
                    v-if="row.section === 'extension'"
                    :model-value="row.enabled"
                    :loading="isExtensionToggleLoading(row.capability)"
                    :disabled="!row.provider_bindings.length || isExtensionToggleLoading(row.capability)"
                    size="small"
                    inline-prompt
                    active-text="开"
                    inactive-text="关"
                    @click.stop
                    @keydown.stop
                    @change="(value) => handleToggleModeExtension(row, value)"
                  />
                </div>
              </div>
              <p class="card-field-preview" :title="fieldPreview(row)">{{ fieldPreview(row) }}</p>
              <div v-if="row.section === 'extension' && row.enabled" class="extension-use-summary">
                <span>模式用途</span>
                <strong>{{ extensionUsePreview(row) }}</strong>
              </div>
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
                <el-button size="small" plain @click.stop="openExtensionBindDialog(row)">
                  {{ row.enabled ? '编辑用法' : '配置并开启' }}
                </el-button>
              </div>
            </div>
          </div>
          <el-empty
            v-else
            class="section-empty"
            :image-size="68"
            :description="section === 'extension' ? '暂无公共扩展能力，请先到扩展能力库新增' : '暂无能力'"
          />
        </section>
      </main>
    </section>

    <el-dialog
      v-model="configDialogVisible"
      width="min(980px, 92vw)"
      class="capability-dialog"
      align-center
      append-to-body
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
            保存当前模式字段映射
          </el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog
      v-model="extensionBindDialogVisible"
      width="min(920px, 92vw)"
      class="mode-extension-dialog"
      align-center
      append-to-body
      destroy-on-close
    >
      <template #header>
        <div class="dialog-title">
          <div>
            <p>{{ selectedMode?.mode_name }} / 扩展能力</p>
            <h2>{{ extensionBindDialogMode === 'edit' ? '编辑模式用法' : '配置并开启扩展能力' }}</h2>
            <span>公共能力定义在扩展能力库维护，这里只配置当前模式如何使用。</span>
          </div>
        </div>
      </template>

      <section class="extension-bind-body">
        <div v-if="selectedExtensionCapabilityRow" class="extension-public-summary">
          <div>
            <strong>{{ selectedExtensionCapabilityRow.name || selectedExtensionCapabilityRow.capability }}</strong>
            <span>{{ selectedExtensionCapabilityRow.description || '暂无说明' }}</span>
          </div>
          <div class="provider-tags">
            <el-tag size="small" type="info" effect="plain">
              {{ outputScopeLabel(selectedExtensionCapabilityRow.output_scope) }}
            </el-tag>
            <el-tag
              v-for="node in selectedExtensionCapabilityRow.provider_nodes"
              :key="node"
              size="small"
              effect="plain"
            >
              {{ node }}
            </el-tag>
          </div>
        </div>

        <el-alert
          v-if="selectedExtensionCapabilityRow && !modeAcceptsExtensionScope"
          type="warning"
          :closable="false"
          show-icon
          title="当前模式不接受这个扩展能力的输出维度"
        />

        <section class="extension-bind-panel">
          <div class="extension-bind-title">
            <div>
              <strong>字段用途确认</strong>
              <span>每行是一个字段，每列是当前模式可承接的业务用途；默认继承扩展能力库。</span>
            </div>
            <el-button size="small" text type="primary" @click="resetExtensionFieldUsagesToDefault">恢复默认用途</el-button>
          </div>
          <el-skeleton v-if="extensionContractLoading" :rows="2" animated />
          <div
            v-else-if="extensionFieldUsageRows.length && extensionUsageColumns.length"
            class="field-usage-table"
            :style="{ '--usage-column-count': extensionUsageColumns.length }"
          >
            <div class="field-usage-head">
              <span>字段</span>
              <div class="usage-head-cell">
                <el-checkbox
                  :model-value="isAllExtensionFieldsEnabled()"
                  :indeterminate="isExtensionFieldsIndeterminate()"
                  @change="setAllExtensionFieldsEnabled"
                >
                  启用
                </el-checkbox>
              </div>
              <div
                v-for="slot in extensionUsageColumns"
                :key="slot.slot"
                class="usage-head-cell"
                :title="slot.description"
              >
                <el-checkbox
                  :model-value="isExtensionUsageColumnAllSelected(slot.slot)"
                  :indeterminate="isExtensionUsageColumnIndeterminate(slot.slot)"
                  @change="(checked) => setExtensionUsageColumn(slot.slot, checked)"
                >
                  {{ slot.name || slotLabel(slot.slot) }}
                </el-checkbox>
              </div>
            </div>
            <div
              v-for="field in extensionFieldUsageRows"
              :key="field.name"
              class="field-usage-row"
              :class="{ disabled: !field.enabled }"
            >
              <div class="field-usage-name">
                <strong>{{ field.label }}</strong>
                <code>{{ field.name }}</code>
                <small>{{ field.description || '来自公共扩展能力' }}</small>
              </div>
              <el-switch
                :model-value="field.enabled"
                size="small"
                @change="() => toggleExtensionFieldEnabled(field.name)"
              />
              <el-checkbox
                v-for="slot in extensionUsageColumns"
                :key="`${field.name}-${slot.slot}`"
                :model-value="isExtensionUsageSelected(field.name, slot.slot)"
                :disabled="!field.enabled"
                @change="() => toggleExtensionUsage(field.name, slot.slot)"
              />
            </div>
          </div>
          <el-empty v-else description="当前模式没有可自动承接的位置或扩展能力未声明字段" :image-size="64" />
        </section>

        <div v-if="extensionFieldUsageRows.length" class="extension-usage-summary">
          已启用 {{ selectedExtensionFieldCount }} 个字段，{{ fieldUsageSlots(activeExtensionFieldUsages()).length }} 类用途。
        </div>
      </section>

      <template #footer>
        <div class="dialog-actions">
          <el-button @click="extensionBindDialogVisible = false">关闭</el-button>
          <el-button
            v-if="extensionBindDialogMode === 'edit'"
            plain
            type="danger"
            :loading="saving"
            @click="handleDeleteModeExtensionBinding"
          >
            解绑
          </el-button>
          <el-button type="primary" :loading="saving" @click="handleSaveModeExtensionBinding">
            {{ extensionBindDialogMode === 'edit' ? '保存模式用法' : '开启当前模式' }}
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

.card-head .capability-id,
.dialog-title p,
.dialog-title span {
  color: #6c786f;
  font-size: 12px;
}

.mode-list {
  display: flex;
  flex-direction: column;
  gap: 14px;
  padding-top: 14px;
}

.mode-group {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.mode-group-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 0 2px;
}

.mode-group-title span {
  color: #2b4637;
  font-size: 12px;
  font-weight: 700;
}

.mode-group-title em {
  min-width: 22px;
  height: 20px;
  padding: 0 7px;
  border-radius: 999px;
  background: rgba(57, 102, 74, 0.08);
  color: #526259;
  font-size: 11px;
  font-style: normal;
  line-height: 20px;
  text-align: center;
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

.mode-context > div:first-child {
  min-width: 0;
}

.mode-context span,
.inline-stats span {
  color: #6c786f;
  font-size: 12px;
}

.inline-stats {
  display: grid;
  grid-template-columns: repeat(3, 58px);
  gap: 6px;
  flex: 0 0 auto;
  justify-content: flex-end;
}

.inline-stats span {
  display: grid;
  grid-template-columns: 16px 1fr;
  align-items: baseline;
  column-gap: 4px;
  height: 28px;
  padding: 5px 7px;
  border-radius: 8px;
  background: rgba(57, 102, 74, 0.08);
  white-space: nowrap;
}

.inline-stats strong {
  color: #1e3327;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.inline-stats em {
  color: #6c786f;
  font-style: normal;
  text-align: left;
}

h2,
h3 {
  margin: 0;
  color: #1e3327;
}

.section-subtitle {
  margin: 4px 0 0;
  color: #6c786f;
  font-size: 12px;
  line-height: 1.45;
}

.section-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex: 0 0 auto;
}

.cap-section {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

:global(.el-overlay:has(.capability-dialog)),
:global(.el-overlay:has(.mode-extension-dialog)) {
  z-index: 10020 !important;
}

:global(.el-overlay-dialog:has(.capability-dialog)),
:global(.el-overlay-dialog:has(.mode-extension-dialog)) {
  padding: 12px;
  overflow: hidden;
}

:global(.capability-dialog),
:global(.mode-extension-dialog) {
  margin: 0;
  max-height: calc(100vh - 48px);
}

.capability-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 7px;
  justify-content: start;
}

.capability-card {
  display: flex;
  flex-direction: column;
  min-height: 88px;
  padding: 9px 10px;
  border-radius: 8px;
  border: 1px solid rgba(65, 88, 72, 0.14);
  background: rgba(255, 255, 255, 0.9);
  text-align: left;
  cursor: pointer;
  transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
}

.capability-card:hover,
.capability-card.active {
  transform: translateY(-1px);
  border-color: rgba(31, 109, 79, 0.42);
  box-shadow: var(--shadow-md);
}

.capability-card.extension-disabled {
  background: rgba(249, 251, 249, 0.72);
}

.capability-card.extension-disabled p,
.capability-card.extension-disabled .provider-tags {
  opacity: 0.78;
}

.card-head {
  display: flex;
  justify-content: space-between;
  gap: 6px;
  align-items: flex-start;
}

.card-head > div {
  min-width: 0;
}

.card-head strong,
.card-head .capability-id {
  display: block;
}

.card-head strong {
  max-width: 100%;
  overflow: hidden;
  font-size: 13px;
  line-height: 1.25;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.card-head .capability-id {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.card-controls {
  display: flex;
  flex: 0 0 auto;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  min-width: auto;
}

.card-controls :deep(.el-switch) {
  --el-switch-on-color: #187856;
}

.capability-card .card-field-preview {
  display: -webkit-box;
  margin: 6px 0 7px;
  min-height: 28px;
  max-height: 28px;
  overflow: hidden;
  color: #526259;
  font-size: 11px;
  line-height: 1.3;
  text-overflow: ellipsis;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.provider-tags {
  display: flex;
  flex-wrap: nowrap;
  gap: 4px;
  max-width: 100%;
  min-height: 20px;
  max-height: 20px;
  overflow: hidden;
}

.extension-use-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin: auto 0 7px;
  padding: 6px 8px;
  border-radius: 7px;
  background: rgba(57, 102, 74, 0.08);
}

.extension-use-summary span {
  flex: 0 0 auto;
  color: #6c786f;
  font-size: 11px;
}

.extension-use-summary strong {
  min-width: 0;
  overflow: hidden;
  color: #1e3327;
  font-size: 11px;
  font-weight: 700;
  text-align: right;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.capability-card :deep(.el-tag) {
  display: inline-flex;
  flex: 0 1 auto;
  align-items: center;
  justify-content: center;
  max-width: 100%;
  height: 20px;
  padding: 0 6px;
  font-size: 11px;
  line-height: 1;
}

.capability-card :deep(.el-tag__content) {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.extension-card-actions {
  display: flex;
  justify-content: flex-end;
  gap: 5px;
  margin-top: auto;
  padding-top: 6px;
}

.extension-card-actions :deep(.el-button) {
  height: 24px;
  padding: 0 8px;
  font-size: 11px;
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
  max-height: min(500px, calc(100vh - 260px));
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 2px 2px 10px;
}

.node-form {
  max-width: 520px;
}

.extension-bind-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
  max-height: min(500px, calc(100vh - 260px));
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 2px 2px 10px;
}

.extension-public-summary {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(248, 251, 248, 0.9);
}

.extension-public-summary strong,
.extension-public-summary span {
  display: block;
}

.extension-public-summary strong {
  color: #1e3327;
  font-size: 14px;
}

.extension-public-summary span {
  margin-top: 4px;
  color: #526259;
  font-size: 12px;
  line-height: 1.45;
}

.extension-bind-panel {
  padding: 12px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.86);
}

.extension-bind-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}

.extension-bind-title strong,
.extension-bind-title span {
  display: block;
}

.extension-bind-title strong {
  color: #1e3327;
  font-size: 13px;
}

.extension-bind-title span {
  margin-top: 4px;
  color: #526259;
  font-size: 12px;
  line-height: 1.45;
}

.field-usage-table {
  max-width: 100%;
  overflow-x: auto;
  overflow-y: hidden;
  scrollbar-gutter: stable;
  -webkit-overflow-scrolling: touch;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
}

.field-usage-head,
.field-usage-row {
  display: grid;
  grid-template-columns: 220px 58px repeat(var(--usage-column-count, 8), 104px);
  align-items: center;
  width: max-content;
  min-width: 100%;
}

.field-usage-head {
  min-height: 28px;
  background: rgba(57, 102, 74, 0.08);
}

.field-usage-head > span,
.field-usage-head > .usage-head-cell {
  padding: 6px 8px;
  color: #344c3d;
  font-size: 10.5px;
  font-weight: 800;
  text-align: center;
}

.field-usage-head > span:first-child {
  position: sticky;
  left: 0;
  z-index: 3;
  background: #edf5ef;
  box-shadow: 1px 0 0 rgba(65, 88, 72, 0.12);
  text-align: left;
}

.usage-head-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  min-width: 0;
}

.usage-head-cell :deep(.el-checkbox) {
  max-width: 100%;
  height: 18px;
}

.usage-head-cell :deep(.el-checkbox__label) {
  min-width: 0;
  overflow: hidden;
  padding-left: 4px;
  color: #344c3d;
  font-size: 10.5px;
  font-weight: 800;
  line-height: 1.1;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.usage-head-cell :deep(.el-checkbox__inner) {
  width: 12px;
  height: 12px;
}

.field-usage-row {
  min-height: 42px;
  border-top: 1px solid rgba(65, 88, 72, 0.1);
  background: rgba(255, 255, 255, 0.92);
}

.field-usage-row.disabled {
  background: rgba(249, 251, 249, 0.72);
  opacity: 0.72;
}

.field-usage-row > * {
  justify-self: center;
}

.field-usage-name {
  display: grid;
  position: sticky;
  left: 0;
  z-index: 2;
  grid-template-columns: minmax(0, auto) minmax(0, 1fr);
  column-gap: 6px;
  row-gap: 1px;
  align-items: baseline;
  justify-self: stretch;
  min-width: 0;
  padding: 5px 8px;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 1px 0 0 rgba(65, 88, 72, 0.1);
}

.field-usage-row.disabled .field-usage-name {
  background: rgba(249, 251, 249, 0.98);
}

.field-usage-name strong,
.field-usage-name code,
.field-usage-name small {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-usage-name strong {
  color: #1e3327;
  font-size: 12px;
}

.field-usage-name code {
  margin-top: 0;
  color: #187856;
  font-size: 10.5px;
}

.field-usage-name small {
  grid-column: 1 / -1;
  margin-top: 0;
  color: #526259;
  font-size: 10px;
}

.extension-usage-summary {
  color: #526259;
  font-size: 12px;
  text-align: right;
}

.field-usage-table :deep(.el-checkbox) {
  height: 22px;
}

.field-usage-table :deep(.el-checkbox__inner) {
  width: 13px;
  height: 13px;
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
  .workbench-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 760px) {
  .capability-grid {
    grid-template-columns: 1fr;
  }

  .section-title,
  .section-actions,
  .extension-public-summary,
  .extension-bind-title {
    align-items: stretch;
    flex-direction: column;
  }

  .field-usage-table {
    margin-right: -4px;
  }
}
</style>
