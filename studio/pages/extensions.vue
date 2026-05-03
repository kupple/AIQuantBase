<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
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
  loadWorkspace,
  saveProviderMapping,
  setCapabilityEnabled,
  deleteModeCapability,
} = useCapabilityAccess()

const selectedCapability = ref('')
const searchText = ref('')
const extensionDialogVisible = ref(false)
const extensionDialogMode = ref('create')
const editingCapability = ref('')
const suppressFieldReset = ref(false)
const capabilityToggleLoading = ref('')

const extensionForm = reactive({
  capability: '',
  capabilityName: '',
  capabilityDescription: '',
  nodeName: '',
  fields: [],
  outputScope: defaultOutputScope(),
})

const FIELD_HELP = {
  market_cap: ['总市值', '总股本市值'],
  float_market_cap: ['流通市值', '流通股本市值'],
  turnover_rate: ['换手率', '成交活跃度指标'],
  industry_code: ['行业代码', '行业分类代码'],
  industry_index_code: ['行业指数代码', '行业指数代码'],
  industry_index_name: ['行业指数名称', '行业指数名称'],
  industry_level1_name: ['一级行业', '一级行业名称'],
  industry_level2_name: ['二级行业', '二级行业名称'],
  industry_level3_name: ['三级行业', '三级行业名称'],
  industry_name: ['行业名称', '股票所属行业名称'],
  open: ['开盘价', '交易日开盘价格'],
  high: ['最高价', '交易日最高价格'],
  low: ['最低价', '交易日最低价格'],
  close: ['收盘价', '交易日收盘价格'],
  close_adj: ['后复权收盘价', '用于因子和收益计算的复权价格'],
  volume: ['成交量', '成交股数'],
  amount: ['成交额', '成交金额'],
  buy_amount: ['买入额', '龙虎榜买入金额'],
  sell_amount: ['卖出额', '龙虎榜卖出金额'],
  total_amount: ['总成交额', '事件总成交金额'],
  total_volume: ['总成交量', '事件总成交量'],
  reason_type: ['上榜原因', '龙虎榜上榜原因代码'],
  reason_type_name: ['上榜原因名称', '龙虎榜上榜原因名称'],
  change_range: ['涨跌幅', '事件当日涨跌幅'],
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

const SLOT_LABELS = {
  universe_fields: '候选池',
  ranking_fields: '排序选股',
  filter_fields: '过滤条件',
  groupby_fields: '分组约束',
  neutralization_fields: '中性化',
  signal_fields: '买卖信号',
  weighting_fields: '仓位权重',
  report_fields: '报告展示',
  factor_inputs: '因子输入',
  risk_fields: '风控判断',
}

const OUTPUT_SCOPE_OPTIONS = [
  {
    value: 'daily_panel',
    label: '股票日频数据',
    badge: '日',
    summary: '每只股票每天一行',
  },
  {
    value: 'linked_daily_panel',
    label: '关联日频数据',
    badge: '联',
    summary: '行业/指数等数据映射回股票',
  },
  {
    value: 'intraday_panel',
    label: '股票盘中数据',
    badge: '分',
    summary: '每只股票每个分钟一行',
  },
  {
    value: 'event_stream',
    label: '事件流水数据',
    badge: '事',
    summary: '按事件发生时间记录',
  },
]

const ENTITY_TYPE_OPTIONS = [
  { value: 'stock', label: '股票' },
  { value: 'index', label: '指数' },
  { value: 'industry', label: '行业' },
  { value: 'etf', label: 'ETF' },
  { value: 'fund', label: '基金' },
  { value: 'custom', label: '自定义实体' },
]

const workspaceReady = computed(() => Boolean(workspacePayload.value))

const extensionCapabilities = computed(() =>
  capabilityRegistry.value
    .filter((item) => !CORE_CAPABILITIES.has(String(item.capability || '')))
    .map((item) => ({
      ...item,
      enabled: item.enabled !== false,
      fields: capabilityFields(item.capability),
      provider_nodes: capabilityProviderNodes(item.capability),
      provider_bindings: capabilityProviderBindings(item.capability),
      mode_bindings: modeBindingsForCapability(item.capability),
    }))
)

const filteredCapabilities = computed(() => {
  const keyword = searchText.value.trim().toLowerCase()
  if (!keyword) return extensionCapabilities.value
  return extensionCapabilities.value.filter((item) => {
    const haystack = [
      item.capability,
      item.name,
      item.description,
      item.enabled === false ? '已关闭' : '已开启',
      ...item.fields.map((field) => field.name),
      ...item.fields.map((field) => field.label),
    ].join(' ').toLowerCase()
    return haystack.includes(keyword)
  })
})

const selectedCapabilityRow = computed(() =>
  extensionCapabilities.value.find((item) => item.capability === selectedCapability.value)
  || extensionCapabilities.value[0]
  || null
)

const selectedModeBindingRows = computed(() =>
  selectedCapabilityRow.value?.mode_bindings || []
)

const extensionNodeOptions = computed(() =>
  providerNodes.value.filter((item) => !providerQueryProfiles(item).includes('virtual_runtime_rule'))
)

const selectedExtensionNode = computed(() =>
  providerNodes.value.find((item) => item.name === extensionForm.nodeName)
)

const selectedFormRegistryEntry = computed(() => {
  const capability = normalizeCapabilityIdForLookup(extensionForm.capability)
  return capabilityRegistry.value.find((item) => item.capability === capability) || null
})

const selectedFormProviderBinding = computed(() => {
  const capability = normalizeCapabilityIdForLookup(extensionForm.capability)
  return capabilities.value.find(
    (item) => item.capability === capability && item.provider_node === extensionForm.nodeName
  ) || null
})

const extensionFieldOptions = computed(() => {
  const options = new Map()
  const addOption = (value) => {
    const text = formatFieldValue(value).trim()
    if (!text || options.has(text)) return
    const help = fieldHelp(text)
    options.set(text, {
      value: text,
      label: help.label === text ? text : `${text}｜${help.label}`,
      description: help.description || '来自所选数据节点',
    })
  }

  for (const value of selectedExtensionNode.value?.source_fields || []) addOption(value)
  for (const [semanticField, sourceField] of Object.entries(selectedFormProviderBinding.value?.fields || {})) {
    addOption(semanticField)
    addOption(sourceField)
  }
  for (const value of extensionForm.fields) addOption(value)
  return [...options.values()]
})

const scopeKeyFieldOptions = computed(() => buildScopeKeyFieldOptions(selectedExtensionNode.value))

const extensionDialogTitle = computed(() => {
  if (extensionDialogMode.value === 'edit') return '编辑扩展能力'
  return '新增扩展能力'
})

onMounted(async () => {
  try {
    await loadWorkspace()
    selectedCapability.value = extensionCapabilities.value[0]?.capability || ''
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '扩展能力工作区加载失败')
  }
})

watch(
  () => extensionForm.nodeName,
  () => {
    if (suppressFieldReset.value) return
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

function capabilityFields(capability) {
  const rows = []
  const seen = new Set()
  for (const binding of capabilityProviderBindings(capability)) {
    const fields = binding.fields || {}
    for (const [field, source] of Object.entries(fields)) {
      if (seen.has(field)) continue
      seen.add(field)
      rows.push({
        name: field,
        source: sourceFieldLabel(source),
        ...fieldHelp(field),
      })
    }
  }
  return rows
}

function capabilityProviderBindings(capability) {
  return capabilities.value.filter((item) => item.capability === capability)
}

function capabilityProviderNodes(capability) {
  return capabilityProviderBindings(capability).map((item) => item.provider_node)
}

function modeBindingsForCapability(capability) {
  if (!capability) return []
  const rows = []
  for (const mode of modeProfiles.value || []) {
    const binding = (mode.extension_capability_bindings || [])
      .find((item) => item.capability === capability)
    if (!binding) continue
    rows.push({
      mode_id: mode.mode_id,
      mode_name: mode.mode_name,
      mode_kind: mode.mode_kind,
      fields: binding.fields || [],
      slots: binding.slots || binding.allowed_slots || [],
    })
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
  const fallback = [field, '来自扩展能力的数据字段']
  const [label, description] = FIELD_HELP[field] || fallback
  return { label, description }
}

function stringList(value) {
  if (Array.isArray(value)) return value.map((item) => String(item || '').trim()).filter(Boolean)
  if (typeof value === 'string') return value.split(',').map((item) => item.trim()).filter(Boolean)
  return []
}

function outputScopeLabel(scope) {
  const value = normalizeOutputScope(scope)
  const type = value.scope_type || '未设置'
  if (type === 'daily_panel') return `股票日频：${value.entity_type || 'stock'}`
  if (type === 'linked_daily_panel') return `关联日频：${value.base_entity_type || '-'} -> ${value.linked_entity_type || '-'} -> ${value.output_entity_type || '-'}`
  if (type === 'intraday_panel') return `分钟面板：${value.entity_type || 'stock'}`
  if (type === 'event_stream') return `事件流：${value.entity_type || 'stock'}`
  return type
}

function capabilityLabel(capability) {
  const meta = capabilityRegistry.value.find((item) => item.capability === capability)
  return meta?.name || capability
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

function setOutputScopeType(scopeType) {
  if (!scopeType) return
  extensionForm.outputScope = applyNodeKeyDefaults({
    ...extensionForm.outputScope,
    scope_type: scopeType,
  }, selectedExtensionNode.value)
}

function isOutputScopeSelected(scopeType) {
  return extensionForm.outputScope.scope_type === scopeType
}

function normalizeExtensionSlot(item) {
  if (typeof item === 'string') {
    return {
      slot: item,
      name: SLOT_LABELS[item] || item,
      description: slotDescription(item),
    }
  }
  const slot = String(item?.slot || item?.id || item?.name || '').trim()
  if (!slot) return { slot: '' }
  return {
    ...item,
    slot,
    name: item?.name || SLOT_LABELS[slot] || slot,
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
  }
  return descriptions[slot] || '当前模式定义的扩展数据使用位置。'
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

function normalizeCapabilityIdForLookup(value) {
  return String(value || '').trim()
}

function normalizeExtensionCapabilityId(value) {
  const text = String(value || '').trim()
  if (!/^[a-z][a-z0-9_]*$/.test(text)) return ''
  return text
}

function formatFieldValue(value) {
  if (value && typeof value === 'object') return JSON.stringify(value)
  return String(value ?? '')
}

function resetExtensionForm() {
  extensionForm.capability = ''
  extensionForm.capabilityName = ''
  extensionForm.capabilityDescription = ''
  extensionForm.nodeName = ''
  extensionForm.fields = []
  extensionForm.outputScope = defaultOutputScope()
}

function openCreateDialog() {
  extensionDialogMode.value = 'create'
  editingCapability.value = ''
  resetExtensionForm()
  extensionDialogVisible.value = true
}

function openEditDialog(row = selectedCapabilityRow.value) {
  if (!row) return
  extensionDialogMode.value = 'edit'
  editingCapability.value = row.capability
  suppressFieldReset.value = true
  extensionForm.capability = row.capability
  extensionForm.capabilityName = row.name || capabilityLabel(row.capability)
  extensionForm.capabilityDescription = row.description || ''
  extensionForm.nodeName = row.provider_bindings[0]?.provider_node || ''
  extensionForm.fields = row.fields.map((field) => field.name)
  extensionForm.outputScope = cloneOutputScope(row.output_scope)
  extensionDialogVisible.value = true
  setTimeout(() => {
    suppressFieldReset.value = false
  })
}

function isFieldSelected(field) {
  return extensionForm.fields.includes(field)
}

function toggleField(field) {
  if (!field) return
  if (isFieldSelected(field)) {
    extensionForm.fields = extensionForm.fields.filter((item) => item !== field)
  } else {
    extensionForm.fields = [...extensionForm.fields, field]
  }
}

function selectAllFields() {
  extensionForm.fields = extensionFieldOptions.value.map((item) => item.value)
}

function clearFields() {
  extensionForm.fields = []
}

async function handleSaveExtensionCapability() {
  const capability = normalizeExtensionCapabilityId(extensionForm.capability)
  if (!capability) {
    ElMessage.error('能力英文 ID 只能使用小写英文、数字和下划线，例如 sentiment_news_daily')
    return
  }
  if (!extensionForm.nodeName) {
    ElMessage.error('请选择数据节点')
    return
  }
  if (!extensionForm.fields.length) {
    ElMessage.error('请选择至少一个策略可用字段')
    return
  }

  const selectedNode = providerNodes.value.find((item) => item.name === extensionForm.nodeName)
  const capabilityName = String(extensionForm.capabilityName || '').trim() || capability
  const capabilityDescription = String(extensionForm.capabilityDescription || '').trim()
  try {
    await saveProviderMapping(
      {
        nodeName: extensionForm.nodeName,
        capability,
        capabilityName,
        capabilityDescription,
        outputScope: extensionForm.outputScope,
        keys: selectedNode?.keys || {},
        assetTypes: (selectedNode?.asset_types || ['stock']).join(', '),
        queryProfiles: providerQueryProfiles(selectedNode).join(', ') || 'panel_time_series',
        defaultSlots: [],
        fieldUsages: {},
        clearDefaultSlots: true,
        clearFieldUsages: true,
        replaceProviderNodes: true,
        replaceFields: true,
      },
      extensionForm.fields.map((field) => ({
        semantic_field: field,
        source_field: field,
      }))
    )
    selectedCapability.value = capability
    extensionDialogVisible.value = false
    ElMessage.success(extensionDialogMode.value === 'create' ? '扩展能力已添加' : '扩展能力已保存')
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '保存失败')
  }
}

function isCapabilityToggleLoading(row) {
  return capabilityToggleLoading.value === row?.capability
}

async function handleToggleGlobalCapability(row, enabled) {
  if (!row?.capability) return
  capabilityToggleLoading.value = row.capability
  try {
    await setCapabilityEnabled({
      capability: row.capability,
      enabled,
    })
    ElMessage.success(enabled ? '扩展能力已开启' : '扩展能力已关闭')
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '开关更新失败')
  } finally {
    capabilityToggleLoading.value = ''
  }
}

async function handleDeleteGlobalCapability(row = selectedCapabilityRow.value) {
  if (!row?.capability) return
  if (!modeProfiles.value.length) {
    ElMessage.error('当前没有可用模式配置，无法执行全局删除')
    return
  }
  try {
    await ElMessageBox.confirm(
      `确认彻底删除公共扩展能力「${row.name || row.capability}」？这会删除所有模式绑定，并移除全局注册和字段映射。`,
      '删除公共扩展能力',
      {
        confirmButtonText: '彻底删除',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )
    await deleteModeCapability({
      modeId: selectedModeBindingRows.value[0]?.mode_id || modeProfiles.value[0].mode_id,
      section: 'extension',
      capability: row.capability,
      deleteProviderRegistration: true,
    })
    selectedCapability.value = extensionCapabilities.value[0]?.capability || ''
    ElMessage.success('扩展能力已删除')
  } catch (error) {
    if (error === 'cancel' || error === 'close') return
    ElMessage.error(error instanceof Error ? error.message : '删除失败')
  }
}
</script>

<template>
  <section class="extensions-page">
    <header class="extensions-header">
      <div class="header-actions">
        <el-input
          v-model="searchText"
          clearable
          class="extension-search"
          placeholder="搜索能力或字段"
        />
        <el-button type="primary" @click="openCreateDialog">新增扩展能力</el-button>
      </div>
    </header>

    <el-skeleton v-if="loading && !workspaceReady" :rows="8" animated />

    <section v-else class="extensions-layout">
      <aside class="extension-list-panel">
        <div class="panel-title">
          <strong>扩展能力</strong>
          <span>{{ filteredCapabilities.length }}</span>
        </div>
        <div
          v-for="item in filteredCapabilities"
          :key="item.capability"
          role="button"
          tabindex="0"
          class="extension-list-item"
          :class="{
            active: selectedCapabilityRow?.capability === item.capability,
            disabled: !item.enabled,
          }"
          @click="selectedCapability = item.capability"
          @keydown.enter="selectedCapability = item.capability"
          @keydown.space.prevent="selectedCapability = item.capability"
        >
          <strong>{{ item.name || item.capability }}</strong>
          <span class="extension-list-id">{{ item.capability }}</span>
          <em>{{ outputScopeLabel(item.output_scope) }}</em>
          <small>{{ item.enabled ? '已开启' : '已关闭' }} · {{ item.mode_bindings.length ? `已被 ${item.mode_bindings.length} 个模式使用` : '未被模式使用' }}</small>
        </div>
        <el-empty v-if="!filteredCapabilities.length" description="没有匹配的扩展能力" :image-size="64" />
      </aside>

      <main v-if="selectedCapabilityRow" class="extension-detail">
        <section class="detail-card hero-card">
          <div class="hero-main">
            <div class="hero-title-row">
              <div class="hero-title-block">
                <p>公共能力定义</p>
                <h2>{{ selectedCapabilityRow.name || selectedCapabilityRow.capability }}</h2>
              </div>
              <el-tag
                class="hero-status-tag"
                :type="selectedCapabilityRow.enabled ? 'success' : 'info'"
                effect="plain"
              >
                {{ selectedCapabilityRow.enabled ? '已开启' : '已关闭' }}
              </el-tag>
            </div>
            <span class="hero-description">{{ selectedCapabilityRow.description || '暂无说明' }}</span>

            <div class="hero-meta-grid">
              <div class="hero-meta-item">
                <span>Capability ID</span>
                <code>{{ selectedCapabilityRow.capability }}</code>
              </div>
              <div class="hero-meta-item">
                <span>输出维度</span>
                <strong>{{ outputScopeLabel(selectedCapabilityRow.output_scope) }}</strong>
              </div>
              <div class="hero-meta-item hero-meta-wide">
                <span>Provider 节点</span>
                <div class="provider-list hero-provider-list">
                  <el-tag
                    v-for="node in selectedCapabilityRow.provider_nodes"
                    :key="node"
                    effect="plain"
                  >
                    {{ node }}
                  </el-tag>
                  <em v-if="!selectedCapabilityRow.provider_nodes.length">未绑定数据节点</em>
                </div>
              </div>
            </div>
          </div>

          <div class="hero-side">
            <div class="hero-enable-panel">
              <div>
                <strong>全局开关</strong>
                <span>关闭后不在模式工作台展示或开启，公共定义仍会保留。</span>
              </div>
              <el-switch
                :model-value="selectedCapabilityRow.enabled"
                :loading="isCapabilityToggleLoading(selectedCapabilityRow)"
                :disabled="isCapabilityToggleLoading(selectedCapabilityRow)"
                inline-prompt
                active-text="开"
                inactive-text="关"
                @change="(value) => handleToggleGlobalCapability(selectedCapabilityRow, value)"
              />
            </div>
            <div class="hero-actions">
              <el-button plain @click="openEditDialog(selectedCapabilityRow)">编辑公共定义</el-button>
              <el-button plain type="danger" @click="handleDeleteGlobalCapability(selectedCapabilityRow)">
                删除
              </el-button>
            </div>
          </div>
        </section>

        <section class="detail-grid">
          <div class="detail-card">
            <div class="section-title">
              <div>
                <p>数据字段</p>
                <h3>能力可提供字段</h3>
              </div>
              <span>{{ selectedCapabilityRow.fields.length }} 个字段</span>
            </div>
            <div v-if="selectedCapabilityRow.fields.length" class="field-table">
              <div v-for="field in selectedCapabilityRow.fields" :key="field.name" class="field-row">
                <div>
                  <strong>{{ field.label }}</strong>
                  <code>{{ field.name }}</code>
                </div>
                <span>{{ field.description }}</span>
                <em>{{ field.source }}</em>
              </div>
            </div>
            <el-empty v-else description="该能力允许动态字段或暂未声明字段" :image-size="64" />
          </div>
        </section>

        <section class="detail-card mode-binding-card">
            <div class="section-title">
              <div>
                <p>使用情况</p>
                <h3>已接入模式</h3>
              </div>
            </div>

          <div v-if="selectedModeBindingRows.length" class="binding-layout">
            <div v-for="binding in selectedModeBindingRows" :key="binding.mode_id" class="mode-use-row">
              <div>
                <h4>{{ binding.mode_name }}</h4>
                <span class="mode-id">{{ binding.mode_id }}</span>
              </div>
              <div class="field-chip-list">
                <el-tag v-for="slot in binding.slots" :key="slot" effect="plain">
                  {{ SLOT_LABELS[slot] || slot }}
                </el-tag>
                <el-tag v-if="!binding.slots.length" effect="plain">未声明用途</el-tag>
              </div>
            </div>
          </div>

          <el-empty
            v-else
            description="还没有模式接入这个扩展能力"
            :image-size="72"
          />
        </section>
      </main>
    </section>

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
              <el-form-item label="能力英文 ID">
                <el-input
                  v-model="extensionForm.capability"
                  clearable
                  :disabled="extensionDialogMode !== 'create'"
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
                  <el-select v-model="extensionForm.outputScope.base_entity_type" popper-class="extension-capability-popper">
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
                  <el-select v-model="extensionForm.outputScope.linked_entity_type" popper-class="extension-capability-popper">
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
                  <el-select v-model="extensionForm.outputScope.output_entity_type" popper-class="extension-capability-popper">
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
                  <el-select v-model="extensionForm.outputScope.entity_type" popper-class="extension-capability-popper">
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
              <h3>能力可用字段</h3>
            </div>
            <div class="field-picker-toolbar">
              <span>已选择 {{ extensionForm.fields.length }} / {{ extensionFieldOptions.length }}</span>
              <div>
                <el-button size="small" text type="primary" @click="selectAllFields">全选</el-button>
                <el-button size="small" text @click="clearFields">清空</el-button>
              </div>
            </div>
            <div v-if="extensionFieldOptions.length" class="field-choice-list">
              <button
                v-for="option in extensionFieldOptions"
                :key="option.value"
                type="button"
                class="field-choice-row"
                :class="{ active: isFieldSelected(option.value) }"
                @click="toggleField(option.value)"
              >
                <span class="field-choice-main">
                  <strong>{{ fieldHelp(option.value).label }}</strong>
                  <code>{{ option.value }}</code>
                </span>
                <small>{{ option.description || fieldHelp(option.value).description || '来自所选数据节点' }}</small>
                <em>{{ isFieldSelected(option.value) ? '已选' : '选择' }}</em>
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
            {{ extensionDialogMode === 'create' ? '添加公共能力' : '保存公共能力' }}
          </el-button>
        </div>
      </template>
    </el-dialog>
  </section>
</template>

<style scoped>
.extensions-page {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.extensions-header {
  display: flex;
  align-items: flex-end;
  justify-content: flex-end;
  gap: 16px;
  padding: 0;
}

.section-title p,
.hero-card p {
  margin: 0 0 4px;
  color: #5e7165;
  font-size: 12px;
  font-weight: 700;
}

.hero-card h2,
.section-title h3 {
  margin: 0;
  color: #17261d;
  letter-spacing: 0;
}

.hero-description {
  display: block;
  margin-top: 8px;
  color: #5e7165;
  font-size: 13px;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.extension-search {
  width: 300px;
}

.extensions-layout {
  display: grid;
  grid-template-columns: 320px minmax(0, 1fr);
  gap: 14px;
  align-items: start;
}

.extension-list-panel,
.detail-card {
  border: 1px solid rgba(49, 87, 65, 0.1);
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 10px 30px rgba(38, 68, 49, 0.07);
}

.extension-list-panel {
  padding: 12px;
}

.panel-title,
.section-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.panel-title strong {
  color: #17261d;
}

.panel-title span,
.section-title > span {
  color: #5e7165;
  font-size: 12px;
}

.extension-list-item {
  display: block;
  width: 100%;
  margin-bottom: 8px;
  padding: 10px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(249, 251, 249, 0.9);
  text-align: left;
  cursor: pointer;
}

.extension-list-item.active,
.extension-list-item:hover {
  border-color: rgba(23, 128, 91, 0.42);
  background: rgba(237, 248, 242, 0.96);
}

.extension-list-item.disabled {
  background: rgba(249, 251, 249, 0.7);
}

.extension-list-item.disabled strong,
.extension-list-item.disabled .extension-list-id,
.extension-list-item.disabled em {
  opacity: 0.68;
}

.extension-list-item strong,
.extension-list-item .extension-list-id,
.extension-list-item em,
.extension-list-item small {
  display: block;
}

.extension-list-item strong {
  color: #1e3327;
  font-size: 14px;
}

.extension-list-item .extension-list-id {
  margin-top: 2px;
  color: #187856;
  font-size: 12px;
}

.extension-list-item em,
.extension-list-item small {
  margin-top: 6px;
  color: #5e7165;
  font-size: 11px;
  font-style: normal;
}

.extension-detail {
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-width: 0;
}

.detail-card {
  padding: 16px;
}

.hero-card {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}

.hero-main {
  flex: 1 1 auto;
  min-width: 0;
}

.hero-title-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.hero-title-block {
  min-width: 0;
}

.hero-title-block h2 {
  overflow-wrap: anywhere;
}

.hero-status-tag {
  flex: 0 0 auto;
  margin-top: 1px;
}

.hero-meta-grid {
  display: grid;
  grid-template-columns: minmax(160px, 0.85fr) minmax(220px, 1.1fr) minmax(220px, 1fr);
  gap: 10px;
  margin-top: 14px;
  padding-top: 12px;
  border-top: 1px solid rgba(49, 87, 65, 0.1);
}

.hero-meta-item {
  min-width: 0;
  padding: 10px 12px;
  border-radius: 8px;
  background: rgba(247, 250, 247, 0.86);
}

.hero-meta-item > span,
.hero-meta-item strong,
.hero-meta-item code {
  display: block;
}

.hero-meta-item > span {
  margin-bottom: 6px;
  color: #344c3d;
  font-size: 11px;
  font-weight: 700;
}

.hero-meta-item strong,
.hero-meta-item code {
  overflow: hidden;
  color: #1e3327;
  font-size: 12px;
  line-height: 1.35;
  text-overflow: ellipsis;
}

.hero-meta-item strong {
  overflow-wrap: anywhere;
}

.hero-meta-item code {
  color: #187856;
  white-space: nowrap;
}

.hero-provider-list {
  align-items: center;
  min-height: 22px;
}

.hero-provider-list em {
  color: #7a897f;
  font-size: 12px;
  font-style: normal;
}

.hero-side {
  display: flex;
  flex: 0 0 340px;
  flex-direction: column;
  align-items: flex-end;
  gap: 10px;
}

.hero-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.hero-enable-panel {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  width: min(360px, 100%);
  padding: 10px 12px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(248, 251, 248, 0.92);
}

.hero-enable-panel > div strong,
.hero-enable-panel > div span {
  display: block;
}

.hero-enable-panel > div strong {
  color: #1e3327;
  font-size: 13px;
}

.hero-enable-panel > div span {
  margin-top: 2px;
  color: #6c786f;
  font-size: 11px;
  line-height: 1.35;
}

.hero-status-tag,
.hero-provider-list :deep(.el-tag) {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin-top: 0;
  line-height: 1;
}

.detail-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 14px;
}

.field-table {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 420px;
  overflow-y: auto;
  padding-right: 4px;
}

.field-row {
  display: grid;
  grid-template-columns: minmax(160px, 0.8fr) minmax(180px, 1fr) minmax(120px, 0.8fr);
  gap: 10px;
  align-items: center;
  padding: 9px 10px;
  border-radius: 8px;
  background: rgba(247, 250, 247, 0.9);
}

.field-row strong,
.field-row code,
.field-row span,
.field-row em {
  display: block;
}

.field-row strong {
  color: #1e3327;
  font-size: 13px;
}

.field-row code {
  margin-top: 2px;
  color: #187856;
  font-size: 11px;
}

.field-row span,
.field-row em {
  color: #5e7165;
  font-size: 12px;
  font-style: normal;
}

.provider-list,
.field-chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.field-chip-list {
  align-items: center;
}

.field-chip-list :deep(.el-tag) {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  margin-top: 0;
  line-height: 1;
}

.binding-layout {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.mode-use-row {
  display: grid;
  grid-template-columns: minmax(180px, 0.8fr) minmax(0, 1.2fr);
  gap: 12px;
  padding: 10px;
  border: 1px solid rgba(65, 88, 72, 0.12);
  border-radius: 8px;
  background: rgba(248, 251, 248, 0.92);
}

.binding-layout h4 {
  margin: 0;
  color: #1e3327;
  font-size: 14px;
}

.mode-id {
  display: block;
  margin-top: 4px;
  color: #5e7165;
  font-size: 12px;
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
  background: linear-gradient(180deg, #ffffff 0%, #f8fbf8 100%);
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
  overflow-y: auto;
  overscroll-behavior: contain;
  padding: 14px 20px 16px;
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

.full-width {
  width: 100%;
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

.scope-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
}

.output-scope-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  width: 100%;
}

.output-scope-card {
  min-height: 76px;
  padding: 9px 10px;
  border: 1px solid rgba(65, 88, 72, 0.14);
  border-radius: 12px;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(245, 250, 246, 0.94));
  text-align: left;
  cursor: pointer;
}

.output-scope-card:hover,
.output-scope-card.active {
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

.output-scope-card strong {
  display: block;
  color: #14281d;
  font-size: 13px;
  line-height: 1.25;
}

.output-scope-card em {
  display: inline-block;
  margin-top: 4px;
  color: #187856;
  font-size: 11px;
  font-style: normal;
  font-weight: 800;
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

.dialog-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
}

@media (max-width: 1100px) {
  .extensions-layout,
  .detail-grid,
  .hero-meta-grid,
  .mode-use-row,
  .scope-grid,
  .scope-grid.three {
    grid-template-columns: 1fr;
  }

  .extensions-header,
  .hero-card,
  .header-actions {
    flex-direction: column;
    align-items: stretch;
  }

  .hero-side {
    width: 100%;
    flex-basis: auto;
    align-items: flex-start;
  }

  .hero-actions {
    justify-content: flex-start;
  }

  .hero-enable-panel {
    width: 100%;
  }

  .extension-search {
    width: 100%;
  }
}

@media (max-width: 760px) {
  .hero-title-row {
    flex-direction: column;
    align-items: flex-start;
  }

  .extension-form-grid.two,
  .output-scope-grid {
    grid-template-columns: 1fr;
  }

  .field-choice-row {
    grid-template-columns: minmax(160px, 1fr) auto;
  }

  .field-choice-row small {
    display: none;
  }
}
</style>
