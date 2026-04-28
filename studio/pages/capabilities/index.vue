<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
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
} = useCapabilityAccess()

const selectedModeId = ref('')
const activeCapabilityKey = ref('')
const configDialogVisible = ref(false)
const extensionDialogVisible = ref(false)
const fieldRows = ref([])

const providerForm = reactive({
  nodeName: '',
  capability: '',
  capabilityName: '',
  capabilityDescription: '',
  defaultSlots: [],
  keys: {},
  assetTypes: 'stock',
  accessPatterns: 'panel_time_series',
  methods: 'query_daily',
})

const extensionForm = reactive({
  capability: '',
  capabilityName: '',
  capabilityDescription: '',
  nodeName: '',
  slots: [],
  fields: [],
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
  universe_fields: '候选池字段',
  ranking_fields: '排序字段',
  filter_fields: '过滤字段',
  signal_fields: '信号字段',
  weighting_fields: '权重字段',
  report_fields: '报告字段',
  factor_inputs: '因子输入',
}

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

const selectedProviderBinding = computed(() =>
  capabilities.value.find(
    (item) => item.capability === providerForm.capability && item.provider_node === providerForm.nodeName
  )
)

const activeProviderIsVirtual = computed(() =>
  (selectedProviderNode.value?.access_patterns || []).includes('virtual_runtime_rule')
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
    })
  }
  return rows
})

const extensionCapabilityOptions = computed(() => {
  const rows = new Map()
  const currentModeCapabilities = new Set(modeCapabilityRows.value.map((item) => item.capability))
  for (const item of capabilityRegistry.value || []) {
    const capability = String(item.capability || '').trim()
    if (!capability || currentModeCapabilities.has(capability) || capability.startsWith('order_constraints.')) continue
    rows.set(capability, {
      capability,
      label: capabilityLabel(capability),
      description: item.description || '',
      default_slots: item.default_slots || [],
    })
  }
  return [...rows.values()].sort((left, right) => left.capability.localeCompare(right.capability))
})

const extensionNodeOptions = computed(() =>
  providerNodes.value.filter((item) => !(item.access_patterns || []).includes('virtual_runtime_rule'))
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
  providerForm.keys = binding?.keys || node?.keys || {}
  providerForm.assetTypes = (binding?.asset_types || node?.asset_types || ['stock']).join(', ')
  providerForm.accessPatterns = (binding?.access_patterns || node?.access_patterns || ['panel_time_series']).join(', ')
  providerForm.methods = (binding?.methods || node?.methods || ['query_daily']).join(', ')

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

function openExtensionDialog() {
  extensionForm.capability = ''
  extensionForm.capabilityName = ''
  extensionForm.capabilityDescription = ''
  extensionForm.nodeName = ''
  extensionForm.slots = []
  extensionForm.fields = []
  extensionDialogVisible.value = true
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

async function handleAddExtensionCapability() {
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
        keys: selectedNode?.keys || {},
        assetTypes: (selectedNode?.asset_types || ['stock']).join(', '),
        accessPatterns: (selectedNode?.access_patterns || ['panel_time_series']).join(', '),
        methods: (selectedNode?.methods || ['query_daily']).join(', '),
      },
      extensionForm.fields.map((field) => ({
        semantic_field: field,
        source_field: field,
      }))
    )
    ElMessage.success('扩展能力已添加')
    extensionDialogVisible.value = false
    await nextTick()
    const refreshed = modeCapabilityRows.value.find(
      (item) => item.capability === capability && item.section === 'extension'
    )
    if (refreshed) selectCapability(refreshed)
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : '添加失败')
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
    (binding.access_patterns || []).includes('virtual_runtime_rule')
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

function capabilityDescription(capability) {
  const meta = capabilityMetaMap.value.get(capability)
  return meta?.description || ''
}

function applyExtensionCapability(item) {
  const allowedSlots = new Set(extensionSlotOptions.value.map((slot) => slot.slot))
  extensionForm.capability = item.capability
  extensionForm.capabilityName = capabilityLabel(item.capability)
  extensionForm.capabilityDescription = item.description || capabilityDescription(item.capability)
  extensionForm.slots = [...(item.default_slots || [])].filter((slot) => allowedSlots.has(slot))
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

function toggleExtensionSlot(slot) {
  if (!slot) return
  if (isExtensionSlotSelected(slot)) {
    extensionForm.slots = extensionForm.slots.filter((item) => item !== slot)
  } else {
    extensionForm.slots = [...extensionForm.slots, slot]
  }
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
    extensionForm.fields = []
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
            <button
              v-for="row in dataCapabilityRows.filter((item) => item.section === section)"
              :key="row.key"
              type="button"
              class="capability-card"
              :class="{ active: activeCapabilityKey === row.key }"
              @click="openCapabilityDialog(row)"
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
                <el-tag v-if="!row.provider_bindings.length" size="small" type="danger" effect="plain">未绑定</el-tag>
              </div>
            </button>
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
                :label="`${node.name} (${(node.access_patterns || []).join(', ') || '-'})`"
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
      title="新增扩展能力"
      width="min(760px, 92vw)"
      destroy-on-close
    >
      <el-form label-position="top" class="extension-form">
        <el-form-item label="能力英文 ID">
          <el-input
            v-model="extensionForm.capability"
            clearable
            placeholder="例如 sentiment_news_daily / fund_flow_daily"
          />
          <div v-if="extensionCapabilityOptions.length" class="quick-options">
            <el-button
              v-for="item in extensionCapabilityOptions"
              :key="item.capability"
              size="small"
              plain
              @click="applyExtensionCapability(item)"
            >
              {{ item.label }}
            </el-button>
          </div>
        </el-form-item>

        <el-form-item label="中文名">
          <el-input
            v-model="extensionForm.capabilityName"
            clearable
            placeholder="例如 新闻情绪日频 / 资金流日频"
          />
        </el-form-item>

        <el-form-item label="语义说明">
          <el-input
            v-model="extensionForm.capabilityDescription"
            type="textarea"
            :rows="2"
            placeholder="说明这个能力在策略里用来做什么，例如用于排序、过滤或输出"
          />
        </el-form-item>

        <el-form-item label="允许使用位置">
          <div v-if="extensionSlotOptions.length" class="slot-choice-grid">
            <button
              v-for="slot in extensionSlotOptions"
              :key="slot.slot"
              type="button"
              class="slot-choice-card"
              :class="{ active: isExtensionSlotSelected(slot.slot) }"
              @click="toggleExtensionSlot(slot.slot)"
            >
              <div class="slot-choice-head">
                <strong>{{ slot.name || slotLabel(slot.slot) }}</strong>
                <span>{{ isExtensionSlotSelected(slot.slot) ? '已选择' : '可选择' }}</span>
              </div>
              <code>{{ slot.slot }}</code>
              <p>{{ slot.description || slotDescription(slot.slot) }}</p>
              <small>
                {{ slot.access_pattern || 'panel_time_series' }}
                <template v-if="slot.freq"> / {{ slot.freq }}</template>
              </small>
            </button>
          </div>
          <el-empty v-else description="当前模式没有定义扩展 slot" :image-size="72" />
        </el-form-item>

        <el-form-item label="数据节点">
          <el-select
            v-model="extensionForm.nodeName"
            filterable
            allow-create
            default-first-option
            class="full-width"
            placeholder="选择或输入 real 表节点名，例如 my_sentiment_daily_real"
          >
            <el-option
              v-for="node in extensionNodeOptions"
              :key="node.name"
              :label="`${node.name} (${(node.access_patterns || []).join(', ') || '-'})`"
              :value="node.name"
            />
          </el-select>
        </el-form-item>

        <el-form-item label="能力字段（来自所选数据节点）">
          <el-select
            v-model="extensionForm.fields"
            multiple
            filterable
            allow-create
            default-first-option
            class="full-width"
            placeholder="选择要暴露给策略使用的字段"
          >
            <el-option
              v-for="option in extensionFieldOptions"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
        </el-form-item>
      </el-form>

      <template #footer>
        <div class="dialog-actions">
          <el-button @click="extensionDialogVisible = false">关闭</el-button>
          <el-button type="primary" :loading="saving" @click="handleAddExtensionCapability">
            添加到当前模式
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
  max-width: 640px;
}

.full-width {
  width: 100%;
}

.quick-options {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 8px;
}

.slot-choice-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  width: 100%;
}

.slot-choice-card {
  min-height: 92px;
  padding: 10px 12px;
  border: 1px solid rgba(65, 88, 72, 0.16);
  border-radius: 8px;
  background: rgba(248, 251, 248, 0.92);
  text-align: left;
  cursor: pointer;
  transition: border-color 0.18s ease, background 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
}

.slot-choice-card:hover,
.slot-choice-card.active {
  border-color: rgba(23, 128, 91, 0.56);
  background: rgba(237, 248, 242, 0.98);
  box-shadow: 0 6px 14px rgba(31, 109, 79, 0.1);
  transform: translateY(-1px);
}

.slot-choice-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.slot-choice-head strong {
  color: #1e3327;
  font-size: 13px;
}

.slot-choice-head span {
  flex: 0 0 auto;
  padding: 2px 6px;
  border-radius: 999px;
  background: rgba(68, 91, 76, 0.1);
  color: #526259;
  font-size: 11px;
}

.slot-choice-card.active .slot-choice-head span {
  background: rgba(24, 120, 86, 0.16);
  color: #187856;
  font-weight: 700;
}

.slot-choice-card code {
  display: inline-block;
  margin-top: 5px;
  color: #187856;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: 11px;
}

.slot-choice-card p {
  display: -webkit-box;
  min-height: 30px;
  margin: 5px 0;
  overflow: hidden;
  color: #526259;
  font-size: 11px;
  line-height: 1.35;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.slot-choice-card small {
  color: #7a867f;
  font-size: 10px;
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
}
</style>
