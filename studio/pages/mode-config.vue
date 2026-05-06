<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { navigateTo } from '#imports'
import { useCapabilityAccess } from '~/composables/useCapabilityAccess'

const {
  loading,
  saving,
  providerNodes,
  dataAssets,
  modeProfiles,
  modeDataAccess,
  loadWorkspace,
  loadDataAssets,
  loadModeDataAccess,
  saveModeDataAccess,
} = useCapabilityAccess()

const DEFAULT_BASE_ENTITY = 'stock'

const ENTITY_OPTIONS = [
  { value: 'stock', label: '股票' },
  { value: 'index', label: '指数' },
  { value: 'industry', label: '行业' },
  { value: 'concept', label: '概念' },
  { value: 'etf', label: 'ETF' },
  { value: 'fund', label: '基金' },
]

const ACCESS_GROUPS = [
  {
    key: 'base_market_data',
    title: '股票主体行情 / 基础数据',
    description: '按股票代码和时间直接进入模式的日线、分钟、估值、状态字段。',
  },
  {
    key: 'base_extension_data',
    title: '股票主体扩展 / 事件数据',
    description: '已经落在股票维度上的事件、标签、信号或附加字段。',
  },
  {
    key: 'bound_entity_data',
    title: '其他实体映射到股票',
    description: '行业、概念等实体数据通过关系映射后，投影到股票维度使用。',
  },
  {
    key: 'entity_relation',
    title: '实体关系 / 股票池映射',
    description: '指数、行业、主题等实体和股票之间的成分或归属关系。',
  },
  {
    key: 'reference_entity_data',
    title: '参考实体 / Benchmark',
    description: '指数、ETF 等独立参考序列，默认不投影到每只股票。',
  },
]

const FIELD_USAGE_OPTIONS = [
  { value: 'auto', label: '自动判断' },
  { value: 'factor', label: '计算 / 因子' },
  { value: 'filter', label: '过滤条件' },
  { value: 'group', label: '分类 / 分组' },
  { value: 'display', label: '展示字段' },
  { value: 'reference', label: '参考序列' },
]

const CALENDAR_SOURCE_OPTIONS = [
  { value: 'default_exchange', label: '默认交易所日历' },
  { value: 'real_node', label: 'real 节点' },
]

const DEFAULT_MODE_SETTINGS = {
  calendar: {
    source: 'real_node',
    node: 'trade_calendar_real',
    market: 'CN_A',
  },
  benchmark: {
    symbol: '000300.SH',
    name: '沪深300',
    source: 'index_daily_real',
  },
  costs: {
    buy_commission_rate: 0.0003,
    sell_commission_rate: 0.0003,
    stamp_tax_rate: 0.001,
    min_commission: 5,
    slippage_rate: 0,
  },
}

const FIELD_PICKER_SEPARATOR = '::'
const SELECTABLE_BINDING_ROLES = ['time', 'event_time', 'announce_date', 'report_period', 'start', 'end']

const DEFAULT_REQUIREMENT_GROUPS = [
  {
    key: 'market_panel',
    title: '行情面板',
    level: '必需',
    description: '模式运行的基础价格、成交量和成交额字段。',
    fields: [
      req('open', '开盘价', 'factor'),
      req('high', '最高价', 'factor'),
      req('low', '最低价', 'factor'),
      req('close', '收盘价', 'factor'),
      req('volume', '成交量', 'factor'),
      req('amount', '成交额', 'factor'),
    ],
  },
]

const MODE_REQUIREMENT_PRESETS = {
  'strategy_modes.discrete_stock': [
    {
      key: 'daily_market',
      title: '日线基础数据',
      level: '必需能力',
      description: '选股、排序、调仓和收益计算最基础的数据面板。',
      asset_group: 'market',
      market_grain: 'daily',
      data_shape: 'time_series',
      fields: [
        req('open', '开盘价', 'factor'),
        req('high', '最高价', 'factor'),
        req('low', '最低价', 'factor'),
        req('close', '收盘价', 'factor'),
        req('volume', '成交量', 'factor'),
        req('amount', '成交额', 'factor'),
      ],
    },
    {
      key: 'stock_basic_data',
      title: '基础数据',
      level: '必需能力',
      description: '交易限制、昨收、复权因子、板块标识等策略运行基础字段。',
      asset_group: 'market',
      market_grain: 'daily',
      data_shape: 'time_series',
      fields: [
        req('pre_close', '昨收价', 'factor'),
        req('backward_adj_factor', '复权因子', 'factor'),
        req('high_limited', '涨停价', 'filter'),
        req('low_limited', '跌停价', 'filter'),
        req('is_st', '是否 ST', 'filter', ['is_st_sec']),
        req('is_suspended', '是否停牌', 'filter', ['is_susp_sec']),
        req('is_wd_sec', '退市风险', 'filter'),
        req('is_kcb', '是否科创板', 'filter'),
        req('is_cyb', '是否创业板', 'filter'),
        req('is_bjs', '是否北交所', 'filter'),
      ],
    },
    {
      key: 'corporate_actions',
      title: '分红转送数据',
      level: '必需能力',
      description: '确认分红、送股、转增、除权除息等事件字段是否已经从 real 或实体资产接入。',
      asset_group: 'extension',
      data_shape: 'event',
      fields: [
        req('dividend_cash_pre_tax', '税前现金分红', 'reference', ['dvd_per_share_pre_tax_cash']),
        req('dividend_cash_after_tax', '税后现金分红', 'reference', ['dvd_per_share_after_tax_cash']),
        req('dividend_stock_per_share', '送股比例', 'reference', ['dvd_per_share_stk']),
        req('dividend_bonus_rate', '送股率', 'reference', ['div_bonusrate']),
        req('dividend_conversed_rate', '转增率', 'reference', ['div_conversedrate']),
        req('ex_dividend_date', '除权除息日', 'reference', ['date_ex', 'ex_date']),
        req('dividend_payout_date', '派息日', 'reference', ['date_dvd_payout']),
        req('equity_record_date', '股权登记日', 'reference', ['date_eqy_record']),
      ],
    },
    {
      key: 'minute_market',
      title: '分钟基础数据',
      level: '可选能力',
      description: '只有策略需要分钟级执行、盘中窗口或分钟信号时才需要接入。',
      asset_group: 'market',
      market_grain: 'minute',
      data_shape: 'time_series',
      fields: [
        optionalReq('minute_open', '分钟开盘价', 'factor', ['open']),
        optionalReq('minute_high', '分钟最高价', 'factor', ['high']),
        optionalReq('minute_low', '分钟最低价', 'factor', ['low']),
        optionalReq('minute_close', '分钟收盘价', 'factor', ['close']),
        optionalReq('minute_amount', '分钟成交额', 'factor', ['amount']),
        optionalReq('minute_volume', '分钟成交量', 'factor', ['volume']),
      ],
    },
  ],
  'research_modes.factor_research': [
    {
      key: 'factor_inputs',
      title: '因子输入面板',
      level: '必需',
      description: '用于计算、标准化和检验因子的基础输入字段。',
      fields: [
        req('close', '收盘价', 'factor'),
        req('close_adj', '复权收盘价', 'factor'),
        req('volume', '成交量', 'factor'),
        req('amount', '成交额', 'factor'),
        req('market_cap', '总市值', 'factor'),
        req('float_market_cap', '流通市值', 'factor'),
        req('turnover_rate', '换手率', 'factor'),
      ],
    },
    {
      key: 'factor_filters',
      title: '研究样本过滤',
      level: '必需',
      description: '用于剔除 ST、停牌、不可交易样本。',
      fields: [
        req('is_st', '是否 ST', 'filter', ['is_st_sec']),
        req('is_suspended', '是否停牌', 'filter', ['is_susp_sec']),
        req('is_wd_sec', '退市风险', 'filter'),
      ],
    },
    {
      key: 'factor_grouping',
      title: '分组与中性化',
      level: '可选',
      description: '行业、指数成分等实体信息可用于分组统计和中性化。',
      fields: [
        req('industry_name', '行业名称', 'group', ['industry_index_name']),
        req('index_name', '指数名称', 'group'),
      ],
    },
  ],
  'research_modes.event_research': [
    {
      key: 'event_fields',
      title: '事件字段',
      level: '必需',
      description: '事件研究需要确认事件发生日、公告日和事件属性字段。',
      fields: [
        req('ex_dividend_date', '除权除息日', 'reference', ['date_ex', 'ex_date']),
        req('dividend_payout_date', '派息日', 'reference', ['date_dvd_payout']),
        req('equity_record_date', '股权登记日', 'reference', ['date_eqy_record']),
        req('reason_type_name', '事件原因', 'group'),
        req('change_range', '事件当日涨跌幅', 'factor'),
      ],
    },
    {
      key: 'event_market_window',
      title: '事件窗口行情',
      level: '必需',
      description: '事件日前后收益、成交额和波动需要基础行情面板支持。',
      fields: [
        req('close', '收盘价', 'factor'),
        req('pre_close', '昨收价', 'factor'),
        req('volume', '成交量', 'factor'),
        req('amount', '成交额', 'factor'),
      ],
    },
  ],
}

const selectedModeId = ref('')
const activeSection = ref('requirements')
const draftMode = reactive(emptyDraftMode())
const fieldPicker = reactive({
  visible: false,
  rowKey: '',
  query: '',
  selectedValue: '',
})

const selectedMode = computed(() =>
  modeProfiles.value.find((item) => item.mode_id === selectedModeId.value)
)

const modeGroups = computed(() => {
  const groups = [
    { key: 'research_modes', title: '研究模式', modes: [] },
    { key: 'strategy_modes', title: '回测模式', modes: [] },
  ]
  const map = new Map(groups.map((item) => [item.key, item]))
  for (const mode of modeProfiles.value) {
    const group = map.get(mode.mode_kind)
    if (group) {
      group.modes.push(mode)
      continue
    }
    let fallback = map.get('other_modes')
    if (!fallback) {
      fallback = { key: 'other_modes', title: '其他模式', modes: [] }
      map.set('other_modes', fallback)
      groups.push(fallback)
    }
    fallback.modes.push(mode)
  }
  return groups.filter((item) => item.modes.length)
})

const normalizedAssets = computed(() =>
  dataAssets.value
    .map((item) => normalizeAsset(item))
    .filter((item) => item.asset_id)
)

const providerNodeByName = computed(() => {
  const rows = new Map()
  for (const node of providerNodes.value || []) {
    const name = String(node?.name || '').trim()
    if (name) rows.set(name, node)
  }
  return rows
})

const enabledAssets = computed(() =>
  normalizedAssets.value.filter((item) => item.enabled)
)

const modeRequirementGroups = computed(() =>
  resolveModeRequirements(selectedMode.value)
    .filter((group) => !group.relation)
    .map((group) => {
      const fields = group.fields.map((field) => resolveRequirementField(group, field))
      const readyCount = fields.filter((field) => field.status === 'ready').length
      const blockingCount = fields.filter((field) => field.status !== 'ready' && field.required !== false).length
      return {
        ...group,
        fields,
        readyCount,
        blockingCount,
        totalCount: fields.length,
      }
    })
)

const requirementFieldRows = computed(() =>
  modeRequirementGroups.value.flatMap((group) =>
    group.fields.map((field) => ({
      ...field,
      group_key: group.key,
      group_title: group.title,
      group_description: group.description,
      group_level: group.level,
      group_relation: Boolean(group.relation),
      group_asset_group: group.asset_group || '',
      group_market_grain: group.market_grain || '',
      group_data_shape: group.data_shape || '',
      __rowKey: `${group.key}:${field.field}`,
    }))
  )
)

const fieldPickerRow = computed(() =>
  requirementFieldRows.value.find((row) => row.__rowKey === fieldPicker.rowKey)
)

const fieldPickerOptions = computed(() => {
  const row = fieldPickerRow.value
  if (!row) return []
  const query = fieldPicker.query.trim().toLowerCase()
  const queryTerms = equivalentFieldTerms(query)
  return requirementFieldCandidates(row)
    .filter((option) => {
      if (!query) return true
      const optionTerms = [
        option.realPath,
        option.field.name,
        option.field.source,
        option.field.label,
        option.bindingRole,
        option.asset.name,
        option.asset.asset_id,
        ...option.asset.provider_nodes,
      ]
        .flatMap((item) => equivalentFieldTerms(item))
      return optionTerms.some((item) => queryTerms.some((term) => item.includes(term)))
    })
    .sort((left, right) => {
      if (left.exact !== right.exact) return left.exact ? -1 : 1
      return left.label.localeCompare(right.label)
    })
})

const assetAccessRows = computed(() =>
  normalizedAssets.value.map((asset) => {
    const policy = assetPolicy(asset.asset_id)
    const enabledFields = asset.fields.filter((field) => fieldPolicy(asset.asset_id, field.name).enabled)
    const accessGroup = classifyAsset(asset)
    return {
      ...asset,
      access_group: accessGroup,
      access_group_label: accessGroupLabel(accessGroup),
      mode_enabled: policy.enabled !== false,
      provider_label: asset.provider_nodes.join('、') || asset.asset_id,
      source_label: assetSourceLabel(asset),
      source_detail_label: assetSourceDetailLabel(asset),
      access_method_label: assetAccessMethodLabel(asset, accessGroup),
      access_description: assetAccessDescription(asset, accessGroup),
      enabled_field_count: enabledFields.length,
      total_field_count: asset.fields.length,
    }
  })
)

const assetAccessEntitySections = computed(() => {
  const sections = new Map()
  for (const asset of assetAccessRows.value) {
    const entityId = asset.entity_id || DEFAULT_BASE_ENTITY
    if (!sections.has(entityId)) {
      sections.set(entityId, {
        entity_id: entityId,
        title: entityLabel(entityId),
        role_label: entityRoleLabel(entityId),
        description: entityAccessDescription(entityId),
        rows: [],
      })
    }
    sections.get(entityId).rows.push(asset)
  }
  return [...sections.values()]
    .map((section) => {
      const activeCount = section.rows.filter((row) => row.enabled && row.mode_enabled).length
      return {
        ...section,
        rows: sortEntityAssetRows(section.rows),
        active_count: activeCount,
        total_count: section.rows.length,
        enabled: activeCount > 0,
      }
    })
    .sort((left, right) => entitySortRank(left.entity_id) - entitySortRank(right.entity_id))
})

const compiledPreview = computed(() => buildCompiledModeConfig())

const validationSummary = computed(() => compiledPreview.value.validation.summary)

const validationIssues = computed(() => compiledPreview.value.validation.issues)

const runtimeContractPreview = computed(() =>
  JSON.stringify(compiledPreview.value.runtime_contract, null, 2)
)

const runtimeCheckCards = computed(() => {
  const validation = compiledPreview.value.validation
  const assets = runtimeAssetRows.value
  return [
    {
      key: 'status',
      label: '运行状态',
      value: validation.ok ? '可运行' : '需补齐',
      type: validation.ok ? 'success' : 'danger',
      tag: validation.ok ? '正常' : '处理',
      hint: validation.ok ? '必要字段已经满足' : '还有必要字段缺失或被关闭',
    },
    {
      key: 'required',
      label: '必要字段',
      value: `${validation.summary.ready} / ${validation.summary.total}`,
      type: validation.summary.missing ? 'warning' : 'success',
      tag: validation.summary.missing ? '需补齐' : '正常',
      hint: `${validation.summary.required} 个必需字段，${validation.summary.optional_missing} 个可选缺失`,
    },
    {
      key: 'assets',
      label: '启用资产',
      value: String(assets.length),
      type: assets.length ? 'success' : 'warning',
      tag: assets.length ? '已启用' : '未启用',
      hint: '保存后会进入运行合约的实体资产',
    },
    {
      key: 'sources',
      label: '数据源类型',
      value: String(runtimeSourceRows.value.filter((row) => row.count > 0).length),
      type: 'info',
      tag: '已分类',
      hint: '面板、事件、关系、参考序列的覆盖情况',
    },
  ]
})

const runtimeSourceRows = computed(() => {
  const dataSources = compiledPreview.value.runtime_contract?.data_sources || {}
  const rows = [
    { key: 'panels', label: '面板数据', description: '日线、分钟和其他按实体 + 时间读取的数据' },
    { key: 'events', label: '事件数据', description: '分红、龙虎榜等按事件日或披露日读取的数据' },
    { key: 'relations', label: '关系数据', description: '行业、指数等实体映射到股票的关系表' },
    { key: 'references', label: '参考序列', description: '指数、ETF 等不默认展开到股票的参考数据' },
  ]
  return rows.map((row) => {
    const items = Array.isArray(dataSources[row.key]) ? dataSources[row.key] : []
    return {
      ...row,
      count: items.length,
      assets: items.map((item) => item.name || item.asset_id).filter(Boolean).join('、') || '无',
    }
  })
})

const runtimeAssetRows = computed(() =>
  assetAccessRows.value.filter((asset) => asset.enabled && assetPolicy(asset.asset_id).enabled)
)

const runtimeRequirementRows = computed(() =>
  requirementFieldRows.value.map((row) => ({
    ...row,
    runtime_real_field: runtimeRequirementRealField(row),
    runtime_asset_name: row.match?.asset?.name || '未绑定',
    runtime_provider: row.match?.asset?.provider_nodes?.join('、') || '-',
  }))
)

function req(field, label, usage = 'auto', aliases = []) {
  return {
    field,
    label,
    usage,
    aliases,
    required: true,
  }
}

function optionalReq(field, label, usage = 'auto', aliases = []) {
  return {
    ...req(field, label, usage, aliases),
    required: false,
  }
}

function emptyDraftMode() {
  return {
    mode_id: '',
    mode_name: '',
    mode_kind: '',
    base_entity: DEFAULT_BASE_ENTITY,
    description: '',
    enabled: true,
    mode_settings: defaultModeSettings(),
    access_groups: {},
    asset_policies: {},
    requirement_bindings: {},
    runtime_contract: {},
    validation: {},
    source_snapshot: {},
  }
}

function hydrateDraft() {
  const mode = selectedMode.value
  if (!mode) return
  const saved = modeDataAccess.value?.[mode.mode_id] || {}
  const next = {
    ...emptyDraftMode(),
    ...clone(saved),
    mode_id: mode.mode_id,
    mode_name: saved.mode_name || mode.mode_name,
    mode_kind: saved.mode_kind || mode.mode_kind,
    description: saved.description || mode.description || '',
    base_entity: saved.base_entity || DEFAULT_BASE_ENTITY,
    enabled: saved.enabled !== false,
    mode_settings: normalizeModeSettings(saved.mode_settings),
    access_groups: normalizeAccessGroups(saved.access_groups),
    asset_policies: normalizeAssetPolicies(saved.asset_policies),
    requirement_bindings: normalizeRequirementBindings(saved.requirement_bindings),
    runtime_contract: clone(saved.runtime_contract || {}),
    validation: clone(saved.validation || {}),
    source_snapshot: clone(saved.source_snapshot || {}),
  }
  Object.assign(draftMode, next)
  ensureDraftPolicies()
}

function defaultModeSettings() {
  return clone(DEFAULT_MODE_SETTINGS)
}

function normalizeModeSettings(value) {
  const source = value && typeof value === 'object' ? value : {}
  return {
    calendar: {
      ...DEFAULT_MODE_SETTINGS.calendar,
      ...(source.calendar && typeof source.calendar === 'object' ? source.calendar : {}),
    },
    benchmark: {
      ...DEFAULT_MODE_SETTINGS.benchmark,
      ...(source.benchmark && typeof source.benchmark === 'object' ? source.benchmark : {}),
    },
    costs: {
      ...DEFAULT_MODE_SETTINGS.costs,
      ...(source.costs && typeof source.costs === 'object' ? source.costs : {}),
    },
  }
}

function ensureDraftPolicies() {
  for (const group of ACCESS_GROUPS) {
    if (!draftMode.access_groups[group.key]) {
      draftMode.access_groups[group.key] = { enabled: true, notes: '' }
    }
  }
  for (const asset of enabledAssets.value) {
    if (!draftMode.asset_policies[asset.asset_id]) {
      draftMode.asset_policies[asset.asset_id] = { enabled: true, notes: '', fields: {} }
    }
    const assetPolicy = draftMode.asset_policies[asset.asset_id]
    if (!assetPolicy.fields || typeof assetPolicy.fields !== 'object') assetPolicy.fields = {}
    for (const field of asset.fields) {
      if (!assetPolicy.fields[field.name]) {
        assetPolicy.fields[field.name] = {
          enabled: true,
          usage: inferFieldUsage(field.name),
          alias: '',
          notes: '',
        }
      }
    }
  }
}

function normalizeAccessGroups(value) {
  const rows = {}
  const source = value && typeof value === 'object' ? value : {}
  for (const group of ACCESS_GROUPS) {
    const item = source[group.key]
    rows[group.key] = {
      enabled: item?.enabled !== false,
      notes: String(item?.notes || ''),
    }
  }
  return rows
}

function normalizeAssetPolicies(value) {
  const rows = {}
  const source = value && typeof value === 'object' ? value : {}
  for (const [assetId, item] of Object.entries(source)) {
    const fields = {}
    for (const [fieldName, fieldItem] of Object.entries(item?.fields || {})) {
      fields[fieldName] = {
        enabled: fieldItem?.enabled !== false,
        usage: fieldItem?.usage || 'auto',
        alias: String(fieldItem?.alias || ''),
        notes: String(fieldItem?.notes || ''),
      }
    }
    rows[assetId] = {
      enabled: item?.enabled !== false,
      notes: String(item?.notes || ''),
      fields,
    }
  }
  return rows
}

function normalizeRequirementBindings(value) {
  const rows = {}
  const source = value && typeof value === 'object' ? value : {}
  for (const [groupKey, groupValue] of Object.entries(source)) {
    if (!groupValue || typeof groupValue !== 'object') continue
    rows[groupKey] = {}
    for (const [fieldKey, binding] of Object.entries(groupValue)) {
      if (!binding || typeof binding !== 'object') continue
      rows[groupKey][fieldKey] = clone(binding)
    }
  }
  return rows
}

function normalizeAsset(asset) {
  const assetId = String(asset.capability || asset.asset_id || '').trim()
  const fields = Array.isArray(asset.fields) ? asset.fields.map(normalizeField).filter((item) => item.name) : []
  const entityId = String(asset.entity_id || DEFAULT_BASE_ENTITY).trim()
  const targetEntityId = String(asset.target_entity_id || entityId || DEFAULT_BASE_ENTITY).trim()
  const normalized = {
    asset_id: assetId,
    capability: assetId,
    name: String(asset.name || assetId).trim(),
    description: String(asset.description || '').trim(),
    enabled: asset.enabled !== false,
    entity_id: entityId,
    target_entity_id: targetEntityId,
    access_type: String(asset.access_type || 'entity_data').trim(),
    asset_group: String(asset.asset_group || '').trim(),
    market_grain: String(asset.market_grain || '').trim(),
    data_shape: String(asset.data_shape || '').trim(),
    fields,
    provider_nodes: Array.isArray(asset.provider_nodes)
      ? asset.provider_nodes.map((item) => String(item || '').trim()).filter(Boolean)
      : [],
    provider_bindings: Array.isArray(asset.provider_bindings) ? asset.provider_bindings : [],
  }
  return normalized
}

function normalizeField(field) {
  if (typeof field === 'string') return { name: field, source: field, label: field }
  const name = String(field?.name || field?.source || field?.field || '').trim()
  return {
    name,
    source: String(field?.source || name).trim(),
    label: String(field?.label || name).trim(),
    db_type: String(field?.db_type || field?.database_type || '').trim(),
  }
}

function resolveModeRequirements(mode) {
  if (!mode) return DEFAULT_REQUIREMENT_GROUPS
  const exact = MODE_REQUIREMENT_PRESETS[mode.mode_id]
  if (exact) return exact
  const name = String(mode.mode_name || mode.mode_id || '').toLowerCase()
  if (name.includes('factor')) return MODE_REQUIREMENT_PRESETS['research_modes.factor_research']
  if (name.includes('event')) return MODE_REQUIREMENT_PRESETS['research_modes.event_research']
  if (mode.mode_kind === 'strategy_modes') return MODE_REQUIREMENT_PRESETS['strategy_modes.discrete_stock']
  return DEFAULT_REQUIREMENT_GROUPS
}

function resolveRequirementField(group, requirement) {
  const match = findRequirementMatch(group, requirement)
  const status = requirementStatus(match)
  return {
    ...requirement,
    match,
    status,
    statusLabel: requirementStatusLabel(status, requirement.required !== false),
    statusType: requirementStatusType(status, requirement.required !== false),
  }
}

function findRequirementMatch(group, requirement) {
  const explicit = findExplicitRequirementMatch(group, requirement)
  if (explicit) return explicit
  const candidateAssets = stockSubjectAssets(group)
  for (const asset of candidateAssets) {
    const field = assetSelectableFields(asset).find((item) => fieldAliasMatchesRequirement(item, requirement, asset))
    if (field) return { asset, field }
  }
  for (const asset of candidateAssets) {
    const field = assetSelectableFields(asset).find((item) => fieldNaturalMatchesRequirement(item, requirement))
    if (field) return { asset, field }
  }
  return null
}

function findExplicitRequirementMatch(group, requirement) {
  const binding = draftMode.requirement_bindings?.[group.key]?.[requirement.field]
  if (!binding?.asset_id || !binding?.field_name) return null
  const asset = normalizedAssets.value.find((item) => item.asset_id === binding.asset_id)
  if (!asset) return null
  const field = assetSelectableFields(asset).find((item) =>
    item.name === binding.field_name || item.source === binding.source_field
  )
  if (!field) return null
  return { asset, field, explicit: true }
}

function stockSubjectAssets(scope = {}) {
  return normalizedAssets.value.filter((asset) => isStockSubjectAsset(asset) && assetMatchesRequirementScope(asset, scope))
}

function isStockSubjectAsset(asset) {
  return asset.entity_id === DEFAULT_BASE_ENTITY
    && asset.target_entity_id === DEFAULT_BASE_ENTITY
    && asset.access_type !== 'relation_data'
}

function assetMatchesRequirementScope(asset, scope = {}) {
  const assetGroup = scope.asset_group || scope.group_asset_group || ''
  const marketGrain = scope.market_grain || scope.group_market_grain || ''
  const dataShape = scope.data_shape || scope.group_data_shape || ''
  if (assetGroup && asset.asset_group !== assetGroup) return false
  if (marketGrain && asset.market_grain !== marketGrain) return false
  if (dataShape && asset.data_shape !== dataShape) return false
  return true
}

function fieldMatchesRequirement(field, requirement, asset) {
  return fieldNaturalMatchesRequirement(field, requirement) || fieldAliasMatchesRequirement(field, requirement, asset)
}

function fieldNaturalMatchesRequirement(field, requirement) {
  const candidates = [requirement.field, ...(requirement.aliases || [])]
    .flatMap((item) => equivalentFieldTerms(item))
  const names = [field.name, field.source, field.label]
    .flatMap((item) => equivalentFieldTerms(item))
  return candidates.some((candidate) => names.includes(candidate))
}

function fieldAliasMatchesRequirement(field, requirement, asset) {
  if (!asset) return false
  const alias = fieldPolicy(asset.asset_id, field.name).alias
  const aliases = String(alias || '')
    .split(',')
    .flatMap((item) => equivalentFieldTerms(item))
  if (!aliases.length) return false
  const candidates = [requirement.field, ...(requirement.aliases || [])]
    .flatMap((item) => equivalentFieldTerms(item))
  return candidates.some((candidate) => aliases.includes(candidate))
}

function equivalentFieldTerms(value) {
  const text = String(value || '').trim().toLowerCase()
  if (!text) return []
  const terms = new Set([text])
  const swaps = [
    ['date_ex', 'ex_date'],
    ['ex_date', 'date_ex'],
  ]
  for (const [from, to] of swaps) {
    if (text.includes(from)) terms.add(text.replaceAll(from, to))
  }
  return Array.from(terms)
}

function requirementStatus(match) {
  if (!match) return 'missing'
  if (match.asset.enabled === false) return 'asset_disabled'
  if (!isAssetEffectivelyEnabled(match.asset)) return 'mode_disabled'
  if (!fieldPolicy(match.asset.asset_id, match.field.name).enabled) return 'field_disabled'
  return 'ready'
}

function requirementStatusLabel(status, required = true) {
  if (status === 'missing' && !required) return '可选未接入'
  const labels = {
    ready: '已满足',
    missing: '缺字段',
    asset_disabled: '资产关闭',
    mode_disabled: '模式未启用',
    field_disabled: '字段未启用',
  }
  return labels[status] || '未满足'
}

function requirementStatusType(status, required = true) {
  if (status === 'ready') return 'success'
  if (status === 'missing' && !required) return 'info'
  if (status === 'missing') return 'danger'
  return 'warning'
}

function classifyAsset(asset) {
  const baseEntity = draftMode.base_entity || DEFAULT_BASE_ENTITY
  if (asset.access_type === 'relation_data' && asset.target_entity_id === baseEntity) return 'entity_relation'
  if (asset.entity_id === baseEntity && asset.target_entity_id === baseEntity) {
    return asset.asset_group === 'market' ? 'base_market_data' : 'base_extension_data'
  }
  if (asset.target_entity_id === baseEntity && asset.entity_id !== baseEntity) return 'bound_entity_data'
  if (asset.entity_id !== baseEntity && asset.target_entity_id === asset.entity_id) return 'reference_entity_data'
  return 'base_extension_data'
}

function accessGroupLabel(groupKey) {
  return ACCESS_GROUPS.find((item) => item.key === groupKey)?.title || groupKey || '-'
}

function assetAccessMethodLabel(asset, accessGroup = classifyAsset(asset)) {
  if (accessGroup === 'entity_relation') return '关系映射'
  if (accessGroup === 'bound_entity_data') return '投影到股票'
  if (accessGroup === 'reference_entity_data') return '参考序列'
  if (accessGroup === 'base_market_data') return '直接读取'
  return '股票附加'
}

function assetAccessDescription(asset, accessGroup = classifyAsset(asset)) {
  const baseEntity = entityLabel(draftMode.base_entity || DEFAULT_BASE_ENTITY)
  const sourceEntity = entityLabel(asset.entity_id)
  if (accessGroup === 'entity_relation') {
    return `${sourceEntity} 和 ${baseEntity} 的关系表，给模式提供成分、归属或股票池映射。`
  }
  if (accessGroup === 'bound_entity_data') {
    return `${sourceEntity} 的字段先保留在实体资产里，运行时通过关系映射到 ${baseEntity}。`
  }
  if (accessGroup === 'reference_entity_data') {
    return `${sourceEntity} 自己的参考序列，适合做基准或对照，不默认展开到每只 ${baseEntity}。`
  }
  if (accessGroup === 'base_market_data') {
    return `按 ${baseEntity} 代码和时间直接读取，通常用于策略必要字段。`
  }
  return `已经是 ${baseEntity} 维度的扩展数据，可用于事件、过滤、报告或研究输出。`
}

function entityRoleLabel(entityId) {
  const baseEntity = draftMode.base_entity || DEFAULT_BASE_ENTITY
  if (entityId === baseEntity) return '中心实体'
  const rows = assetAccessRows.value.filter((asset) => asset.entity_id === entityId)
  if (rows.some((asset) => asset.target_entity_id === baseEntity)) return '映射实体'
  return '参考实体'
}

function entityAccessDescription(entityId) {
  const baseEntity = draftMode.base_entity || DEFAULT_BASE_ENTITY
  const baseLabel = entityLabel(baseEntity)
  const entityName = entityLabel(entityId)
  if (entityId === baseEntity) {
    return `${entityName} 是当前模式的中心实体，行情、事件和扩展字段可以直接进入模式。`
  }
  const rows = assetAccessRows.value.filter((asset) => asset.entity_id === entityId)
  const hasRelation = rows.some((asset) => asset.access_type === 'relation_data' || asset.data_shape === 'relation')
  const mapsToBase = rows.some((asset) => asset.target_entity_id === baseEntity)
  if (mapsToBase || hasRelation) {
    return `${entityName} 通过关系接入 ${baseLabel}，可提供成分、归属、股票池或映射后的实体字段。`
  }
  return `${entityName} 作为独立参考数据接入，适合做基准、对照序列或外部参考。`
}

function entitySortRank(entityId) {
  const baseEntity = draftMode.base_entity || DEFAULT_BASE_ENTITY
  if (entityId === baseEntity) return -100
  const preferredOrder = ['industry', 'index', 'etf', 'concept', 'fund']
  const index = preferredOrder.indexOf(entityId)
  return index >= 0 ? index : 100
}

function sortEntityAssetRows(rows) {
  return [...rows].sort((left, right) => {
    const leftRank = assetRowSortRank(left)
    const rightRank = assetRowSortRank(right)
    if (leftRank !== rightRank) return leftRank - rightRank
    return left.name.localeCompare(right.name)
  })
}

function assetRowSortRank(asset) {
  if (asset.access_group === 'base_market_data' && asset.market_grain === 'daily') return 10
  if (asset.access_group === 'base_market_data' && asset.market_grain === 'minute') return 20
  if (asset.access_group === 'base_market_data') return 30
  if (asset.access_group === 'entity_relation') return 40
  if (asset.access_group === 'bound_entity_data') return 50
  if (asset.access_group === 'base_extension_data' && asset.data_shape === 'event') return 60
  if (asset.access_group === 'base_extension_data') return 70
  if (asset.access_group === 'reference_entity_data') return 80
  return accessGroupSortRank(asset.access_group)
}

function accessGroupSortRank(groupKey) {
  const ranks = {
    base_market_data: 1,
    base_extension_data: 2,
    entity_relation: 3,
    bound_entity_data: 4,
    reference_entity_data: 5,
  }
  return ranks[groupKey] || 99
}

function assetSourceLabel(asset) {
  return providerNodesForAsset(asset)
    .map((node) => node?.description || node?.name)
    .filter(Boolean)
    .join('、')
    || asset.provider_nodes.join('、')
    || asset.asset_id
}

function assetSubtitle(asset) {
  const description = String(asset?.description || '').trim()
  if (description && !description.includes('本地测试能力')) return description
  return `${entityLabel(asset.entity_id)} -> ${entityLabel(asset.target_entity_id)}`
}

function assetSourceDetailLabel(asset) {
  const nodes = providerNodesForAsset(asset)
  const details = nodes
    .map((node) => {
      const table = [node.database, node.table].filter(Boolean).join('.')
      return [node.name, table].filter((item) => item && item !== '-').join(' · ')
    })
    .filter(Boolean)
  return details.join(' / ') || asset.provider_nodes.join('、') || '-'
}

function providerNodesForAsset(asset) {
  return (asset?.provider_nodes || [])
    .map((name) => providerNodeByName.value.get(String(name || '').trim()))
    .filter(Boolean)
}

function primaryProviderNode(asset) {
  return providerNodesForAsset(asset)[0] || null
}

function providerNodeSnapshot(node) {
  if (!node) return null
  return {
    name: node.name || '',
    description: node.description || '',
    database: node.database || '',
    table: node.table || '',
    asset_type: node.asset_type || '',
    keys: clone(node.keys || {}),
    source_fields: nodeSourceFields(node),
    field_facts: clone(node.field_facts || {}),
  }
}

function nodeSourceFields(node) {
  return uniqueList([
    ...(Array.isArray(node?.source_fields) ? node.source_fields : []),
    ...(Array.isArray(node?.fields) ? node.fields : []),
  ])
}

function fieldFact(asset, field) {
  const sourceField = String(field?.source || field?.name || '').trim()
  if (!sourceField) return {}
  for (const node of providerNodesForAsset(asset)) {
    const facts = normalizeFieldFacts(node?.field_facts)
    if (facts[sourceField]) return facts[sourceField]
  }
  return {}
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

function fieldDbType(asset, field) {
  const explicit = String(field?.db_type || field?.database_type || '').trim()
  if (explicit) return simplifyDatabaseType(explicit, field)
  const fact = fieldFact(asset, field)
  return simplifyDatabaseType(fact?.db_type || fact?.database_type || '', field)
}

function simplifyDatabaseType(value, field = null) {
  let text = String(value || '').trim()
  if (!text || text === '-' || text === '加载中') return text

  let changed = true
  while (changed) {
    changed = false
    const wrapped = text.match(/^(?:Nullable|LowCardinality)\((.*)\)$/i)
    if (wrapped) {
      text = wrapped[1].trim()
      changed = true
    }
  }

  const fieldName = String(field?.name || field?.source || '').trim().toLowerCase()
  if (/^bool(?:ean)?$/i.test(text)) return 'Bool'
  if (/^(is_|has_|can_|should_)/.test(fieldName) && /u?int(?:8|16|32|64)?/i.test(text)) return 'Bool'
  if (/^array\(/i.test(text)) return 'Array'
  if (/^map\(/i.test(text) || /^tuple\(/i.test(text) || /^object/i.test(text)) return 'Object'
  if (/datetime/i.test(text)) return 'DateTime'
  if (/^date/i.test(text)) return 'Date'
  if (/string|fixedstring|uuid|enum/i.test(text)) return 'String'
  if (/float|decimal|numeric/i.test(text)) return 'Float'
  if (/u?int/i.test(text)) return 'Int'
  return text
}

function uniqueList(values) {
  return [...new Set(values.map((item) => String(item || '').trim()).filter(Boolean))]
}

function entityLabel(entityId) {
  return ENTITY_OPTIONS.find((item) => item.value === entityId)?.label || entityId || '-'
}

function assetTypeLabel(asset) {
  if (asset.access_type === 'relation_data') return '关系数据'
  if (asset.asset_group === 'market' && asset.market_grain === 'daily') return '日线数据'
  if (asset.asset_group === 'market' && asset.market_grain === 'minute') return '分钟数据'
  if (asset.data_shape === 'event') return '事件数据'
  return asset.data_shape || asset.access_type || '-'
}

function inferFieldUsage(fieldName) {
  const name = String(fieldName || '').toLowerCase()
  if (name === 'code' || name.endsWith('_code') || name.includes('date') || name.includes('time')) return 'display'
  if (name.startsWith('is_') || name.startsWith('has_') || name.includes('flag')) return 'filter'
  if (name.includes('name') || name.includes('type') || name.includes('industry') || name.includes('reason')) return 'group'
  if (/(amount|volume|close|open|high|low|cap|rate|ratio|share|pe|pb|score|factor|weight|value)$/.test(name)) return 'factor'
  return 'auto'
}

function groupPolicy(groupKey) {
  if (!draftMode.access_groups[groupKey]) {
    draftMode.access_groups[groupKey] = { enabled: true, notes: '' }
  }
  return draftMode.access_groups[groupKey]
}

function assetPolicy(assetId) {
  if (!draftMode.asset_policies[assetId]) {
    draftMode.asset_policies[assetId] = { enabled: true, notes: '', fields: {} }
  }
  return draftMode.asset_policies[assetId]
}

function fieldPolicy(assetId, fieldName) {
  const policy = assetPolicy(assetId)
  if (!policy.fields) policy.fields = {}
  if (!policy.fields[fieldName]) {
    policy.fields[fieldName] = {
      enabled: true,
      usage: inferFieldUsage(fieldName),
      alias: '',
      notes: '',
    }
  }
  return policy.fields[fieldName]
}

function isAssetEffectivelyEnabled(asset) {
  return assetPolicy(asset.asset_id).enabled
}

function usageLabel(value) {
  return FIELD_USAGE_OPTIONS.find((item) => item.value === value)?.label || value || '自动判断'
}

function fieldChoiceValue(asset, field) {
  return `${asset.asset_id}${FIELD_PICKER_SEPARATOR}${field.name}`
}

function parseFieldChoiceValue(value) {
  const raw = String(value || '')
  const separatorIndex = raw.indexOf(FIELD_PICKER_SEPARATOR)
  if (separatorIndex < 0) return { assetId: '', fieldName: '' }
  return {
    assetId: raw.slice(0, separatorIndex),
    fieldName: raw.slice(separatorIndex + FIELD_PICKER_SEPARATOR.length),
  }
}

function assetSelectableFields(asset) {
  const rows = new Map()
  for (const field of asset.fields || []) {
    if (!field?.name) continue
    const fact = fieldFact(asset, field)
    rows.set(field.name, field)
    if (fact && Object.keys(fact).length) {
      rows.set(field.name, {
        ...field,
        binding_role: field.binding_role || String(fact.role || fact.field_role || '').trim() || 'value',
      })
    }
  }
  for (const binding of asset.provider_bindings || []) {
    const fieldBindings = binding?.field_bindings || {}
    for (const role of SELECTABLE_BINDING_ROLES) {
      const fieldName = String(fieldBindings[role] || '').trim()
      if (!fieldName || rows.has(fieldName)) continue
      rows.set(fieldName, {
        name: fieldName,
        source: fieldName,
        label: fieldName,
        binding_role: role,
      })
    }
  }
  return Array.from(rows.values())
}

function requirementFieldCandidates(row) {
  return stockSubjectAssets(row)
    .flatMap((asset) =>
      assetSelectableFields(asset).map((field) => ({
        value: fieldChoiceValue(asset, field),
        label: `${realFieldPath(asset, field)} / ${asset.name}`,
        realPath: realFieldPath(asset, field),
        bindingRole: field.binding_role || '',
        asset,
        field,
        exact: fieldMatchesRequirement(field, row, asset),
      }))
    )
}

function requirementFieldLabel(row) {
  if (!row?.match?.field) return '选择字段'
  return realFieldPath(row.match.asset, row.match.field)
}

function requirementFieldMeta(row) {
  if (!row?.match?.asset || !row?.match?.field) return '未绑定字段'
  return `${row.match.field.label || row.match.field.name} · ${row.match.asset.name}`
}

function requirementFieldActionLabel(row) {
  return row?.match?.asset && row?.match?.field ? '更换' : '选择'
}

function realFieldPath(asset, field) {
  const sourceField = String(field?.source || field?.name || '').trim()
  const providerNode = Array.isArray(asset?.provider_nodes)
    ? String(asset.provider_nodes[0] || '').trim()
    : ''
  const sourceNode = providerNode || String(asset?.asset_id || '').trim()
  if (!sourceNode) return sourceField
  if (!sourceField) return sourceNode
  return `${sourceNode}.${sourceField}`
}

function runtimeRequirementRealField(row) {
  if (!row?.match?.asset || !row?.match?.field) return '未绑定'
  return realFieldPath(row.match.asset, row.match.field)
}

function openRequirementFieldPicker(row) {
  fieldPicker.rowKey = row.__rowKey
  fieldPicker.query = ''
  fieldPicker.selectedValue = row?.match?.asset && row?.match?.field
    ? fieldChoiceValue(row.match.asset, row.match.field)
    : ''
  fieldPicker.visible = true
}

function selectFieldPickerOption(option) {
  fieldPicker.selectedValue = option.value
}

function applyFieldPickerSelection() {
  const row = fieldPickerRow.value
  if (!row) return
  const { assetId, fieldName } = parseFieldChoiceValue(fieldPicker.selectedValue)
  const asset = normalizedAssets.value.find((item) => item.asset_id === assetId)
  const field = asset ? assetSelectableFields(asset).find((item) => item.name === fieldName) : null
  if (!asset || !field) return
  applyRequirementField({ ...row, match: { asset, field } })
  setRequirementBinding(row, asset, field)
  const policy = fieldPolicy(asset.asset_id, field.name)
  if (field.name !== row.field) {
    policy.alias = row.field
  }
  fieldPicker.visible = false
  ElMessage.success({ message: `${row.label} 已绑定到 ${field.label || field.name}`, duration: 1800 })
}

function requirementAssetLabel(row) {
  return row?.match?.asset?.name || '未匹配'
}

function requirementSourceLabel(row) {
  const asset = row?.match?.asset
  if (!asset) return '未在当前实体资产中找到'
  return asset.provider_nodes.join('、') || asset.asset_id
}

function applyRequirementField(row) {
  if (!row?.match?.asset || !row?.match?.field) return
  const accessGroup = classifyAsset(row.match.asset)
  groupPolicy(accessGroup).enabled = true
  assetPolicy(row.match.asset.asset_id).enabled = true
  const policy = fieldPolicy(row.match.asset.asset_id, row.match.field.name)
  policy.enabled = true
  policy.usage = row.usage || policy.usage || inferFieldUsage(row.match.field.name)
  setRequirementBinding(row, row.match.asset, row.match.field)
}

function applyRequirementGroup(group) {
  for (const field of group.fields) {
    if (!field.match?.asset || !field.match?.field) continue
    const accessGroup = classifyAsset(field.match.asset)
    groupPolicy(accessGroup).enabled = true
    assetPolicy(field.match.asset.asset_id).enabled = true
    const policy = fieldPolicy(field.match.asset.asset_id, field.match.field.name)
    policy.enabled = true
    policy.usage = field.usage || policy.usage || inferFieldUsage(field.match.field.name)
    setRequirementBinding(field, field.match.asset, field.match.field)
  }
  ElMessage.success({ message: `${group.title} 已应用到当前模式`, duration: 1800 })
}

function applyAllMatchedRequirements() {
  for (const row of requirementFieldRows.value) {
    applyRequirementField(row)
  }
  ElMessage.success({ message: '已应用所有可匹配字段', duration: 1800 })
}

function openEntityAssetsPage() {
  navigateTo('/extensions-lab')
}

function setAssetEnabled(assetId, enabled) {
  assetPolicy(assetId).enabled = Boolean(enabled)
}

function setEntityAssetsEnabled(entityId, enabled) {
  for (const asset of assetAccessRows.value) {
    if (asset.entity_id !== entityId) continue
    setAssetEnabled(asset.asset_id, enabled)
  }
}

function setRequirementBinding(row, asset, field) {
  if (!row?.group_key || !row?.field || !asset?.asset_id || !field?.name) return
  if (!draftMode.requirement_bindings[row.group_key]) {
    draftMode.requirement_bindings[row.group_key] = {}
  }
  draftMode.requirement_bindings[row.group_key][row.field] = buildRequirementBinding(row, asset, field)
}

function buildCompiledModeConfig() {
  const requirementBindings = {}
  for (const row of requirementFieldRows.value) {
    if (!requirementBindings[row.group_key]) requirementBindings[row.group_key] = {}
    requirementBindings[row.group_key][row.field] = buildRequirementBinding(
      row,
      row.match?.asset,
      row.match?.field,
    )
  }
  const validation = buildValidation(requirementBindings)
  return {
    requirement_bindings: requirementBindings,
    runtime_contract: buildRuntimeContract(requirementBindings, validation),
    validation,
    source_snapshot: buildSourceSnapshot(),
  }
}

function buildRequirementBinding(row, asset, field) {
  const providerBinding = firstProviderBinding(asset)
  const fieldBindings = providerBinding?.field_bindings || {}
  const sourceField = String(field?.source || field?.name || '').trim()
  const providerNode = providerBinding?.provider_node || asset?.provider_nodes?.[0] || ''
  const node = providerNode ? providerNodeByName.value.get(providerNode) : primaryProviderNode(asset)
  return {
    field: row.field,
    label: row.label,
    group_key: row.group_key,
    group_title: row.group_title,
    level: row.group_level,
    required: row.required !== false,
    status: row.status,
    usage: row.usage || 'auto',
    asset_id: asset?.asset_id || '',
    asset_name: asset?.name || '',
    entity_id: asset?.entity_id || '',
    target_entity_id: asset?.target_entity_id || '',
    access_type: asset?.access_type || '',
    asset_group: asset?.asset_group || '',
    market_grain: asset?.market_grain || '',
    data_shape: asset?.data_shape || '',
    provider_node: providerNode,
    provider_nodes: asset?.provider_nodes || [],
    source_database: node?.database || '',
    source_table: node?.table || '',
    field_name: field?.name || '',
    source_field: sourceField,
    field_label: field?.label || field?.name || '',
    database_type: asset && field ? fieldDbType(asset, field) : '',
    real_field: asset && field ? realFieldPath(asset, field) : '',
    binding_role: field?.binding_role || 'value',
    entity_field: fieldBindings.entity || '',
    time_field: fieldBindings.time || '',
    event_time_field: fieldBindings.event_time || '',
    announce_date_field: fieldBindings.announce_date || '',
    report_period_field: fieldBindings.report_period || '',
    group_field: fieldBindings.group || '',
    member_field: fieldBindings.member || '',
    start_field: fieldBindings.start || '',
    end_field: fieldBindings.end || '',
  }
}

function firstProviderBinding(asset) {
  return Array.isArray(asset?.provider_bindings) ? asset.provider_bindings[0] : null
}

function buildValidation(requirementBindings) {
  const issues = []
  let required = 0
  let ready = 0
  let optionalMissing = 0
  for (const group of Object.values(requirementBindings)) {
    for (const binding of Object.values(group)) {
      if (binding.required) required += 1
      if (binding.status === 'ready') {
        ready += 1
        continue
      }
      if (!binding.required && binding.status === 'missing') {
        optionalMissing += 1
        continue
      }
      issues.push({
        severity: binding.required ? 'error' : 'warning',
        code: binding.status === 'missing' ? 'MISSING_FIELD' : 'DISABLED_FIELD',
        group_key: binding.group_key,
        field: binding.field,
        label: binding.label,
        message: `${binding.group_title} / ${binding.label} 未满足`,
        status: binding.status,
      })
    }
  }
  return {
    ok: !issues.some((item) => item.severity === 'error'),
    summary: {
      total: requirementFieldRows.value.length,
      required,
      ready,
      missing: issues.length,
      optional_missing: optionalMissing,
    },
    issues,
  }
}

function buildRuntimeContract(requirementBindings, validation) {
  const requirementGroups = {}
  for (const group of modeRequirementGroups.value) {
    requirementGroups[group.key] = {
      title: group.title,
      level: group.level,
      asset_group: group.asset_group || '',
      market_grain: group.market_grain || '',
      data_shape: group.data_shape || '',
      fields: requirementBindings[group.key] || {},
    }
  }
  const assets = assetAccessRows.value
    .filter((asset) => asset.enabled && assetPolicy(asset.asset_id).enabled)
    .map((asset) => buildRuntimeAsset(asset))
  return {
    version: 1,
    mode_id: draftMode.mode_id,
    mode_name: draftMode.mode_name,
    mode_kind: draftMode.mode_kind,
    base_entity: draftMode.base_entity,
    enabled: draftMode.enabled,
    settings: normalizeModeSettings(draftMode.mode_settings),
    requirement_groups: requirementGroups,
    asset_access: assets,
    data_sources: {
      panels: assets.filter((asset) => asset.data_shape === 'time_series'),
      events: assets.filter((asset) => asset.data_shape === 'event'),
      relations: assets.filter((asset) => asset.access_type === 'relation_data' || asset.data_shape === 'relation'),
      references: assets.filter((asset) => asset.access_group === 'reference_entity_data'),
    },
    validation_summary: validation.summary,
  }
}

function buildRuntimeAsset(asset) {
  const fields = asset.fields
    .filter((field) => fieldPolicy(asset.asset_id, field.name).enabled)
    .map((field) => ({
      name: field.name,
      source: field.source,
      label: field.label,
      database_type: fieldDbType(asset, field),
      usage: fieldPolicy(asset.asset_id, field.name).usage,
      alias: fieldPolicy(asset.asset_id, field.name).alias,
    }))
  return {
    asset_id: asset.asset_id,
    name: asset.name,
    access_group: asset.access_group,
    entity_id: asset.entity_id,
    target_entity_id: asset.target_entity_id,
    access_type: asset.access_type,
    asset_group: asset.asset_group,
    market_grain: asset.market_grain,
    data_shape: asset.data_shape,
    provider_nodes: asset.provider_nodes,
    source_nodes: providerNodesForAsset(asset).map(providerNodeSnapshot).filter(Boolean),
    provider_bindings: asset.provider_bindings,
    fields,
  }
}

function buildSourceSnapshot() {
  return {
    generated_at: new Date().toISOString(),
    data_asset_count: normalizedAssets.value.length,
    data_asset_ids: normalizedAssets.value.map((asset) => asset.asset_id).sort(),
    mode_profile_count: modeProfiles.value.length,
  }
}

async function refresh() {
  await Promise.all([loadWorkspace(), loadDataAssets(), loadModeDataAccess()])
  hydrateDraft()
  ElMessage.success({ message: '模式配置已刷新', duration: 1800 })
}

async function saveCurrentMode() {
  if (!selectedMode.value) return
  ensureDraftPolicies()
  Object.assign(draftMode, buildCompiledModeConfig())
  await saveModeDataAccess(clone(draftMode))
  ElMessage.success({ message: 'v3 模式配置已保存', duration: 1800 })
}

function clone(value) {
  return JSON.parse(JSON.stringify(value || {}))
}

onMounted(async () => {
  await Promise.all([loadWorkspace(), loadDataAssets(), loadModeDataAccess()])
})

watch(
  modeProfiles,
  () => {
    if (selectedModeId.value && modeProfiles.value.some((item) => item.mode_id === selectedModeId.value)) return
    const discreteStock = modeProfiles.value.find((item) => item.mode_id === 'strategy_modes.discrete_stock')
    selectedModeId.value = discreteStock?.mode_id || modeProfiles.value[0]?.mode_id || ''
  },
  { immediate: true }
)

watch(
  [selectedMode, modeDataAccess, enabledAssets],
  () => hydrateDraft(),
  { immediate: true }
)
</script>

<template>
  <section class="mode-config-page">
    <header class="mode-config-header">
      <div>
        <p>Mode Config V3</p>
        <h1>模式数据接入配置</h1>
        <span>基于实体、数据资产和关系配置不同模式的数据接入策略。</span>
      </div>
      <div class="header-actions">
        <el-button plain :loading="loading" @click="refresh">刷新</el-button>
        <el-button type="primary" :loading="saving" :disabled="!selectedMode" @click="saveCurrentMode">保存配置</el-button>
      </div>
    </header>

    <section class="mode-config-layout" v-loading="loading">
      <aside class="mode-config-sidebar">
        <div class="sidebar-title">
          <strong>模式列表</strong>
          <span>{{ modeProfiles.length }} 个模式</span>
        </div>
        <section v-for="group in modeGroups" :key="group.key" class="mode-group">
          <div class="mode-group-title">{{ group.title }}</div>
          <button
            v-for="mode in group.modes"
            :key="mode.mode_id"
            class="mode-option"
            :class="{ active: selectedModeId === mode.mode_id }"
            type="button"
            @click="selectedModeId = mode.mode_id"
          >
            <strong>{{ mode.mode_name }}</strong>
            <span>{{ mode.mode_id }}</span>
          </button>
        </section>
      </aside>

      <main v-if="selectedMode" class="mode-config-main">
        <section class="config-workspace">
          <div class="mode-config-toolbar">
            <div class="mode-identity">
              <strong>{{ selectedMode.mode_name }}</strong>
              <span>{{ selectedMode.mode_id }}</span>
            </div>
            <div class="mode-toolbar-item">
              <span>中心实体</span>
              <strong>{{ entityLabel(draftMode.base_entity) }}</strong>
            </div>
            <div class="mode-toolbar-item">
              <span>模式启用</span>
              <el-switch v-model="draftMode.enabled" />
            </div>
            <div class="mode-toolbar-item">
              <span>必要字段</span>
              <strong>{{ validationSummary.ready }} / {{ validationSummary.total }}</strong>
            </div>
            <div class="mode-toolbar-item warning">
              <span>待处理</span>
              <strong>{{ validationSummary.missing }}</strong>
            </div>
          </div>

          <el-tabs v-model="activeSection" class="mode-config-tabs">
            <el-tab-pane label="策略必要字段" name="requirements">
              <div class="mode-tab-body">
                <div class="block-title">
                  <div>
                    <h3>策略必要字段</h3>
                    <p>每一行就是模式运行前必须确认的数据需求，绑定到股票实体已经接入的 real 字段。</p>
                  </div>
                  <div class="block-title-actions">
                    <el-button type="primary" plain size="small" @click="applyAllMatchedRequirements">应用可匹配字段</el-button>
                  </div>
                </div>

                <div class="requirement-table-shell">
                  <el-table
                    :data="requirementFieldRows"
                    row-key="__rowKey"
                    height="430"
                    class="compact-table requirement-table"
                    empty-text="当前模式没有需求字段"
                  >
                  <el-table-column label="分类" min-width="150" show-overflow-tooltip>
                    <template #default="{ row }">
                      <div class="requirement-category-cell">
                        <strong>{{ row.group_title }}</strong>
                        <el-tag
                          size="small"
                          :type="row.required === false ? 'info' : 'success'"
                          effect="plain"
                        >
                          {{ row.group_level }}
                        </el-tag>
                      </div>
                    </template>
                  </el-table-column>
                  <el-table-column label="字段" min-width="170" show-overflow-tooltip>
                    <template #default="{ row }">
                      <strong class="field-name">{{ row.label }}</strong>
                      <span class="muted-code">{{ row.field }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column label="状态" width="104">
                    <template #default="{ row }">
                      <el-tag size="small" :type="row.statusType" effect="plain">
                        {{ row.statusLabel }}
                      </el-tag>
                    </template>
                  </el-table-column>
                  <el-table-column label="股票实体字段" min-width="230">
                    <template #default="{ row }">
                      <button
                        class="field-picker-trigger"
                        :class="{ empty: !row.match }"
                        type="button"
                        @click="openRequirementFieldPicker(row)"
                      >
                        <span class="field-picker-trigger-main">
                          <strong>{{ requirementFieldLabel(row) }}</strong>
                          <em>{{ requirementFieldMeta(row) }}</em>
                        </span>
                        <span class="field-picker-trigger-action">
                          {{ requirementFieldActionLabel(row) }}
                        </span>
                      </button>
                    </template>
                  </el-table-column>
                  <el-table-column label="来源资产" min-width="220" show-overflow-tooltip>
                    <template #default="{ row }">
                      <strong class="field-name">{{ requirementAssetLabel(row) }}</strong>
                      <span class="muted-code">{{ requirementSourceLabel(row) }}</span>
                    </template>
                  </el-table-column>
                  </el-table>
                </div>
              </div>
            </el-tab-pane>

            <el-tab-pane label="实体资产接入" name="assets">
              <div class="mode-tab-body">
                <div class="block-title">
                  <div>
                    <h3>实体资产接入</h3>
                    <p>按实体确认当前模式能读取哪些数据资产；接入方式只作为每行说明。</p>
                  </div>
                  <div class="block-title-actions">
                    <el-button plain size="small" @click="openEntityAssetsPage">实体资产页</el-button>
                  </div>
                </div>

                <div class="asset-access-note">
                  <strong>实体视角。</strong>
                  <span>股票是中心实体；行业、指数、ETF 等实体通过关系、参考序列或扩展数据进入当前模式。</span>
                </div>

                <div class="asset-access-sections">
                  <section
                    v-for="section in assetAccessEntitySections"
                    :key="section.entity_id"
                    class="asset-access-section"
                  >
                    <div class="asset-section-head">
                      <div>
                        <div class="asset-section-title-row">
                          <h4>{{ section.title }}</h4>
                          <el-tag size="small" effect="plain">{{ section.role_label }}</el-tag>
                        </div>
                        <p>{{ section.description }}</p>
                      </div>
                      <div class="asset-section-actions">
                        <span>已启用 {{ section.active_count }} / {{ section.total_count }}</span>
                        <el-switch
                          :model-value="section.enabled"
                          @change="(value) => setEntityAssetsEnabled(section.entity_id, value)"
                        />
                      </div>
                    </div>

                    <el-table
                      :data="section.rows"
                      row-key="asset_id"
                      class="compact-table requirement-table asset-access-table"
                      empty-text="暂无实体资产"
                    >
                      <el-table-column label="启用" width="74" align="center">
                        <template #default="{ row }">
                          <el-switch
                            :model-value="row.mode_enabled"
                            :disabled="row.enabled === false"
                            @change="(value) => setAssetEnabled(row.asset_id, value)"
                          />
                        </template>
                      </el-table-column>
                      <el-table-column label="数据资产" min-width="220" show-overflow-tooltip>
                        <template #default="{ row }">
                          <strong class="field-name">{{ row.name }}</strong>
                          <span class="muted-code">{{ assetSubtitle(row) }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="数据类型" width="130" show-overflow-tooltip>
                        <template #default="{ row }">
                          <strong class="field-name">{{ assetTypeLabel(row) }}</strong>
                          <span class="muted-code">{{ row.market_grain || row.data_shape || '-' }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="接入方式" width="120">
                        <template #default="{ row }">
                          <el-tag size="small" effect="plain">{{ row.access_method_label }}</el-tag>
                        </template>
                      </el-table-column>
                      <el-table-column label="对当前模式的作用" min-width="300" show-overflow-tooltip>
                        <template #default="{ row }">
                          <strong class="field-name">{{ entityLabel(row.entity_id) }} -> {{ entityLabel(row.target_entity_id) }}</strong>
                          <span class="muted-code">{{ row.access_description }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="来源 real 表" min-width="260" show-overflow-tooltip>
                        <template #default="{ row }">
                          <strong class="field-name">{{ row.source_label }}</strong>
                          <span class="muted-code">{{ row.source_detail_label }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="字段" width="98">
                        <template #default="{ row }">
                          {{ row.enabled_field_count }} / {{ row.total_field_count }}
                        </template>
                      </el-table-column>
                    </el-table>
                  </section>
                </div>
              </div>
            </el-tab-pane>

            <el-tab-pane label="模式参数" name="settings">
              <div class="mode-tab-body">
                <div class="block-title">
                  <div>
                    <h3>模式参数</h3>
                    <p>这些是模式运行默认值；不修改也会跟随模式配置一起保存。</p>
                  </div>
                </div>

                <div class="settings-grid">
                <section class="settings-panel">
                  <div class="settings-panel-title">
                    <strong>交易日历</strong>
                    <el-tag size="small" effect="plain">默认</el-tag>
                  </div>
                  <div class="settings-form-grid">
                    <label class="settings-field">
                      <span>日历来源</span>
                      <el-select v-model="draftMode.mode_settings.calendar.source">
                        <el-option
                          v-for="item in CALENDAR_SOURCE_OPTIONS"
                          :key="item.value"
                          :label="item.label"
                          :value="item.value"
                        />
                      </el-select>
                    </label>
                    <label class="settings-field">
                      <span>日历节点</span>
                      <el-input v-model="draftMode.mode_settings.calendar.node" placeholder="trade_calendar_real" />
                    </label>
                    <label class="settings-field">
                      <span>市场</span>
                      <el-input v-model="draftMode.mode_settings.calendar.market" placeholder="CN_A" />
                    </label>
                  </div>
                </section>

                <section class="settings-panel">
                  <div class="settings-panel-title">
                    <strong>默认基准</strong>
                    <el-tag size="small" effect="plain">默认</el-tag>
                  </div>
                  <div class="settings-form-grid">
                    <label class="settings-field">
                      <span>基准代码</span>
                      <el-input v-model="draftMode.mode_settings.benchmark.symbol" placeholder="000300.SH" />
                    </label>
                    <label class="settings-field">
                      <span>基准名称</span>
                      <el-input v-model="draftMode.mode_settings.benchmark.name" placeholder="沪深300" />
                    </label>
                    <label class="settings-field">
                      <span>基准来源</span>
                      <el-input v-model="draftMode.mode_settings.benchmark.source" placeholder="index_daily_real" />
                    </label>
                  </div>
                </section>

                <section class="settings-panel">
                  <div class="settings-panel-title">
                    <strong>手续费与滑点</strong>
                    <el-tag size="small" effect="plain">默认</el-tag>
                  </div>
                  <div class="settings-form-grid compact">
                    <label class="settings-field">
                      <span>买入佣金率</span>
                      <el-input-number
                        v-model="draftMode.mode_settings.costs.buy_commission_rate"
                        :min="0"
                        :step="0.0001"
                        :precision="6"
                        controls-position="right"
                      />
                    </label>
                    <label class="settings-field">
                      <span>卖出佣金率</span>
                      <el-input-number
                        v-model="draftMode.mode_settings.costs.sell_commission_rate"
                        :min="0"
                        :step="0.0001"
                        :precision="6"
                        controls-position="right"
                      />
                    </label>
                    <label class="settings-field">
                      <span>印花税率</span>
                      <el-input-number
                        v-model="draftMode.mode_settings.costs.stamp_tax_rate"
                        :min="0"
                        :step="0.0001"
                        :precision="6"
                        controls-position="right"
                      />
                    </label>
                    <label class="settings-field">
                      <span>最低佣金</span>
                      <el-input-number
                        v-model="draftMode.mode_settings.costs.min_commission"
                        :min="0"
                        :step="1"
                        :precision="2"
                        controls-position="right"
                      />
                    </label>
                    <label class="settings-field">
                      <span>滑点率</span>
                      <el-input-number
                        v-model="draftMode.mode_settings.costs.slippage_rate"
                        :min="0"
                        :step="0.0001"
                        :precision="6"
                        controls-position="right"
                      />
                    </label>
                  </div>
                  <p class="settings-hint">费率按小数保存，例如 0.0003 表示万分之三。</p>
                </section>
                </div>
              </div>
            </el-tab-pane>

            <el-tab-pane label="运行检查" name="contract">
              <div class="mode-tab-body">
                <div class="block-title">
                  <div>
                    <h3>运行检查</h3>
                    <p>保存前检查模式是否可运行，并确认运行层会读取哪些字段和实体资产。</p>
                  </div>
                </div>

                <div class="runtime-summary-grid">
                  <section
                    v-for="card in runtimeCheckCards"
                    :key="card.key"
                    class="runtime-summary-card"
                  >
                    <span>{{ card.label }}</span>
                    <div>
                      <strong>{{ card.value }}</strong>
                      <el-tag size="small" :type="card.type" effect="plain">{{ card.tag }}</el-tag>
                    </div>
                    <em>{{ card.hint }}</em>
                  </section>
                </div>

                <div class="runtime-config-grid">
                  <section class="contract-panel">
                    <div class="block-title contract-block-title">
                      <div>
                        <h3>数据源分类</h3>
                        <p>运行层会按这些类型组织读取入口。</p>
                      </div>
                    </div>
                    <el-table
                      :data="runtimeSourceRows"
                      height="220"
                      class="compact-table requirement-table"
                      empty-text="暂无数据源"
                    >
                      <el-table-column label="类型" width="110">
                        <template #default="{ row }">
                          <strong class="field-name">{{ row.label }}</strong>
                          <span class="muted-code">{{ row.count }} 项</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="说明" min-width="220" show-overflow-tooltip>
                        <template #default="{ row }">
                          <strong class="field-name">{{ row.description }}</strong>
                          <span class="muted-code">{{ row.assets }}</span>
                        </template>
                      </el-table-column>
                    </el-table>
                  </section>

                  <section class="contract-panel">
                    <div class="block-title contract-block-title">
                      <div>
                        <h3>检查结果</h3>
                        <p>必要字段缺失、字段被关闭或资产未启用时会出现在这里。</p>
                      </div>
                      <el-tag :type="compiledPreview.validation.ok ? 'success' : 'danger'" effect="plain">
                        {{ compiledPreview.validation.ok ? '可运行' : '需补齐' }}
                      </el-tag>
                    </div>
                    <div v-if="!validationIssues.length" class="runtime-ok-state">
                      <strong>当前模式可以运行</strong>
                      <span>必要字段已经绑定到股票实体资产，启用资产会写入运行合约。</span>
                    </div>
                    <el-table
                      v-else
                      :data="validationIssues"
                      height="220"
                      class="compact-table requirement-table"
                    >
                      <el-table-column label="级别" width="80">
                        <template #default="{ row }">
                          <el-tag size="small" :type="row.severity === 'error' ? 'danger' : 'warning'" effect="plain">
                            {{ row.severity === 'error' ? '错误' : '提醒' }}
                          </el-tag>
                        </template>
                      </el-table-column>
                      <el-table-column label="字段" min-width="150" show-overflow-tooltip>
                        <template #default="{ row }">
                          <strong class="field-name">{{ row.label }}</strong>
                          <span class="muted-code">{{ row.group_key }}.{{ row.field }}</span>
                        </template>
                      </el-table-column>
                      <el-table-column label="说明" min-width="220" show-overflow-tooltip>
                        <template #default="{ row }">
                          {{ row.message }}
                        </template>
                      </el-table-column>
                    </el-table>
                  </section>
                </div>

                <section class="contract-panel">
                  <div class="block-title contract-block-title">
                    <div>
                      <h3>必要字段绑定</h3>
                      <p>模式运行前必须确认的数据需求，以及它们最终绑定到哪个 real 字段。</p>
                    </div>
                  </div>
                  <el-table
                    :data="runtimeRequirementRows"
                    height="270"
                    class="compact-table requirement-table"
                    empty-text="暂无必要字段"
                  >
                    <el-table-column label="分类" min-width="140" show-overflow-tooltip>
                      <template #default="{ row }">
                        <strong class="field-name">{{ row.group_title }}</strong>
                        <span class="muted-code">{{ row.group_level }}</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="模式字段" min-width="150" show-overflow-tooltip>
                      <template #default="{ row }">
                        <strong class="field-name">{{ row.label }}</strong>
                        <span class="muted-code">{{ row.field }}</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="real 字段" min-width="240" show-overflow-tooltip>
                      <template #default="{ row }">
                        <strong class="field-name">{{ row.runtime_real_field }}</strong>
                        <span class="muted-code">{{ row.runtime_asset_name }}</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="状态" width="96">
                      <template #default="{ row }">
                        <el-tag size="small" :type="row.statusType" effect="plain">
                          {{ row.statusLabel }}
                        </el-tag>
                      </template>
                    </el-table-column>
                  </el-table>
                </section>

                <section class="contract-panel">
                  <div class="block-title contract-block-title">
                    <div>
                      <h3>启用实体资产</h3>
                      <p>保存后进入运行合约的数据资产清单。</p>
                    </div>
                  </div>
                  <el-table
                    :data="runtimeAssetRows"
                    height="240"
                    class="compact-table requirement-table"
                    empty-text="暂无启用资产"
                  >
                    <el-table-column label="实体" width="100">
                      <template #default="{ row }">
                        {{ entityLabel(row.entity_id) }}
                      </template>
                    </el-table-column>
                    <el-table-column label="数据资产" min-width="210" show-overflow-tooltip>
                      <template #default="{ row }">
                        <strong class="field-name">{{ row.name }}</strong>
                        <span class="muted-code">{{ assetSubtitle(row) }}</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="接入方式" width="120">
                      <template #default="{ row }">
                        <el-tag size="small" effect="plain">{{ row.access_method_label }}</el-tag>
                      </template>
                    </el-table-column>
                    <el-table-column label="来源 real 表" min-width="230" show-overflow-tooltip>
                      <template #default="{ row }">
                        <strong class="field-name">{{ row.source_label }}</strong>
                        <span class="muted-code">{{ row.source_detail_label }}</span>
                      </template>
                    </el-table-column>
                    <el-table-column label="字段" width="96">
                      <template #default="{ row }">
                        {{ row.enabled_field_count }} / {{ row.total_field_count }}
                      </template>
                    </el-table-column>
                  </el-table>
                </section>

                <el-collapse class="contract-raw-collapse">
                  <el-collapse-item title="高级详情：运行合约 JSON（调试用）" name="json">
                    <pre class="contract-preview compact-json">{{ runtimeContractPreview }}</pre>
                  </el-collapse-item>
                </el-collapse>
              </div>
            </el-tab-pane>
          </el-tabs>
        </section>
      </main>

      <main v-else class="mode-config-main empty-main">
        <el-empty description="暂无模式配置" />
      </main>
    </section>

    <el-dialog
      v-model="fieldPicker.visible"
      title="选择股票实体字段"
      width="760px"
      class="field-picker-dialog"
      append-to-body
      destroy-on-close
    >
      <template v-if="fieldPickerRow">
        <div class="field-picker-summary">
          <div>
            <span>需求字段</span>
            <strong>{{ fieldPickerRow.label }}</strong>
            <em>{{ fieldPickerRow.field }} · {{ usageLabel(fieldPickerRow.usage) }}</em>
          </div>
          <el-tag :type="fieldPickerRow.statusType" effect="plain">{{ fieldPickerRow.statusLabel }}</el-tag>
        </div>

        <el-input
          v-model="fieldPicker.query"
          class="field-picker-search"
          clearable
          placeholder="搜索字段名、中文名、能力或 real 表"
        />

        <el-table
          :data="fieldPickerOptions"
          height="360"
          class="compact-table field-picker-table"
          empty-text="当前需求没有可选字段"
          @row-click="selectFieldPickerOption"
        >
          <el-table-column width="52">
            <template #default="{ row }">
              <span class="field-choice-dot" :class="{ active: row.value === fieldPicker.selectedValue }" />
            </template>
          </el-table-column>
          <el-table-column label="real 字段" min-width="240" show-overflow-tooltip>
            <template #default="{ row }">
              <strong class="field-name">{{ row.realPath }}</strong>
              <span class="muted-code">
                {{ row.field.label || row.field.name }}{{ row.bindingRole ? ` · ${row.bindingRole}` : '' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="所属能力" min-width="230" show-overflow-tooltip>
            <template #default="{ row }">
              <strong class="field-name">{{ row.asset.name }}</strong>
              <span class="muted-code">{{ row.asset.provider_nodes.join('、') || row.asset.asset_id }}</span>
            </template>
          </el-table-column>
          <el-table-column label="匹配" width="86">
            <template #default="{ row }">
              <el-tag v-if="row.exact" size="small" type="success" effect="plain">推荐</el-tag>
              <el-tag v-else size="small" effect="plain">可选</el-tag>
            </template>
          </el-table-column>
        </el-table>
      </template>

      <template #footer>
        <el-button @click="fieldPicker.visible = false">取消</el-button>
        <el-button type="primary" :disabled="!fieldPicker.selectedValue" @click="applyFieldPickerSelection">
          应用字段
        </el-button>
      </template>
    </el-dialog>
  </section>
</template>

<style scoped>
.mode-config-page {
  display: grid;
  gap: 14px;
  color: var(--text);
  overflow-anchor: none;
}

.mode-config-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  padding: 18px 20px;
  border: 1px solid rgba(15, 118, 110, 0.14);
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.9);
  box-shadow: var(--shadow-sm);
}

.mode-config-header p,
.mode-config-header h1,
.mode-config-header span,
.block-title h3,
.block-title p {
  margin: 0;
}

.mode-config-header p {
  font-size: 12px;
  font-weight: 800;
  color: var(--accent);
  text-transform: uppercase;
}

.mode-config-header h1 {
  margin-top: 4px;
  font-size: 24px;
  line-height: 1.2;
}

.mode-config-header span {
  display: block;
  margin-top: 6px;
  color: var(--muted);
  font-size: 13px;
}

.header-actions {
  display: flex;
  gap: 10px;
}

.mode-config-layout {
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr);
  gap: 14px;
  min-height: 680px;
}

.mode-config-sidebar,
.mode-config-main {
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.92);
  box-shadow: var(--shadow-sm);
}

.mode-config-sidebar {
  align-self: start;
  display: grid;
  gap: 12px;
  padding: 14px;
  max-height: calc(100vh - 160px);
  overflow: auto;
}

.sidebar-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  color: var(--muted);
  font-size: 12px;
}

.sidebar-title strong {
  color: var(--text);
  font-size: 16px;
}

.mode-group {
  display: grid;
  gap: 8px;
}

.mode-group-title {
  padding: 4px 2px;
  color: var(--accent);
  font-size: 12px;
  font-weight: 800;
}

.mode-option {
  display: grid;
  gap: 5px;
  width: 100%;
  padding: 11px 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 8px;
  background: #fff;
  color: var(--text);
  text-align: left;
  cursor: pointer;
}

.mode-option:hover,
.mode-option.active {
  border-color: rgba(15, 118, 110, 0.34);
  background: rgba(15, 118, 110, 0.06);
}

.mode-option strong,
.mode-option span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mode-option span {
  color: var(--muted);
  font-size: 12px;
}

.mode-config-main {
  display: grid;
  align-content: start;
  gap: 14px;
  padding: 14px;
  min-width: 0;
}

.config-workspace {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 14px;
  align-content: start;
  align-items: start;
}

.mode-config-toolbar {
  display: grid;
  grid-template-columns: minmax(180px, 1.5fr) repeat(4, minmax(110px, 0.7fr));
  gap: 10px;
  align-items: stretch;
}

.mode-identity,
.mode-toolbar-item {
  min-width: 0;
  padding: 10px 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 8px;
  background: #fff;
}

.mode-identity,
.mode-toolbar-item {
  display: grid;
  gap: 4px;
}

.mode-identity strong,
.mode-toolbar-item strong {
  min-width: 0;
  overflow: hidden;
  color: var(--text);
  font-size: 16px;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mode-identity span,
.mode-toolbar-item span {
  min-width: 0;
  overflow: hidden;
  color: var(--muted);
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mode-toolbar-item.warning strong {
  color: #b45309;
}

.mode-config-tabs {
  min-width: 0;
  overflow-anchor: none;
}

.mode-config-tabs :deep(.el-tabs__header) {
  margin: 0 0 12px;
}

.mode-config-tabs :deep(.el-tabs__nav-wrap::after) {
  height: 1px;
  background: rgba(15, 118, 110, 0.12);
}

.mode-config-tabs :deep(.el-tabs__item) {
  height: 36px;
  color: var(--muted);
  font-size: 13px;
  font-weight: 800;
}

.mode-config-tabs :deep(.el-tabs__item.is-active) {
  color: var(--accent);
}

.mode-config-tabs :deep(.el-tab-pane) {
  padding-top: 0;
}

.mode-tab-body {
  display: grid;
  align-content: start;
  gap: 0;
  min-width: 0;
  margin-top: 0;
}

.access-config {
  display: grid;
  gap: 12px;
}

.block-title {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 10px;
}

.block-title-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.block-title h3 {
  font-size: 17px;
}

.block-title p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 12px;
}

.requirement-table-shell {
  overflow: hidden;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: #fff;
  box-shadow: var(--shadow-sm);
}

.asset-access-note {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 10px;
  padding: 10px 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 8px;
  background: rgba(15, 118, 110, 0.05);
  color: var(--muted);
  font-size: 12px;
}

.asset-access-note strong {
  flex: 0 0 auto;
  color: var(--text);
}

.asset-access-sections {
  display: grid;
  gap: 12px;
}

.asset-access-section {
  min-width: 0;
  overflow: hidden;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: #fff;
  box-shadow: var(--shadow-sm);
}

.asset-section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 11px 12px;
  border-bottom: 1px solid rgba(15, 118, 110, 0.1);
  background: rgba(15, 118, 110, 0.04);
}

.asset-section-head h4,
.asset-section-head p {
  margin: 0;
}

.asset-section-title-row {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.asset-section-head h4 {
  min-width: 0;
  overflow: hidden;
  color: var(--text);
  font-size: 14px;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.asset-section-head p {
  margin-top: 4px;
  color: var(--muted);
  font-size: 12px;
}

.asset-section-actions {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  gap: 10px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 800;
}

.asset-access-table {
  width: 100%;
}

.runtime-summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}

.runtime-summary-card {
  display: grid;
  gap: 6px;
  min-width: 0;
  padding: 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: #fff;
  box-shadow: var(--shadow-sm);
}

.runtime-summary-card > span {
  color: var(--muted);
  font-size: 12px;
  font-weight: 800;
}

.runtime-summary-card > div {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.runtime-summary-card strong {
  min-width: 0;
  overflow: hidden;
  color: var(--text);
  font-size: 20px;
  line-height: 1.15;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.runtime-summary-card em {
  min-width: 0;
  overflow: hidden;
  color: var(--muted);
  font-size: 12px;
  font-style: normal;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.runtime-config-grid,
.contract-grid {
  display: grid;
  grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
  gap: 12px;
  margin-bottom: 12px;
}

.contract-block-title {
  align-items: start;
  margin-bottom: 8px;
}

.runtime-ok-state {
  display: grid;
  place-content: center;
  gap: 8px;
  min-height: 220px;
  padding: 18px;
  border: 1px solid rgba(34, 197, 94, 0.22);
  border-radius: 8px;
  background: rgba(240, 253, 244, 0.88);
  color: #14532d;
  text-align: center;
}

.runtime-ok-state strong {
  font-size: 16px;
  line-height: 1.25;
}

.runtime-ok-state span {
  max-width: 360px;
  color: #3f5f49;
  font-size: 12px;
  line-height: 1.6;
}

.contract-raw-collapse {
  overflow: hidden;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: #fff;
  box-shadow: var(--shadow-sm);
}

.settings-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.settings-panel {
  min-width: 0;
  padding: 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: #fff;
  box-shadow: var(--shadow-sm);
}

.settings-panel-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 10px;
}

.settings-panel-title strong {
  color: var(--text);
  font-size: 14px;
}

.settings-form-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.settings-form-grid.compact {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.settings-field {
  display: grid;
  gap: 6px;
  min-width: 0;
}

.settings-field span {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}

.settings-field :deep(.el-select),
.settings-field :deep(.el-input-number) {
  width: 100%;
}

.settings-hint {
  margin: 10px 0 0;
  color: var(--muted);
  font-size: 12px;
}

.contract-panel {
  min-width: 0;
  padding: 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 10px;
  background: #fff;
  box-shadow: var(--shadow-sm);
  margin-bottom: 12px;
}

.contract-preview {
  height: 330px;
  margin: 0;
  padding: 12px;
  overflow: auto;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 8px;
  background: #0f172a;
  color: #dbeafe;
  font-size: 12px;
  line-height: 1.55;
  white-space: pre;
}

.contract-preview.compact-json {
  height: 300px;
  border-radius: 0 0 8px 8px;
}

.contract-raw-collapse :deep(.el-collapse-item__header) {
  padding: 0 12px;
  color: var(--text);
  font-size: 13px;
  font-weight: 800;
}

.contract-raw-collapse :deep(.el-collapse-item__content) {
  padding: 0 12px 12px;
}

.requirement-table :deep(.el-table__header-wrapper th) {
  background: rgba(15, 118, 110, 0.05);
  color: #314238;
  font-size: 12px;
  font-weight: 800;
}

.requirement-table :deep(.el-table__cell) {
  padding: 3px 0;
}

.requirement-table :deep(.el-table__row) {
  cursor: default;
}

.field-picker-trigger {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  width: 100%;
  min-height: 32px;
  padding: 4px 6px 4px 8px;
  border: 1px solid rgba(15, 118, 110, 0.22);
  border-radius: 6px;
  background: linear-gradient(180deg, rgba(15, 118, 110, 0.07), rgba(15, 118, 110, 0.035));
  color: var(--text);
  text-align: left;
  cursor: pointer;
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.45);
  transition: border-color 0.15s ease, background 0.15s ease, box-shadow 0.15s ease, transform 0.15s ease;
}

.field-picker-trigger:hover {
  border-color: rgba(15, 118, 110, 0.48);
  background: rgba(15, 118, 110, 0.1);
  box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.08);
}

.field-picker-trigger:active {
  transform: translateY(1px);
}

.field-picker-trigger:focus-visible {
  outline: 2px solid rgba(15, 118, 110, 0.5);
  outline-offset: 2px;
}

.field-picker-trigger.empty {
  border-style: dashed;
  background: rgba(255, 255, 255, 0.78);
}

.field-picker-trigger-main {
  display: grid;
  gap: 1px;
  min-width: 0;
}

.field-picker-trigger-main strong {
  font-size: 12px;
  line-height: 1.15;
}

.field-picker-trigger-main strong,
.field-picker-trigger-main em {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-picker-trigger-main em {
  color: var(--muted);
  font-size: 11px;
  font-style: normal;
  line-height: 1.1;
}

.field-picker-trigger-action {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 2px 6px;
  border-radius: 999px;
  background: rgba(15, 118, 110, 0.12);
  color: var(--accent);
  font-size: 11px;
  font-weight: 800;
}

.field-picker-trigger-action::after {
  content: ">";
  font-size: 10px;
}

.requirement-category-cell {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
}

.requirement-category-cell strong {
  min-width: 0;
  overflow: hidden;
  color: #314238;
  font-size: 12px;
  font-weight: 800;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-picker-summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 12px;
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 8px;
  background: rgba(15, 118, 110, 0.05);
}

.field-picker-summary > div {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.field-picker-summary span,
.field-picker-summary em {
  color: var(--muted);
  font-size: 12px;
  font-style: normal;
}

.field-picker-search {
  margin: 12px 0;
}

.field-picker-table :deep(.el-table__row) {
  cursor: pointer;
}

.field-choice-dot {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid rgba(15, 118, 110, 0.28);
  border-radius: 50%;
  vertical-align: middle;
}

.field-choice-dot.active {
  border-width: 4px;
  border-color: var(--accent);
}

.muted-code {
  color: var(--muted);
  font-size: 12px;
}

.field-name,
.muted-code {
  display: block;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.compact-table :deep(.el-table__cell) {
  padding: 5px 0;
}

.empty-main {
  min-height: 420px;
  place-items: center;
}

@media (max-width: 1280px) {
  .mode-config-layout,
  .config-workspace,
  .runtime-config-grid,
  .contract-grid,
  .settings-grid {
    grid-template-columns: 1fr;
  }

  .runtime-summary-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .mode-config-toolbar {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .settings-form-grid,
  .settings-form-grid.compact {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .mode-config-header,
  .header-actions,
  .block-title,
  .block-title-actions {
    flex-direction: column;
    align-items: stretch;
  }

  .requirement-table-shell {
    overflow-x: auto;
  }

  .mode-config-toolbar {
    grid-template-columns: 1fr;
  }

  .runtime-summary-grid {
    grid-template-columns: 1fr;
  }

  .settings-form-grid,
  .settings-form-grid.compact {
    grid-template-columns: 1fr;
  }
}
</style>
