import { computed, toRef } from 'vue'
import { ElMessage } from 'element-plus'
import { useRuntimeConfig, useState } from '#imports'

export function useWorkbench() {
  const config = useRuntimeConfig()
  const state = useState('workbench-state', createWorkbenchState)
  const loading = useState('workbench-loading', () => false)
  const saveMessage = useState('workbench-save-message', () => '等待加载工作区')
  const initialized = useState('workbench-initialized', () => false)
  const lastLoadedSignature = useState('workbench-last-loaded-signature', () => '')

  const workspace = toRef(state.value, 'workspace')
  const runtime = toRef(state.value, 'runtime')
  const graph = toRef(state.value, 'graph')
  const fields = toRef(state.value, 'fields')
  const schema = toRef(state.value, 'schema')
  const queryLab = toRef(state.value, 'queryLab')
  const nodeForm = toRef(state.value, 'nodeForm')
  const edgeForm = toRef(state.value, 'edgeForm')
  const fieldForm = toRef(state.value, 'fieldForm')

  const graphSummary = computed(() => ({
    nodes: graph.value.nodes.length,
    edges: graph.value.edges.length,
    fields: fields.value.length,
  }))

  const flowNodes = computed(() =>
    graph.value.nodes.map((node, index) => ({
      id: node.name,
      position: {
        x: 120 + (index % 3) * 320,
        y: 100 + Math.floor(index / 3) * 200,
      },
      data: node,
      type: 'default',
    }))
  )

  const flowEdges = computed(() =>
    graph.value.edges.map((edge, index) => ({
      id: `${edge.name}-${index}`,
      source: edge.from,
      target: edge.to,
      label: edge.time_binding?.mode || edge.relation_type,
      animated: edge.time_binding?.mode === 'asof',
      style: { stroke: '#25635f', strokeWidth: 2.2 },
      labelStyle: { fill: '#25635f', fontWeight: 700 },
    }))
  )

  const graphPreview = computed(() => toYaml({ nodes: graph.value.nodes, edges: graph.value.edges }))
  const fieldsPreview = computed(() => toYaml({ fields: fields.value }))

  async function api(path, options = {}) {
    const base = config.public.backendBase || ''
    const response = await fetch(`${base}${path}`, {
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
      ...options,
    })

    if (!response.ok) {
      throw new Error(await response.text())
    }

    return response.json()
  }

  async function notifyAction(successMessage, action) {
    try {
      const result = await action()
      if (successMessage) {
        ElMessage.success(successMessage)
      }
      return result
    } catch (error) {
      const message = error instanceof Error ? error.message : '操作失败'
      ElMessage.error(message)
      throw error
    }
  }

  async function ensureWorkspaceLoaded(force = false) {
    const signature = currentWorkspaceSignature(workspace.value)
    if (!force && initialized.value && lastLoadedSignature.value === signature) {
      return true
    }
    return loadWorkspace()
  }

  async function loadWorkspace() {
    loading.value = true
    try {
      const payload = await api(
        `/api/workspace?graph_path=${encodeURIComponent(workspace.value.graphPath)}&fields_path=${encodeURIComponent(workspace.value.fieldsPath)}&runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`
      )

      runtime.value.datasource = payload.runtime?.datasource || {}
      runtime.value.discovery = payload.runtime?.discovery || {}
      graph.value.nodes = payload.graph?.nodes || []
      graph.value.edges = payload.graph?.edges || []
      fields.value = payload.fields || []

      saveMessage.value = '工作区已载入'
      initialized.value = true
      lastLoadedSignature.value = currentWorkspaceSignature(workspace.value)

      await loadDatabases()
      return payload
    } catch (error) {
      saveMessage.value = `载入失败: ${error.message}`
      throw error
    } finally {
      loading.value = false
    }
  }

  async function saveWorkspace() {
    loading.value = true
    try {
      await api('/api/workspace', {
        method: 'POST',
        body: JSON.stringify({
          workspace: {
            graph_path: workspace.value.graphPath,
            fields_path: workspace.value.fieldsPath,
            runtime_path: workspace.value.runtimePath,
          },
          graph: graph.value,
          fields: fields.value,
        }),
      })

      saveMessage.value = '配置已保存到工作目录'
      return true
    } catch (error) {
      saveMessage.value = `保存失败: ${error.message}`
      throw error
    } finally {
      loading.value = false
    }
  }

  async function loadDatabases() {
    const payload = await api(`/api/schema/databases?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`)
    schema.value.databases = payload.items || []
  }

  async function selectDatabase(value) {
    schema.value.selectedDatabase = value
    schema.value.tables = []
    schema.value.selectedTable = ''
    schema.value.columns = []
    schema.value.selectedColumns = []

    const payload = await api(
      `/api/schema/tables?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(value)}`
    )
    schema.value.tables = payload.items || []
  }

  async function selectTable(name) {
    schema.value.selectedTable = name
    schema.value.columns = []
    schema.value.selectedColumns = []

    const payload = await api(
      `/api/schema/columns?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(schema.value.selectedDatabase)}&table=${encodeURIComponent(name)}`
    )
    schema.value.columns = payload.items || []
  }

  function toggleColumn(name) {
    if (schema.value.selectedColumns.includes(name)) {
      schema.value.selectedColumns = schema.value.selectedColumns.filter((item) => item !== name)
      return
    }

    schema.value.selectedColumns = [...schema.value.selectedColumns, name]
  }

  function clearSelectedColumns() {
    schema.value.selectedColumns = []
  }

  function inferNodeTemplate() {
    const selectedTable = String(schema.value.selectedTable || '')
    const allFields = schema.value.columns.map((item) => item.name)
    const selected = schema.value.selectedColumns.length ? schema.value.selectedColumns : allFields

    Object.assign(nodeForm.value, {
      editIndex: '',
      name: selectedTable.replace(/^ad_/, ''),
      table: `${schema.value.selectedDatabase}.${selectedTable}`,
      entity_keys: selected.includes('code') ? 'code' : selected.includes('market_code') ? 'market_code' : '',
      time_key: selected.includes('trade_time')
        ? 'trade_time'
        : selected.includes('trade_date')
          ? 'trade_date'
          : selected.includes('change_date')
            ? 'change_date'
            : '',
      grain: inferGrain(selectedTable),
      fields: selected.join(', '),
      description: `从 ${schema.value.selectedDatabase}.${selectedTable} 生成`,
    })
  }

  function addSelectedColumnsAsFields() {
    const sourceNode = nodeForm.value.name || String(schema.value.selectedTable || '').replace(/^ad_/, '')

    schema.value.selectedColumns.forEach((column) => {
      fields.value.push({
        standard_field: column,
        source_node: sourceNode,
        source_field: column,
        field_role: 'direct_field',
        resolver_type: 'direct',
        applies_to_grain: inferGrain(schema.value.selectedTable),
        depends_on: [],
        formula: null,
        notes: ['从数据库字段批量加入'],
      })
    })
  }

  function addSingleField(column) {
    Object.assign(fieldForm.value, {
      editIndex: '',
      standard_field: column,
      source_node: nodeForm.value.name || String(schema.value.selectedTable || '').replace(/^ad_/, ''),
      source_field: column,
      field_role: 'direct_field',
      resolver_type: 'direct',
      applies_to_grain: inferGrain(schema.value.selectedTable),
      depends_on: '',
      formula: '',
      notes: '从数据库字段直接加入',
    })
  }

  function saveNode() {
    const payload = {
      name: nodeForm.value.name.trim(),
      table: nodeForm.value.table.trim(),
      entity_keys: splitCsv(nodeForm.value.entity_keys),
      time_key: nodeForm.value.time_key.trim() || null,
      grain: nodeForm.value.grain.trim() || null,
      fields: splitCsv(nodeForm.value.fields),
      description: nodeForm.value.description.trim() || null,
    }

    upsert(graph.value.nodes, nodeForm.value.editIndex, payload)
    Object.assign(nodeForm.value, blankNode())
  }

  function saveEdge() {
    const [sourceStartField, sourceEndField] = splitCsv(edgeForm.value.source_range)
    const payload = {
      name: edgeForm.value.name.trim(),
      from: edgeForm.value.from.trim(),
      to: edgeForm.value.to.trim(),
      relation_type: edgeForm.value.relation_type,
      join_keys: parseJoinKeys(edgeForm.value.join_keys),
      time_binding: edgeForm.value.time_mode
        ? {
            mode: edgeForm.value.time_mode,
            base_time_field: edgeForm.value.base_time_field.trim() || null,
            base_time_cast: edgeForm.value.base_time_cast.trim() || null,
            source_time_field: edgeForm.value.source_time_field.trim() || null,
            source_start_field: sourceStartField || null,
            source_end_field: sourceEndField || null,
          }
        : null,
      description: edgeForm.value.description.trim() || null,
    }

    upsert(graph.value.edges, edgeForm.value.editIndex, payload)
    Object.assign(edgeForm.value, blankEdge())
  }

  function saveField() {
    const payload = {
      standard_field: fieldForm.value.standard_field.trim(),
      source_node: fieldForm.value.source_node.trim() || null,
      source_field: fieldForm.value.source_field.trim() || null,
      field_role: fieldForm.value.field_role.trim() || null,
      resolver_type: fieldForm.value.resolver_type,
      depends_on: splitCsv(fieldForm.value.depends_on),
      formula: fieldForm.value.formula.trim() || null,
      applies_to_grain: fieldForm.value.applies_to_grain.trim() || null,
      notes: splitLines(fieldForm.value.notes),
    }

    upsert(fields.value, fieldForm.value.editIndex, payload)
    Object.assign(fieldForm.value, blankField())
  }

  async function runNaturalLanguageQuery() {
    queryLab.value.error = ''
    try {
      const payload = await api('/api/query/nl', {
        method: 'POST',
        body: JSON.stringify({
          runtime_path: workspace.value.runtimePath,
          graph_path: workspace.value.graphPath,
          fields_path: workspace.value.fieldsPath,
          query: queryLab.value.naturalQuery,
        }),
      })

      queryLab.value.queryIntent = payload.query_intent
      queryLab.value.sql = payload.sql
      queryLab.value.result = payload
      return payload
    } catch (error) {
      queryLab.value.error = error.message
      queryLab.value.result = null
      throw error
    }
  }

  async function runSql() {
    queryLab.value.error = ''
    try {
      const payload = await api('/api/query/execute', {
        method: 'POST',
        body: JSON.stringify({
          runtime_path: workspace.value.runtimePath,
          sql: queryLab.value.sql,
        }),
      })

      queryLab.value.result = payload
      return payload
    } catch (error) {
      queryLab.value.error = error.message
      queryLab.value.result = null
      throw error
    }
  }

  function editNode(node, index) {
    Object.assign(nodeForm.value, {
      editIndex: String(index),
      name: node.name || '',
      table: node.table || '',
      entity_keys: (node.entity_keys || []).join(', '),
      time_key: node.time_key || '',
      grain: node.grain || '',
      fields: (node.fields || []).join(', '),
      description: node.description || '',
    })
  }

  function editEdge(edge, index) {
    Object.assign(edgeForm.value, {
      editIndex: String(index),
      name: edge.name || '',
      from: edge.from || '',
      to: edge.to || '',
      relation_type: edge.relation_type || 'direct',
      join_keys: (edge.join_keys || []).map((item) => `base:${item.base} -> source:${item.source}`).join('\n'),
      time_mode: edge.time_binding?.mode || '',
      base_time_field: edge.time_binding?.base_time_field || '',
      base_time_cast: edge.time_binding?.base_time_cast || '',
      source_time_field: edge.time_binding?.source_time_field || '',
      source_range: [edge.time_binding?.source_start_field, edge.time_binding?.source_end_field].filter(Boolean).join(', '),
      description: edge.description || '',
    })
  }

  function editField(field, index) {
    Object.assign(fieldForm.value, {
      editIndex: String(index),
      standard_field: field.standard_field || '',
      source_node: field.source_node || '',
      source_field: field.source_field || '',
      field_role: field.field_role || '',
      resolver_type: field.resolver_type || 'direct',
      applies_to_grain: field.applies_to_grain || '',
      depends_on: (field.depends_on || []).join(', '),
      formula: field.formula || '',
      notes: (field.notes || []).join('\n'),
    })
  }

  function removeItem(collection, index) {
    collection.splice(index, 1)
  }

  return {
    loading,
    saveMessage,
    initialized,
    workspace,
    runtime,
    graph,
    fields,
    schema,
    queryLab,
    nodeForm,
    edgeForm,
    fieldForm,
    graphSummary,
    flowNodes,
    flowEdges,
    graphPreview,
    fieldsPreview,
    notifyAction,
    ensureWorkspaceLoaded,
    loadWorkspace,
    saveWorkspace,
    loadDatabases,
    selectDatabase,
    selectTable,
    toggleColumn,
    clearSelectedColumns,
    inferNodeTemplate,
    addSelectedColumnsAsFields,
    addSingleField,
    saveNode,
    saveEdge,
    saveField,
    runNaturalLanguageQuery,
    runSql,
    editNode,
    editEdge,
    editField,
    removeItem,
  }
}

function createWorkbenchState() {
  return {
    workspace: {
      graphPath: 'config/graph.yaml',
      fieldsPath: 'config/fields.yaml',
      runtimePath: 'config/runtime.local.yaml',
    },
    runtime: {
      datasource: {},
      discovery: {},
    },
    graph: {
      nodes: [],
      edges: [],
    },
    fields: [],
    schema: {
      databases: [],
      selectedDatabase: '',
      tables: [],
      selectedTable: '',
      columns: [],
      selectedColumns: [],
    },
    queryLab: {
      naturalQuery: '查询 2026年4月1日到2026年4月7日 000004.SZ 的市值、流通市值和换手率，按日期升序返回',
      queryIntent: null,
      sql: '',
      result: null,
      error: '',
    },
    nodeForm: blankNode(),
    edgeForm: blankEdge(),
    fieldForm: blankField(),
  }
}

function blankNode() {
  return {
    editIndex: '',
    name: '',
    table: '',
    entity_keys: '',
    time_key: '',
    grain: '',
    fields: '',
    description: '',
  }
}

function blankEdge() {
  return {
    editIndex: '',
    name: '',
    from: '',
    to: '',
    relation_type: 'direct',
    join_keys: '',
    time_mode: '',
    base_time_field: '',
    base_time_cast: '',
    source_time_field: '',
    source_range: '',
    description: '',
  }
}

function blankField() {
  return {
    editIndex: '',
    standard_field: '',
    source_node: '',
    source_field: '',
    field_role: '',
    resolver_type: 'direct',
    applies_to_grain: '',
    depends_on: '',
    formula: '',
    notes: '',
  }
}

function currentWorkspaceSignature(workspace) {
  return [workspace.graphPath, workspace.fieldsPath, workspace.runtimePath].join('::')
}

function inferGrain(tableName) {
  const table = String(tableName || '')
  if (table.includes('minute')) return 'minute'
  if (table.includes('daily')) return 'daily'
  if (table.includes('factor')) return 'daily_factor'
  return 'daily'
}

function upsert(collection, index, payload) {
  if (index !== null && index !== undefined && index !== '') {
    collection[Number(index)] = payload
  } else {
    collection.push(payload)
  }
}

function splitCsv(value) {
  return String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function splitLines(value) {
  return String(value || '')
    .split('\n')
    .map((item) => item.trim())
    .filter(Boolean)
}

function parseJoinKeys(text) {
  return String(text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = line.match(/base:(.+?)\s*->\s*source:(.+)/i)
      if (!match) return null
      return { base: match[1].trim(), source: match[2].trim() }
    })
    .filter(Boolean)
}

function toYaml(value, indent = 0) {
  const space = '  '.repeat(indent)

  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (item && typeof item === 'object') {
          const nested = toYaml(item, indent + 1).split('\n')
          return `${space}- ${nested[0].trimStart()}\n${nested
            .slice(1)
            .map((line) => `${space}  ${line.trimStart()}`)
            .join('\n')}`
        }
        return `${space}- ${yamlScalar(item)}`
      })
      .join('\n')
  }

  if (value && typeof value === 'object') {
    return Object.entries(value)
      .map(([key, entry]) => {
        if (Array.isArray(entry)) {
          return entry.length ? `${space}${key}:\n${toYaml(entry, indent + 1)}` : `${space}${key}: []`
        }
        if (entry && typeof entry === 'object') {
          return `${space}${key}:\n${toYaml(entry, indent + 1)}`
        }
        return `${space}${key}: ${yamlScalar(entry)}`
      })
      .join('\n')
  }

  return `${space}${yamlScalar(value)}`
}

function yamlScalar(value) {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  const text = String(value)
  return /[:{}\[\],&*#?|\-<>=!%@`]/.test(text) ? JSON.stringify(text) : text
}
