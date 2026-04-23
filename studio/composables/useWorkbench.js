import { computed, toRef } from 'vue'
import { ElMessage } from 'element-plus'
import { useState } from '#imports'

const SUPPORT_NODE_PREFIX = 'support__'

export function useWorkbench() {
  const state = useState('workbench-state', createWorkbenchState)
  const loading = useState('workbench-loading', () => false)
  const saveMessage = useState('workbench-save-message', () => '等待加载工作区')
  const initialized = useState('workbench-initialized', () => false)
  const lastLoadedSignature = useState('workbench-last-loaded-signature', () => '')

  const workspace = toRef(state.value, 'workspace')
  const runtime = toRef(state.value, 'runtime')
  const graph = toRef(state.value, 'graph')
  const fields = toRef(state.value, 'fields')
  const protocolSummary = toRef(state.value, 'protocolSummary')
  const schema = toRef(state.value, 'schema')
  const attach = toRef(state.value, 'attach')
  const queryLab = toRef(state.value, 'queryLab')
  const nodeForm = toRef(state.value, 'nodeForm')
  const bindingForm = toRef(state.value, 'bindingForm')
  const selectedNodeName = toRef(state.value, 'selectedNodeName')

  const graphSummary = computed(() => ({
    nodes: graph.value.nodes.length,
    fields: fields.value.length,
  }))

  const visibleNodes = computed(() =>
    graph.value.nodes.filter((node) => node.node_role !== 'support' && !String(node.name || '').startsWith(SUPPORT_NODE_PREFIX))
  )

  const currentTableProfile = computed(() => ({
    database: nodeForm.value.database,
    table: nodeForm.value.tableName,
    columnCount: schema.value.columns.length,
    selectedCount: nodeForm.value.fields.length,
  }))

  function getNodeFieldBindings(nodeNameInput) {
    const nodeName = String(nodeNameInput || '').trim()
    if (!nodeName) return []

    const nodeLookup = new Map(graph.value.nodes.map((node) => [node.name, node]))
    const currentNode = nodeLookup.get(nodeName)
    const currentBaseNodeName = String(currentNode?.name || '')
    const relatedEdges = graph.value.edges.filter(
      (edge) => edge.from === nodeName
    )
    const edgeLookup = new Map(relatedEdges.map((edge) => [edge.to, edge]))

    const supportNodeNames = new Set(relatedEdges.map((edge) => edge.to))
    supportNodeNames.add(nodeName)
    const catalogRows = fields.value
      .map((field) => {
        const bindingMode = field.binding_mode || (field.source_table ? 'source_table' : (field.source_node ? 'source_node' : 'derived'))
        const fieldBaseNode = String(field.base_node || '')
        const belongsToCurrentNode = fieldBaseNode
          ? fieldBaseNode === nodeName
          : (
              field.source_table === currentNode?.table ||
              supportNodeNames.has(String(field.source_node || '')) ||
              relatedEdges.some((edge) => {
                const targetNode = nodeLookup.get(edge.to)
                return targetNode?.table === field.source_table
              })
            )

        if (!belongsToCurrentNode) {
          return null
        }

        let resolvedSourceNodeName = bindingMode === 'source_node' ? (field.source_node || '') : ''
        if (!resolvedSourceNodeName && !fieldBaseNode && field.source_table) {
          if (field.source_table === currentNode?.table) {
            resolvedSourceNodeName = nodeName
          } else {
            const matchedEdge = relatedEdges.find((edge) => {
              const targetNode = nodeLookup.get(edge.to)
              return targetNode?.table === field.source_table
            })
            resolvedSourceNodeName = matchedEdge?.to || ''
          }
        }
        const sourceNode = nodeLookup.get(resolvedSourceNodeName)
        const relationEdge = resolvedSourceNodeName === nodeName ? null : edgeLookup.get(resolvedSourceNodeName)
        const joinKeys = field.join_keys?.length ? field.join_keys : (relationEdge?.join_keys || [])
        const timeBinding = field.time_binding || relationEdge?.time_binding || null
        const joinKeysText = joinKeys.map((item) => `${item.base} -> ${item.source}`).join(', ')
        const sourceTableRef = field.source_table || sourceNode?.table || ''
        const bindingType = bindingMode === 'derived'
          ? 'derived'
          : (sourceTableRef === currentNode?.table || resolvedSourceNodeName === nodeName ? 'base' : 'relation')

        return {
          ...field,
          base_node: fieldBaseNode || nodeName,
          binding_mode: bindingMode,
          source_node: bindingMode === 'source_node' ? (resolvedSourceNodeName || field.source_node || '') : '',
          source_table: sourceTableRef,
          binding_type: bindingType,
          relation_mode: timeBinding?.mode || '',
          join_keys_text: joinKeysText,
          edge_name: relationEdge?.name || '',
          base_join_field: joinKeys?.[0]?.base || '',
          source_join_field: joinKeys?.[0]?.source || '',
          base_time_field: timeBinding?.base_time_field || '',
          source_time_field: timeBinding?.source_time_field || '',
          source_start_field: timeBinding?.source_start_field || '',
          source_end_field: timeBinding?.source_end_field || '',
        }
      })
      .filter(Boolean)

    const existingBaseFields = new Set(
      catalogRows.filter((item) => item.binding_type === 'base').map((item) => item.source_field)
    )

    const rawBaseRows = (currentNode?.fields || [])
      .filter((fieldName) => !existingBaseFields.has(fieldName))
      .map((fieldName) => ({
        standard_field: fieldName,
        binding_mode: 'source_table',
        base_node: nodeName,
        source_node: '',
        source_field: fieldName,
        source_table: currentNode?.table || '',
        field_role: 'base_field',
        binding_type: 'base',
        relation_mode: '',
        join_keys_text: '',
        edge_name: '',
        base_join_field: '',
        source_join_field: '',
        base_time_field: '',
        source_time_field: '',
        source_start_field: '',
        source_end_field: '',
        notes: ['来自节点主表字段'],
        __isRawOnly: true,
      }))

    return [...rawBaseRows, ...catalogRows]
  }

  const currentNodeFieldBindings = computed(() => {
    const nodeName = selectedNodeName.value || nodeForm.value.name
    return getNodeFieldBindings(nodeName)
  })

  const currentNodeGraph = computed(() => {
    const nodeName = selectedNodeName.value || nodeForm.value.name
    if (!nodeName) return { nodes: [], edges: [], related: [] }

    const currentNode = graph.value.nodes.find((node) => node.name === nodeName)
    const outgoing = graph.value.edges.filter((edge) => getEdgeFrom(edge) === nodeName)
    const relatedNodes = outgoing
      .map((edge) => graph.value.nodes.find((node) => node.name === getEdgeTo(edge)))
      .filter(Boolean)

    const nodes = [currentNode, ...relatedNodes].filter(Boolean).map((node, index) => ({
      id: node.name,
      position: {
        x: index === 0 ? 80 : 420,
        y: index === 0 ? 160 : 80 + (index - 1) * 120,
      },
      data: node,
      type: 'default',
    }))

    const edges = outgoing.map((edge, index) => ({
      id: `${edge.name}-${index}`,
      source: getEdgeFrom(edge),
      target: getEdgeTo(edge),
      label: edge.time_binding?.mode || edge.relation_type,
      style: { stroke: '#409eff', strokeWidth: 2 },
      labelStyle: { fill: '#606266', fontWeight: 600 },
      animated: edge.time_binding?.mode === 'asof',
    }))

    return {
      nodes,
      edges,
      related: outgoing.map((edge) => {
        const target = graph.value.nodes.find((node) => node.name === edge.to)
        return {
          edge_name: edge.name,
          target_node: getEdgeTo(edge),
          target_table: target?.table || '',
          relation_type: edge.relation_type,
          time_mode: edge.time_binding?.mode || '-',
          join_keys_text: edge.join_keys?.map((item) => `${item.base} -> ${item.source}`).join(', ') || '-',
        }
      }),
    }
  })

  const hasDatasourceConfigured = computed(() => {
    const datasource = runtime.value.datasource || {}
    return Boolean(datasource.db_type && datasource.host && datasource.port && datasource.username)
  })

  const hasLlmConfigured = computed(() => {
    const llm = runtime.value.llm || {}
    return Boolean(llm.enabled && llm.base_url && llm.model_name && llm.api_key)
  })

  function datasourceConfigError() {
    return new Error('ClickHouse 数据源未配置，请先完善 config/runtime.local.yaml 中的 datasource.host 等配置')
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
      ...options,
    })

    const contentType = response.headers.get('content-type') || ''
    const isJson = contentType.includes('application/json')
    const payload = isJson ? await response.json() : await response.text()

    if (!response.ok) {
      const message = typeof payload === 'string'
        ? payload
        : payload?.detail || payload?.message || payload?.error || JSON.stringify(payload)
      throw new Error(message || '请求失败')
    }

    return payload
  }

  async function notifyAction(successMessage, action) {
    try {
      const result = await action()
      if (successMessage) {
        ElMessage.success({
          message: successMessage,
          duration: 1800,
          grouping: true,
        })
      }
      return result
    } catch (error) {
      const message = error instanceof Error ? error.message : '操作失败'
      ElMessage.error({
        message,
        duration: 2600,
        grouping: true,
      })
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
      runtime.value.llm = payload.runtime?.llm || {}
      runtime.value.discovery = payload.runtime?.discovery || {}
      graph.value.nodes = payload.graph?.nodes || []
      graph.value.edges = normalizeEdges(payload.graph?.edges || [])
      fields.value = normalizeFieldCatalog(payload.fields || [], graph.value.nodes, graph.value.edges)
      protocolSummary.value = await loadProtocolSummary()

      saveMessage.value = '工作区已载入'
      initialized.value = true
      lastLoadedSignature.value = currentWorkspaceSignature(workspace.value)

      let schemaReady = true
      try {
        await loadDatabases()
      } catch (error) {
        schemaReady = false
        schema.value.databases = []
        schema.value.tables = []
        schema.value.columns = []
        attach.value.databases = []
        attach.value.tables = []
        attach.value.columns = []
        saveMessage.value = `数据库未连接: ${error instanceof Error ? error.message : '请检查设置'}`
      }

      const firstNode = visibleNodes.value[0]
      if (firstNode) {
        if (schemaReady) {
          await editNode(firstNode)
        } else {
          applyNodeSnapshot(firstNode)
        }
      } else {
        startNewNode()
      }

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
          runtime: runtime.value,
          graph: graph.value,
          fields: fields.value,
        }),
      })
      protocolSummary.value = await loadProtocolSummary()

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
    if (!hasDatasourceConfigured.value) {
      throw datasourceConfigError()
    }
    const payload = await api(`/api/schema/databases?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`)
    const items = payload.items || []
    schema.value.databases = items
    attach.value.databases = items
  }

  async function loadProtocolSummary() {
    const payload = await api(
      `/api/metadata/protocol-summary?graph_path=${encodeURIComponent(workspace.value.graphPath)}&fields_path=${encodeURIComponent(workspace.value.fieldsPath)}&runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`
    )
    return payload
  }

  async function testDatasourceConnection() {
    if (!hasDatasourceConfigured.value) {
      throw datasourceConfigError()
    }
    const payload = await api(`/api/schema/databases?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}`)
    const items = payload.items || []
    schema.value.databases = items
    attach.value.databases = items
    saveMessage.value = items.length ? '数据库连接成功' : '数据库连接成功，但没有可见库'
    return payload
  }

  async function selectDatabase(value) {
    nodeForm.value.database = value || ''
    nodeForm.value.tableName = ''
    nodeForm.value.entity_keys = []
    nodeForm.value.time_key = ''
    nodeForm.value.fields = []

    schema.value.selectedDatabase = value || ''
    schema.value.tables = []
    schema.value.columns = []

    if (!value || !hasDatasourceConfigured.value) return []

    const payload = await api(
      `/api/schema/tables?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(value)}`
    )
    schema.value.tables = payload.items || []
    return schema.value.tables
  }

  async function selectTable(name) {
    nodeForm.value.tableName = name || ''
    nodeForm.value.fields = []
    schema.value.selectedTable = name || ''
    schema.value.columns = []

    if (!schema.value.selectedDatabase || !name || !hasDatasourceConfigured.value) return []

    const payload = await api(
      `/api/schema/columns?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(schema.value.selectedDatabase)}&table=${encodeURIComponent(name)}`
    )
    schema.value.columns = payload.items || []
    syncNodeSelectionDefaults()
    return schema.value.columns
  }

  async function selectAttachDatabase(value) {
    attach.value.selectedDatabase = value || ''
    attach.value.selectedTable = ''
    attach.value.tables = []
    attach.value.columns = []
    resetBindingSourceSelection()

    if (!value || !hasDatasourceConfigured.value) return []

    const payload = await api(
      `/api/schema/tables?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(value)}`
    )
    attach.value.tables = payload.items || []
    return attach.value.tables
  }

  async function selectAttachTable(name) {
    attach.value.selectedTable = name || ''
    attach.value.columns = []
    resetBindingSourceSelection({ keepDatabase: true, keepTable: true })

    if (!attach.value.selectedDatabase || !name || !hasDatasourceConfigured.value) return []

    const payload = await api(
      `/api/schema/columns?runtime_path=${encodeURIComponent(workspace.value.runtimePath)}&database=${encodeURIComponent(attach.value.selectedDatabase)}&table=${encodeURIComponent(name)}`
    )
    attach.value.columns = payload.items || []
    syncBindingDefaults()
    return attach.value.columns
  }

  function inferNodeTemplate() {
    const selectedTable = String(nodeForm.value.tableName || '')
    const allFields = schema.value.columns.map((item) => item.name)

    if (!nodeForm.value.name) {
      nodeForm.value.name = selectedTable.replace(/^ad_/, '')
    }
    nodeForm.value.grain = inferGrain(selectedTable)
    nodeForm.value.entity_keys = nodeForm.value.entity_keys.length ? nodeForm.value.entity_keys : inferEntityKeys(allFields)
    nodeForm.value.time_key = nodeForm.value.time_key || inferTimeKey(allFields)
    nodeForm.value.fields = nodeForm.value.fields.length ? nodeForm.value.fields : allFields

    if (!nodeForm.value.description) {
      nodeForm.value.description = `从 ${nodeForm.value.database}.${selectedTable} 生成`
    }

    syncBindingDefaults()
  }

  function startNewNode() {
    selectedNodeName.value = ''
    Object.assign(nodeForm.value, blankNode())
    Object.assign(bindingForm.value, blankBinding())
    schema.value.selectedDatabase = ''
    schema.value.selectedTable = ''
    schema.value.tables = []
    schema.value.columns = []
    attach.value.selectedDatabase = ''
    attach.value.selectedTable = ''
    attach.value.tables = []
    attach.value.columns = []
  }

  function applyNodeSnapshot(target) {
    const { database, tableName } = parseTableRef(target.table)
    Object.assign(nodeForm.value, {
      editIndex: '',
      name: target.name || '',
      database,
      tableName,
      entity_keys: Array.isArray(target.entity_keys) ? [...target.entity_keys] : [],
      time_key: target.time_key || '',
      grain: target.grain || inferGrain(tableName),
      fields: Array.isArray(target.fields) ? [...target.fields] : [],
      description: target.description || target.description_zh || '',
      node_role: target.node_role || 'real',
      status: target.status || 'enabled',
      asset_type: target.asset_type || '',
      query_freq: target.query_freq || '',
      base_filters: normalizeBaseFilters(target.base_filters),
    })
    selectedNodeName.value = target.name || ''
    syncBindingDefaults()
  }

  function resetBindingForm() {
    Object.assign(bindingForm.value, blankBinding())
    syncBindingDefaults()
  }

  async function editNode(node, explicitIndex = null) {
    const target = typeof node === 'string'
      ? graph.value.nodes.find((item) => item.name === node)
      : node

    if (!target) return null

    const nodeIndex = explicitIndex !== null && explicitIndex !== undefined
      ? explicitIndex
      : graph.value.nodes.findIndex((item) => item.name === target.name)

    const { database, tableName } = parseTableRef(target.table)

    if (hasDatasourceConfigured.value) {
      if (database) {
        await selectDatabase(database)
      }
      if (tableName) {
        await selectTable(tableName)
      }
    } else {
      schema.value.selectedDatabase = database || ''
      schema.value.selectedTable = tableName || ''
      schema.value.tables = []
      schema.value.columns = []
    }

    Object.assign(nodeForm.value, {
      editIndex: nodeIndex >= 0 ? String(nodeIndex) : '',
      name: target.name || '',
      database,
      tableName,
      entity_keys: Array.isArray(target.entity_keys) ? [...target.entity_keys] : [],
      time_key: target.time_key || '',
      grain: target.grain || inferGrain(tableName),
      fields: Array.isArray(target.fields) ? [...target.fields] : [],
      description: target.description || target.description_zh || '',
      node_role: target.node_role || 'real',
      status: target.status || 'enabled',
      asset_type: target.asset_type || '',
      query_freq: target.query_freq || '',
      base_filters: normalizeBaseFilters(target.base_filters),
    })

    selectedNodeName.value = target.name || ''

    syncBindingDefaults()
    return target
  }

  function saveNode() {
    const name = String(nodeForm.value.name || '').trim()
    const database = String(nodeForm.value.database || '').trim()
    const tableName = String(nodeForm.value.tableName || '').trim()

    if (!name) {
      throw new Error('请先填写节点名')
    }
    if (!database || !tableName) {
      throw new Error('请先为节点选择主表')
    }

    const payload = {
      name,
      table: `${database}.${tableName}`,
      entity_keys: uniqueList(nodeForm.value.entity_keys),
      time_key: nodeForm.value.time_key || null,
      grain: nodeForm.value.grain || inferGrain(tableName),
      fields: uniqueList(nodeForm.value.fields),
      description: String(nodeForm.value.description || '').trim() || null,
      node_role: nodeForm.value.node_role || 'real',
      status: String(nodeForm.value.status || 'enabled'),
      asset_type: String(nodeForm.value.asset_type || '').trim() || null,
      query_freq: String(nodeForm.value.query_freq || '').trim() || null,
      base_filters: normalizeBaseFilters(nodeForm.value.base_filters),
    }

    const index = upsert(graph.value.nodes, nodeForm.value.editIndex, payload, (item) => item.name === payload.name)
    nodeForm.value.editIndex = String(index)
    selectedNodeName.value = payload.name
    return payload
  }

  function saveRelatedFieldBinding() {
    const currentNode = saveNode()
    const standardField = String(bindingForm.value.standard_field || '').trim()
    const sourceDatabase = String(attach.value.selectedDatabase || '').trim()
    const sourceTable = String(attach.value.selectedTable || '').trim()
    const sourceField = String(bindingForm.value.source_field || '').trim()
    const baseJoinField = String(bindingForm.value.base_join_field || '').trim()
    const sourceJoinField = String(bindingForm.value.source_join_field || '').trim()

    if (!standardField) {
      throw new Error('请选择或填写标准字段名')
    }

    if (
      bindingForm.value.edit_mode &&
      bindingForm.value.original_standard_field &&
      (bindingForm.value.original_source_node || bindingForm.value.original_source_table)
    ) {
      removeFieldBindingByKey(
        bindingForm.value.original_standard_field,
        bindingForm.value.original_base_node,
        bindingForm.value.original_source_node,
        bindingForm.value.original_source_table,
        bindingForm.value.original_source_field,
      )
      cleanupSupportNodeIfUnused(bindingForm.value.original_source_node)
    }

    if (bindingForm.value.binding_mode === 'source_node') {
      if (!bindingForm.value.source_node || !sourceField) {
        throw new Error('请先选择来源节点和来源字段')
      }
      upsertField(fields.value, {
        base_node: String(currentNode.name),
        standard_field: standardField,
        binding_mode: 'source_node',
        source_table: null,
        source_node: bindingForm.value.source_node,
        source_field: sourceField,
        relation_type: null,
        join_keys: [],
        time_binding: null,
        bridge_steps: [],
        field_role: bindingForm.value.field_role || 'direct_field',
        resolver_type: bindingForm.value.resolver_type || 'direct',
        depends_on: [],
        formula: null,
        applies_to_grain: currentNode.grain,
        notes: uniqueList([String(bindingForm.value.notes || '').trim()]),
      })

      Object.assign(bindingForm.value, {
        ...blankBinding(),
        binding_mode: 'source_table',
        field_role: bindingForm.value.field_role,
        resolver_type: bindingForm.value.resolver_type,
        base_join_field: currentNode.entity_keys[0] || '',
        base_time_field: currentNode.time_key || '',
        time_mode: inferDefaultTimeMode(currentNode.grain),
      })
      return
    }

    if (!sourceDatabase || !sourceTable || !sourceField) {
      throw new Error('请先选择关联来源表和来源字段')
    }
    if (!baseJoinField || !sourceJoinField) {
      throw new Error('请先选择主表键和来源表键')
    }

    const sourceTableRef = `${sourceDatabase}.${sourceTable}`
    const baseNodeName = String(currentNode.name)
    const timeBinding = buildTimeBinding(bindingForm.value)
    ensureNodeFields(currentNode.name, [baseJoinField, currentNode.time_key])

    upsertField(fields.value, {
      base_node: baseNodeName,
      standard_field: standardField,
      binding_mode: 'source_table',
      source_table: sourceTableRef,
      source_node: null,
      source_field: sourceField,
      relation_type: 'direct',
      join_keys: [{ base: baseJoinField, source: sourceJoinField }],
      time_binding: timeBinding,
      bridge_steps: [],
      field_role: bindingForm.value.field_role || 'direct_field',
      resolver_type: bindingForm.value.resolver_type || 'direct',
      depends_on: [],
      formula: null,
      applies_to_grain: currentNode.grain,
      notes: uniqueList([
        `挂载自 ${sourceTableRef}.${sourceField}`,
        String(bindingForm.value.notes || '').trim(),
      ]),
    })

    Object.assign(bindingForm.value, {
      ...blankBinding(),
      binding_mode: 'source_table',
      field_role: bindingForm.value.field_role,
      resolver_type: bindingForm.value.resolver_type,
      base_join_field: currentNode.entity_keys[0] || '',
      base_time_field: currentNode.time_key || '',
      time_mode: inferDefaultTimeMode(currentNode.grain),
    })
  }

  function addBaseFieldBinding() {
    const currentNode = saveNode()
    const sourceField = String(bindingForm.value.base_source_field || '').trim()
    const standardField = String(bindingForm.value.standard_field || '').trim() || sourceField

    if (!sourceField) {
      throw new Error('请先选择主表字段')
    }

    if (
      bindingForm.value.edit_mode &&
      bindingForm.value.original_standard_field &&
      (bindingForm.value.original_source_node || bindingForm.value.original_source_table || bindingForm.value.original_base_node)
    ) {
      removeFieldBindingByKey(
        bindingForm.value.original_standard_field,
        bindingForm.value.original_base_node,
        bindingForm.value.original_source_node,
        bindingForm.value.original_source_table,
        bindingForm.value.original_source_field,
      )
      cleanupSupportNodeIfUnused(bindingForm.value.original_source_node)
    }

    ensureNodeFields(currentNode.name, [sourceField])

    upsertField(fields.value, {
      base_node: String(currentNode.name),
      standard_field: standardField,
      binding_mode: 'source_table',
      source_table: currentNode.table,
      source_node: null,
      source_field: sourceField,
      relation_type: null,
      join_keys: [],
      time_binding: null,
      bridge_steps: [],
      field_role: bindingForm.value.field_role || 'direct_field',
      resolver_type: bindingForm.value.resolver_type || 'direct',
      depends_on: [],
      formula: null,
      applies_to_grain: currentNode.grain,
      notes: uniqueList([
        `挂载自主表字段 ${currentNode.table}.${sourceField}`,
        String(bindingForm.value.notes || '').trim(),
      ]),
    })

    Object.assign(bindingForm.value, {
      ...blankBinding(),
      binding_mode: 'source_table',
      field_role: bindingForm.value.field_role,
      resolver_type: bindingForm.value.resolver_type,
      base_join_field: currentNode.entity_keys[0] || '',
      base_time_field: currentNode.time_key || '',
      time_mode: inferDefaultTimeMode(currentNode.grain),
    })
  }

  function saveDerivedFieldBinding() {
    const currentNode = saveNode()
    const standardField = String(bindingForm.value.standard_field || '').trim()
    const formula = String(bindingForm.value.formula || '').trim()
    const dependsOn = uniqueList(bindingForm.value.depends_on || [])

    if (!standardField) {
      throw new Error('请先填写标准字段名')
    }
    if (!formula) {
      throw new Error('请先填写派生公式')
    }
    if (!dependsOn.length) {
      throw new Error('请至少选择一个依赖字段')
    }

    if (
      bindingForm.value.edit_mode &&
      bindingForm.value.original_standard_field &&
      (bindingForm.value.original_source_node || bindingForm.value.original_source_table || bindingForm.value.original_base_node)
    ) {
      removeFieldBindingByKey(
        bindingForm.value.original_standard_field,
        bindingForm.value.original_base_node,
        bindingForm.value.original_source_node,
        bindingForm.value.original_source_table,
        bindingForm.value.original_source_field,
      )
      cleanupSupportNodeIfUnused(bindingForm.value.original_source_node)
    }

    upsertField(fields.value, {
      base_node: String(currentNode.name),
      standard_field: standardField,
      binding_mode: 'derived',
      source_table: null,
      source_node: null,
      source_field: null,
      relation_type: null,
      join_keys: [],
      time_binding: null,
      bridge_steps: [],
      field_role: bindingForm.value.field_role || 'derived_field',
      resolver_type: 'derived',
      depends_on: dependsOn,
      formula,
      applies_to_grain: currentNode.grain,
      notes: uniqueList([String(bindingForm.value.notes || '').trim()]),
    })

    Object.assign(bindingForm.value, {
      ...blankBinding(),
      field_role: 'derived_field',
      resolver_type: 'derived',
    })
  }

  function ensureSupportNodeAndEdge(currentNode) {
    const sourceDatabase = attach.value.selectedDatabase
    const sourceTable = attach.value.selectedTable
    const sourceTableRef = `${sourceDatabase}.${sourceTable}`
    const supportNodeName = makeSupportNodeName(currentNode.name, sourceDatabase, sourceTable)
    const timeBinding = buildTimeBinding(bindingForm.value)
    const supportFields = uniqueList([
      bindingForm.value.source_field,
      bindingForm.value.source_join_field,
      bindingForm.value.source_time_field,
      bindingForm.value.source_start_field,
      bindingForm.value.source_end_field,
    ])

    const existingSupportNode = graph.value.nodes.find((item) => item.name === supportNodeName)
    const supportNodePayload = {
      name: supportNodeName,
      table: sourceTableRef,
      entity_keys: uniqueList([bindingForm.value.source_join_field]),
      time_key: inferSupportTimeKey(bindingForm.value),
      grain: inferGrain(sourceTable),
      fields: uniqueList([...(existingSupportNode?.fields || []), ...supportFields]),
      description: `${currentNode.name} 的关联来源表`,
      node_role: 'support',
      status: 'disabled',
    }
    upsert(graph.value.nodes, null, supportNodePayload, (item) => item.name === supportNodePayload.name)

    const edgePayload = {
      name: makeEdgeName(currentNode.name, supportNodeName),
      from: currentNode.name,
      to: supportNodeName,
      relation_type: 'direct',
      source_table: sourceTableRef,
      join_keys: [{ base: bindingForm.value.base_join_field, source: bindingForm.value.source_join_field }],
      time_binding: timeBinding,
      description: `${currentNode.name} 关联 ${sourceTableRef}`,
    }
    upsert(graph.value.edges, null, edgePayload, (item) => item.name === edgePayload.name)

    ensureNodeFields(currentNode.name, [bindingForm.value.base_join_field, bindingForm.value.base_time_field])
    return supportNodeName
  }

  function ensureNodeFields(nodeName, extraFields) {
    const node = graph.value.nodes.find((item) => item.name === nodeName)
    if (!node) return
    node.fields = uniqueList([...(node.fields || []), ...extraFields])
  }

  function makeCopyNodeName(baseName) {
    const sourceName = String(baseName || '').trim()
    if (!sourceName) {
      return 'node_copy'
    }
    let candidate = `${sourceName}_copy`
    let index = 2
    while (graph.value.nodes.some((node) => node.name === candidate)) {
      candidate = `${sourceName}_copy${index}`
      index += 1
    }
    return candidate
  }

  async function duplicateNode(nodeName) {
    const original = graph.value.nodes.find((node) => node.name === nodeName)
    if (!original) {
      throw new Error(`节点不存在: ${nodeName}`)
    }
    if (original.node_role === 'support' || String(original.name || '').startsWith(SUPPORT_NODE_PREFIX)) {
      throw new Error('不支持复制 support 节点')
    }

    const nextName = makeCopyNodeName(original.name)
    const supportPrefix = `${SUPPORT_NODE_PREFIX}${sanitizeName(original.name)}__`
    const supportNodes = graph.value.nodes.filter((node) => String(node.name || '').startsWith(supportPrefix))
    const supportNameMap = new Map(
      supportNodes.map((node) => [
        node.name,
        String(node.name || '').replace(supportPrefix, `${SUPPORT_NODE_PREFIX}${sanitizeName(nextName)}__`),
      ])
    )

    const clonedNode = {
      ...original,
      name: nextName,
      description: String(original.description || '').trim() || null,
      fields: Array.isArray(original.fields) ? [...original.fields] : [],
      entity_keys: Array.isArray(original.entity_keys) ? [...original.entity_keys] : [],
      base_filters: Array.isArray(original.base_filters) ? original.base_filters.map((item) => ({ ...item })) : [],
    }
    graph.value.nodes.push(clonedNode)

    for (const supportNode of supportNodes) {
      graph.value.nodes.push({
        ...supportNode,
        name: supportNameMap.get(supportNode.name),
        fields: Array.isArray(supportNode.fields) ? [...supportNode.fields] : [],
        entity_keys: Array.isArray(supportNode.entity_keys) ? [...supportNode.entity_keys] : [],
      })
    }

    const edgesToClone = graph.value.edges.filter((edge) => {
      const fromName = String(edge.from || edge.from_node || '')
      const toName = String(edge.to || edge.to_node || '')
      return fromName === original.name || supportNameMap.has(fromName) || supportNameMap.has(toName)
    })

    for (const edge of edgesToClone) {
      const fromName = String(edge.from || edge.from_node || '')
      const toName = String(edge.to || edge.to_node || '')
      const nextFrom = fromName === original.name ? nextName : (supportNameMap.get(fromName) || fromName)
      const nextTo = toName === original.name ? nextName : (supportNameMap.get(toName) || toName)
      const clonedEdge = {
        ...edge,
        name: `${String(edge.name || 'edge')}__${sanitizeName(nextName)}`,
        from: nextFrom,
        to: nextTo,
      }
      upsert(graph.value.edges, null, clonedEdge, (item) => item.name === clonedEdge.name)
    }

    const fieldsToClone = fields.value.filter((field) => String(field.base_node || '') === original.name)
    for (const field of fieldsToClone) {
      upsertField(fields.value, {
        ...field,
        base_node: nextName,
        source_node: supportNameMap.get(String(field.source_node || '')) || field.source_node,
        join_keys: Array.isArray(field.join_keys) ? field.join_keys.map((item) => ({ ...item })) : [],
        bridge_steps: Array.isArray(field.bridge_steps) ? field.bridge_steps.map((item) => ({ ...item })) : [],
        depends_on: Array.isArray(field.depends_on) ? [...field.depends_on] : [],
        notes: Array.isArray(field.notes) ? [...field.notes] : [],
      })
    }

    await editNode(clonedNode)
    return clonedNode
  }

  function getNodeDeleteImpact(nodeName) {
    const supportPrefix = `${SUPPORT_NODE_PREFIX}${nodeName}__`
    const targetNode = graph.value.nodes.find((node) => node.name === nodeName)
    const targetTable = String(targetNode?.table || '')
    const referencedBySourceFields = fields.value.filter(
      (field) => String(field.base_node || '') !== nodeName && String(field.source_node || '') === nodeName
    )
    const referencedByLegacyTableFields = fields.value.filter(
      (field) =>
        !String(field.base_node || '').trim() &&
        String(field.source_table || '') === targetTable &&
        !hasDirectFieldRelation(field)
    )
    const referencedByEdges = graph.value.edges.filter((edge) => {
      const fromName = String(edge.from || edge.from_node || '')
      const toName = String(edge.to || edge.to_node || '')
      if (fromName === nodeName && toName.startsWith(supportPrefix)) return false
      if (toName === nodeName && fromName === nodeName) return false
      return fromName === nodeName || toName === nodeName
    })
    return {
      referencedBySourceFields,
      referencedByLegacyTableFields,
      referencedByEdges,
      canDelete:
        referencedBySourceFields.length === 0 &&
        referencedByLegacyTableFields.length === 0,
    }
  }

  function deleteNode(nodeName) {
    const supportPrefix = `${SUPPORT_NODE_PREFIX}${nodeName}__`
    const impact = getNodeDeleteImpact(nodeName)
    if (!impact.canDelete) {
      const messages = []
      if (impact.referencedBySourceFields.length) {
        messages.push(`被 ${impact.referencedBySourceFields.length} 个字段作为 source_node 引用`)
      }
      if (impact.referencedByLegacyTableFields.length) {
        messages.push(`仍有 ${impact.referencedByLegacyTableFields.length} 个旧版 source_table 字段依赖这个节点`)
      }
      throw new Error(`节点仍被引用，不能删除：${messages.join('；')}`)
    }

    graph.value.nodes = graph.value.nodes.filter((node) => node.name !== nodeName && !node.name.startsWith(supportPrefix))
    graph.value.edges = graph.value.edges.filter(
      (edge) => edge.from !== nodeName && edge.to !== nodeName && !String(edge.to || '').startsWith(supportPrefix)
    )
    fields.value = fields.value.filter(
      (field) =>
        String(field.base_node || '') !== nodeName &&
        field.source_node !== nodeName &&
        !String(field.source_node || '').startsWith(supportPrefix)
    )

    if (selectedNodeName.value === nodeName) {
      const nextNode = visibleNodes.value[0]
      if (nextNode) {
        editNode(nextNode)
      } else {
        startNewNode()
      }
    }
  }

  function removeFieldBindingByKey(standardField, baseNode = '', sourceNode = '', sourceTable = '', sourceField = '') {
    fields.value = fields.value.filter(
      (field) => !(
        field.standard_field === standardField &&
        String(field.base_node || '') === String(baseNode || '') &&
        String(field.source_node || '') === String(sourceNode || '') &&
        String(field.source_table || '') === String(sourceTable || '') &&
        String(field.source_field || '') === String(sourceField || '')
      )
    )
  }

  function cleanupSupportNodeIfUnused(nodeName) {
    if (!String(nodeName || '').startsWith(SUPPORT_NODE_PREFIX)) return
    const stillReferenced = fields.value.some((field) => field.source_node === nodeName)
    if (stillReferenced) return
    graph.value.nodes = graph.value.nodes.filter((node) => node.name !== nodeName)
    graph.value.edges = graph.value.edges.filter((edge) => edge.from !== nodeName && edge.to !== nodeName)
  }

  function removeFieldBinding(fieldEntry) {
    removeFieldBindingByKey(
      fieldEntry.standard_field,
      fieldEntry.base_node,
      fieldEntry.source_node,
      fieldEntry.source_table,
      fieldEntry.source_field,
    )
    cleanupSupportNodeIfUnused(fieldEntry.source_node)
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

      if (payload && payload.ok === false) {
        queryLab.value.queryIntent = payload.query_intent || null
        queryLab.value.sql = payload.sql || ''
        queryLab.value.result = payload
        queryLab.value.error = payload.error || '生成失败'
        return payload
      }

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

      if (payload && payload.ok === false) {
        queryLab.value.error = payload.error || 'SQL 执行失败'
        queryLab.value.result = payload
        return payload
      }

      queryLab.value.result = payload
      return payload
    } catch (error) {
      queryLab.value.error = error.message
      queryLab.value.result = null
      throw error
    }
  }

  function syncNodeSelectionDefaults() {
    const columnNames = schema.value.columns.map((item) => item.name)
    if (!nodeForm.value.entity_keys.length) {
      nodeForm.value.entity_keys = inferEntityKeys(columnNames)
    }
    if (!nodeForm.value.time_key) {
      nodeForm.value.time_key = inferTimeKey(columnNames)
    }
    if (!nodeForm.value.grain) {
      nodeForm.value.grain = inferGrain(nodeForm.value.tableName)
    }
    if (!nodeForm.value.name && nodeForm.value.tableName) {
      nodeForm.value.name = String(nodeForm.value.tableName).replace(/^ad_/, '')
    }
    syncBindingDefaults()
  }

  function syncBindingDefaults() {
    bindingForm.value.base_join_field = bindingForm.value.base_join_field || nodeForm.value.entity_keys[0] || ''
    bindingForm.value.base_time_field = bindingForm.value.base_time_field || nodeForm.value.time_key || ''
    bindingForm.value.time_mode = bindingForm.value.time_mode || inferDefaultTimeMode(nodeForm.value.grain)
    bindingForm.value.field_role = bindingForm.value.field_role || 'direct_field'
    bindingForm.value.resolver_type = bindingForm.value.resolver_type || 'direct'
  }

  function resetBindingSourceSelection(options = {}) {
    bindingForm.value.source_field = ''
    bindingForm.value.source_join_field = ''
    bindingForm.value.source_time_field = ''
    bindingForm.value.source_start_field = ''
    bindingForm.value.source_end_field = ''
    if (!options.keepTable) {
      bindingForm.value.standard_field = ''
    }
  }

  return {
    loading,
    saveMessage,
    initialized,
    workspace,
    runtime,
    graph,
    fields,
    protocolSummary,
    schema,
    attach,
    queryLab,
    nodeForm,
    bindingForm,
    selectedNodeName,
    graphSummary,
    hasDatasourceConfigured,
    hasLlmConfigured,
    visibleNodes,
    currentTableProfile,
    currentNodeFieldBindings,
    getNodeFieldBindings,
    currentNodeGraph,
    notifyAction,
    ensureWorkspaceLoaded,
    loadWorkspace,
    saveWorkspace,
    loadProtocolSummary,
    loadDatabases,
    testDatasourceConnection,
    selectDatabase,
    selectTable,
    selectAttachDatabase,
    selectAttachTable,
    inferNodeTemplate,
    startNewNode,
    resetBindingForm,
    saveNode,
    addBaseFieldBinding,
    saveDerivedFieldBinding,
    saveRelatedFieldBinding,
    duplicateNode,
    deleteNode,
    getNodeDeleteImpact,
    removeFieldBinding,
    runNaturalLanguageQuery,
    runSql,
    editNode,
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
      llm: {},
      datasource: {},
      discovery: {},
    },
    graph: {
      nodes: [],
      edges: [],
    },
    fields: [],
    protocolSummary: {
      code: 0,
      message: '',
      summary: {
        enabled_real_nodes: 0,
        disabled_real_nodes: 0,
        total_fields_across_enabled_nodes: 0,
        edge_count: 0,
      },
      items: [],
    },
    schema: {
      databases: [],
      selectedDatabase: '',
      tables: [],
      selectedTable: '',
      columns: [],
    },
    attach: {
      databases: [],
      selectedDatabase: '',
      tables: [],
      selectedTable: '',
      columns: [],
    },
    queryLab: {
      naturalQuery: '查询 2026年4月1日到2026年4月7日 000004.SZ 的市值、流通市值和换手率，按日期升序返回',
      queryIntent: null,
      sql: '',
      result: null,
      error: '',
    },
    selectedNodeName: '',
    nodeForm: blankNode(),
    bindingForm: blankBinding(),
  }
}

function blankNode() {
  return {
    editIndex: '',
    name: '',
    database: '',
    tableName: '',
    entity_keys: [],
    time_key: '',
    grain: 'daily',
    fields: [],
    description: '',
    node_role: 'real',
    status: 'enabled',
    asset_type: '',
    query_freq: '',
    base_filters: [],
  }
}

function blankBinding() {
  return {
    base_source_field: '',
    binding_mode: 'source_table',
    standard_field: '',
    source_node: '',
    source_field: '',
    field_role: 'direct_field',
    resolver_type: 'direct',
    base_join_field: '',
    source_join_field: '',
    time_mode: '',
    base_time_field: '',
    source_time_field: '',
    source_start_field: '',
    source_end_field: '',
    depends_on: [],
    formula: '',
    notes: '',
    edit_mode: false,
    original_base_node: '',
    original_source_table: '',
    original_standard_field: '',
    original_source_node: '',
    original_source_field: '',
    original_binding_type: '',
  }
}

function currentWorkspaceSignature(workspace) {
  return [workspace.graphPath, workspace.fieldsPath, workspace.runtimePath].join('::')
}

function getEdgeFrom(edge) {
  return edge?.from || edge?.from_node || ''
}

function getEdgeTo(edge) {
  return edge?.to || edge?.to_node || ''
}

function normalizeEdges(edges) {
  return (edges || []).map((edge) => ({
    ...edge,
    from: edge?.from || edge?.from_node || '',
    to: edge?.to || edge?.to_node || '',
  }))
}

function normalizeFieldCatalog(fields, nodes, edges) {
  const nodeLookup = new Map((nodes || []).map((node) => [node.name, node]))
  const normalizedEdges = normalizeEdges(edges || [])
  const realNodes = (nodes || []).filter((node) => node.node_role !== 'support' && !String(node.name || '').startsWith(SUPPORT_NODE_PREFIX))

  return (fields || []).map((field) => {
    const bindingMode = field.binding_mode || (field.source_table ? 'source_table' : (field.source_node ? 'source_node' : 'derived'))
    const nextField = {
      ...field,
      binding_mode: bindingMode,
      join_keys: Array.isArray(field.join_keys) ? field.join_keys : [],
      bridge_steps: Array.isArray(field.bridge_steps) ? field.bridge_steps : [],
    }

    if (!nextField.base_node) {
      const owner = inferBaseNodeForField(nextField, realNodes, nodeLookup, normalizedEdges)
      if (owner) {
        nextField.base_node = owner
      }
    }

    if (
      bindingMode === 'source_table' &&
      nextField.source_table &&
      !hasDirectFieldRelation(nextField) &&
      nextField.base_node
    ) {
      const ownerEdge = inferDirectEdgeForField(nextField, nodeLookup, normalizedEdges)
      if (ownerEdge) {
        nextField.relation_type = nextField.relation_type || ownerEdge.relation_type || 'direct'
        nextField.join_keys = ownerEdge.join_keys || []
        nextField.time_binding = nextField.time_binding || ownerEdge.time_binding || null
        nextField.bridge_steps = nextField.bridge_steps?.length ? nextField.bridge_steps : (ownerEdge.bridge_steps || [])
      }
    }

    return nextField
  })
}

function inferBaseNodeForField(field, realNodes, nodeLookup, edges) {
  if (field.base_node) return String(field.base_node)

  const grainMatchedNodes = (realNodes || []).filter((node) => {
    if (!field.applies_to_grain) return true
    return String(node.grain || '') === String(field.applies_to_grain || '')
  })

  if (field.binding_mode === 'source_table' && field.source_table) {
    const byDirectTable = grainMatchedNodes.filter((node) => String(node.table || '') === String(field.source_table || ''))
    if (byDirectTable.length === 1) {
      return byDirectTable[0].name
    }

    const byDirectEdge = grainMatchedNodes.filter((node) =>
      edges.some((edge) => {
        if (String(edge.from || '') !== String(node.name || '')) return false
        const targetNode = nodeLookup.get(String(edge.to || ''))
        return String(targetNode?.table || '') === String(field.source_table || '')
      })
    )
    if (byDirectEdge.length === 1) {
      return byDirectEdge[0].name
    }

    if (field.via_node) {
      const viaCandidates = grainMatchedNodes.filter((node) =>
        edges.some((edge) => String(edge.from || '') === String(node.name || '') && String(edge.to || '') === String(field.via_node || ''))
      )
      if (viaCandidates.length === 1) {
        return viaCandidates[0].name
      }
    }
  }

  if (field.binding_mode === 'source_node' && field.source_node) {
    const byIncoming = grainMatchedNodes.filter((node) =>
      edges.some((edge) => String(edge.from || '') === String(node.name || '') && String(edge.to || '') === String(field.source_node || ''))
    )
    if (byIncoming.length === 1) {
      return byIncoming[0].name
    }
  }

  return ''
}

function inferDirectEdgeForField(field, nodeLookup, edges) {
  if (!field.base_node || !field.source_table) return null
  const ownerNode = nodeLookup.get(String(field.base_node || ''))
  if (!ownerNode || String(ownerNode.table || '') === String(field.source_table || '')) {
    return null
  }

  const matches = edges.filter((edge) => {
    if (String(edge.from || '') !== String(ownerNode.name || '')) return false
    const targetNode = nodeLookup.get(String(edge.to || ''))
    return String(targetNode?.table || '') === String(field.source_table || '')
  })
  if (matches.length !== 1) {
    return null
  }
  return matches[0]
}

function inferGrain(tableName) {
  const table = String(tableName || '')
  if (table.includes('minute')) return 'minute'
  if (table.includes('daily')) return 'daily'
  if (table.includes('factor')) return 'daily_factor'
  return 'daily'
}

function inferEntityKeys(fields) {
  const candidates = ['code', 'symbol', 'ts_code', 'market_code', 'index_code', 'fund_code', 'industry_code']
  const hit = candidates.find((item) => fields.includes(item))
  return hit ? [hit] : []
}

function inferTimeKey(fields) {
  const candidates = ['trade_time', 'trade_date', 'date', 'change_date', 'ann_date', 'report_date', 'end_date']
  return candidates.find((item) => fields.includes(item)) || ''
}

function inferDefaultTimeMode(grain) {
  return String(grain || '').includes('minute') ? 'same_timestamp' : 'same_date'
}

function inferSupportTimeKey(bindingForm) {
  if (bindingForm.time_mode === 'effective_range') {
    return bindingForm.source_start_field || bindingForm.source_end_field || null
  }
  return bindingForm.source_time_field || null
}

function buildTimeBinding(bindingForm) {
  const mode = String(bindingForm.time_mode || '').trim()
  if (!mode || mode === 'none') return null

  if (mode === 'effective_range') {
    return {
      mode,
      base_time_field: bindingForm.base_time_field || null,
      source_start_field: bindingForm.source_start_field || null,
      source_end_field: bindingForm.source_end_field || null,
    }
  }

  return {
    mode,
    base_time_field: bindingForm.base_time_field || null,
    source_time_field: bindingForm.source_time_field || null,
  }
}

function parseTableRef(tableRef) {
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

function makeSupportNodeName(baseNodeName, database, table) {
  return `${SUPPORT_NODE_PREFIX}${sanitizeName(baseNodeName)}__${sanitizeName(database)}__${sanitizeName(table)}`
}

function makeEdgeName(baseNodeName, supportNodeName) {
  return `edge__${sanitizeName(baseNodeName)}__${sanitizeName(supportNodeName)}`
}

function sanitizeName(value) {
  return String(value || '').replace(/[^a-zA-Z0-9_]+/g, '_')
}

function upsert(collection, index, payload, matcher = null) {
  let targetIndex = index
  if ((targetIndex === null || targetIndex === undefined || targetIndex === '') && matcher) {
    targetIndex = collection.findIndex(matcher)
  }

  if (targetIndex !== null && targetIndex !== undefined && targetIndex !== '' && Number(targetIndex) >= 0) {
    collection[Number(targetIndex)] = payload
    return Number(targetIndex)
  }

  collection.push(payload)
  return collection.length - 1
}

function upsertField(collection, payload) {
  const index = collection.findIndex(
    (item) =>
      String(item.base_node || '') === String(payload.base_node || '') &&
      item.standard_field === payload.standard_field &&
      String(item.source_node || '') === String(payload.source_node || '') &&
      String(item.source_table || '') === String(payload.source_table || '') &&
      String(item.source_field || '') === String(payload.source_field || '') &&
      String(item.binding_mode || '') === String(payload.binding_mode || '')
  )

  if (index >= 0) {
    collection[index] = payload
    return index
  }

  collection.push(payload)
  return collection.length - 1
}

function hasDirectFieldRelation(field) {
  return Boolean(
    String(field.binding_mode || '') === 'source_table' &&
    (
      (Array.isArray(field.join_keys) && field.join_keys.length) ||
      field.time_binding ||
      (Array.isArray(field.bridge_steps) && field.bridge_steps.length)
    )
  )
}

function uniqueList(values) {
  return [...new Set((values || []).map((item) => String(item || '').trim()).filter(Boolean))]
}

function splitCsv(value) {
  return uniqueList(String(value || '').split(','))
}

function splitLines(value) {
  return uniqueList(String(value || '').split('\n'))
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

function normalizeBaseFilters(items) {
  return (items || [])
    .map((item) => ({
      field: String(item?.field || '').trim(),
      op: String(item?.op || '').trim() || '=',
      value: item?.value === undefined || item?.value === null ? '' : String(item.value),
    }))
    .filter((item) => item.field)
}
