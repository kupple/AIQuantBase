<script setup>
import { computed, ref } from 'vue'
import { VueFlow } from '@vue-flow/core'
import { Controls } from '@vue-flow/controls'
import { MiniMap } from '@vue-flow/minimap'
import { navigateTo } from '#imports'
import { useWorkbench } from '~/composables/useWorkbench'

const { graph, protocolSummary } = useWorkbench()
const selectedNodeId = ref('')
const graphSearch = ref('')
const graphAssetFilter = ref('all')

const assetFilterOptions = [
  { label: '全部资产', value: 'all' },
  { label: '股票', value: 'stock' },
  { label: 'ETF', value: 'etf' },
  { label: '指数', value: 'index' },
  { label: '基金', value: 'fund' },
  { label: '宏观', value: 'macro' },
  { label: '可转债', value: 'kzz' },
]

const protocolSummaryMap = computed(() =>
  new Map((protocolSummary.value?.items || []).map((item) => [item.name, item]))
)

const filteredGraphNodes = computed(() => {
  const keyword = graphSearch.value.trim().toLowerCase()

  return graph.value.nodes.filter((node) => {
    if (node.status !== 'enabled') return false
    if (graphAssetFilter.value !== 'all' && node.asset_type !== graphAssetFilter.value) return false

    if (!keyword) return true

    const haystack = [
      node.name,
      node.table,
      node.asset_type,
      node.query_freq,
      node.grain,
      node.description,
      node.description_zh,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()

    return haystack.includes(keyword)
  })
})

const visibleNodeIds = computed(() => new Set(filteredGraphNodes.value.map((node) => node.name)))

const graphNodes = computed(() =>
  filteredGraphNodes.value.map((node, index) => ({
    id: node.name,
    position: {
      x: 80 + (index % 3) * 320,
      y: 60 + Math.floor(index / 3) * 140,
    },
    data: node,
    type: 'default',
  }))
)

const graphEdges = computed(() =>
  graph.value.edges
    .filter((edge) => visibleNodeIds.value.has(edge.from) && visibleNodeIds.value.has(edge.to))
    .map((edge, index) => ({
      id: `${edge.name}-${index}`,
      source: edge.from,
      target: edge.to,
      label: edge.time_binding?.mode || edge.relation_type,
      style: { stroke: '#409eff', strokeWidth: 2 },
      labelStyle: { fill: '#606266', fontWeight: 600 },
      animated: edge.time_binding?.mode === 'asof',
    }))
)

const selectedNode = computed(() =>
  graph.value.nodes.find((node) => node.name === selectedNodeId.value) || null
)

const selectedNodeSummary = computed(() =>
  protocolSummaryMap.value.get(selectedNodeId.value) || null
)

const selectedRelations = computed(() => {
  if (!selectedNodeId.value) return []
  return graph.value.edges
    .filter((edge) => edge.from === selectedNodeId.value || edge.to === selectedNodeId.value)
    .map((edge) => ({
      edge_name: edge.name,
      from_node: edge.from,
      to_node: edge.to,
      relation_type: edge.relation_type,
      time_mode: edge.time_binding?.mode || '-',
      join_keys_text: edge.join_keys?.map((item) => `${item.base} -> ${item.source}`).join(', ') || '-',
    }))
})

async function jumpToDatabaseNode(nodeName) {
  if (!nodeName) return
  await navigateTo({
    path: '/database',
    query: { node: nodeName },
  })
}

function handleNodeClick(event) {
  selectedNodeId.value = event.node.id
}
</script>

<template>
  <div class="page-stack">
    <section class="workbench-grid workbench-grid-graph-page">
      <el-card shadow="never" class="surface-card surface-card-strong">
        <template #header>
          <div class="panel-heading panel-heading-space">
            <div>
              <span class="panel-title">图谱关系</span>
              <p class="panel-subtitle">这里展示当前启用节点之间仍然保留的关系边，用于核对入口结构和 Join 规则。</p>
            </div>
            <div class="panel-actions panel-actions-compact graph-filter-toolbar">
              <el-input
                v-model="graphSearch"
                clearable
                placeholder="搜索节点 / 主表 / 资产类型"
                class="panel-search panel-search-wide"
              />
              <el-select v-model="graphAssetFilter" style="width: 150px">
                <el-option v-for="option in assetFilterOptions" :key="option.value" :label="option.label" :value="option.value" />
              </el-select>
            </div>
          </div>
        </template>

        <ClientOnly>
          <div v-if="graphNodes.length" class="graph-page-canvas">
            <VueFlow :nodes="graphNodes" :edges="graphEdges" fit-view-on-init @node-click="handleNodeClick">
              <Controls />
              <MiniMap />

              <template #node-default="nodeProps">
                <div class="graph-page-node" :class="{ 'is-selected': selectedNodeId === nodeProps.id }">
                  <strong>{{ nodeProps.data.name }}</strong>
                  <span>{{ nodeProps.data.table }}</span>
                </div>
              </template>
            </VueFlow>
          </div>
          <el-empty v-else description="当前没有可展示的图谱节点" :image-size="88" />
        </ClientOnly>
      </el-card>

      <div class="stack-block">
        <el-card shadow="never" class="surface-card">
          <template #header>
            <div class="panel-heading">
              <div>
                <span class="panel-title">节点摘要</span>
                <p class="panel-subtitle">点击节点后，这里展示它的主表、粒度和字段规模。</p>
              </div>
            </div>
          </template>

          <div v-if="selectedNode" class="stack-block">
            <el-descriptions :column="1" border>
              <el-descriptions-item label="节点">{{ selectedNode.name }}</el-descriptions-item>
              <el-descriptions-item label="主表">{{ selectedNode.table }}</el-descriptions-item>
              <el-descriptions-item label="状态">{{ selectedNode.status === 'enabled' ? '启用' : '停用' }}</el-descriptions-item>
              <el-descriptions-item label="资产类型">{{ selectedNode.asset_type || '-' }}</el-descriptions-item>
              <el-descriptions-item label="查询频率">{{ selectedNode.query_freq || '-' }}</el-descriptions-item>
              <el-descriptions-item label="粒度">{{ selectedNode.grain || '-' }}</el-descriptions-item>
              <el-descriptions-item label="时间键">{{ selectedNode.time_key || '-' }}</el-descriptions-item>
              <el-descriptions-item label="字段数">{{ selectedNodeSummary?.field_count || '-' }}</el-descriptions-item>
              <el-descriptions-item label="主键">{{ (selectedNodeSummary?.identity_fields || []).join(', ') || '-' }}</el-descriptions-item>
              <el-descriptions-item label="示例字段">{{ (selectedNodeSummary?.sample_fields || []).join(', ') || '-' }}</el-descriptions-item>
            </el-descriptions>
            <div class="panel-actions panel-actions-compact">
              <el-button type="primary" plain @click="jumpToDatabaseNode(selectedNode.name)">
                打开节点工作台
              </el-button>
            </div>
          </div>
          <el-empty v-else description="请先点击图谱中的一个节点" :image-size="72" />
        </el-card>

        <el-card shadow="never" class="surface-card">
          <template #header>
            <div class="panel-heading">
              <div>
                <span class="panel-title">关系边</span>
                <p class="panel-subtitle">这里展示当前选中节点关联的边、时间模式和 Join Keys。</p>
              </div>
            </div>
          </template>

          <el-table :data="selectedRelations" height="360" empty-text="当前节点没有关联边">
            <el-table-column prop="edge_name" label="边名" min-width="180" show-overflow-tooltip />
            <el-table-column prop="from_node" label="From" min-width="160" show-overflow-tooltip />
            <el-table-column prop="to_node" label="To" min-width="180" show-overflow-tooltip />
            <el-table-column prop="relation_type" label="关系类型" min-width="110" />
            <el-table-column prop="time_mode" label="时间模式" min-width="120" />
            <el-table-column prop="join_keys_text" label="Join Keys" min-width="200" show-overflow-tooltip />
          </el-table>
        </el-card>
      </div>
    </section>
  </div>
</template>
