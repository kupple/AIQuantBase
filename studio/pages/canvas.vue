<script setup>
import { ref } from 'vue'
import { VueFlow } from '@vue-flow/core'
import { Controls } from '@vue-flow/controls'
import { MiniMap } from '@vue-flow/minimap'
import { useWorkbench } from '~/composables/useWorkbench'

const { flowNodes, flowEdges } = useWorkbench()
const flowInstance = ref(null)

function onPaneReady(instance) {
  flowInstance.value = instance
  instance.fitView({ padding: 0.25 })
}
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="page-card">
      <WorkbenchPageHeader
        title="图谱画板"
        description="用画板视图检查主干节点、补充节点和关键关系。"
      >
        <template #actions>
          <el-button @click="flowInstance?.fitView({ padding: 0.25 })">适配视图</el-button>
        </template>
      </WorkbenchPageHeader>
    </el-card>

    <el-card shadow="never" class="page-card">
      <template #header>
        <div class="card-header">
          <div>
            <span>关系画板</span>
            <p class="card-caption">画板和配置页共用同一份图谱数据，编辑后这里会立即刷新。</p>
          </div>
        </div>
      </template>

      <ClientOnly>
        <div v-if="flowNodes.length" class="canvas-shell">
          <VueFlow :nodes="flowNodes" :edges="flowEdges" fit-view-on-init @pane-ready="onPaneReady">
            <Controls />
            <MiniMap />

            <template #node-default="nodeProps">
              <div class="flow-node">
                <h4>{{ nodeProps.data.name }}</h4>
                <p>{{ nodeProps.data.table }}</p>
                <div class="flow-badges">
                  <span class="flow-pill">grain: {{ nodeProps.data.grain || '-' }}</span>
                  <span class="flow-pill">fields: {{ (nodeProps.data.fields || []).length }}</span>
                </div>
              </div>
            </template>
          </VueFlow>
        </div>
        <el-empty v-else description="当前没有可展示的节点" :image-size="100" />
      </ClientOnly>
    </el-card>
  </div>
</template>
