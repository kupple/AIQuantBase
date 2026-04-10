<script setup>
import { onMounted } from 'vue'
import { useRoute } from '#imports'
import { useWorkbench } from '~/composables/useWorkbench'

const route = useRoute()

const menuItems = [
  { path: '/database', label: '数据库一条龙' },
  { path: '/canvas', label: '图谱画板' },
  { path: '/graph', label: '图谱配置' },
  { path: '/fields', label: '字段目录' },
  { path: '/query', label: '自然语言测试' },
  { path: '/preview', label: '配置预览' },
]

const {
  loading,
  saveMessage,
  workspace,
  runtime,
  graphSummary,
  ensureWorkspaceLoaded,
  saveWorkspace,
  notifyAction,
} = useWorkbench()

onMounted(() => {
  notifyAction('', () => ensureWorkspaceLoaded()).catch(() => {})
})
</script>

<template>
  <div class="app-shell">
    <el-container class="shell-frame">
      <el-aside width="320px" class="shell-aside">
        <el-scrollbar class="aside-scroll">
          <div class="aside-inner">
            <div class="brand-block">
              <p class="brand-eyebrow">AIQuantBase</p>
              <h1 class="brand-title">Workbench</h1>
              <p class="brand-description">左侧菜单负责导航，右侧每个功能都是独立页面，数据状态跨页面共享。</p>
              <el-tag v-if="loading" type="warning" effect="plain" size="small">同步中</el-tag>
            </div>

            <el-card shadow="never" class="shell-card">
              <template #header>
                <div class="card-header">
                  <span>功能导航</span>
                </div>
              </template>
              <el-menu :default-active="route.path" class="shell-menu" router>
                <el-menu-item v-for="item in menuItems" :key="item.path" :index="item.path">
                  {{ item.label }}
                </el-menu-item>
              </el-menu>
            </el-card>

            <el-card shadow="never" class="shell-card">
              <template #header>
                <div class="card-header">
                  <span>Workspace</span>
                </div>
              </template>
              <el-form label-position="top">
                <el-form-item label="graph.yaml">
                  <el-input v-model="workspace.graphPath" />
                </el-form-item>
                <el-form-item label="fields.yaml">
                  <el-input v-model="workspace.fieldsPath" />
                </el-form-item>
                <el-form-item label="runtime.local.yaml">
                  <el-input v-model="workspace.runtimePath" />
                </el-form-item>
              </el-form>
              <div class="action-row">
                <el-button :loading="loading" @click="notifyAction('工作区已重载', () => ensureWorkspaceLoaded(true))">重载</el-button>
                <el-button type="primary" :loading="loading" @click="notifyAction('配置已保存', saveWorkspace)">保存</el-button>
              </div>
              <p class="status-copy">{{ saveMessage }}</p>
            </el-card>

            <el-card shadow="never" class="shell-card">
              <template #header>
                <div class="card-header">
                  <span>Summary</span>
                </div>
              </template>
              <div class="summary-grid">
                <div class="summary-tile">
                  <span class="summary-label">节点</span>
                  <strong class="summary-value">{{ graphSummary.nodes }}</strong>
                </div>
                <div class="summary-tile">
                  <span class="summary-label">边</span>
                  <strong class="summary-value">{{ graphSummary.edges }}</strong>
                </div>
                <div class="summary-tile">
                  <span class="summary-label">字段</span>
                  <strong class="summary-value">{{ graphSummary.fields }}</strong>
                </div>
              </div>
            </el-card>

            <el-card shadow="never" class="shell-card">
              <template #header>
                <div class="card-header">
                  <span>Datasource</span>
                </div>
              </template>
              <el-descriptions :column="1" border class="datasource-descriptions">
                <el-descriptions-item label="名称">
                  {{ runtime.datasource.name || '未载入' }}
                </el-descriptions-item>
                <el-descriptions-item label="类型">
                  {{ runtime.datasource.db_type || '-' }}
                </el-descriptions-item>
                <el-descriptions-item label="地址">
                  {{ runtime.datasource.host || '-' }}
                </el-descriptions-item>
                <el-descriptions-item label="端口">
                  {{ runtime.datasource.port || '-' }}
                </el-descriptions-item>
              </el-descriptions>
            </el-card>
          </div>
        </el-scrollbar>
      </el-aside>

      <el-main class="shell-main">
        <div class="content-shell">
          <slot />
        </div>
      </el-main>
    </el-container>
  </div>
</template>
