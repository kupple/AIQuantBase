<script setup>
import { onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { navigateTo, useRoute } from '#imports'
import { useWorkbench } from '~/composables/useWorkbench'

const route = useRoute()

const menuItems = [
  { path: '/database', label: '节点工作台' },
  { path: '/graph', label: '图谱关系' },
  { path: '/guide', label: '项目说明' },
  { path: '/query', label: '查询测试' },
  { path: '/settings', label: '设置' },
]

const {
  ensureWorkspaceLoaded,
  saveMessage,
  hasDatasourceConfigured,
} = useWorkbench()

function handleMenuSelect(index) {
  navigateTo(index)
}

onMounted(() => {
  ensureWorkspaceLoaded()
    .then(() => {
      const missingDatasource = !hasDatasourceConfigured.value || String(saveMessage.value || '').includes('数据库未连接')
      if (missingDatasource) {
        ElMessage.warning({
          message: '请先配置数据库连接',
          duration: 2400,
          grouping: true,
        })
        if (!['/settings', '/guide'].includes(route.path)) {
          navigateTo('/settings')
        }
        return
      }
      ElMessage.success({
        message: '工作区已载入',
        duration: 2200,
      })
    })
    .catch((error) => {
      ElMessage.error({
        message: error instanceof Error ? error.message : '工作区载入失败',
        duration: 3200,
      })
      if (!['/settings', '/guide'].includes(route.path)) {
        navigateTo('/settings')
      }
    })
})
</script>

<template>
  <div class="shell-root">
    <div class="shell-backdrop"></div>

    <el-container class="app-layout">
      <el-header class="app-header">
        <div class="header-inner">
          <div class="app-brand">AIQuantBase</div>
          <el-menu
            :default-active="route.path"
            mode="horizontal"
            class="app-menu"
            :ellipsis="false"
            @select="handleMenuSelect"
          >
            <el-menu-item v-for="item in menuItems" :key="item.path" :index="item.path">
              {{ item.label }}
            </el-menu-item>
          </el-menu>
        </div>
      </el-header>

      <el-main class="app-main">
        <section class="main-content">
          <slot />
        </section>
      </el-main>
    </el-container>
  </div>
</template>
