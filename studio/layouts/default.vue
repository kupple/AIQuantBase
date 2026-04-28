<script setup>
import { computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { navigateTo, useRoute } from '#imports'
import { useWorkbench } from '~/composables/useWorkbench'

const route = useRoute()

const planningItems = [
  { path: '/database', label: '节点工作台' },
  { path: '/membership', label: '归属管理' },
  { path: '/query', label: '查询测试' },
]

const capabilityItems = [
  { path: '/capabilities', label: '模式工作台' },
]

const syncItems = [
  { path: '/sync', label: '同步总览' },
  { path: '/sync/tasks', label: '同步任务' },
  { path: '/sync/jobs', label: '同步记录' },
  { path: '/sync/configs', label: '配置同步' },
]

const otherItems = [
  { path: '/guide', label: '项目说明' },
  { path: '/settings', label: '设置' },
]

const activeTopMenu = computed(() => {
  if (planningItems.some((item) => item.path === route.path)) return '/planning'
  if (route.path === '/capabilities') return '/capabilities-root'
  if (route.path === '/sync' || route.path.startsWith('/sync/')) return '/sync-root'
  if (otherItems.some((item) => item.path === route.path)) return '/other'
  return route.path
})

const {
  ensureWorkspaceLoaded,
  saveMessage,
  hasDatasourceConfigured,
} = useWorkbench()

function handleMenuSelect(index) {
  if (index.startsWith('/')) {
    navigateTo(index)
  }
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
        if (!['/settings', '/guide', '/membership'].includes(route.path) && !route.path.startsWith('/sync') && !route.path.startsWith('/capabilities')) {
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
      if (!['/settings', '/guide', '/membership'].includes(route.path) && !route.path.startsWith('/sync') && !route.path.startsWith('/capabilities')) {
        navigateTo('/settings')
      }
    })
})
</script>

<template>
  <div class="platform-shell-top">
    <div class="platform-backdrop-top"></div>

    <header class="platform-header-top">
      <div class="platform-header-inner">
        <div class="platform-brand-inline">AIQuantBase</div>

        <el-menu
          :default-active="activeTopMenu"
          mode="horizontal"
          class="platform-top-menu"
          :ellipsis="false"
          @select="handleMenuSelect"
        >
          <el-sub-menu index="/planning">
            <template #title>表格规划</template>
            <el-menu-item v-for="item in planningItems" :key="item.path" :index="item.path">
              {{ item.label }}
            </el-menu-item>
          </el-sub-menu>

          <el-sub-menu index="/capabilities-root">
            <template #title>能力接入</template>
            <el-menu-item v-for="item in capabilityItems" :key="item.path" :index="item.path">
              {{ item.label }}
            </el-menu-item>
          </el-sub-menu>

          <el-sub-menu index="/sync-root">
            <template #title>数据同步</template>
            <el-menu-item v-for="item in syncItems" :key="item.path" :index="item.path">
              {{ item.label }}
            </el-menu-item>
          </el-sub-menu>

          <el-sub-menu index="/other">
            <template #title>其他</template>
            <el-menu-item v-for="item in otherItems" :key="item.path" :index="item.path">
              {{ item.label }}
            </el-menu-item>
          </el-sub-menu>
        </el-menu>
      </div>
    </header>

    <main class="platform-main-top">
      <section class="main-content">
        <slot />
      </section>
    </main>
  </div>
</template>

<style scoped>
.platform-shell-top {
  position: relative;
  min-height: 100vh;
  padding: 0;
}

.platform-backdrop-top {
  position: fixed;
  inset: 0;
  z-index: 0;
  border: none;
  border-radius: 0;
  background: rgba(255, 255, 255, 0.28);
  box-shadow: var(--shadow-xl);
  backdrop-filter: blur(24px);
}

.platform-header-top,
.platform-main-top {
  position: relative;
  z-index: 1;
}

.platform-header-top {
  position: sticky;
  top: 0;
  z-index: 20;
  margin-bottom: 14px;
}

.platform-header-inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  align-items: center;
  gap: 18px;
  padding: 10px 16px;
  border-radius: 0;
  background: rgba(255, 255, 255, 0.88);
  border: none;
  box-shadow: var(--shadow-md);
  backdrop-filter: blur(14px);
}

.platform-brand-inline {
  flex: 0 0 auto;
  font-size: 22px;
  line-height: 1;
  letter-spacing: -0.04em;
  font-weight: 700;
  color: var(--text);
}

.platform-top-menu {
  flex: 1 1 auto;
  justify-content: flex-start;
  padding: 0 16px;
  border-bottom: none;
  background: transparent;
  --el-menu-bg-color: transparent;
  --el-menu-hover-bg-color: rgba(15, 118, 110, 0.06);
  --el-menu-active-color: var(--accent);
  --el-menu-text-color: var(--text);
}

.platform-top-menu :deep(.el-menu-item),
.platform-top-menu :deep(.el-sub-menu__title) {
  border-radius: 12px;
}

.platform-main-top {
  display: grid;
  min-width: 0;
  padding: 0 16px;
}

.main-content {
  max-width: none !important;
  width: 100%;
  margin: 0;
  padding-bottom: 24px;
}

@media (max-width: 1180px) {
  .platform-header-inner {
    flex-direction: column;
    align-items: stretch;
  }

  .platform-top-menu {
    justify-content: flex-start;
    padding: 0 12px;
  }
}

@media (max-width: 900px) {
  .platform-shell-top {
    padding: 0;
  }

  .platform-main-top {
    padding: 0 12px;
  }

  .platform-backdrop-top {
    inset: 0;
  }

  .platform-header-inner {
    padding: 16px 12px;
  }
}
</style>
