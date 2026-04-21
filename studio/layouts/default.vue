<script setup>
import { computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { navigateTo, useRoute } from '#imports'
import { useWorkbench } from '~/composables/useWorkbench'

const route = useRoute()

const menuItems = [
  {
    section: '数据底座',
    items: [
      { path: '/database', label: '节点工作台', hint: '普通节点与宽表节点' },
      { path: '/membership', label: '归属管理', hint: '关系表与成员信息表' },
      { path: '/query', label: '查询测试', hint: '验证 Query Intent 与 SQL' },
    ],
  },
  {
    section: '同步管理',
    items: [
      { path: '/sync', label: '同步任务', hint: '任务执行、日志与状态' },
    ],
  },
  {
    section: '其他',
    items: [
      { path: '/guide', label: '项目说明', hint: '需求文档与说明' },
      { path: '/settings', label: '设置', hint: '数据库与运行时配置' },
    ],
  },
]

const flatMenuItems = computed(() => menuItems.flatMap((group) => group.items))

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
        if (!['/settings', '/guide', '/membership', '/sync'].includes(route.path)) {
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
      if (!['/settings', '/guide', '/membership', '/sync'].includes(route.path)) {
        navigateTo('/settings')
      }
    })
})
</script>

<template>
  <div class="shell-root">
    <div class="shell-backdrop"></div>

    <aside class="shell-sidebar">
      <div class="sidebar-brand">
        <p class="sidebar-eyebrow">Quant Platform</p>
        <h1 class="sidebar-title">AIQuantBase</h1>
        <p class="sidebar-copy">统一管理数据底座、同步任务和后续上层能力。</p>
      </div>

      <div v-for="group in menuItems" :key="group.section" class="sidebar-section">
        <div class="sidebar-section-heading">{{ group.section }}</div>
        <el-menu
          :default-active="route.path"
          class="sidebar-menu"
          @select="handleMenuSelect"
        >
          <el-menu-item
            v-for="item in group.items"
            :key="item.path"
            :index="item.path"
            class="sidebar-menu-item"
          >
            <div class="menu-item-copy">
              <span class="menu-item-label">{{ item.label }}</span>
              <span class="menu-item-hint">{{ item.hint }}</span>
            </div>
          </el-menu-item>
        </el-menu>
      </div>

      <div class="sidebar-footer sidebar-section-soft">
        <div class="workspace-line">
          <span class="workspace-key">当前页面</span>
          <span class="workspace-value">{{ flatMenuItems.find((item) => item.path === route.path)?.label || '工作台' }}</span>
        </div>
      </div>
    </aside>

    <main class="shell-main">
      <section class="main-content">
        <slot />
      </section>
    </main>
  </div>
</template>

<style scoped>
.shell-root {
  position: relative;
  display: grid !important;
  grid-template-columns: var(--sidebar-width) minmax(0, 1fr) !important;
  min-height: 100vh;
  gap: 20px;
  padding: 18px;
}

.shell-backdrop {
  display: block !important;
  position: fixed;
  inset: 18px;
  z-index: 0;
}

.shell-sidebar,
.shell-main {
  position: relative;
  z-index: 1;
}

.shell-sidebar {
  display: grid !important;
  align-content: start;
  gap: 16px;
  min-height: calc(100vh - 36px);
  padding: 24px 18px 20px;
  border-radius: 28px;
  background:
    linear-gradient(180deg, rgba(10, 19, 25, 0.94), rgba(14, 26, 34, 0.9));
  color: var(--sidebar-text);
  overflow: hidden;
}

.shell-main {
  display: grid !important;
  align-content: start;
  gap: 18px;
  min-width: 0;
}

.sidebar-menu {
  border-right: none;
  background: transparent;
  --el-menu-bg-color: transparent;
  --el-menu-hover-bg-color: rgba(140, 225, 218, 0.08);
  --el-menu-text-color: rgba(238, 247, 245, 0.92);
  --el-menu-active-color: #ffffff;
}

.sidebar-menu-item {
  height: auto;
  min-height: 68px;
  margin-bottom: 8px;
  border-radius: var(--radius-md);
  line-height: 1.2;
}

.sidebar-menu :deep(.el-menu-item.is-active) {
  background: linear-gradient(135deg, rgba(140, 225, 218, 0.16), rgba(140, 225, 218, 0.08));
}

.menu-item-copy {
  display: grid;
  gap: 6px;
  padding: 6px 0;
}

.menu-item-label {
  font-weight: 700;
}

.menu-item-hint {
  color: rgba(238, 247, 245, 0.64);
  font-size: 12px;
  white-space: normal;
}

@media (max-width: 1280px) {
  .shell-root {
    grid-template-columns: 1fr !important;
  }

  .shell-sidebar {
    min-height: auto;
  }
}

@media (max-width: 900px) {
  .shell-root {
    padding: 12px;
  }

  .shell-backdrop {
    inset: 12px;
  }

  .shell-sidebar {
    padding-left: 18px;
    padding-right: 18px;
  }
}
</style>
