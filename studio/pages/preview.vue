<script setup>
import { useWorkbench } from '~/composables/useWorkbench'

const { graphPreview, fieldsPreview } = useWorkbench()
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="page-card">
      <WorkbenchPageHeader
        title="配置预览"
        description="把 graph.yaml 和 fields.yaml 的当前内存状态直接摊开检查。"
      />
    </el-card>

    <el-row :gutter="20" class="page-grid">
      <el-col :xl="12" :md="24">
        <el-card shadow="never" class="page-card">
          <template #header>
            <div class="card-header">
              <div>
                <span>graph.yaml</span>
                <p class="card-caption">节点和关系的 YAML 预览。</p>
              </div>
            </div>
          </template>

          <pre class="code-frame">{{ graphPreview }}</pre>
        </el-card>
      </el-col>

      <el-col :xl="12" :md="24">
        <el-card shadow="never" class="page-card">
          <template #header>
            <div class="card-header">
              <div>
                <span>fields.yaml</span>
                <p class="card-caption">字段目录的 YAML 预览。</p>
              </div>
            </div>
          </template>

          <pre class="code-frame">{{ fieldsPreview }}</pre>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>
