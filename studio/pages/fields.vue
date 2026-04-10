<script setup>
import { useWorkbench } from '~/composables/useWorkbench'

const {
  fields,
  fieldForm,
  fieldsPreview,
  editField,
  removeItem,
  saveField,
  notifyAction,
} = useWorkbench()
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="page-card">
      <WorkbenchPageHeader
        title="字段目录"
        description="字段目录、字段编辑和 YAML 预览拆成同一页，便于一边维护一边核对输出。"
      />
    </el-card>

    <el-row :gutter="20" class="page-grid">
      <el-col :xl="14" :lg="13" :md="24">
        <el-card shadow="never" class="page-card">
          <template #header>
            <div class="card-header">
              <div>
                <span>标准字段目录</span>
                <p class="card-caption">点击编辑会把当前字段带到右侧表单。</p>
              </div>
            </div>
          </template>

          <el-table :data="fields" max-height="680" empty-text="当前没有字段">
            <el-table-column prop="standard_field" label="标准字段名" min-width="180" />
            <el-table-column prop="resolver_type" label="解析方式" width="120" />
            <el-table-column prop="source_node" label="来源节点" min-width="160" />
            <el-table-column label="操作" width="160">
              <template #default="scope">
                <el-button link type="primary" @click="editField(scope.row, scope.$index)">编辑</el-button>
                <el-button link type="danger" @click="removeItem(fields, scope.$index)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <el-col :xl="10" :lg="11" :md="24">
        <div class="stack-grid">
          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>字段编辑</span>
                  <p class="card-caption">数据库页和字段页共用同一份字段草稿。</p>
                </div>
              </div>
            </template>

            <el-form label-position="top">
              <el-form-item label="标准字段名">
                <el-input v-model="fieldForm.standard_field" />
              </el-form-item>
              <el-form-item label="字段角色">
                <el-input v-model="fieldForm.field_role" />
              </el-form-item>
              <el-form-item label="解析方式">
                <el-select v-model="fieldForm.resolver_type" style="width: 100%">
                  <el-option label="direct" value="direct" />
                  <el-option label="derived" value="derived" />
                </el-select>
              </el-form-item>
              <el-form-item label="来源节点">
                <el-input v-model="fieldForm.source_node" />
              </el-form-item>
              <el-form-item label="来源字段">
                <el-input v-model="fieldForm.source_field" />
              </el-form-item>
              <el-form-item label="适用粒度">
                <el-input v-model="fieldForm.applies_to_grain" />
              </el-form-item>
              <el-form-item label="依赖字段">
                <el-input v-model="fieldForm.depends_on" />
              </el-form-item>
              <el-form-item label="公式">
                <el-input v-model="fieldForm.formula" type="textarea" :rows="3" />
              </el-form-item>
              <el-form-item label="备注">
                <el-input v-model="fieldForm.notes" type="textarea" :rows="3" />
              </el-form-item>
            </el-form>

            <el-button type="primary" @click="notifyAction('字段已保存', saveField)">保存字段</el-button>
          </el-card>

          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>字段 YAML 预览</span>
                  <p class="card-caption">保存字段后，这里的预览会立即刷新。</p>
                </div>
              </div>
            </template>

            <pre class="code-frame">{{ fieldsPreview }}</pre>
          </el-card>
        </div>
      </el-col>
    </el-row>
  </div>
</template>
