<script setup>
import { useWorkbench } from '~/composables/useWorkbench'

const {
  schema,
  nodeForm,
  fieldForm,
  loading,
  selectDatabase,
  selectTable,
  toggleColumn,
  clearSelectedColumns,
  inferNodeTemplate,
  addSelectedColumnsAsFields,
  addSingleField,
  saveNode,
  saveField,
  notifyAction,
} = useWorkbench()

function handleDatabaseChange(value) {
  notifyAction('', () => selectDatabase(value)).catch(() => {})
}

function handleTableChange(name) {
  notifyAction('', () => selectTable(name)).catch(() => {})
}
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="page-card">
      <WorkbenchPageHeader
        title="数据库一条龙"
        description="从数据库选择表和字段，生成节点草稿，并把字段同步进字段目录。"
      />
    </el-card>

    <el-row :gutter="20" class="page-grid">
      <el-col :xl="15" :lg="14" :md="24">
        <el-card shadow="never" class="page-card">
          <template #header>
            <div class="card-header">
              <div>
                <span>数据库浏览</span>
                <p class="card-caption">先选库，再选表，再选择要进入图谱的字段。</p>
              </div>
            </div>
          </template>

          <div class="browser-grid">
            <div class="browser-side">
              <el-form label-position="top">
                <el-form-item label="数据库">
                  <el-select
                    placeholder="选择数据库"
                    :loading="loading"
                    :model-value="schema.selectedDatabase"
                    style="width: 100%"
                    @change="handleDatabaseChange"
                  >
                    <el-option v-for="db in schema.databases" :key="db.name" :label="db.name" :value="db.name" />
                  </el-select>
                </el-form-item>
              </el-form>

              <div class="table-button-list">
                <el-button
                  v-for="table in schema.tables"
                  :key="table.name"
                  :type="schema.selectedTable === table.name ? 'primary' : 'default'"
                  plain
                  @click="handleTableChange(table.name)"
                >
                  {{ table.name }}
                </el-button>
              </div>

              <el-empty
                v-if="schema.selectedDatabase && !schema.tables.length"
                description="当前数据库下没有可选表"
                :image-size="72"
              />
            </div>

            <div class="browser-main">
              <div class="toolbar-row">
                <div>
                  <h3 class="section-title">{{ schema.selectedTable || '字段列表' }}</h3>
                  <p class="card-caption">已选 {{ schema.selectedColumns.length }} 个字段</p>
                </div>
                <el-space wrap>
                  <el-button @click="clearSelectedColumns">清空</el-button>
                  <el-button type="primary" :disabled="!schema.selectedTable" @click="inferNodeTemplate">生成节点</el-button>
                  <el-button
                    type="primary"
                    plain
                    :disabled="!schema.selectedColumns.length"
                    @click="addSelectedColumnsAsFields"
                  >
                    批量加字段
                  </el-button>
                </el-space>
              </div>

              <el-table :data="schema.columns" row-key="name" max-height="560" empty-text="先从左侧选择一张表">
                <el-table-column prop="name" label="字段名" min-width="220" />
                <el-table-column label="类型" width="150">
                  <template #default="{ row }">
                    <el-tag effect="plain" size="small">{{ row.type }}</el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="状态" width="100">
                  <template #default="{ row }">
                    <el-tag
                      size="small"
                      :type="schema.selectedColumns.includes(row.name) ? 'success' : 'info'"
                      effect="plain"
                    >
                      {{ schema.selectedColumns.includes(row.name) ? '已选' : '未选' }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column label="操作" min-width="220">
                  <template #default="{ row }">
                    <el-space wrap>
                      <el-button size="small" @click="toggleColumn(row.name)">
                        {{ schema.selectedColumns.includes(row.name) ? '取消' : '选择' }}
                      </el-button>
                      <el-button size="small" type="primary" plain @click="addSingleField(row.name)">单加字段</el-button>
                    </el-space>
                  </template>
                </el-table-column>
              </el-table>
            </div>
          </div>
        </el-card>
      </el-col>

      <el-col :xl="9" :lg="10" :md="24">
        <div class="stack-grid">
          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>节点草稿</span>
                  <p class="card-caption">生成后可以直接在这里补全和保存节点。</p>
                </div>
              </div>
            </template>

            <el-form label-position="top">
              <el-form-item label="节点名">
                <el-input v-model="nodeForm.name" />
              </el-form-item>
              <el-form-item label="表名">
                <el-input v-model="nodeForm.table" />
              </el-form-item>
              <el-form-item label="实体键">
                <el-input v-model="nodeForm.entity_keys" />
              </el-form-item>
              <el-form-item label="时间键">
                <el-input v-model="nodeForm.time_key" />
              </el-form-item>
              <el-form-item label="粒度">
                <el-input v-model="nodeForm.grain" />
              </el-form-item>
              <el-form-item label="字段列表">
                <el-input v-model="nodeForm.fields" type="textarea" :rows="4" />
              </el-form-item>
              <el-form-item label="描述">
                <el-input v-model="nodeForm.description" type="textarea" :rows="3" />
              </el-form-item>
            </el-form>

            <el-button type="primary" @click="notifyAction('节点已保存', saveNode)">保存节点</el-button>
          </el-card>

          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>字段草稿</span>
                  <p class="card-caption">单加字段或跨页面编辑字段时，都会落到这里。</p>
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
        </div>
      </el-col>
    </el-row>
  </div>
</template>
