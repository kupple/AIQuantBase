<script setup>
import { useWorkbench } from '~/composables/useWorkbench'

const {
  graph,
  nodeForm,
  edgeForm,
  editNode,
  editEdge,
  removeItem,
  saveNode,
  saveEdge,
  notifyAction,
} = useWorkbench()
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="page-card">
      <WorkbenchPageHeader
        title="图谱配置"
        description="节点、关系和编辑表单都收在这个页面里，便于集中维护图谱结构。"
      />
    </el-card>

    <el-row :gutter="20" class="page-grid">
      <el-col :xl="15" :lg="14" :md="24">
        <div class="stack-grid">
          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>节点列表</span>
                  <p class="card-caption">点击编辑会把当前节点带到右侧表单。</p>
                </div>
              </div>
            </template>

            <el-table :data="graph.nodes" max-height="320" empty-text="当前没有节点">
              <el-table-column prop="name" label="节点名" min-width="160" />
              <el-table-column prop="table" label="表名" min-width="240" />
              <el-table-column prop="grain" label="粒度" width="120" />
              <el-table-column label="操作" width="160">
                <template #default="scope">
                  <el-button link type="primary" @click="editNode(scope.row, scope.$index)">编辑</el-button>
                  <el-button link type="danger" @click="removeItem(graph.nodes, scope.$index)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </el-card>

          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>关系列表</span>
                  <p class="card-caption">这里用于维护边、Join Key 和时间绑定模式。</p>
                </div>
              </div>
            </template>

            <el-table :data="graph.edges" max-height="320" empty-text="当前没有关系">
              <el-table-column prop="name" label="关系名" min-width="220" />
              <el-table-column prop="from" label="From" min-width="140" />
              <el-table-column prop="to" label="To" min-width="140" />
              <el-table-column label="模式" width="140">
                <template #default="scope">
                  {{ scope.row.time_binding?.mode || '-' }}
                </template>
              </el-table-column>
              <el-table-column label="操作" width="160">
                <template #default="scope">
                  <el-button link type="primary" @click="editEdge(scope.row, scope.$index)">编辑</el-button>
                  <el-button link type="danger" @click="removeItem(graph.edges, scope.$index)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </div>
      </el-col>

      <el-col :xl="9" :lg="10" :md="24">
        <div class="stack-grid">
          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>节点编辑</span>
                  <p class="card-caption">节点名、时间键和字段列表都在这里维护。</p>
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
                  <span>关系编辑</span>
                  <p class="card-caption">编辑时间模式、Join Keys 和关联方向。</p>
                </div>
              </div>
            </template>

            <el-form label-position="top">
              <el-form-item label="关系名">
                <el-input v-model="edgeForm.name" />
              </el-form-item>
              <el-form-item label="来源节点">
                <el-input v-model="edgeForm.from" />
              </el-form-item>
              <el-form-item label="目标节点">
                <el-input v-model="edgeForm.to" />
              </el-form-item>
              <el-form-item label="关系类型">
                <el-select v-model="edgeForm.relation_type" style="width: 100%">
                  <el-option label="direct" value="direct" />
                  <el-option label="bridge" value="bridge" />
                </el-select>
              </el-form-item>
              <el-form-item label="Join Keys">
                <el-input v-model="edgeForm.join_keys" type="textarea" :rows="3" />
              </el-form-item>
              <el-form-item label="时间模式">
                <el-select v-model="edgeForm.time_mode" style="width: 100%">
                  <el-option label="same_date" value="same_date" />
                  <el-option label="same_timestamp" value="same_timestamp" />
                  <el-option label="asof" value="asof" />
                  <el-option label="effective_range" value="effective_range" />
                </el-select>
              </el-form-item>
              <el-form-item label="Base Time Field">
                <el-input v-model="edgeForm.base_time_field" />
              </el-form-item>
              <el-form-item label="Base Time Cast">
                <el-input v-model="edgeForm.base_time_cast" />
              </el-form-item>
              <el-form-item label="Source Time Field">
                <el-input v-model="edgeForm.source_time_field" />
              </el-form-item>
              <el-form-item label="Source Start / End">
                <el-input v-model="edgeForm.source_range" />
              </el-form-item>
              <el-form-item label="描述">
                <el-input v-model="edgeForm.description" type="textarea" :rows="3" />
              </el-form-item>
            </el-form>

            <el-button type="primary" @click="notifyAction('关系已保存', saveEdge)">保存关系</el-button>
          </el-card>
        </div>
      </el-col>
    </el-row>
  </div>
</template>
