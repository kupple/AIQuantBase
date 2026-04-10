<script setup>
import { useWorkbench } from '~/composables/useWorkbench'

const {
  queryLab,
  runNaturalLanguageQuery,
  runSql,
  notifyAction,
} = useWorkbench()
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never" class="page-card">
      <WorkbenchPageHeader
        title="自然语言测试"
        description="通过自然语言直接查看 Query Intent、SQL 和执行结果。"
      />
    </el-card>

    <el-row :gutter="20" class="page-grid">
      <el-col :xl="13" :lg="12" :md="24">
        <el-card shadow="never" class="page-card">
          <template #header>
            <div class="card-header">
              <div>
                <span>自然语言测试台</span>
                <p class="card-caption">输入问题后会同时生成 Intent 和 SQL。</p>
              </div>
              <el-button type="primary" @click="notifyAction('自然语言查询已完成', runNaturalLanguageQuery)">
                生成并执行
              </el-button>
            </div>
          </template>

          <el-form label-position="top">
            <el-form-item label="自然语言问题">
              <el-input v-model="queryLab.naturalQuery" type="textarea" :rows="5" />
            </el-form-item>
            <el-form-item label="Query Intent">
              <pre class="code-frame">{{
                queryLab.queryIntent ? JSON.stringify(queryLab.queryIntent, null, 2) : '等待生成 Intent'
              }}</pre>
            </el-form-item>
          </el-form>
        </el-card>
      </el-col>

      <el-col :xl="11" :lg="12" :md="24">
        <div class="stack-grid">
          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>SQL 预览</span>
                  <p class="card-caption">可以手动修改 SQL 后再次执行。</p>
                </div>
                <el-button @click="notifyAction('SQL 执行完成', runSql)">执行当前 SQL</el-button>
              </div>
            </template>

            <el-input v-model="queryLab.sql" type="textarea" :rows="10" />
          </el-card>

          <el-card shadow="never" class="page-card">
            <template #header>
              <div class="card-header">
                <div>
                  <span>结果预览</span>
                  <p class="card-caption">这里会显示接口返回结果或错误信息。</p>
                </div>
              </div>
            </template>

            <el-alert
              v-if="queryLab.error"
              :title="queryLab.error"
              type="error"
              :closable="false"
              show-icon
              class="inline-alert"
            />
            <pre class="code-frame">{{ queryLab.result ? JSON.stringify(queryLab.result, null, 2) : '等待结果' }}</pre>
          </el-card>
        </div>
      </el-col>
    </el-row>
  </div>
</template>
