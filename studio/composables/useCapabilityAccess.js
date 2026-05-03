import { computed } from 'vue'
import { useState } from '#imports'

export function useCapabilityAccess() {
  const loading = useState('capability-access-loading', () => false)
  const saving = useState('capability-access-saving', () => false)
  const workspacePayload = useState('capability-access-workspace', () => null)

  const workspaceInfo = computed(() => workspacePayload.value?.workspace || {})
  const providerNodes = computed(() => workspacePayload.value?.provider_nodes || [])
  const capabilityRegistry = computed(() => workspacePayload.value?.capability_registry || [])
  const capabilities = computed(() => workspacePayload.value?.capabilities || [])
  const modeProfiles = computed(() => workspacePayload.value?.mode_profiles || [])
  const queryTemplates = computed(() => workspacePayload.value?.query_templates || [])
  const diagnostics = computed(() => workspacePayload.value?.diagnostics || [])
  const modeOptions = computed(() =>
    modeProfiles.value.map((item) => ({
      label: `${item.mode_kind}.${item.mode_name}`,
      value: item.mode_id,
    }))
  )

  async function api(path, options = {}) {
    const response = await fetch(path, {
      headers: {
        'Content-Type': 'application/json',
        ...(options.headers || {}),
      },
      ...options,
    })
    const payload = await response.json()
    if (!response.ok) {
      throw new Error(payload?.error || payload?.message || payload?.detail || '请求失败')
    }
    return payload
  }

  async function loadWorkspace() {
    loading.value = true
    try {
      const payload = await api('/api/capabilities/workspace')
      workspacePayload.value = payload
      return payload
    } finally {
      loading.value = false
    }
  }

  async function saveProviderMapping(form, fieldRows) {
    saving.value = true
    try {
      const fields = {}
      for (const row of fieldRows || []) {
        const semanticField = String(row.semantic_field || '').trim()
        const sourceField = normalizeSourceField(row.source_field)
        if (semanticField && sourceField !== undefined) {
          fields[semanticField] = sourceField
        }
      }
      if (!form.nodeName || !form.capability || !Object.keys(fields).length) {
        throw new Error('请先选择节点、填写 capability，并至少配置一个字段映射')
      }
      const payload = await api('/api/capabilities/provider-node', {
        method: 'POST',
        body: JSON.stringify({
          node_name: form.nodeName,
          capability: form.capability,
          capability_name: form.capabilityName,
          capability_description: form.capabilityDescription,
          default_slots: form.defaultSlots || form.allowedSlots || [],
          output_scope: form.outputScope || undefined,
          asset_types: splitList(form.assetTypes),
          query_profiles: splitList(form.queryProfiles || form.accessPatterns),
          keys: form.keys || undefined,
          field_usages: form.fieldUsages || form.field_usages || undefined,
          clear_default_slots: Boolean(form.clearDefaultSlots || form.clear_default_slots),
          clear_field_usages: Boolean(form.clearFieldUsages || form.clear_field_usages),
          replace_provider_nodes: Boolean(form.replaceProviderNodes || form.replace_provider_nodes),
          replace_fields: Boolean(form.replaceFields || form.replace_fields),
          fields,
        }),
      })
      workspacePayload.value = payload.workspace
      return payload
    } finally {
      saving.value = false
    }
  }

  async function saveModeCapability(form) {
    saving.value = true
    try {
      if (!form.modeId || !form.capability) {
        throw new Error('请先选择模式配置并填写 capability')
      }
      const sectionMap = {
        required: 'required_capabilities',
        conditional: 'conditional_capabilities',
        optional: 'optional_capabilities',
        extension: 'extension_capability_bindings',
      }
      const payload = await api('/api/capabilities/mode-capability', {
        method: 'POST',
        body: JSON.stringify({
          mode_id: form.modeId,
          section: sectionMap[form.section] || form.section,
          capability: form.capability,
          fields: Array.isArray(form.fields) ? form.fields : splitList(form.fieldsText),
          provider_node: form.providerNode || form.provider_node || undefined,
          field_map: form.fieldMap || form.field_map || undefined,
          slots: form.slots || form.allowedSlots,
          allowed_slots: form.allowedSlots || form.slots,
          field_usages: form.fieldUsages || form.field_usages || undefined,
        }),
      })
      workspacePayload.value = payload.workspace
      return payload
    } finally {
      saving.value = false
    }
  }

  async function setCapabilityEnabled(form) {
    saving.value = true
    try {
      if (!form.capability) {
        throw new Error('请先选择扩展能力')
      }
      const payload = await api('/api/capabilities/registry-capability', {
        method: 'POST',
        body: JSON.stringify({
          capability: form.capability,
          enabled: Boolean(form.enabled),
        }),
      })
      workspacePayload.value = payload.workspace
      return payload
    } finally {
      saving.value = false
    }
  }

  async function deleteModeCapability(form) {
    saving.value = true
    try {
      if (!form.modeId || !form.capability) {
        throw new Error('请先选择模式配置和要删除的 capability')
      }
      const sectionMap = {
        required: 'required_capabilities',
        conditional: 'conditional_capabilities',
        optional: 'optional_capabilities',
        extension: 'extension_capability_bindings',
      }
      const payload = await api('/api/capabilities/mode-capability/delete', {
        method: 'POST',
        body: JSON.stringify({
          mode_id: form.modeId,
          section: sectionMap[form.section] || form.section,
          capability: form.capability,
          delete_provider_registration: Boolean(form.deleteProviderRegistration),
        }),
      })
      workspacePayload.value = payload.workspace
      return payload
    } finally {
      saving.value = false
    }
  }

  async function resolveModeExtensionContract(form) {
    const payload = await api('/api/capabilities/mode-extension-contract', {
      method: 'POST',
      body: JSON.stringify({
        mode_id: form.modeId,
        capability: form.capability,
        output_scope: form.outputScope || undefined,
        default_slots: form.defaultSlots || [],
        fields: form.fields || [],
        default_field_usages: form.defaultFieldUsages || form.default_field_usages || undefined,
        field_usages: form.fieldUsages || form.field_usages || undefined,
        slots: form.slots || form.allowedSlots || [],
      }),
    })
    return payload
  }

  function splitList(value) {
    return String(value || '')
      .replace(/\n/g, ',')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
      .filter((item, index, arr) => arr.indexOf(item) === index)
  }

  function formatJson(value) {
    return JSON.stringify(value || {}, null, 2)
  }

  function normalizeSourceField(value) {
    if (value === null || value === undefined) return undefined
    if (typeof value !== 'string') return value
    const text = value.trim()
    if (!text) return undefined
    if (text.startsWith('{') || text.startsWith('[')) {
      try {
        return JSON.parse(text)
      } catch {
        return text
      }
    }
    return text
  }

  return {
    loading,
    saving,
    workspacePayload,
    workspaceInfo,
    providerNodes,
    capabilityRegistry,
    capabilities,
    modeProfiles,
    queryTemplates,
    diagnostics,
    modeOptions,
    loadWorkspace,
    saveProviderMapping,
    saveModeCapability,
    setCapabilityEnabled,
    deleteModeCapability,
    resolveModeExtensionContract,
    splitList,
    formatJson,
  }
}
