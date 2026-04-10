<script setup>
defineProps({
  eyebrow: {
    type: String,
    default: 'AIQuantBase Workbench',
  },
  title: {
    type: String,
    required: true,
  },
  description: {
    type: String,
    default: '',
  },
})
</script>

<template>
  <div class="page-header">
    <div class="page-header-copy">
      <p class="page-eyebrow">{{ eyebrow }}</p>
      <h2 class="page-title">{{ title }}</h2>
      <p v-if="description" class="page-description">{{ description }}</p>
    </div>
    <div v-if="$slots.actions" class="page-header-actions">
      <slot name="actions" />
    </div>
  </div>
</template>
