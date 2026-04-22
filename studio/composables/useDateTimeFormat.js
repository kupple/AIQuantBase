function pad(value) {
  return String(value).padStart(2, '0')
}

function normalizeStringDateTime(value) {
  const text = String(value || '').trim()
  if (!text) return '-'

  const normalized = text
    .replace('T', ' ')
    .replace(/\.\d+/, '')
    .replace(/(?:Z|[+-]\d{2}:\d{2})$/, '')
    .trim()

  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    return `${normalized} 00:00:00`
  }

  if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$/.test(normalized)) {
    return `${normalized}:00`
  }

  if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(normalized)) {
    return normalized
  }

  return null
}

export function formatDateTime(value) {
  if (!value) return '-'

  if (typeof value === 'string') {
    const normalized = normalizeStringDateTime(value)
    if (normalized) return normalized
  }

  const date = value instanceof Date ? value : new Date(value)
  if (!Number.isNaN(date.getTime())) {
    return [
      date.getFullYear(),
      pad(date.getMonth() + 1),
      pad(date.getDate()),
    ].join('-') + ` ${[
      pad(date.getHours()),
      pad(date.getMinutes()),
      pad(date.getSeconds()),
    ].join(':')}`
  }

  return String(value)
}
