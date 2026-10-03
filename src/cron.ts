// Cron parsing shared by the scheduler backends and the next-run calculation.
// Supports "*", "*/n", single numbers and comma lists.

export function splitCronExpression(cron: string): [string, string, string, string, string] {
  const parts = cron.trim().split(/\s+/)
  if (parts.length !== 5) {
    throw new Error(`Invalid cron: ${cron}`)
  }
  return parts as [string, string, string, string, string]
}

export function uniqueSorted(values: number[]): number[] {
  return Array.from(new Set(values)).sort((a, b) => a - b)
}

export function parseCronField(
  field: string,
  min: number,
  max: number,
  label: string,
  allowSundaySeven = false
): number[] | null {
  if (field === "*") return null

  if (field.startsWith("*/")) {
    const step = parseInt(field.slice(2), 10)
    if (!Number.isFinite(step) || step <= 0) {
      throw new Error(`Invalid cron ${label} step: ${field}`)
    }
    const values: number[] = []
    for (let value = min; value <= max; value += step) {
      values.push(value)
    }
    return values
  }

  const parts = field.split(",")
  if (parts.length > 1) {
    const values = parts.map((part) => parseCronNumber(part, min, max, label, allowSundaySeven))
    return uniqueSorted(values)
  }

  if (/^\d+$/.test(field)) {
    return [parseCronNumber(field, min, max, label, allowSundaySeven)]
  }

  throw new Error(`Invalid cron ${label} field: ${field}`)
}

export function parseCronNumber(
  value: string,
  min: number,
  max: number,
  label: string,
  allowSundaySeven: boolean
): number {
  const parsed = parseInt(value, 10)
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid cron ${label} value: ${value}`)
  }
  const normalized = allowSundaySeven && parsed === 7 ? 0 : parsed
  if (normalized < min || normalized > max) {
    throw new Error(`Invalid cron ${label} value: ${value}`)
  }
  return normalized
}

export function validateCronExpression(cron: string): void {
  const [minute, hour, dayOfMonth, month, dayOfWeek] = splitCronExpression(cron)
  parseCronField(minute, 0, 59, "minute")
  parseCronField(hour, 0, 23, "hour")
  parseCronField(dayOfMonth, 1, 31, "day of month")
  parseCronField(month, 1, 12, "month")
  parseCronField(dayOfWeek, 0, 7, "day of week", true)
}

// Next local time `cron` fires strictly after `from`, or undefined when no
// match exists within ~5 years (e.g. "0 0 31 2 *"). Follows Vixie cron: when
// both day fields are restricted, either one matching is enough; a field
// starting with "*" (including "*/n") counts as unrestricted for that rule.
export function nextCronRun(cron: string, from: Date = new Date()): Date | undefined {
  const [minuteField, hourField, domField, monthField, dowField] = splitCronExpression(cron)
  const minutes = parseCronField(minuteField, 0, 59, "minute")
  const hours = parseCronField(hourField, 0, 23, "hour")
  const doms = parseCronField(domField, 1, 31, "day of month")
  const months = parseCronField(monthField, 1, 12, "month")
  const dows = parseCronField(dowField, 0, 7, "day of week", true)
  const domStar = domField.startsWith("*")
  const dowStar = dowField.startsWith("*")

  const has = (values: number[] | null, value: number) => values === null || values.includes(value)
  const dayMatches = (date: Date) => {
    const domOk = has(doms, date.getDate())
    const dowOk = has(dows, date.getDay())
    if (domStar || dowStar) return domOk && dowOk
    return domOk || dowOk
  }

  const t = new Date(from.getTime())
  t.setSeconds(0, 0)
  t.setMinutes(t.getMinutes() + 1)
  const limit = from.getTime() + 5 * 366 * 24 * 60 * 60 * 1000

  while (t.getTime() <= limit) {
    if (!has(months, t.getMonth() + 1)) {
      t.setMonth(t.getMonth() + 1, 1)
      t.setHours(0, 0, 0, 0)
      continue
    }
    if (!dayMatches(t)) {
      t.setDate(t.getDate() + 1)
      t.setHours(0, 0, 0, 0)
      continue
    }
    if (!has(hours, t.getHours())) {
      t.setHours(t.getHours() + 1, 0, 0, 0)
      continue
    }
    if (!has(minutes, t.getMinutes())) {
      t.setMinutes(t.getMinutes() + 1, 0, 0)
      continue
    }
    return t
  }
  return undefined
}
