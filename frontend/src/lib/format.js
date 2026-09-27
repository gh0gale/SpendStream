// Money is rupees with Indian digit grouping; dates are shown in India time.

export const formatINR = (n, { paise = false } = {}) =>
  '₹' + Number(n).toLocaleString('en-IN', {
    minimumFractionDigits: paise ? 2 : 0,
    maximumFractionDigits: paise ? 2 : 0,
  })

// A Postgres date ('2026-09-26') is a calendar day, not an instant.
const asDay = (d) => new Date(`${String(d).slice(0, 10)}T00:00:00`)

export const formatDay = (d) =>
  asDay(d).toLocaleDateString('en-IN', { day: 'numeric', month: 'short' })

export const formatMonth = (m) =>
  asDay(m).toLocaleDateString('en-IN', { month: 'long', year: 'numeric' })

export const formatMonthShort = (m) =>
  asDay(m).toLocaleDateString('en-IN', { month: 'short' })

export const formatDateTime = (ts) =>
  new Date(ts).toLocaleString('en-IN', {
    day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false,
    timeZone: 'Asia/Kolkata',
  })

// '2026-09-01' -> ['2026-09-01', '2026-10-01'], the half-open range of that month.
export function monthRange(month) {
  const [y, m] = month.slice(0, 7).split('-').map(Number)
  const next = m === 12 ? `${y + 1}-01` : `${y}-${String(m + 1).padStart(2, '0')}`
  return [`${month.slice(0, 7)}-01`, `${next}-01`]
}

export const plural = (n, one, many = `${one}s`) => `${n} ${n === 1 ? one : many}`
