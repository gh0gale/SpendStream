// Payments as a CSV file, built in the browser from rows the user can already read.

// A cell that starts with = + - or @ is read as a formula by spreadsheets, and
// merchant names come from bank alert text, so such a cell is prefixed with a quote.
const cell = (value) => {
  let s = value === null || value === undefined ? '' : String(value)
  if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
}

// rows: silver_transactions rows with transaction_date, merchant, category, amount, merchant_is_person.
export function paymentsToCsv(rows) {
  const lines = [['Date', 'Merchant', 'Category', 'Amount (INR)', 'To a person'].join(',')]
  for (const r of rows) {
    lines.push([
      r.transaction_date ?? '',
      r.merchant ?? '',
      r.category ?? 'Unsure',
      r.amount,
      r.merchant_is_person ? 'yes' : 'no',
    ].map(cell).join(','))
  }
  return lines.join('\r\n') + '\r\n'
}

export function downloadCsv(filename, text) {
  // The byte order mark makes Excel read the file as UTF-8.
  const url = URL.createObjectURL(new Blob(['﻿', text], { type: 'text/csv;charset=utf-8' }))
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  link.remove()
  URL.revokeObjectURL(url)
}
