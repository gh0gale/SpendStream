import { useCallback } from 'react'
import { supabase } from './supabase'
import { must, useLoad } from './useLoad'

// ponytail: reads every unsure row's amount in one request, capped here; fine
// while the review queue is small, move the sum into a view if it grows.
const UNSURE_ROW_CAP = 5000

// Monthly spending: categorised totals from the gold_monthly_summary view,
// plus uncategorised ("Unsure") spend, which the view leaves out (DATA-02).
// data: newest month first,
// [{ month: 'YYYY-MM-01', rows: [{ category, total, count }], unsure: { total, count } }]
export function useSpending(userId) {
  const fetcher = useCallback(async () => {
    const [gold, unsure] = await Promise.all([
      must(supabase.from('gold_monthly_summary')
        .select('month, category, total_amount, txn_count').eq('user_id', userId)),
      must(supabase.from('silver_transactions')
        .select('amount, transaction_date').eq('user_id', userId)
        .eq('is_categorised', false).not('transaction_date', 'is', null)
        .range(0, UNSURE_ROW_CAP - 1)),
    ])
    return groupByMonth(gold, unsure)
  }, [userId])

  const load = useLoad(fetcher)
  return { ...load, months: load.data ?? [] }
}

function groupByMonth(gold, unsure) {
  const byMonth = new Map()
  const monthOf = (m) => {
    if (!byMonth.has(m)) byMonth.set(m, { month: m, rows: [], unsure: { total: 0, count: 0 } })
    return byMonth.get(m)
  }
  for (const g of gold) {
    monthOf(g.month.slice(0, 10)).rows.push({
      category: g.category, total: Number(g.total_amount), count: g.txn_count,
    })
  }
  for (const u of unsure) {
    const m = monthOf(`${u.transaction_date.slice(0, 7)}-01`)
    m.unsure = { total: m.unsure.total + Number(u.amount), count: m.unsure.count + 1 }
  }
  return [...byMonth.values()].sort((a, b) => b.month.localeCompare(a.month))
}

export const monthTotal = (m) => m.rows.reduce((s, r) => s + r.total, 0) + m.unsure.total
