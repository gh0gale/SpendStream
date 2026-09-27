import { useCallback, useState } from 'react'
import { Link, useSearchParams } from 'react-router'
import CategoryChip from '../components/CategoryChip'
import CategoryPicker from '../components/CategoryPicker'
import ResultNotice from '../components/ResultNotice'
import { ErrorNotice, SkeletonRows } from '../components/States'
import { useAuth } from '../lib/auth'
import { CATEGORIES, UNSURE } from '../lib/categories'
import { formatDay, formatINR, formatMonth, monthRange, plural } from '../lib/format'
import { supabase } from '../lib/supabase'
import { useCorrection } from '../lib/useCorrection'
import { must, useLoad } from '../lib/useLoad'
import { usePageTitle } from '../lib/usePageTitle'
import { useSpending } from '../lib/useSpending'
import styles from './Transactions.module.css'

const PAGE_SIZE = 50
const COLUMNS = 'id, merchant, merchant_key, merchant_is_person, amount, transaction_date, category'

export default function Transactions() {
  usePageTitle('Transactions')
  const { user } = useAuth()
  const [params, setParams] = useSearchParams()
  const month = params.get('month') || ''          // 'YYYY-MM' or '' for all
  const category = params.get('category') || ''    // a category, UNSURE, or '' for all
  const { months } = useSpending(user.id)

  const [limit, setLimit] = useState(PAGE_SIZE)
  const [trace, setTrace] = useState(null)       // { id, key, before } after a correction moved rows
  const [picking, setPicking] = useState(null)
  const [result, setResult] = useState(null)
  const { saving, correct } = useCorrection()

  const fetcher = useCallback(() => {
    let q = supabase.from('silver_transactions').select(COLUMNS, { count: 'exact' })
      .eq('user_id', user.id)
      .order('transaction_date', { ascending: false, nullsFirst: false })
      .order('id')
      .range(0, limit - 1)
    if (month) {
      const [start, end] = monthRange(month)
      q = q.gte('transaction_date', start).lt('transaction_date', end)
    }
    if (category === UNSURE) q = q.eq('is_categorised', false)
    else if (category) q = q.eq('category', category)
    return must(q)
  }, [user.id, month, category, limit])
  const list = useLoad(fetcher)
  const rows = list.data?.rows ?? []
  const total = list.data?.count ?? 0

  // Rows the last correction moved: same merchant, category changed since.
  const traced = new Set(trace && list.status === 'ready'
    ? rows.filter(r => r.id !== trace.id && r.merchant_key === trace.key
        && trace.before.has(r.id) && trace.before.get(r.id) !== r.category).map(r => r.id)
    : [])

  const setFilter = (key, value) => {
    const next = new URLSearchParams(params)
    if (value) next.set(key, value)
    else next.delete(key)
    setParams(next, { replace: true })
    setLimit(PAGE_SIZE)
    setTrace(null)
  }

  const handlePick = async (cat) => {
    const row = picking
    if (cat === row.category) { setPicking(null); return }
    try {
      const { alsoUpdated } = await correct(row, cat)
      setPicking(null)
      setResult({ text: alsoUpdated > 0
        ? `Saved. Also updated ${plural(alsoUpdated, 'other payment')} to ${row.merchant}.`
        : 'Saved.' })
      setTrace(alsoUpdated > 0
        ? { id: row.id, key: row.merchant_key, before: new Map(rows.map(r => [r.id, r.category])) }
        : null)
      list.reload()
    } catch (err) {
      setPicking(null)
      setResult({ error: true, text: `Couldn't save. ${err.message}`, retry: () => setPicking(row) })
    }
  }

  const categoryWord = category === UNSURE ? 'unsure' : category
  const monthWords = month ? ` in ${formatMonth(`${month}-01`)}` : ''
  const filtered = Boolean(category || month)

  return (
    <div className={styles.page}>
      <header className="page-head">
        <p className="caption">Every payment</p>
        <h1 className="display">Transactions</h1>
      </header>

      <div className={styles.filters}>
        <div className="field">
          <label htmlFor="f-month">Month</label>
          <select id="f-month" className="input" value={month} onChange={e => setFilter('month', e.target.value)}>
            <option value="">All months</option>
            {month && !months.some(m => m.month.startsWith(month)) && <option value={month}>{formatMonth(`${month}-01`)}</option>}
            {months.map(m => <option key={m.month} value={m.month.slice(0, 7)}>{formatMonth(m.month)}</option>)}
          </select>
        </div>
        <div className="field">
          <label htmlFor="f-category">Category</label>
          <select id="f-category" className="input" value={category} onChange={e => setFilter('category', e.target.value)}>
            <option value="">All categories</option>
            {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
            <option value={UNSURE}>Unsure</option>
          </select>
        </div>
      </div>

      <ResultNotice result={result} onClose={() => setResult(null)} onRetry={() => { const r = result.retry; setResult(null); r() }} />

      {list.status === 'error' ? (
        <ErrorNotice title="Couldn't load your payments." detail={list.detail} onRetry={list.reload} />
      ) : list.status === 'loading' && !list.data ? (
        <SkeletonRows rows={8} height={28} />
      ) : rows.length === 0 ? (
        <p className="muted">
          {filtered ? <>No {categoryWord ? `${categoryWord} ` : ''}payments{monthWords}. <button type="button" className="btn btn-text" onClick={() => { setParams({}, { replace: true }); setLimit(PAGE_SIZE) }}>Show all payments</button></>
            : <>No payments yet. <Link to="/app">Sync from the dashboard</Link></>}
        </p>
      ) : (
        <>
          <p className="small muted">
            Showing {rows.length} of {total} {categoryWord ? `${categoryWord} ` : ''}{total === 1 ? 'payment' : 'payments'}{monthWords}.
          </p>
          <table className={`ledger ${styles.table}`} aria-busy={list.status === 'loading'}>
            <thead>
              <tr>
                <th scope="col" className="caption">Date</th>
                <th scope="col" className="caption">Merchant</th>
                <th scope="col" className="caption">Category</th>
                <th scope="col" className="caption amount">Amount</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(r => (
                <tr key={r.id} className={traced.has(r.id) ? 'trace' : ''}>
                  <td className={`num ${styles.date}`}>{r.transaction_date ? formatDay(r.transaction_date) : 'No date'}</td>
                  <th scope="row" className={styles.merchant}>
                    {r.merchant || 'Unknown merchant'}
                    {r.merchant_is_person && <span className="small muted"> to a person</span>}
                    {traced.has(r.id) && <span className="small pencil"> · updated just now</span>}
                  </th>
                  <td className={styles.category}>
                    <CategoryChip
                      category={r.category}
                      label={`Category for ${r.merchant || 'this payment'}: ${r.category || 'Unsure'}. Change`}
                      onClick={() => setPicking(r)}
                    />
                  </td>
                  <td className={`amount num ${styles.amount}`}>{formatINR(r.amount, { paise: true })}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length < total && (
            <button type="button" className="btn" onClick={() => setLimit(l => l + PAGE_SIZE)} disabled={list.status === 'loading'}>
              {list.status === 'loading' ? 'Loading' : `Load ${Math.min(PAGE_SIZE, total - rows.length)} more`}
            </button>
          )}
        </>
      )}

      {picking && (
        <CategoryPicker
          merchant={picking.merchant || 'this payment'}
          current={picking.category}
          saving={saving}
          onPick={handlePick}
          onClose={() => setPicking(null)}
        />
      )}
    </div>
  )
}
