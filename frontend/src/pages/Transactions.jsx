import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router'
import CategoryChip from '../components/CategoryChip'
import CategoryPicker from '../components/CategoryPicker'
import ResultNotice from '../components/ResultNotice'
import { ErrorNotice, SkeletonRows } from '../components/States'
import { useAuth } from '../lib/auth'
import { CATEGORIES, UNSURE } from '../lib/categories'
import { downloadCsv, paymentsToCsv } from '../lib/csv'
import { formatINR, formatMonth, formatWeekday, monthRange, plural } from '../lib/format'
import { supabase } from '../lib/supabase'
import { track } from '../lib/track'
import { useCorrection } from '../lib/useCorrection'
import { must, useLoad } from '../lib/useLoad'
import { usePageTitle } from '../lib/usePageTitle'
import { useSpendingData } from '../lib/spendingContext'
import styles from './Transactions.module.css'

const PAGE_SIZE = 50
const COLUMNS = 'id, merchant, merchant_key, merchant_is_person, amount, transaction_date, category'
const SEARCH_DELAY_MS = 300
const EXPORT_PAGE = 1000       // PostgREST returns at most this many rows per request
const EXPORT_CAP = 5000        // newest payments in one file
const CSV_COLUMNS = 'transaction_date, merchant, category, amount, merchant_is_person'

// ilike patterns treat % and _ as wildcards: searching for them means the characters themselves.
const likePattern = (text) => `%${text.replace(/[\\%_]/g, '\\$&')}%`

// Rows arrive newest first; consecutive rows on the same day share a group.
function byDay(rows) {
  const days = []
  for (const r of rows) {
    const date = r.transaction_date || ''
    const last = days[days.length - 1]
    if (last && last.date === date) { last.rows.push(r); last.total += Number(r.amount) }
    else days.push({ date, rows: [r], total: Number(r.amount) })
  }
  return days
}

export default function Transactions() {
  usePageTitle('Transactions')
  const { user } = useAuth()
  const [params, setParams] = useSearchParams()
  const month = params.get('month') || ''          // 'YYYY-MM' or '' for all
  const category = params.get('category') || ''    // a category, UNSURE, or '' for all
  const search = params.get('q') || ''             // part of a merchant name, or '' for all
  const { months } = useSpendingData()

  const [limit, setLimit] = useState(PAGE_SIZE)
  const [trace, setTrace] = useState(null)       // { id, key, before } after a correction moved rows
  const [picking, setPicking] = useState(null)
  const [result, setResult] = useState(null)
  const [text, setText] = useState(search)       // what is typed; the URL follows after a pause
  const [exporting, setExporting] = useState(false)
  const { saving, correct } = useCorrection()

  // The month, category and search filters, for both the list and the CSV export.
  const applyFilters = useCallback((q) => {
    if (month) {
      const [start, end] = monthRange(month)
      q = q.gte('transaction_date', start).lt('transaction_date', end)
    }
    if (category === UNSURE) q = q.eq('is_categorised', false)
    else if (category) q = q.eq('category', category)
    if (search) q = q.ilike('merchant', likePattern(search))
    return q
  }, [month, category, search])

  const fetcher = useCallback(() => must(applyFilters(
    supabase.from('silver_transactions').select(COLUMNS, { count: 'exact' }).eq('user_id', user.id),
  ).order('transaction_date', { ascending: false, nullsFirst: false }).order('id').range(0, limit - 1)),
  [user.id, applyFilters, limit])
  const list = useLoad(fetcher)
  const rows = list.data?.rows ?? []
  const total = list.data?.count ?? 0

  // Rows the last correction moved: same merchant, category changed since.
  const traced = new Set(trace && list.status === 'ready'
    ? rows.filter(r => r.id !== trace.id && r.merchant_key === trace.key
        && trace.before.has(r.id) && trace.before.get(r.id) !== r.category).map(r => r.id)
    : [])

  const setFilter = (key, value) => {
    // Functional update: the search pause fires later and must not overwrite a filter changed meanwhile.
    setParams((current) => {
      const next = new URLSearchParams(current)
      if (value) next.set(key, value)
      else next.delete(key)
      return next
    }, { replace: true })
    setLimit(PAGE_SIZE)
    setTrace(null)
    track('filter_used', { kind: key })
  }

  const handlePick = async (cat) => {
    const row = picking
    if (cat === row.category) { setPicking(null); return }
    try {
      const { alsoUpdated } = await correct(row, cat, 'transactions')
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

  useEffect(() => {
    const timer = setTimeout(() => {
      const next = text.trim()
      if (next !== search) setFilter('q', next)
    }, SEARCH_DELAY_MS)
    return () => clearTimeout(timer)
    // setFilter is recreated every render and only reads params; the typed text and the URL are what matter.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [text, search])

  const exportCsv = async () => {
    setExporting(true)
    try {
      const all = []
      for (let start = 0; start < EXPORT_CAP; start += EXPORT_PAGE) {
        const page = await must(applyFilters(
          supabase.from('silver_transactions').select(CSV_COLUMNS).eq('user_id', user.id),
        ).order('transaction_date', { ascending: false, nullsFirst: false }).order('id')
          .range(start, start + EXPORT_PAGE - 1))
        all.push(...page)
        if (page.length < EXPORT_PAGE) break
      }
      const name = ['spendstream-payments', month, category && category.toLowerCase(), search && 'search']
        .filter(Boolean).join('-')
      downloadCsv(`${name}.csv`, paymentsToCsv(all))
      setResult({ text: `Downloaded ${plural(all.length, 'payment')}.${all.length >= EXPORT_CAP ? ` The file holds the newest ${EXPORT_CAP}; narrow the month or category for the rest.` : ''}` })
    } catch (err) {
      setResult({ error: true, text: `Couldn't build the file. ${err.message}`, retry: exportCsv })
    } finally {
      setExporting(false)
    }
  }

  const categoryWord = category === UNSURE ? 'unsure' : category
  const monthWords = month ? ` in ${formatMonth(`${month}-01`)}` : ''
  const filtered = Boolean(category || month || search)

  return (
    <div className={styles.page}>
      <header className="page-head">
        <p className="caption">Every payment</p>
        <h1 className="display">Transactions</h1>
      </header>

      <div className={`card ${styles.filters}`}>
        <div className="field">
          <label htmlFor="f-month">Month</label>
          <select id="f-month" className="input" value={month} onChange={e => setFilter('month', e.target.value)}>
            <option value="">All months</option>
            {month && !months.some(m => m.month.startsWith(month)) && <option value={month}>{formatMonth(`${month}-01`)}</option>}
            {months.map(m => <option key={m.month} value={m.month.slice(0, 7)}>{formatMonth(m.month)}</option>)}
          </select>
        </div>
        <div className="field">
          <label htmlFor="f-search">Merchant</label>
          <input id="f-search" className="input" type="search" placeholder="Search by name" autoComplete="off"
            value={text} onChange={e => setText(e.target.value)} />
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
          {filtered ? <>No {categoryWord ? `${categoryWord} ` : ''}payments{monthWords}. <button type="button" className="btn btn-text" onClick={() => { setParams({}, { replace: true }); setText(''); setLimit(PAGE_SIZE) }}>Show all payments</button></>
            : <>No payments yet. <Link to="/app">Sync from the dashboard</Link></>}
        </p>
      ) : (
        <>
          <div className={styles.summary}>
            <p className="small muted">
              Showing {rows.length} of {total} {categoryWord ? `${categoryWord} ` : ''}{total === 1 ? 'payment' : 'payments'}{monthWords}{search ? ` matching "${search}"` : ''}.
            </p>
            <button type="button" className="btn btn-text" onClick={exportCsv} disabled={exporting}>
              {exporting ? 'Building the file' : 'Download CSV'}
            </button>
          </div>
          <table className={`ledger ${styles.table}`} aria-busy={list.status === 'loading'}>
            <thead>
              <tr>
                <th scope="col" className="caption"><span className="visually-hidden">Kind</span></th>
                <th scope="col" className="caption">Merchant</th>
                <th scope="col" className="caption">Category</th>
                <th scope="col" className="caption amount">Amount</th>
              </tr>
            </thead>
            {byDay(rows).map(day => (
              <tbody key={day.date}>
                <tr className={styles.dayRow}>
                  <th scope="rowgroup" colSpan={3}>{day.date ? formatWeekday(day.date) : 'No date'}</th>
                  <td className="amount num">{formatINR(day.total, { paise: true })}</td>
                </tr>
                {day.rows.map(r => (
                  <tr key={r.id} className={traced.has(r.id) ? 'trace' : ''}>
                    <td className={styles.mono}>
                      <span className={`monogram ${r.merchant_is_person ? 'monogram-person' : ''}`} aria-hidden="true">
                        {(r.merchant || '?').trim().charAt(0)}
                      </span>
                    </td>
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
            ))}
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
