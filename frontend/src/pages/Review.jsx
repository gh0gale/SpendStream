import { useCallback, useRef, useState } from 'react'
import { Link } from 'react-router'
import CategoryPicker from '../components/CategoryPicker'
import ResultNotice from '../components/ResultNotice'
import { ErrorNotice, SkeletonRows } from '../components/States'
import { useAuth } from '../lib/auth'
import { formatDay, formatINR, plural } from '../lib/format'
import { supabase } from '../lib/supabase'
import { useCorrection } from '../lib/useCorrection'
import { must, useLoad } from '../lib/useLoad'
import { usePageTitle } from '../lib/usePageTitle'
import styles from './Review.module.css'

// ponytail: loads the whole queue in one request up to this cap; page it if
// people ever have more unsure payments than this.
const QUEUE_CAP = 1000

// Unsure payments grouped by merchant, largest total first: one pick settles
// every payment in a group, because the rule is keyed on the merchant.
function groupByMerchant(rows) {
  const groups = new Map()
  for (const r of rows) {
    const key = r.merchant_key || `name:${r.merchant}`
    if (!groups.has(key)) groups.set(key, { key, merchant: r.merchant || 'Unknown merchant', isPerson: r.merchant_is_person, rows: [], total: 0 })
    const g = groups.get(key)
    g.rows.push(r)
    g.total += Number(r.amount)
  }
  return [...groups.values()].sort((a, b) => b.total - a.total)
}

export default function Review() {
  usePageTitle('Needs review')
  const { user } = useAuth()
  const [settled, setSettled] = useState(new Set())   // merchant keys categorised on this visit
  const [picking, setPicking] = useState(null)
  const [result, setResult] = useState(null)
  const { saving, correct } = useCorrection()
  const buttons = useRef(new Map())
  const heading = useRef(null)

  const fetcher = useCallback(async () => groupByMerchant(await must(
    supabase.from('silver_transactions')
      .select('id, merchant, merchant_key, merchant_is_person, amount, transaction_date')
      .eq('user_id', user.id).eq('is_categorised', false)
      .order('transaction_date', { ascending: false, nullsFirst: false })
      .range(0, QUEUE_CAP - 1),
  )), [user.id])
  const queue = useLoad(fetcher)
  const groups = (queue.data ?? []).filter(g => !settled.has(g.key))

  const handlePick = async (cat) => {
    const group = picking
    const index = groups.findIndex(g => g.key === group.key)
    try {
      const { alsoUpdated } = await correct(group.rows[0], cat)
      setPicking(null)
      setResult({ text: `Saved. ${plural(1 + alsoUpdated, 'payment')} to ${group.merchant} ${alsoUpdated ? 'are' : 'is'} now ${cat}.` })
      const rest = groups.filter(g => g.key !== group.key)
      setSettled(prev => new Set(prev).add(group.key))
      // Focus the next group's button, or the heading when the queue is empty.
      const next = rest[Math.min(index, rest.length - 1)]
      requestAnimationFrame(() => (next ? buttons.current.get(next.key) : heading.current)?.focus())
    } catch (err) {
      setPicking(null)
      setResult({ error: true, text: `Couldn't save. ${err.message}`, retry: () => setPicking(group) })
    }
  }

  const count = groups.reduce((s, g) => s + g.rows.length, 0)

  return (
    <div className={styles.page}>
      <div className={styles.head}>
        <div className="page-head">
          <p className="caption">Not sure yet</p>
          <h1 ref={heading} tabIndex={-1} className="display">Needs <span className="mark">review</span></h1>
        </div>
        {queue.status === 'ready' && count > 0 && <p className="num muted">{plural(count, 'payment')}</p>}
      </div>
      <p className="muted">
        These are payments SpendStream was not confident about. Pick a category once and future
        payments to the same merchant are sorted automatically.
      </p>

      <ResultNotice result={result} onClose={() => setResult(null)} onRetry={() => { const r = result.retry; setResult(null); r() }} />

      {queue.status === 'error' ? (
        <ErrorNotice title="Couldn't load the payments to review." detail={queue.detail} onRetry={queue.reload} />
      ) : !queue.data ? (
        <SkeletonRows rows={5} height={48} />
      ) : groups.length === 0 ? (
        <p>Nothing to review. <Link to="/app">Back to the dashboard</Link></p>
      ) : (
        <ul className={styles.groups}>
          {groups.map(g => (
            <li key={g.key} className={styles.group}>
              <div className={styles.summary}>
                <p className={styles.merchant}>
                  {g.merchant}
                  {g.isPerson && <span className="small muted"> to a person</span>}
                </p>
                <p className="small muted num">{plural(g.rows.length, 'payment')} · {formatINR(g.total, { paise: true })}</p>
                <details className={styles.dates}>
                  <summary className="small">Show {g.rows.length === 1 ? 'the payment' : `all ${g.rows.length}`}</summary>
                  <p className="small num">
                    {g.rows.map(r => `${r.transaction_date ? formatDay(r.transaction_date) : 'No date'} ${formatINR(r.amount, { paise: true })}`).join(' · ')}
                  </p>
                </details>
              </div>
              <button
                type="button"
                className="btn"
                ref={el => { if (el) buttons.current.set(g.key, el); else buttons.current.delete(g.key) }}
                onClick={() => setPicking(g)}
              >
                Choose category
              </button>
            </li>
          ))}
        </ul>
      )}

      {picking && (
        <CategoryPicker
          merchant={picking.merchant}
          current={null}
          saving={saving}
          onPick={handlePick}
          onClose={() => setPicking(null)}
        />
      )}
    </div>
  )
}
