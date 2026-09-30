import { useCallback, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router'
import CategoryPicker from '../components/CategoryPicker'
import ResultNotice from '../components/ResultNotice'
import { ErrorNotice, SkeletonRows } from '../components/States'
import { useAuth } from '../lib/auth'
import { formatDay, formatINR, plural } from '../lib/format'
import { supabase } from '../lib/supabase'
import { track } from '../lib/track'
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
      const { alsoUpdated } = await correct(group.rows[0], cat, 'review')
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
  const merchants = queue.data?.length ?? 0
  const done = merchants - groups.length

  // Once per visit each: the queue opened, and the queue was emptied by sorting.
  const tracked = useRef({ opened: false, cleared: false })
  useEffect(() => {
    if (queue.status !== 'ready' || !queue.data) return
    if (!tracked.current.opened) {
      tracked.current.opened = true
      track('review_opened', { groups: merchants, payments: count })
    } else if (groups.length === 0 && done > 0 && !tracked.current.cleared) {
      tracked.current.cleared = true
      track('review_cleared', { merchants_sorted: done })
    }
  }, [queue.status, queue.data, groups.length, merchants, count, done])

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

      {merchants > 0 && (
        <div className={`card ${styles.progress}`}>
          <p className="small">
            <strong className="num">{done} of {merchants}</strong> {merchants === 1 ? 'merchant' : 'merchants'} sorted on this visit
          </p>
          <div className={styles.meter} role="progressbar" aria-label="Merchants sorted on this visit"
            aria-valuemin={0} aria-valuemax={merchants} aria-valuenow={done}>
            <span style={{ width: `${(done / merchants) * 100}%` }} />
          </div>
        </div>
      )}

      <ResultNotice result={result} onClose={() => setResult(null)} onRetry={() => { const r = result.retry; setResult(null); r() }} />

      {queue.status === 'error' ? (
        <ErrorNotice title="Couldn't load the payments to review." detail={queue.detail} onRetry={queue.reload} />
      ) : !queue.data ? (
        <SkeletonRows rows={5} height={48} />
      ) : groups.length === 0 ? (
        <div className={`card ${styles.clear}`}>
          <h2>Nothing to review</h2>
          <p className="muted">Every payment has a category. New ones that SpendStream is unsure about will wait here.</p>
          <Link to="/app" className="link-arrow">Back to the dashboard</Link>
        </div>
      ) : (
        <ul className={styles.groups}>
          {groups.map(g => (
            <li key={g.key} className={styles.group}>
              <span className={`monogram ${g.isPerson ? 'monogram-person' : ''}`} aria-hidden="true">
                {g.merchant.trim().charAt(0)}
              </span>
              <div className={styles.summary}>
                <p className={styles.merchant}>
                  {g.merchant}
                  {g.isPerson && <span className="small muted"> to a person</span>}
                </p>
                <p className="small muted">
                  <span className={`num ${styles.total}`}>{formatINR(g.total, { paise: true })}</span> across {plural(g.rows.length, 'payment')}
                </p>
                <details className={styles.dates}>
                  <summary className="small">Show {g.rows.length === 1 ? 'the payment' : `all ${g.rows.length}`}</summary>
                  <p className="small num">
                    {g.rows.map(r => `${r.transaction_date ? formatDay(r.transaction_date) : 'No date'} ${formatINR(r.amount, { paise: true })}`).join(' · ')}
                  </p>
                </details>
              </div>
              <button
                type="button"
                className="btn btn-primary"
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
