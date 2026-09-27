import { useCallback, useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router'
import CategoryBreakdown from '../components/CategoryBreakdown'
import MonthCard from '../components/MonthCard'
import { ErrorNotice, SkeletonBlock, SkeletonRows } from '../components/States'
import SyncLine from '../components/SyncLine'
import { useAuth } from '../lib/auth'
import {
  formatDay, formatINR, formatINRShort, formatMonth, formatMonthShort, monthRange, plural, todayISO,
} from '../lib/format'
import { notifyReviewChanged } from '../lib/reviewQueue'
import { supabase } from '../lib/supabase'
import { useGmailSync } from '../lib/useGmailSync'
import { must, useLoad } from '../lib/useLoad'
import { usePageTitle } from '../lib/usePageTitle'
import { monthTotal, useSpending } from '../lib/useSpending'
import styles from './Dashboard.module.css'

// /auth/callback sends the user back with ?gmail=<result> (contract in
// .claude/rules/general.md).
const GMAIL_RESULTS = {
  connected: { text: 'Gmail connected. Reading this month’s alerts.' },
  denied:    { text: "You didn't grant access. Nothing was read.", action: 'Try again' },
  expired:   { text: 'The link timed out. Start again.', action: 'Start again' },
  error:     { text: "Google didn't complete the connection. Try again.", action: 'Try again' },
}

export default function Dashboard() {
  usePageTitle('Dashboard')
  const { user } = useAuth()
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()
  const [gmailResult] = useState(() => GMAIL_RESULTS[params.get('gmail')] ? params.get('gmail') : null)

  const spending = useSpending(user.id)
  const { reload } = spending
  const onFinished = useCallback(() => { reload(); notifyReviewChanged() }, [reload])
  const sync = useGmailSync(user.id, { onFinished })

  // A new connection starts its first sync at once, exactly once (the ref
  // survives StrictMode's second effect run; a second call would be refused).
  const { start } = sync
  const handledResult = useRef(false)
  useEffect(() => {
    if (!gmailResult || handledResult.current) return
    handledResult.current = true
    setParams({}, { replace: true })
    if (gmailResult === 'connected') start()
  }, [gmailResult, setParams, start])

  const [monthIndex, setMonthIndex] = useState(0)
  const months = spending.months
  const index = Math.min(monthIndex, Math.max(months.length - 1, 0))
  const current = months[index]

  const firstSync = sync.sync?.state === 'running' && spending.status === 'ready' && months.length === 0
  const result = gmailResult && GMAIL_RESULTS[gmailResult]

  return (
    <div className={styles.page}>
      {result && (
        <div className="notice" role="status" tabIndex={-1}>
          <p className="notice-title">{result.text}</p>
          {result.action && <button type="button" className="btn" onClick={() => navigate('/connect')}>{result.action}</button>}
        </div>
      )}

      <GmailNotice gmail={sync.gmail} onRetry={sync.reloadGmail} />

      {firstSync ? (
        <section className={styles.firstSync}>
          <h1 className="display">Reading <span className="mark">this month</span>.</h1>
          <SyncLine sync={sync} />
          <p className="muted">
            SpendStream is searching your Gmail for bank alerts, reading each one, and sorting the
            payments. You can leave this page; it continues on the server.
          </p>
        </section>
      ) : (
        <>
          <div className={styles.topRow}>
            <header className={styles.head}>
              <p className="caption">Dashboard</p>
              <h1 className="display">Where your <span className="mark">money</span> went.</h1>
            </header>
            {sync.gmail.status !== 'none' && <div className={styles.syncCard}><SyncLine sync={sync} /></div>}
          </div>
          <Spending
            spending={spending}
            current={current}
            previous={months[index + 1]}
            index={index}
            onMonth={setMonthIndex}
            months={months}
            userId={user.id}
          />
        </>
      )}
    </div>
  )
}

function GmailNotice({ gmail, onRetry }) {
  if (gmail.status === 'error') {
    return <ErrorNotice title="Couldn't check your Gmail connection." detail={gmail.detail} onRetry={onRetry} />
  }
  if (gmail.status === 'none') {
    return (
      <div className="notice">
        <p className="notice-title">Gmail is not connected. Connect Gmail first.</p>
        <Link to="/connect" className="btn btn-primary">Connect Gmail</Link>
      </div>
    )
  }
  if (gmail.status === 'reconnect') {
    return (
      <div className="notice notice-attention" role="status">
        <p className="notice-title">Gmail access has expired. Reconnect Gmail to keep syncing.</p>
        <Link to="/connect" className="btn btn-primary">Reconnect Gmail</Link>
      </div>
    )
  }
  return null
}

function Spending({ spending, current, previous, index, onMonth, months, userId }) {
  const [today] = useState(todayISO)
  if (spending.status === 'error') {
    return <ErrorNotice title="Couldn't load your payments." detail={spending.detail} onRetry={spending.reload} />
  }
  if (spending.status === 'loading' && !current) {
    return (
      <div className={styles.grid} aria-busy="true">
        <div className={`band ${styles.skeletonCard}`}>
          <span className="visually-hidden">Loading</span>
          <SkeletonBlock height={22} width={180} />
          <SkeletonBlock height={80} width={280} />
          <SkeletonBlock height={44} />
        </div>
        <div className="card"><SkeletonRows rows={8} height={22} /></div>
        <div className="card"><SkeletonRows rows={5} height={30} /></div>
      </div>
    )
  }
  if (!current) {
    return (
      <section className={`card ${styles.empty}`}>
        <h2>No payments yet</h2>
        <p className="muted">
          No bank alerts found this month. SpendStream currently reads HDFC alerts reliably.
          Check this is the Gmail that receives them. <Link to="/how-it-works">How it works</Link>
        </p>
      </section>
    )
  }

  const month = current.month.slice(0, 7)
  const reviewCount = current.unsure.count

  return (
    <div className={styles.grid}>
      <MonthCard
        month={current}
        previous={previous}
        today={today}
        nav={{
          older: months[index + 1],
          newer: months[index - 1],
          onOlder: () => onMonth(index + 1),
          onNewer: () => onMonth(index - 1),
        }}
      >
        {reviewCount > 0 && (
          <Link to="/app/review" className={styles.reviewLink}>Review {plural(reviewCount, 'payment')}</Link>
        )}
        <Link to={`/app/transactions?month=${month}`} className="link-arrow">All payments this month</Link>
      </MonthCard>
      <p className="visually-hidden" aria-live="polite">
        {formatMonth(current.month)}, month {months.length - index} of {months.length}
      </p>

      <section className={`card ${styles.categories}`} aria-labelledby="cat-h">
        <div className="card-head"><h2 id="cat-h">By category</h2></div>
        <CategoryBreakdown
          rows={current.rows}
          unsure={current.unsure}
          compact
          linkFor={(cat) => `/app/transactions?month=${month}&category=${encodeURIComponent(cat)}`}
          unsureLink="/app/review"
        />
      </section>

      <div className={styles.side}>
        <MonthTrend months={months} index={index} onMonth={onMonth} />
        <LargestPayments userId={userId} month={month} />
      </div>
    </div>
  )
}

const TREND_MONTHS = 6

// Month totals as columns, oldest on the left. Pressing a column selects that
// month for the whole page; the selected month is always in the window.
function MonthTrend({ months, index, onMonth }) {
  const start = Math.min(Math.max(index - 2, 0), Math.max(months.length - TREND_MONTHS, 0))
  const shown = months.slice(start, start + TREND_MONTHS).map((m, i) => ({ m, i: start + i })).reverse()
  const max = Math.max(...shown.map(({ m }) => monthTotal(m)), 1)

  return (
    <section className="card" aria-labelledby="trend-h">
      <div className="card-head">
        <h2 id="trend-h">Month by month</h2>
        <p className="small muted">{plural(months.length, 'month')} of alerts</p>
      </div>
      <div className={styles.trend}>
        {shown.map(({ m, i }) => (
          <button
            key={m.month}
            type="button"
            className={styles.trendCol}
            aria-pressed={i === index}
            aria-label={`${formatMonth(m.month)}: ${formatINR(monthTotal(m))}`}
            onClick={() => onMonth(i)}
          >
            <span className={`num ${styles.trendFig}`}>{formatINRShort(monthTotal(m))}</span>
            <span className={styles.trendTrack}>
              <span className={styles.trendBar} style={{ height: `${(monthTotal(m) / max) * 100}%` }} />
            </span>
            <span className={styles.trendName}>{formatMonthShort(m.month)}</span>
          </button>
        ))}
      </div>
    </section>
  )
}

const LARGEST_COUNT = 5

function LargestPayments({ userId, month }) {
  const fetcher = useCallback(() => {
    const [start, end] = monthRange(month)
    return must(supabase.from('silver_transactions')
      .select('id, merchant, merchant_is_person, amount, transaction_date, category')
      .eq('user_id', userId).gte('transaction_date', start).lt('transaction_date', end)
      .order('amount', { ascending: false }).order('id')
      .range(0, LARGEST_COUNT - 1))
  }, [userId, month])
  const list = useLoad(fetcher)

  return (
    <section className="card" aria-labelledby="largest-h">
      <div className="card-head">
        <h2 id="largest-h">Largest payments</h2>
        <Link to={`/app/transactions?month=${month}`} className={styles.seeAll}>See all</Link>
      </div>
      {list.status === 'error' ? (
        <ErrorNotice title="Couldn't load the largest payments." detail={list.detail} onRetry={list.reload} />
      ) : !list.data ? (
        <SkeletonRows rows={LARGEST_COUNT} height={36} />
      ) : list.data.length === 0 ? (
        <p className="muted small">No payments in this month.</p>
      ) : (
        <ol className={styles.largest} aria-busy={list.status === 'loading'}>
          {list.data.map(r => (
            <li key={r.id}>
              <span className={`monogram ${r.merchant_is_person ? 'monogram-person' : ''}`} aria-hidden="true">
                {(r.merchant || '?').trim().charAt(0)}
              </span>
              <span className={styles.who}>
                <strong>{r.merchant || 'Unknown merchant'}</strong>
                <span className="small muted">
                  {r.transaction_date ? formatDay(r.transaction_date) : 'No date'} · {r.category || 'Unsure'}
                </span>
              </span>
              <span className={`num ${styles.largestAmount}`}>{formatINR(r.amount)}</span>
            </li>
          ))}
        </ol>
      )}
    </section>
  )
}
