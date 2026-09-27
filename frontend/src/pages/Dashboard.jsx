import { useCallback, useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router'
import CategoryBreakdown from '../components/CategoryBreakdown'
import { ErrorNotice, SkeletonBlock, SkeletonRows } from '../components/States'
import SyncLine from '../components/SyncLine'
import { useAuth } from '../lib/auth'
import { formatINR, formatMonth, formatMonthShort, plural } from '../lib/format'
import { notifyReviewChanged } from '../lib/reviewQueue'
import { useGmailSync } from '../lib/useGmailSync'
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
            index={index}
            count={months.length}
            onMonth={setMonthIndex}
            months={months}
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

function Spending({ spending, current, index, count, onMonth, months }) {
  if (spending.status === 'error') {
    return <ErrorNotice title="Couldn't load your payments." detail={spending.detail} onRetry={spending.reload} />
  }
  if (spending.status === 'loading' && !current) {
    return (
      <section className={styles.spending} aria-busy="true">
        <div className={styles.summary}>
          <SkeletonBlock height={16} width={140} />
          <SkeletonBlock height={72} width={240} />
          <SkeletonBlock height={16} width={200} />
        </div>
        <div className={styles.table}><SkeletonRows rows={9} height={22} /></div>
      </section>
    )
  }
  if (!current) {
    return (
      <section className={styles.spending}>
        <p className="muted">
          No bank alerts found this month. SpendStream currently reads HDFC alerts reliably.
          Check this is the Gmail that receives them. <Link to="/how-it-works">How it works</Link>
        </p>
      </section>
    )
  }

  const older = months[index + 1]
  const newer = months[index - 1]
  const month = current.month.slice(0, 7)
  const reviewCount = current.unsure.count

  return (
    <section className={styles.spending} aria-labelledby="month-title">
      <div className={styles.summary}>
        <div className={styles.monthBar}>
          <button type="button" className={styles.step} disabled={!older} onClick={() => onMonth(index + 1)}
            aria-label={older ? `Previous month, ${formatMonth(older.month)}` : 'No earlier month'}>
            {older ? formatMonthShort(older.month) : 'Earlier'}
          </button>
          <h2 id="month-title" className={styles.monthName}>{formatMonth(current.month)}</h2>
          <button type="button" className={styles.step} disabled={!newer} onClick={() => onMonth(index - 1)}
            aria-label={newer ? `Next month, ${formatMonth(newer.month)}` : 'No later month'}>
            {newer ? formatMonthShort(newer.month) : 'Later'}
          </button>
        </div>
        <p className="visually-hidden" aria-live="polite">{formatMonth(current.month)}, month {count - index} of {count}</p>

        <div>
          <p className={`num ${styles.total}`}>{formatINR(monthTotal(current))}</p>
          <p className={styles.totalNote}>
            spent across {plural(current.rows.reduce((s, r) => s + r.count, 0) + current.unsure.count, 'payment')}
            {current.unsure.total > 0 && <>, including {formatINR(current.unsure.total)} not yet categorised</>}
          </p>
        </div>

        <div className={styles.summaryLinks}>
          {reviewCount > 0 && (
            <Link to="/app/review" className={styles.reviewLink}>
              Review {plural(reviewCount, 'payment')}
            </Link>
          )}
          <Link to={`/app/transactions?month=${month}`} className="link-arrow">All payments this month</Link>
        </div>
      </div>

      <div className={styles.table}>
        <CategoryBreakdown
          rows={current.rows}
          unsure={current.unsure}
          compact
          linkFor={(cat) => `/app/transactions?month=${month}&category=${encodeURIComponent(cat)}`}
          unsureLink="/app/review"
        />
      </div>
    </section>
  )
}
