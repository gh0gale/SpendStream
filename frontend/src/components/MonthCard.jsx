import { formatINR, formatMonth, formatMonthShort, plural } from '../lib/format'
import { monthTotal } from '../lib/useSpending'
import SpendRibbon from './SpendRibbon'
import styles from './MonthCard.module.css'

// One month on a green band: the total, how it compares with the month before,
// three figures worked out from the same rows, and the month as a ribbon.
// Used by the dashboard (with month steps) and by the home page (the
// developer's real month from demo-export.json, no steps).
// month / previous: { month: 'YYYY-MM-01', rows, unsure } from useSpending.
// today: the caller's clock, so "this month so far" is judged in one place.
export default function MonthCard({ month, previous, today, nav, caption, children }) {
  const total = monthTotal(month)
  const count = month.rows.reduce((s, r) => s + r.count, 0) + month.unsure.count
  const ym = month.month.slice(0, 7)
  const ongoing = today && ym === today.slice(0, 7)
  const days = ongoing ? Number(today.slice(8, 10)) : daysInMonth(ym)
  const top = [...month.rows].sort((a, b) => b.total - a.total)[0]

  return (
    <section className={`band ${styles.card}`} aria-labelledby="month-title">
      <div className={styles.head}>
        {nav ? (
          <div className={styles.monthBar}>
            <button type="button" className={styles.step} disabled={!nav.older} onClick={nav.onOlder}
              aria-label={nav.older ? `Previous month, ${formatMonth(nav.older.month)}` : 'No earlier month'}>
              {nav.older ? formatMonthShort(nav.older.month) : 'Earlier'}
            </button>
            <h2 id="month-title" className={styles.monthName}>{formatMonth(month.month)}</h2>
            <button type="button" className={styles.step} disabled={!nav.newer} onClick={nav.onNewer}
              aria-label={nav.newer ? `Next month, ${formatMonth(nav.newer.month)}` : 'No later month'}>
              {nav.newer ? formatMonthShort(nav.newer.month) : 'Later'}
            </button>
          </div>
        ) : (
          <div>
            {caption && <p className="caption">{caption}</p>}
            <h2 id="month-title" className={styles.monthName}>{formatMonth(month.month)}</h2>
          </div>
        )}
      </div>

      <div className={styles.body}>
        <div className={styles.totalBlock}>
          <p className={`num ${styles.total}`}>{formatINR(total)}</p>
          <p className={styles.note}>
            {ongoing ? 'Spent so far this month' : 'Spent in the month'}
            {month.unsure.total > 0 && <>, including {formatINR(month.unsure.total)} not yet categorised</>}
          </p>
          {previous && <Comparison total={total} previous={previous} ongoing={ongoing} />}
        </div>

        <dl className={styles.figures}>
          <div><dt>Payments</dt><dd className="num">{count}</dd></div>
          <div>
            <dt>Per day</dt>
            <dd className="num">{formatINR(total / days)}</dd>
          </div>
          <div><dt>Biggest category</dt><dd>{top ? top.category : 'None sorted yet'}</dd></div>
        </dl>
      </div>

      <SpendRibbon rows={month.rows} unsure={month.unsure} />
      <p className="visually-hidden">{plural(count, 'payment')} over {plural(days, 'day')}.</p>

      {children && <div className={styles.actions}>{children}</div>}
    </section>
  )
}

function Comparison({ total, previous, ongoing }) {
  const before = monthTotal(previous)
  const name = formatMonthShort(previous.month)
  // A month still in progress is not compared with a whole one.
  if (ongoing) return <p className={styles.compare}>{name} came to <span className="num">{formatINR(before)}</span>.</p>
  const diff = total - before
  if (Math.round(diff) === 0) return <p className={styles.compare}>The same as {name}.</p>
  const pct = before > 0 ? ` (${Math.round((Math.abs(diff) / before) * 100)}%)` : ''
  return (
    <p className={styles.compare}>
      <span className={`num ${styles.delta}`}>{diff > 0 ? '+' : '−'}{formatINR(Math.abs(diff))}{pct}</span>{' '}
      {diff > 0 ? 'more' : 'less'} than {name}
    </p>
  )
}

function daysInMonth(ym) {
  const [y, m] = ym.split('-').map(Number)
  return new Date(y, m, 0).getDate()
}
