import { Link } from 'react-router'
import { formatINR, plural } from '../lib/format'
import styles from './CategoryBreakdown.module.css'

// Where the money went, as a real table: it is its own legend and its own
// screen-reader alternative. Every category bar is one colour, sorted by
// amount; "Unsure" is hatched and always last because it is a state.
// rows: [{ category, total, count }]; unsure: { total, count }.
export default function CategoryBreakdown({ rows, unsure, caption, linkFor, unsureLink, compact = false }) {
  const sorted = [...rows].sort((a, b) => b.total - a.total)
  const grand = sorted.reduce((s, r) => s + r.total, 0) + (unsure?.total ?? 0)
  const max = Math.max(...sorted.map(r => r.total), unsure?.total ?? 0, 1)
  const share = (t) => (grand > 0 ? Math.round((t / grand) * 100) : 0)

  const name = (label, to) => (to ? <Link to={to}>{label}</Link> : label)

  return (
    <table className={`ledger ${styles.table} ${compact ? styles.compact : ''}`}>
      {caption && <caption className="caption">{caption}</caption>}
      <thead>
        <tr>
          <th scope="col" className="caption">Category</th>
          <th scope="col" className="caption"><span className="visually-hidden">Bar</span></th>
          <th scope="col" className="caption amount">Spent</th>
          <th scope="col" className={`caption amount ${styles.share}`}>Share</th>
        </tr>
      </thead>
      <tbody>
        {sorted.map(r => (
          <tr key={r.category} aria-label={`${r.category}: ${formatINR(r.total)}, ${share(r.total)}%, ${plural(r.count, 'payment')}`}>
            <th scope="row" className={styles.name}>{name(r.category, linkFor?.(r.category))}</th>
            <td className={styles.barCell}>
              <div className="bar-track"><div className="bar" style={{ width: `${(r.total / max) * 100}%` }} /></div>
            </td>
            <td className="amount num">{formatINR(r.total)}</td>
            <td className={`amount num muted ${styles.share}`}>{share(r.total)}%</td>
          </tr>
        ))}
        {unsure?.count > 0 && (
          <tr className={styles.unsure} aria-label={`Unsure: ${formatINR(unsure.total)}, ${plural(unsure.count, 'payment')} not yet categorised`}>
            <th scope="row" className={`${styles.name} pencil`}>{name('Unsure', unsureLink)}</th>
            <td className={styles.barCell}>
              <div className="bar-track"><div className="bar bar-unsure" style={{ width: `${(unsure.total / max) * 100}%` }} /></div>
            </td>
            <td className="amount num pencil">{formatINR(unsure.total)}</td>
            <td className={`amount num pencil ${styles.share}`}>{share(unsure.total)}%</td>
          </tr>
        )}
      </tbody>
    </table>
  )
}
