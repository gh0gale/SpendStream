import { formatINR } from '../lib/format'
import styles from './SpendRibbon.module.css'

// The month as one stream: each category's share of the total as a segment of
// a single bar, largest first, Unsure hatched and last. One colour throughout;
// the gaps and the labels tell segments apart. The breakdown table beside it
// carries the exact figures, so the ribbon is one image for screen readers.
const LABEL_MIN_SHARE = 12

export default function SpendRibbon({ rows, unsure }) {
  const parts = [...rows].sort((a, b) => b.total - a.total).map(r => ({ label: r.category, total: r.total }))
  if (unsure?.count > 0) parts.push({ label: 'Unsure', total: unsure.total, unsure: true })
  const grand = parts.reduce((s, p) => s + p.total, 0)
  if (grand <= 0) return null
  const share = (t) => Math.round((t / grand) * 100)

  return (
    <div
      className={styles.strip}
      role="img"
      aria-label={`${formatINR(grand)} split by category: ${parts.map(p => `${p.label} ${share(p.total)}%`).join(', ')}`}
    >
      {parts.map(p => (
        <span
          key={p.label}
          className={`${styles.seg} ${p.unsure ? 'bar-unsure' : ''}`}
          style={{ flexGrow: p.total }}
          title={`${p.label}: ${formatINR(p.total)}, ${share(p.total)}%`}
        >
          {share(p.total) >= LABEL_MIN_SHARE && !p.unsure && (
            <span className={styles.label}>{p.label} <span className="num">{share(p.total)}%</span></span>
          )}
        </span>
      ))}
    </div>
  )
}
