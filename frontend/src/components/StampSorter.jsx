import { useEffect, useRef, useState } from 'react'
import { formatDay, formatINR } from '../lib/format'
import { useReducedMotion } from '../lib/motion'
import styles from './StampSorter.module.css'

// The home page's picture of the product, on a passbook page: a real bank
// alert arrives as a slip, the amount, payee and date are marked with a
// highlighter, the slip becomes a ledger row, and a category stamp lands on
// it. `earlier` is a real row already sorted. Loops while on screen; under
// reduced motion every step is shown at once, standing still.
const PARTS = [
  { key: 'amount', re: /(?:Rs\.?|INR|₹)\s?[\d,]+(?:\.\d+)?/i },
  { key: 'payee', re: /VPA\s+\S+(?:\s\([^)]*\))?/ },
  { key: 'date', re: /\b\d{2}-\d{2}-\d{2,4}\b/ },
]

function markAlert(text) {
  const hits = PARTS.map(p => {
    const m = p.re.exec(text)
    return m && { key: p.key, start: m.index, end: m.index + m[0].length }
  }).filter(Boolean).sort((a, b) => a.start - b.start)
  const out = []
  let at = 0
  for (const h of hits) {
    if (h.start < at) continue
    out.push(text.slice(at, h.start))
    out.push(<mark key={h.key} className={`${styles.hl} ${styles[h.key]}`}>{text.slice(h.start, h.end)}</mark>)
    at = h.end
  }
  out.push(text.slice(at))
  return out
}

export default function StampSorter({ alert, earlier }) {
  const reduce = useReducedMotion()
  const ref = useRef(null)
  const [onScreen, setOnScreen] = useState(false)
  const { row } = alert

  useEffect(() => {
    const io = new IntersectionObserver(([e]) => setOnScreen(e.isIntersecting), { threshold: 0.3 })
    io.observe(ref.current)
    return () => io.disconnect()
  }, [])

  const mode = reduce ? styles.still : onScreen ? styles.play : styles.paused

  return (
    <figure ref={ref} className={`${styles.page} ${mode}`}>
      <div className={styles.head} aria-hidden="true">
        <span className="caption">Passbook</span>
        <span className="caption">Debits</span>
      </div>

      <div className={styles.slip}>
        <p className={styles.slipFrom}>From your bank&apos;s alert email</p>
        <samp className={styles.slipText}>{markAlert(alert.text)}</samp>
      </div>

      <div className={styles.rows}>
        {earlier && (
          <div className={styles.row}>
            <span className="num">{formatDay(earlier.date)}</span>
            <span className={styles.merchant}>{earlier.merchant}</span>
            <span className={`${styles.stamp} ${styles.stampStill}`}>{earlier.category}</span>
            <span className="num">{formatINR(earlier.amount)}</span>
          </div>
        )}
        <div className={`${styles.row} ${styles.newRow}`}>
          <span className="num">{formatDay(row.date)}</span>
          <span className={styles.merchant}>{row.merchant}</span>
          <span className={`${styles.stamp} ${styles.stampNew}`}>{row.category}</span>
          <span className="num">{formatINR(row.amount, { paise: true })}</span>
        </div>
      </div>

      <figcaption className={styles.caption}>
        A real alert from the developer&apos;s account, and the row it became. Account digits removed.
      </figcaption>
    </figure>
  )
}
