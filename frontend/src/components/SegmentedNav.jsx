import { NavLink } from 'react-router'
import styles from './SegmentedNav.module.css'

// Page links as ledger tabs: plain text, the current page ruled underneath in
// ink where the header meets the page. items: [{ to, label, end?, count? }]
export default function SegmentedNav({ items, label, className = '' }) {
  return (
    <nav className={`${styles.seg} ${className}`} aria-label={label}>
      {items.map(i => (
        <NavLink key={i.to} to={i.to} end={i.end} className={styles.item}>
          {i.label}
          {i.count > 0 && <span className={styles.count}>{i.count}</span>}
        </NavLink>
      ))}
    </nav>
  )
}
