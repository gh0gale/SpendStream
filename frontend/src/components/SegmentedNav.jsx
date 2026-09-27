import { useLayoutEffect, useRef, useState } from 'react'
import { NavLink, useLocation } from 'react-router'
import styles from './SegmentedNav.module.css'

// Page links as one segmented control. An ink pill sits behind the current
// page and slides to the next one on navigation: the motion reports where
// you went. items: [{ to, label, end?, count? }]
export default function SegmentedNav({ items, label, className = '' }) {
  const { pathname } = useLocation()
  const box = useRef(null)
  const [pill, setPill] = useState(null)

  useLayoutEffect(() => {
    const place = () => {
      const active = box.current?.querySelector('[aria-current="page"]')
      setPill(active ? { left: active.offsetLeft, width: active.offsetWidth } : null)
    }
    place()
    const ro = new ResizeObserver(place)
    ro.observe(box.current)
    return () => ro.disconnect()
  }, [pathname])

  return (
    <nav ref={box} className={`${styles.seg} ${className}`} aria-label={label}>
      {pill && <span className={styles.pill} style={{ transform: `translateX(${pill.left}px)`, width: pill.width }} aria-hidden="true" />}
      {items.map(i => (
        <NavLink key={i.to} to={i.to} end={i.end} className={styles.item}>
          {i.label}
          {i.count > 0 && <span className={styles.count}>{i.count}</span>}
        </NavLink>
      ))}
    </nav>
  )
}
