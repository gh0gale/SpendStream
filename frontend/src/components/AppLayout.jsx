import { useEffect, useState } from 'react'
import { Link, NavLink, Outlet, useLocation } from 'react-router'
import { supabase } from '../lib/supabase'
import { useAuth } from '../lib/auth'
import { REVIEW_CHANGED } from '../lib/reviewQueue'
import { SpendingContext } from '../lib/spendingContext'
import { useSpending } from '../lib/useSpending'
import AccountMenu from './AccountMenu'
import SegmentedNav from './SegmentedNav'
import styles from './AppLayout.module.css'

export default function AppLayout() {
  const { user } = useAuth()
  const { pathname } = useLocation()
  const reviewCount = useReviewCount(user.id, pathname)

  // One spending read for every app page; a finished sync or a saved correction reloads it.
  const spending = useSpending(user.id)
  const { reload } = spending
  useEffect(() => {
    window.addEventListener(REVIEW_CHANGED, reload)
    return () => window.removeEventListener(REVIEW_CHANGED, reload)
  }, [reload])

  const pages = [
    { to: '/app', end: true, label: 'Dashboard' },
    { to: '/app/transactions', label: 'Transactions' },
    { to: '/app/review', label: 'Needs review', count: reviewCount },
  ]
  const tabs = [
    { to: '/app',              end: true, short: 'Home' },
    { to: '/app/transactions',            short: 'Payments' },
    { to: '/app/review',                  short: 'Review', count: reviewCount },
    { to: '/app/account',                 short: 'Account' },
  ]

  return (
    <div className={styles.shell}>
      <a className="skip-link" href="#main">Skip to content</a>
      <header className={styles.top}>
        <div className={`page ${styles.bar}`}>
          <Link to="/" className={styles.wordmark} aria-label="SpendStream home page">
            <span className="brand-stamp" aria-hidden="true">₹</span>SpendStream
          </Link>
          <SegmentedNav items={pages} label="App" className={styles.seg} />
          <AccountMenu user={user} />
        </div>
      </header>

      <main id="main" className={`page ${styles.main}`}>
        <SpendingContext.Provider value={spending}>
          <Outlet />
        </SpendingContext.Provider>
      </main>

      <nav className={styles.tabs} aria-label="App">
        {tabs.map(t => (
          <NavLink key={t.to} to={t.to} end={t.end} className={styles.tab}>
            {t.short}
            {t.count > 0 && <span className={styles.count}>{t.count}</span>}
          </NavLink>
        ))}
      </nav>
    </div>
  )
}

// Uncategorised payments waiting for the user. Hidden while unknown or zero.
function useReviewCount(userId, pathname) {
  const [count, setCount] = useState(0)

  useEffect(() => {
    let cancelled = false
    const load = () => supabase.from('silver_transactions')
      .select('id', { count: 'exact', head: true })
      .eq('user_id', userId).eq('is_categorised', false)
      .then(({ count: n, error }) => { if (!cancelled && !error) setCount(n ?? 0) })
    load()
    window.addEventListener(REVIEW_CHANGED, load)
    return () => { cancelled = true; window.removeEventListener(REVIEW_CHANGED, load) }
  }, [userId, pathname])

  return count
}
