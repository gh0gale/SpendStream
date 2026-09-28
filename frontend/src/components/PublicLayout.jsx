import { useState } from 'react'
import { Link, Outlet, useLocation } from 'react-router'
import { useAuth } from '../lib/auth'
import AccountMenu from './AccountMenu'
import SegmentedNav from './SegmentedNav'
import styles from './PublicLayout.module.css'

const NAV = [
  { to: '/', end: true, label: 'Home' },
  { to: '/how-it-works', label: 'How it works' },
  { to: '/your-data',    label: 'Your data' },
]

export default function PublicLayout() {
  const { user } = useAuth()
  const { pathname } = useLocation()
  const [menuFor, setMenuFor] = useState(null)
  // The menu closes itself on navigation: it is open only for the page it was opened on.
  const menuOpen = menuFor === pathname

  return (
    <>
      <a className="skip-link" href="#main">Skip to content</a>
      <header className={styles.header}>
        <div className={`page ${styles.bar}`}>
          <Link to="/" className={styles.wordmark}><span className="brand-stamp" aria-hidden="true">₹</span>SpendStream</Link>
          <SegmentedNav items={NAV} label="Main" className={styles.seg} />
          <div className={styles.actions}>
            {user ? (
              <>
                <Link to="/app" className="btn btn-primary">Dashboard</Link>
                <AccountMenu user={user} />
              </>
            ) : (
              <>
                <Link to="/login" className={styles.signIn}>Sign in</Link>
                <Link to="/login?mode=signup" className="btn btn-primary">Create a free account</Link>
              </>
            )}
            <button
              type="button"
              className={`btn ${styles.menuButton}`}
              aria-expanded={menuOpen}
              aria-controls="site-menu"
              onClick={() => setMenuFor(menuOpen ? null : pathname)}
            >
              {menuOpen ? 'Close' : 'Menu'}
            </button>
          </div>
        </div>
        {menuOpen && (
          <nav id="site-menu" className={styles.menu} aria-label="Main">
            {NAV.map(item => <Link key={item.to} to={item.to}>{item.label}</Link>)}
            {user ? <Link to="/app">Dashboard</Link> : (
              <>
                <Link to="/login">Sign in</Link>
                <Link to="/login?mode=signup">Create a free account</Link>
              </>
            )}
          </nav>
        )}
      </header>

      <main id="main" className={styles.main}>
        <Outlet />
      </main>

      <SiteFooter />
    </>
  )
}

export function SiteFooter() {
  return (
    <footer className={styles.footer}>
      <div className={`page ${styles.footerGrid}`}>
        <div className={styles.footerBrand}>
          <p className={styles.wordmark}><span className="brand-stamp" aria-hidden="true">₹</span>SpendStream</p>
          <p className="small">Your UPI payments, sorted from the alerts your bank already sends. Free.</p>
        </div>
        <nav aria-label="Product">
          <p className="caption">Product</p>
          <Link to="/how-it-works">How it works</Link>
          <Link to="/login?mode=signup">Create an account</Link>
          <Link to="/login">Sign in</Link>
        </nav>
        <nav aria-label="Trust">
          <p className="caption">Trust</p>
          <Link to="/your-data">Your data</Link>
          <Link to="/privacy">Privacy</Link>
          <Link to="/terms">Terms</Link>
        </nav>
        <nav aria-label="Project">
          <p className="caption">Project</p>
          <a href="https://github.com/gh0gale/SpendStream">GitHub (gh0gale)</a>
          <span className="small">Built by one developer in India</span>
        </nav>
      </div>
    </footer>
  )
}
