import { useState, useRef, useEffect } from 'react'
import styles from './Navbar.module.css'

export default function Navbar({ user, currentPage, onNavigate, onSignOut }) {
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef(null)

  useEffect(() => {
    const handler = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const initials = user?.email?.slice(0, 2).toUpperCase() || 'SS'
  const navItems = [
    { label: 'Dashboard',    page: 'dashboard' },
    { label: 'Transactions', page: 'transactions' },
  ]

  return (
    <nav className={styles.nav}>
      <div className={styles.inner}>
        <button className={styles.logo} onClick={() => onNavigate('dashboard')}>
          <span className={styles.logoMark}>◈</span>
          SpendStream
        </button>

        <div className={styles.links}>
          {navItems.map(item => (
            <button key={item.page}
              onClick={() => onNavigate(item.page)}
              className={`${styles.link} ${currentPage === item.page ? styles.active : ''}`}
            >
              {item.label}
            </button>
          ))}
        </div>

        <div ref={menuRef} className={styles.accountWrap}>
          <button
            onClick={() => setMenuOpen(o => !o)}
            className={`${styles.accountBtn} ${menuOpen ? styles.open : ''}`}
          >
            <div className={styles.avatar}>{initials}</div>
            <span className={styles.email}>{user?.email}</span>
            <span className={`${styles.chevron} ${menuOpen ? styles.flipped : ''}`}>▾</span>
          </button>

          {menuOpen && (
            <div className={styles.dropdown}>
              <div className={styles.dropHeader}>
                <p className={styles.dropHeaderLabel}>Signed in as</p>
                <p className={styles.dropHeaderEmail}>{user?.email}</p>
              </div>
              {navItems.map(item => (
                <button key={item.page} className={styles.dropItem}
                  onClick={() => { onNavigate(item.page); setMenuOpen(false) }}>
                  {item.label}
                </button>
              ))}
              <div className={styles.dropDivider} />
              <button className={`${styles.dropItem} ${styles.danger}`} onClick={onSignOut}>
                Sign out
              </button>
            </div>
          )}
        </div>
      </div>
    </nav>
  )
}