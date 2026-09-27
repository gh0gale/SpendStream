import { useEffect, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router'
import { supabase } from '../lib/supabase'
import styles from './AccountMenu.module.css'

// The signed-in person's menu, the same on public and app pages: their email,
// a way back to the site and into the app, and sign out.
export default function AccountMenu({ user }) {
  const [open, setOpen] = useState(false)
  const wrap = useRef(null)
  const navigate = useNavigate()
  const { pathname } = useLocation()
  const [openedOn, setOpenedOn] = useState(pathname)
  const isOpen = open && openedOn === pathname   // navigating closes it

  useEffect(() => {
    if (!isOpen) return
    const onDown = (e) => { if (!wrap.current?.contains(e.target)) setOpen(false) }
    const onKey = (e) => { if (e.key === 'Escape') { setOpen(false); wrap.current?.querySelector('button')?.focus() } }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => { document.removeEventListener('mousedown', onDown); document.removeEventListener('keydown', onKey) }
  }, [isOpen])

  const signOut = async () => {
    setOpen(false)
    await supabase.auth.signOut()
    navigate('/', { replace: true })
  }

  const initial = (user.email || '?').slice(0, 1).toUpperCase()

  return (
    <div className={styles.wrap} ref={wrap}>
      <button type="button" className={styles.avatar} aria-haspopup="menu" aria-expanded={isOpen}
        aria-label={`Account menu for ${user.email}`} onClick={() => { setOpenedOn(pathname); setOpen(o => !o) }}>
        {initial}
      </button>
      {isOpen && (
        <div className={styles.menu} role="menu">
          <p className={styles.email}>{user.email}</p>
          <Link role="menuitem" to="/app">Dashboard</Link>
          <Link role="menuitem" to="/">Home page</Link>
          <Link role="menuitem" to="/app/account">Account and Gmail</Link>
          <button type="button" role="menuitem" onClick={signOut}>Sign out</button>
        </div>
      )}
    </div>
  )
}
