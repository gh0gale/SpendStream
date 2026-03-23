import { useState, useEffect } from 'react'
import { supabase } from './lib/supabase'
import LandingPage from './pages/LandingPage'
import LoginPage from './pages/LoginPage'
import Dashboard from './pages/Dashboard'
import Transactions from './pages/Transactions'

export default function App() {
  const [user, setUser]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [page, setPage]       = useState('landing') // landing | login | dashboard | transactions

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      if (data.session?.user) {
        setUser(data.session.user)
        setPage('dashboard')
      }
      setLoading(false)
    })

    const { data: listener } = supabase.auth.onAuthStateChange((_event, session) => {
      if (session?.user) {
        setUser(session.user)
        setPage('dashboard')
      } else {
        setUser(null)
        setPage('landing')
      }
    })

    return () => listener.subscription.unsubscribe()
  }, [])

  const navigate = (p) => setPage(p)

  const handleSignOut = async () => {
    await supabase.auth.signOut()
    setUser(null)
    setPage('landing')
  }

  if (loading) return <Loader />

  if (!user) {
    if (page === 'login') return <LoginPage onNavigate={navigate} />
    return <LandingPage onNavigate={navigate} />
  }

  if (page === 'transactions') {
    return <Transactions user={user} onNavigate={navigate} onSignOut={handleSignOut} />
  }

  return <Dashboard user={user} onNavigate={navigate} onSignOut={handleSignOut} />
}

function Loader() {
  return (
    <div style={{
      height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center',
      background: 'var(--obsidian)'
    }}>
      <div style={{ textAlign: 'center' }}>
        <div style={{
          width: 40, height: 40, borderRadius: '50%',
          border: '2px solid var(--surface-2)',
          borderTopColor: 'var(--gold)',
          animation: 'spin 0.8s linear infinite',
          margin: '0 auto 16px'
        }} />
        <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
        <p style={{ color: 'var(--text-tertiary)', fontSize: 13, fontFamily: 'var(--font-mono)' }}>
          initialising...
        </p>
      </div>
    </div>
  )
}