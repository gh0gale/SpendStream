import { Navigate, Outlet, Route, Routes, useLocation } from 'react-router'
import AppLayout from './components/AppLayout'
import PublicLayout from './components/PublicLayout'
import { useAuth } from './lib/auth'
import Account from './pages/Account'
import Connect from './pages/Connect'
import Dashboard from './pages/Dashboard'
import Home from './pages/Home'
import HowItWorks from './pages/HowItWorks'
import Login from './pages/Login'
import Privacy from './pages/Privacy'
import Review from './pages/Review'
import { Deleted, NotFound } from './pages/SimplePages'
import Terms from './pages/Terms'
import Transactions from './pages/Transactions'
import YourData from './pages/YourData'

// Real URLs: the host must serve index.html for every path (SPA fallback).
export default function App() {
  return (
    <Routes>
      <Route element={<PublicLayout />}>
        <Route index element={<GmailReturn><Home /></GmailReturn>} />
        <Route path="how-it-works" element={<HowItWorks />} />
        <Route path="your-data" element={<YourData />} />
        <Route path="privacy" element={<Privacy />} />
        <Route path="terms" element={<Terms />} />
        <Route path="deleted" element={<Deleted />} />
        <Route path="*" element={<NotFound />} />
      </Route>
      <Route path="login" element={<SignedOutOnly><Login /></SignedOutOnly>} />
      <Route element={<RequireAuth />}>
        <Route path="connect" element={<Connect />} />
        <Route path="app" element={<AppLayout />}>
          <Route index element={<Dashboard />} />
          <Route path="transactions" element={<Transactions />} />
          <Route path="review" element={<Review />} />
          <Route path="account" element={<Account />} />
        </Route>
      </Route>
    </Routes>
  )
}

function RequireAuth() {
  const { user, ready } = useAuth()
  if (!ready) return <SessionCheck />
  if (!user) return <Navigate to="/login" replace />
  return <Outlet />
}

// Signed-in visitors to the login page go to the app.
function SignedOutOnly({ children }) {
  const { user, ready } = useAuth()
  const { search } = useLocation()
  if (!ready) return <SessionCheck />
  if (user) return <Navigate to={`/app${search}`} replace />
  return children
}

// Home is open to signed-in visitors who navigate to it from inside the site.
// A signed-in visitor who *arrives* at / (a fresh page load: Supabase's
// fallback redirect after Google sign-in or an email confirmation link, or a
// typed URL) goes to the dashboard. The backend's /?gmail=<result> redirect
// after connecting Gmail keeps its query string.
function GmailReturn({ children }) {
  const { user, ready } = useAuth()
  const { search, key } = useLocation()
  const arrived = key === 'default'
  if (arrived && !ready) return <SessionCheck />
  const gmail = new URLSearchParams(search).has('gmail')
  if (user && (arrived || gmail)) return <Navigate to={gmail ? `/app${search}` : '/app'} replace />
  return children
}

function SessionCheck() {
  return (
    <div className="page" style={{ paddingTop: 80 }} aria-busy="true">
      <span className="spinner" aria-hidden="true" />
      <span className="visually-hidden">Checking your session</span>
    </div>
  )
}
