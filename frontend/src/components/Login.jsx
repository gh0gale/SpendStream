import { supabase } from '../lib/supabase'

export default function Login() {
  const handleLogin = async () => {
    await supabase.auth.signInWithOAuth({
      provider: 'google',
    })
  }

  return (
    <div>
      <h2>Login</h2>
      <button onClick={handleLogin}>
        Login with Google
      </button>
    </div>
  )
}