import { useState } from 'react'
import { useAuth } from './AuthContext'

export default function LoginPage({ onGoRegister }) {
  const { login, verify2fa } = useAuth()
  const [email, setEmail]         = useState('')
  const [password, setPassword]   = useState('')
  const [code, setCode]           = useState('')
  const [tempToken, setTempToken] = useState(null)
  const [error, setError]         = useState('')
  const [loading, setLoading]     = useState(false)

  async function handleLogin(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const result = await login(email, password)
      if (result.requires_2fa) setTempToken(result.temp_token)
    } catch (err) {
      setError(err?.response?.data?.detail || 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  async function handle2fa(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await verify2fa(tempToken, code)
    } catch (err) {
      setError(err?.response?.data?.detail || 'Invalid code')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-aurora flex items-center justify-center px-4">
      {/* Decorative orbs */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -left-40 w-96 h-96 bg-indigo-600/10 rounded-full blur-3xl" />
        <div className="absolute -bottom-40 -right-40 w-96 h-96 bg-violet-600/10 rounded-full blur-3xl" />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-indigo-900/5 rounded-full blur-3xl" />
      </div>

      <div className="w-full max-w-sm animate-fade-in relative z-10">
        {/* Logo */}
        <div className="text-center mb-8">
          <div className="inline-block bg-white rounded-2xl px-6 py-4 shadow-lg mb-5">
            <img src="/logo.png" alt="Bametta LLC" className="h-16 w-auto object-contain" />
          </div>
          <p className="text-slate-500 text-sm mt-1">
            {tempToken ? 'Two-factor authentication' : 'Sign in to your account'}
          </p>
        </div>

        <div className="card p-7 space-y-5">
          {error && (
            <div className="flex items-start gap-2.5 bg-red-500/10 border border-red-500/20 rounded-xl px-4 py-3">
              <span className="text-red-400 mt-0.5 text-sm">⚠</span>
              <p className="text-red-400 text-sm">{error}</p>
            </div>
          )}

          {!tempToken ? (
            <form onSubmit={handleLogin} className="space-y-4">
              <div className="space-y-1.5">
                <label className="label block">Email</label>
                <input
                  type="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                  required
                  className="input"
                  placeholder="you@example.com"
                />
              </div>
              <div className="space-y-1.5">
                <label className="label block">Password</label>
                <input
                  type="password"
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  required
                  className="input"
                  placeholder="••••••••"
                />
              </div>
              <button type="submit" disabled={loading} className="btn-primary w-full mt-2">
                {loading ? (
                  <span className="flex items-center justify-center gap-2">
                    <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Signing in…
                  </span>
                ) : 'Sign in'}
              </button>
            </form>
          ) : (
            <form onSubmit={handle2fa} className="space-y-4">
              <div className="text-center">
                <div className="inline-flex items-center justify-center w-10 h-10 rounded-xl bg-indigo-500/10 border border-indigo-500/20 mb-3">
                  <span className="text-lg">🔐</span>
                </div>
                <p className="text-slate-400 text-sm">Enter the 6-digit code from your authenticator app.</p>
              </div>
              <input
                type="text"
                value={code}
                onChange={e => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                required
                autoFocus
                className="input text-center tracking-[0.4em] text-lg font-mono"
                placeholder="000 000"
                maxLength={6}
              />
              <button type="submit" disabled={loading || code.length !== 6} className="btn-primary w-full">
                {loading ? 'Verifying…' : 'Verify Code'}
              </button>
              <button
                type="button"
                onClick={() => { setTempToken(null); setCode('') }}
                className="w-full text-slate-500 hover:text-slate-300 text-sm transition-colors text-center"
              >
                ← Back to login
              </button>
            </form>
          )}

          <div className="border-t border-white/5 pt-4 text-center">
            <p className="text-slate-500 text-sm">
              No account?{' '}
              <button onClick={onGoRegister} className="text-indigo-400 hover:text-indigo-300 font-medium transition-colors">
                Create one
              </button>
            </p>
          </div>
        </div>

        <p className="text-center text-slate-700 text-xs mt-6">
          Stage 2 · Dual Momentum · AI-powered
        </p>
      </div>
    </div>
  )
}
