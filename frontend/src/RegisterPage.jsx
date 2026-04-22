import { useState } from 'react'
import { useAuth } from './AuthContext'

export default function RegisterPage({ onGoLogin }) {
  const { register } = useAuth()
  const [email, setEmail]       = useState('')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm]   = useState('')
  const [error, setError]       = useState('')
  const [loading, setLoading]   = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    if (password !== confirm) { setError('Passwords do not match'); return }
    if (password.length < 8)  { setError('Password must be at least 8 characters'); return }
    setLoading(true)
    try {
      await register(email, username, password)
    } catch (err) {
      setError(err?.response?.data?.detail || 'Registration failed')
    } finally {
      setLoading(false)
    }
  }

  const fields = [
    { label: 'Email',            key: 'email',    type: 'email',    value: email,    set: setEmail,    placeholder: 'you@example.com' },
    { label: 'Username',         key: 'username', type: 'text',     value: username, set: setUsername, placeholder: 'tradername', min: 3 },
    { label: 'Password',         key: 'password', type: 'password', value: password, set: setPassword, placeholder: 'Min. 8 characters' },
    { label: 'Confirm Password', key: 'confirm',  type: 'password', value: confirm,  set: setConfirm,  placeholder: '••••••••' },
  ]

  return (
    <div className="min-h-screen bg-aurora flex items-center justify-center px-4">
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-96 h-96 bg-violet-600/10 rounded-full blur-3xl" />
        <div className="absolute -bottom-40 -left-40 w-96 h-96 bg-indigo-600/10 rounded-full blur-3xl" />
      </div>

      <div className="w-full max-w-sm animate-fade-in relative z-10">
        <div className="text-center mb-8">
          <div className="inline-block bg-white rounded-2xl px-6 py-4 shadow-lg mb-5">
            <img src="/logo.png" alt="Bametta LLC" className="h-16 w-auto object-contain" />
          </div>
          <h1 className="text-2xl font-bold text-slate-100 tracking-tight">Create Account</h1>
          <p className="text-slate-500 text-sm mt-1">Join BAMETTA</p>
        </div>

        <div className="card p-7 space-y-5">
          {error && (
            <div className="flex items-start gap-2.5 bg-red-500/10 border border-red-500/20 rounded-xl px-4 py-3">
              <span className="text-red-400 mt-0.5 text-sm">⚠</span>
              <p className="text-red-400 text-sm">{error}</p>
            </div>
          )}

          <form onSubmit={handleSubmit} className="space-y-4">
            {fields.map(f => (
              <div key={f.key} className="space-y-1.5">
                <label className="label block">{f.label}</label>
                <input
                  type={f.type}
                  value={f.value}
                  onChange={e => f.set(e.target.value)}
                  required
                  minLength={f.min}
                  className="input"
                  placeholder={f.placeholder}
                />
              </div>
            ))}
            <button type="submit" disabled={loading} className="btn-primary w-full mt-2">
              {loading ? (
                <span className="flex items-center justify-center gap-2">
                  <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  Creating account…
                </span>
              ) : 'Create account'}
            </button>
          </form>

          <div className="border-t border-white/5 pt-4 text-center">
            <p className="text-slate-500 text-sm">
              Already have an account?{' '}
              <button onClick={onGoLogin} className="text-indigo-400 hover:text-indigo-300 font-medium transition-colors">
                Sign in
              </button>
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
