import { useState, useRef, useEffect } from 'react'
import { useAuth } from '../AuthContext'
import { changePassword } from '../api/client'

export default function UserMenu() {
  const { user, logout }      = useAuth()
  const [open, setOpen]       = useState(false)
  const [showPw, setShowPw]   = useState(false)
  const [current, setCurrent] = useState('')
  const [next, setNext]       = useState('')
  const [pwError, setPwError] = useState('')
  const [pwOk, setPwOk]       = useState(false)
  const ref                   = useRef(null)

  useEffect(() => {
    function handler(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  async function handleChangePassword(e) {
    e.preventDefault()
    setPwError(''); setPwOk(false)
    try {
      await changePassword(current, next)
      setPwOk(true); setCurrent(''); setNext('')
    } catch (err) {
      setPwError(err?.response?.data?.detail || 'Failed to change password')
    }
  }

  if (!user) return null

  const initial = user.username[0].toUpperCase()

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(o => !o)}
        className="flex items-center gap-2 px-2 py-1.5 rounded-xl transition-all hover:bg-white/[0.04] border border-transparent hover:border-white/[0.06]"
      >
        <div className="w-7 h-7 rounded-lg bg-white flex items-center justify-center text-xs font-bold text-white overflow-hidden">
          <img src="/logo.png" alt="" className="w-full h-full object-contain" />
        </div>
        <span className="text-slate-300 text-sm hidden sm:block font-medium">{user.username}</span>
        {user.role === 'admin' && (
          <span className="hidden sm:block text-[10px] font-bold px-1.5 py-0.5 rounded-full bg-indigo-500/15 text-indigo-400 border border-indigo-500/25">
            admin
          </span>
        )}
        <span className="text-slate-600 text-xs hidden sm:block">▾</span>
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-2 w-72 card shadow-2xl z-50 py-2 animate-slide-up">
          {/* User info */}
          <div className="px-4 py-3 border-b border-white/5">
            <div className="flex items-center gap-3">
              <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center text-sm font-bold text-white">
                {initial}
              </div>
              <div>
                <p className="text-sm font-semibold text-slate-200">{user.username}</p>
                <p className="text-xs text-slate-500">{user.email}</p>
              </div>
            </div>
          </div>

          {/* Change password */}
          <div className="px-4 py-3 border-b border-white/5">
            <button
              onClick={() => { setShowPw(s => !s); setPwError(''); setPwOk(false) }}
              className="text-xs text-slate-500 hover:text-slate-300 transition-colors flex items-center gap-1.5"
            >
              <span>{showPw ? '▲' : '▼'}</span>
              {showPw ? 'Hide' : 'Change password'}
            </button>
            {showPw && (
              <form onSubmit={handleChangePassword} className="mt-3 space-y-2 animate-slide-up">
                {pwError && <p className="text-xs text-red-400">{pwError}</p>}
                {pwOk    && <p className="text-xs text-emerald-400">Password updated successfully.</p>}
                <input
                  type="password"
                  value={current}
                  onChange={e => setCurrent(e.target.value)}
                  placeholder="Current password"
                  className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-xs text-slate-200 outline-none focus:border-indigo-500/50"
                />
                <input
                  type="password"
                  value={next}
                  onChange={e => setNext(e.target.value)}
                  placeholder="New password (min 8 chars)"
                  minLength={8}
                  className="w-full bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-xs text-slate-200 outline-none focus:border-indigo-500/50"
                />
                <button
                  type="submit"
                  disabled={!current || next.length < 8}
                  className="btn-primary text-xs px-3 py-1.5"
                >
                  Update password
                </button>
              </form>
            )}
          </div>

          {/* Logout */}
          <div className="px-2 pt-1.5">
            <button
              onClick={logout}
              className="w-full text-left text-sm text-red-400/80 hover:text-red-400 hover:bg-red-500/8 px-3 py-2 rounded-xl transition-all"
            >
              Sign out
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
