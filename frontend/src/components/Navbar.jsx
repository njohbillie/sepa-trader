import { useState } from 'react'
import { runMonitor, fetchAccount } from '../api/client'
import { useQuery, useQueryClient } from 'react-query'
import UserMenu from './UserMenu'

export default function Navbar({ onModeChange }) {
  const qc                    = useQueryClient()
  const [running, setRunning] = useState(false)
  const [result,  setResult]  = useState(null)

  const { data: account } = useQuery('account', () => fetchAccount(), { refetchInterval: 30000 })
  const mode    = account?.mode ?? 'paper'
  const isPaper = mode === 'paper'

  async function handleRun() {
    setRunning(true)
    setResult(null)
    try {
      const res = await runMonitor()
      qc.invalidateQueries()
      setResult(res)
      setTimeout(() => setResult(null), 8000)
    } catch (e) {
      setResult({ status: 'error', error: e.message })
    } finally {
      setRunning(false)
    }
  }

  async function handleModeSwitch() {
    if (isPaper) {
      const confirmed = window.confirm(
        '⚠️ Switch to LIVE trading?\n\nReal money will be used. Make sure your live Alpaca credentials are configured.'
      )
      if (!confirmed) return
    }
    onModeChange && onModeChange(isPaper ? 'live' : 'paper')
  }

  function ResultBanner() {
    if (!result) return null
    if (result.status === 'market_closed')
      return (
        <div className="flex items-center gap-2 text-xs text-amber-400 bg-amber-500/10 border border-amber-500/20 px-3 py-1.5 rounded-lg">
          <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />
          Market closed
        </div>
      )
    if (result.status === 'error')
      return (
        <div className="flex items-center gap-2 text-xs text-red-400 bg-red-500/10 border border-red-500/20 px-3 py-1.5 rounded-lg">
          <span>⚠</span>
          {result.error}
        </div>
      )
    if (result.status === 'ok') {
      const lost = result.stage2_lost?.length  || 0
      const brk  = result.new_breakouts?.length || 0
      const msg  = lost ? `${lost} Stage 2 lost` : brk ? `${brk} breakout(s)` : 'All healthy'
      const isGood = !lost
      return (
        <div className={`flex items-center gap-2 text-xs px-3 py-1.5 rounded-lg border ${
          isGood
            ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20'
            : 'text-red-400 bg-red-500/10 border-red-500/20'
        }`}>
          <span className={`w-1.5 h-1.5 rounded-full ${isGood ? 'bg-emerald-400' : 'bg-red-400'}`} />
          {msg} · {result.day_pnl >= 0 ? '+' : ''}${result.day_pnl?.toFixed(2)}
        </div>
      )
    }
    return null
  }

  return (
    <nav className="sticky top-0 z-40 border-b border-white/[0.05] bg-[#080c14]/80 backdrop-blur-xl px-5 py-3">
      <div className="max-w-7xl mx-auto flex items-center justify-between gap-4">

        {/* Left — logo */}
        <div className="flex items-center gap-4 min-w-0">
          <div className="flex items-center gap-2">
            <div className="bg-white rounded-xl px-2 py-1 flex items-center shadow-sm">
              <img
                src="/logo.png"
                alt="Bametta LLC"
                className="h-7 w-auto object-contain"
              />
            </div>
          </div>

          {/* Mode badge */}
          <button
            onClick={handleModeSwitch}
            title={`${mode.toUpperCase()} mode — click to switch`}
            className={`flex items-center gap-1.5 px-2.5 py-1 rounded-lg border text-xs font-bold transition-all ${
              isPaper
                ? 'bg-blue-500/10 text-blue-400 border-blue-500/25 hover:bg-blue-500/20'
                : 'bg-orange-500/10 text-orange-400 border-orange-500/25 hover:bg-orange-500/20'
            }`}
          >
            <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
              isPaper ? 'bg-blue-400' : 'bg-orange-400 animate-pulse'
            }`} />
            {isPaper ? 'PAPER' : '⚡ LIVE'}
          </button>
        </div>

        {/* Right */}
        <div className="flex items-center gap-2">
          <ResultBanner />
          <button
            onClick={handleRun}
            disabled={running}
            className={`flex items-center gap-2 px-4 py-1.5 rounded-lg text-sm font-semibold transition-all disabled:opacity-50 ${
              isPaper
                ? 'bg-gradient-to-r from-indigo-500 to-violet-600 text-white shadow-glow-indigo hover:opacity-90'
                : 'bg-gradient-to-r from-orange-500 to-amber-500 text-white hover:opacity-90'
            }`}
          >
            {running && <span className="w-3.5 h-3.5 border-2 border-white/40 border-t-white rounded-full animate-spin" />}
            {running ? 'Running…' : 'Run Monitor'}
          </button>
          <UserMenu />
        </div>
      </div>
    </nav>
  )
}
