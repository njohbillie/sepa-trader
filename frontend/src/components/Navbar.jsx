import { useState } from 'react'
import { runMonitor } from '../api/client'
import { useQueryClient } from 'react-query'

export default function Navbar({ lastRun }) {
  const qc = useQueryClient()
  const [running, setRunning] = useState(false)

  async function handleRun() {
    setRunning(true)
    try {
      await runMonitor()
      qc.invalidateQueries()
    } finally { setRunning(false) }
  }

  return (
    <nav className="border-b border-border bg-card px-6 py-4 flex items-center justify-between">
      <div className="flex items-center gap-3">
        <span className="text-2xl">📈</span>
        <div>
          <h1 className="text-lg font-bold text-slate-100 leading-none">SEPA Trader</h1>
          <p className="text-xs text-slate-400">Minervini Stage 2 Monitor</p>
        </div>
      </div>
      <div className="flex items-center gap-4">
        {lastRun && (
          <span className="text-xs text-slate-500 hidden sm:block">
            Last run: {new Date(lastRun).toLocaleTimeString()}
          </span>
        )}
        <button
          onClick={handleRun}
          disabled={running}
          className="px-4 py-2 bg-accent hover:bg-indigo-500 disabled:opacity-50 text-white text-sm font-semibold rounded-lg transition-colors"
        >
          {running ? 'Running…' : 'Run Monitor'}
        </button>
      </div>
    </nav>
  )
}
