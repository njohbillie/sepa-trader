import { useState } from 'react'
import SignalBadge from './SignalBadge'
import { closePosition } from '../api/client'
import { useQueryClient } from 'react-query'

function pct(n) { return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%` }
function usd(n, sign=false) {
  const prefix = sign ? (n >= 0 ? '+$' : '-$') : '$'
  return `${prefix}${Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2 })}`
}

export default function PositionCard({ pos }) {
  const qc      = useQueryClient()
  const [closing, setClosing] = useState(false)
  const isProfit  = pos.unrealized_pl >= 0
  const plColor   = isProfit ? 'text-emerald-400' : 'text-red-400'
  const urgent    = pos.signal === 'NO_SETUP'
  const breakout  = pos.signal === 'BREAKOUT'

  async function handleClose() {
    if (!confirm(`Close ${pos.symbol}?`)) return
    setClosing(true)
    try { await closePosition(pos.symbol) } finally {
      setClosing(false)
      qc.invalidateQueries('positions')
    }
  }

  return (
    <div className={`bg-card border rounded-xl p-5 flex flex-col gap-4 transition-all ${
      urgent   ? 'border-red-500/50 shadow-lg shadow-red-900/20' :
      breakout ? 'border-emerald-500/50 shadow-lg shadow-emerald-900/20' :
                 'border-border'
    }`}>
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <span className="text-xl font-bold text-slate-100">{pos.symbol}</span>
          <div className="text-sm text-slate-400 mt-0.5">{pos.qty} shares @ {usd(pos.entry_price)}</div>
        </div>
        <SignalBadge signal={pos.signal} />
      </div>

      {/* P&L */}
      <div className="grid grid-cols-2 gap-3">
        <div className="bg-surface rounded-lg p-3">
          <div className="text-xs text-slate-400 mb-1">Market Value</div>
          <div className="font-semibold text-slate-100">{usd(pos.market_value)}</div>
        </div>
        <div className="bg-surface rounded-lg p-3">
          <div className="text-xs text-slate-400 mb-1">Unrealized P&L</div>
          <div className={`font-semibold ${plColor}`}>
            {usd(pos.unrealized_pl, true)} ({pct(pos.unrealized_plpc)})
          </div>
        </div>
      </div>

      {/* EMA levels */}
      <div className="grid grid-cols-3 gap-2 text-xs">
        <EmaRow label="EMA 20"  value={pos.ema20}  current={pos.current_price} />
        <EmaRow label="EMA 50"  value={pos.ema50}  current={pos.current_price} />
        <EmaRow label="52W Hi"  value={pos.week52_high} current={pos.current_price} noColor />
      </div>

      {/* Score bar */}
      <div>
        <div className="flex justify-between text-xs text-slate-400 mb-1">
          <span>Stage 2 Score</span>
          <span>{pos.score}/8</span>
        </div>
        <div className="h-1.5 bg-surface rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${pos.score >= 7 ? 'bg-emerald-500' : pos.score >= 5 ? 'bg-yellow-500' : 'bg-red-500'}`}
            style={{ width: `${(pos.score / 8) * 100}%` }}
          />
        </div>
      </div>

      {/* Actions */}
      {urgent && (
        <button
          onClick={handleClose}
          disabled={closing}
          className="w-full py-2 rounded-lg bg-red-500/20 text-red-400 border border-red-500/40 text-sm font-semibold hover:bg-red-500/30 transition-colors disabled:opacity-50"
        >
          {closing ? 'Closing…' : 'Close Position'}
        </button>
      )}
    </div>
  )
}

function EmaRow({ label, value, current, noColor }) {
  if (!value) return <div className="bg-surface rounded p-2"><div className="text-slate-400">{label}</div><div>—</div></div>
  const above = current > value
  const color  = noColor ? 'text-slate-300' : above ? 'text-emerald-400' : 'text-red-400'
  return (
    <div className="bg-surface rounded p-2">
      <div className="text-slate-400 mb-0.5">{label}</div>
      <div className={`font-medium ${color}`}>${value.toFixed(2)}</div>
    </div>
  )
}
