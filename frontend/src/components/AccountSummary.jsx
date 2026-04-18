import { useQuery } from 'react-query'
import { fetchAccount } from '../api/client'

function Stat({ label, value, color }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs text-slate-400 uppercase tracking-wider">{label}</span>
      <span className={`text-xl font-bold ${color || 'text-slate-100'}`}>{value}</span>
    </div>
  )
}

function fmt(n, sign=false) {
  const prefix = sign ? (n >= 0 ? '+' : '') : ''
  return `${prefix}$${Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

export default function AccountSummary({ onModeChange }) {
  const { data, isLoading } = useQuery('account', fetchAccount)

  if (isLoading) return <div className="bg-card rounded-xl p-6 animate-pulse h-28" />

  const pnlColor = data.day_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'

  return (
    <div className="bg-card border border-border rounded-xl p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-slate-100">Account Overview</h2>
        <ModeBadge mode={data.mode} onModeChange={onModeChange} />
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-6">
        <Stat label="Portfolio"    value={fmt(data.portfolio_value)} />
        <Stat label="Cash"         value={fmt(data.cash)} />
        <Stat label="Buying Power" value={fmt(data.buying_power)} />
        <Stat label="Day P&L"
              value={`${fmt(data.day_pnl, true)} (${data.day_pnl_pct >= 0 ? '+' : ''}${data.day_pnl_pct.toFixed(2)}%)`}
              color={pnlColor} />
      </div>
    </div>
  )
}

function ModeBadge({ mode, onModeChange }) {
  const isPaper = mode === 'paper'
  return (
    <div className="flex items-center gap-2">
      <span className={`text-xs font-bold px-3 py-1 rounded-full border ${
        isPaper
          ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
          : 'bg-orange-500/20 text-orange-400 border-orange-500/40'
      }`}>
        {isPaper ? 'PAPER' : 'LIVE'}
      </span>
      <button
        onClick={onModeChange}
        className="text-xs text-slate-400 hover:text-slate-200 underline transition-colors"
      >
        switch
      </button>
    </div>
  )
}
