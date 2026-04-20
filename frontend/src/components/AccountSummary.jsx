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

export default function AccountSummary({ onModeChange, refetchInterval = 5000 }) {
  const { data, isLoading, isError, dataUpdatedAt } = useQuery(
    'account',
    () => fetchAccount(),
    {
      refetchInterval,
      refetchIntervalInBackground: true,
      staleTime: 2000,
    }
  )

  if (isLoading) return <div className="bg-card rounded-xl p-6 animate-pulse h-28" />
  if (isError || !data) return (
    <div className="bg-card border border-red-500/30 rounded-xl p-6 text-red-400 text-sm">
      Unable to reach Alpaca API — check your credentials in <code>.env</code> and restart the backend.
    </div>
  )

  const pnlColor = data.day_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'
  const isPaper  = data.mode === 'paper'
  const lastSync = dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : null

  return (
    <div className={`bg-card border rounded-xl p-6 ${
      isPaper ? 'border-border' : 'border-orange-500/40 shadow-lg shadow-orange-900/10'
    }`}>
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-semibold text-slate-100">Account Overview</h2>
          {!isPaper && (
            <span className="text-[10px] font-bold px-2 py-0.5 rounded bg-orange-500/20 text-orange-400 border border-orange-500/40 uppercase tracking-wider">
              Live Account
            </span>
          )}
          {/* Live pulse indicator */}
          <div className="flex items-center gap-1.5" title={`Last synced: ${lastSync}`}>
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
            <span className="text-[10px] text-slate-500">Live</span>
          </div>
        </div>
        <ModeSwitch mode={data.mode} onSwitch={onModeChange} />
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-6">
        <Stat label="Portfolio"    value={fmt(data.portfolio_value)} />
        <Stat label="Cash"         value={fmt(data.cash)} />
        <Stat label="Buying Power" value={fmt(data.buying_power)} />
        <Stat label="Day P&L"
              value={`${fmt(data.day_pnl, true)} (${data.day_pnl_pct >= 0 ? '+' : ''}${data.day_pnl_pct.toFixed(2)}%)`}
              color={pnlColor} />
      </div>

      {lastSync && (
        <p className="text-[10px] text-slate-600 mt-3 text-right">
          Last synced {lastSync}
        </p>
      )}
    </div>
  )
}

function ModeSwitch({ mode, onSwitch }) {
  const isPaper = mode === 'paper'
  return (
    <div className="flex items-center gap-2">
      <div className={`flex items-center gap-1.5 text-xs font-bold px-3 py-1 rounded-full border ${
        isPaper
          ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
          : 'bg-orange-500/20 text-orange-400 border-orange-500/40'
      }`}>
        <span className={`w-1.5 h-1.5 rounded-full ${isPaper ? 'bg-blue-400' : 'bg-orange-400'}`} />
        {isPaper ? 'PAPER' : 'LIVE'}
      </div>
      <button
        onClick={() => onSwitch && onSwitch(isPaper ? 'live' : 'paper')}
        className="text-xs text-slate-400 hover:text-slate-200 underline transition-colors"
      >
        switch
      </button>
    </div>
  )
}