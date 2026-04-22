/**
 * TapeCheck — Market tape soft-warning banner.
 *
 * Shows today's broad-market verdict (favorable / caution / unfavorable)
 * with the AI summary, key risk, and underlying signals.
 * Always a soft warning — never blocks trading.
 */
import { useState } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import { fetchTapeCheck, refreshTapeCheck } from '../api/client'

const CONDITION_META = {
  favorable:   { label: 'Favorable',   icon: '✓', bg: 'bg-emerald-500/10 border-emerald-500/30', badge: 'bg-emerald-500/20 text-emerald-300', dot: 'bg-emerald-400' },
  caution:     { label: 'Caution',     icon: '⚠', bg: 'bg-yellow-500/10  border-yellow-500/30',  badge: 'bg-yellow-500/20  text-yellow-300',  dot: 'bg-yellow-400'  },
  unfavorable: { label: 'Unfavorable', icon: '✕', bg: 'bg-red-500/10     border-red-500/30',     badge: 'bg-red-500/20     text-red-300',     dot: 'bg-red-400'     },
}

function SignalRow({ label, value, sub }) {
  return (
    <div className="flex justify-between items-start gap-2 py-1 border-b border-border/50 last:border-0">
      <span className="text-xs text-slate-500 shrink-0">{label}</span>
      <div className="text-right">
        <span className="text-xs text-slate-300 font-mono">{value ?? '—'}</span>
        {sub && <span className="text-xs text-slate-500 ml-1">{sub}</span>}
      </div>
    </div>
  )
}

export default function TapeCheck({ compact = false }) {
  const qc                    = useQueryClient()
  const [expanded, setExpanded] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const { data, isLoading, isError } = useQuery('tapeCheck', fetchTapeCheck, {
    staleTime: 5 * 60 * 1000,   // 5 min client-side freshness
    retry: 1,
  })

  async function handleRefresh() {
    setRefreshing(true)
    try {
      await refreshTapeCheck()
      qc.invalidateQueries('tapeCheck')
    } finally {
      setRefreshing(false)
    }
  }

  if (isLoading) {
    return (
      <div className="bg-card border border-border rounded-xl p-3 animate-pulse">
        <div className="h-4 bg-slate-700 rounded w-1/3 mb-2" />
        <div className="h-3 bg-slate-700 rounded w-2/3" />
      </div>
    )
  }

  if (isError || !data) {
    return (
      <div className="bg-card border border-border rounded-xl p-3 text-xs text-slate-500">
        Market tape unavailable — configure AI key in Settings.
      </div>
    )
  }

  const condition = (data.condition || 'caution').toLowerCase()
  const meta      = CONDITION_META[condition] || CONDITION_META.caution
  const signals   = data.signals || {}

  return (
    <div className={`bg-card border rounded-xl overflow-hidden ${meta.bg}`}>
      {/* Header row */}
      <div className="flex items-center gap-3 px-4 py-3">
        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${meta.dot}`} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${meta.badge}`}>
              {meta.label}
            </span>
            <span className="text-xs text-slate-400 truncate">
              {data.summary || 'Market analysis unavailable.'}
            </span>
          </div>
          {data.key_risk && (
            <p className="text-xs text-slate-500 mt-0.5">
              Key risk: <span className="text-slate-400">{data.key_risk}</span>
            </p>
          )}
        </div>

        <div className="flex items-center gap-2 flex-shrink-0">
          {data.cached && (
            <span className="text-xs text-slate-600">cached</span>
          )}
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            title="Refresh tape analysis"
            className="text-slate-500 hover:text-slate-300 disabled:opacity-40 transition-colors text-sm"
          >
            {refreshing ? '…' : '↻'}
          </button>
          {!compact && (
            <button
              onClick={() => setExpanded(e => !e)}
              className="text-slate-500 hover:text-slate-300 transition-colors text-xs"
            >
              {expanded ? '▲' : '▼'}
            </button>
          )}
        </div>
      </div>

      {/* Expanded signals panel */}
      {!compact && expanded && (
        <div className="px-4 pb-3 border-t border-border/50">
          <p className="text-xs text-slate-500 uppercase tracking-wider mt-2 mb-2">Market signals</p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6">
            <div>
              <SignalRow
                label="SPY vs 200 SMA"
                value={signals.spy_price != null ? `$${signals.spy_price}` : null}
                sub={signals.spy_above_200 != null
                  ? (signals.spy_above_200 ? '▲ above 200MA' : '▼ below 200MA')
                  : null}
              />
              <SignalRow
                label="SPY 20-day return"
                value={signals.spy_20d_return != null ? `${signals.spy_20d_return > 0 ? '+' : ''}${signals.spy_20d_return}%` : null}
              />
              <SignalRow
                label="SPY drawdown"
                value={signals.spy_drawdown != null ? `-${signals.spy_drawdown}%` : null}
                sub="from 52W high"
              />
            </div>
            <div>
              <SignalRow
                label="VIX"
                value={signals.vix != null ? signals.vix : null}
                sub={signals.vix != null ? (signals.vix > 25 ? '⚠ elevated' : 'calm') : null}
              />
              <SignalRow
                label="Sector breadth"
                value={signals.breadth_pct != null ? `${signals.breadth_pct}%` : null}
                sub={signals.breadth_above != null
                  ? `${signals.breadth_above}/${signals.breadth_total} ETFs > 50MA`
                  : null}
              />
              <SignalRow
                label="Risk-on/off (TLT)"
                value={signals.tlt_5d_return != null
                  ? `${signals.tlt_5d_return > 0 ? '+' : ''}${signals.tlt_5d_return}% 5d`
                  : null}
                sub={signals.tlt_5d_return != null
                  ? (signals.tlt_5d_return > 0 ? 'risk-off' : 'risk-on')
                  : null}
              />
            </div>
          </div>
          {data.refreshed_at && (
            <p className="text-xs text-slate-600 mt-2 text-right">
              Updated {data.refreshed_at}
            </p>
          )}
        </div>
      )}
    </div>
  )
}
