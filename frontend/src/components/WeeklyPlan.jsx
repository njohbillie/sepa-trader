import { useState, useEffect, useRef } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import {
  fetchWeeklyPlan, fetchWeeklyDD, forceRefreshDD, fetchScreenerStatus,
  runScreener, runMinerviniScreener, runPullbackScreener,
  exportWatchlist, updatePlanStatus,
  fetchAnalyses, runAnalysis, fetchSettings,
} from '../api/client'
import TapeCheck from './TapeCheck'

const SIGNAL_STYLE = {
  BREAKOUT:       'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30',
  PULLBACK_EMA20: 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/30',
  PULLBACK_EMA50: 'bg-blue-500/20 text-blue-300 border border-blue-500/30',
  STAGE2_WATCH:   'bg-yellow-500/20 text-yellow-300 border border-yellow-500/30',
  NO_SETUP:       'bg-red-500/20 text-red-400 border border-red-500/30',
}

const STATUS_STYLE = {
  PENDING:  'bg-slate-700 text-slate-300',
  EXECUTED: 'bg-emerald-500/20 text-emerald-300',
  PARTIAL:  'bg-yellow-500/20 text-yellow-300',
  SKIPPED:  'bg-slate-600 text-slate-400 line-through',
}

function fmtCap(n) {
  if (!n) return '—'
  if (n >= 1e12) return `$${(n / 1e12).toFixed(1)}T`
  if (n >= 1e9)  return `$${(n / 1e9).toFixed(1)}B`
  if (n >= 1e6)  return `$${(n / 1e6).toFixed(0)}M`
  return `$${n}`
}
function fmtPct(n) {
  if (n == null) return '—'
  const v = (n * 100).toFixed(1)
  return n >= 0 ? `+${v}%` : `${v}%`
}
function pctColor(n) {
  if (n == null) return 'text-slate-400'
  return n >= 0.10 ? 'text-emerald-400' : n >= 0 ? 'text-slate-300' : 'text-red-400'
}

const SCREENER_BADGE = {
  minervini: { label: 'Minervini',      cls: 'bg-indigo-500/15 text-indigo-300 border border-indigo-500/20' },
  pullback:  { label: 'Pullback MA',    cls: 'bg-cyan-500/15   text-cyan-300   border border-cyan-500/20'   },
  both:      { label: 'Both screeners', cls: 'bg-violet-500/15 text-violet-300 border border-violet-500/20' },
}

export default function WeeklyPlan() {
  const qc = useQueryClient()
  const [running, setRunning]       = useState(false)
  const [syncing, setSyncing]       = useState(false)
  const [analyzing, setAnalyzing]   = useState(false)
  const [msg, setMsg]               = useState(null)
  const [msgType, setMsgType]       = useState('info')
  const [logOpen, setLogOpen]       = useState(false)
  const [elapsed, setElapsed]       = useState(0)
  const prevStatusRef               = useRef(null)
  const elapsedRef                  = useRef(null)

  const { data: plan = [], isLoading, isError } = useQuery(
    'weeklyPlan',
    () => fetchWeeklyPlan(),
    { refetchInterval: 30000 },
  )

  const { data: status, refetch: refetchStatus } = useQuery(
    'screenerStatus',
    () => fetchScreenerStatus(),
    { refetchInterval: (data) => data?.status === 'running' ? 2000 : 30000 },
  )

  const { data: analyses = [], refetch: refetchAnalyses } = useQuery(
    'aiAnalyses',
    () => fetchAnalyses(),                                          // ← fixed
    { staleTime: 60000, refetchOnWindowFocus: false },
  )

  const { data: settings = {} } = useQuery('settings', fetchSettings, { staleTime: 300000 })
  const tvLayoutId = settings.tv_chart_layout_id || ''

  const weekStart = plan[0]?.week_start
  const {
    data: ddList = [],
    isFetching: ddLoading,
    refetch: refetchDD,
  } = useQuery(
    ['weeklyDD', weekStart],
    () => fetchWeeklyDD(),                                          // ← fixed
    { enabled: plan.length > 0, staleTime: 6 * 60 * 60 * 1000, refetchOnWindowFocus: false },
  )
  const ddMap = Object.fromEntries(ddList.map(d => [d.symbol, d]))

  async function handleRefreshDD() {
    await forceRefreshDD()
    qc.invalidateQueries(['weeklyDD', weekStart])
  }

  // Sync running state from server status (handles page refresh mid-run)
  useEffect(() => {
    const curr = status?.status
    const prev = prevStatusRef.current
    prevStatusRef.current = curr

    if (curr === 'running') {
      setRunning(true)
      // Start elapsed timer
      if (!elapsedRef.current) {
        const startTs = status?.started_at ? parseFloat(status.started_at) * 1000 : Date.now()
        setElapsed(Math.floor((Date.now() - startTs) / 1000))
        elapsedRef.current = setInterval(() => {
          setElapsed(Math.floor((Date.now() - startTs) / 1000))
        }, 1000)
      }
    } else {
      // Stop timer
      if (elapsedRef.current) {
        clearInterval(elapsedRef.current)
        elapsedRef.current = null
      }
    }

    if (prev === 'running' && curr === 'done') {
      setRunning(false)
      qc.invalidateQueries('weeklyPlan')
      setMsg(`Scan complete — ${status?.count ?? 0} stocks selected.`)
      setMsgType('info')
    } else if (curr === 'error' && prev !== 'error') {
      // Catch both transitions: running→error AND idle→error (stale timeout)
      setRunning(false)
      setMsg(`Screener error: ${status?.error || 'Unknown — check docker logs.'}`)
      setMsgType('error')
    }
  }, [status?.status, status?.phase])

  async function handleRunScreener() {
    setMsg(null)
    setElapsed(0)
    setRunning(true)
    try {
      await runScreener()
      refetchStatus()
    } catch (err) {
      setRunning(false)
      setMsg(err?.response?.data?.detail || 'Failed to start screener.')
      setMsgType('error')
    }
  }

  async function handleExportWatchlist() {
    setSyncing(true)
    setMsg(null)
    try {
      const blob = await exportWatchlist()
      // Trigger browser download
      const url = window.URL.createObjectURL(blob)
      const a   = document.createElement('a')
      a.href     = url
      a.download = 'weekly_picks.txt'
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      window.URL.revokeObjectURL(url)
      setMsg('weekly_picks.txt downloaded. In TradingView: Watchlists → ⋮ → Import list from file.')
      setMsgType('info')
      setTimeout(() => setMsg(null), 12000)
    } catch (err) {
      setMsg(err?.response?.data?.detail || 'Export failed — run the screener first.')
      setMsgType('error')
    } finally {
      setSyncing(false)
    }
  }

  async function handleStatus(symbol, newStatus) {
    await updatePlanStatus(symbol, newStatus)
    qc.invalidateQueries('weeklyPlan')
  }

  async function handleRunAnalysis() {
    setAnalyzing(true)
    setMsg(null)
    try {
      await runAnalysis()
      await refetchAnalyses()
      setMsg('AI analysis complete.')
      setMsgType('info')
      setTimeout(() => setMsg(null), 5000)
    } catch (err) {
      setMsg(err?.response?.data?.detail || 'Analysis failed — check Claude API key in Settings.')
      setMsgType('error')
    } finally {
      setAnalyzing(false)
    }
  }

  const weekLabel = weekStart
    ? new Date(weekStart).toLocaleDateString('en-US', {
        month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
      })
    : null

  return (
    <div className="space-y-4">
      {/* Market tape soft-warning banner */}
      <TapeCheck />

      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-base font-semibold text-slate-100">Weekly Trading Plan</h3>
          {weekLabel && <p className="text-xs text-slate-500 mt-0.5">Week of {weekLabel}</p>}
        </div>
        <div className="flex gap-2 flex-wrap">
          {plan.length > 0 && (
            <button
              onClick={handleRefreshDD}
              disabled={ddLoading}
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-slate-700 hover:bg-slate-600 text-slate-200 disabled:opacity-40 transition-colors"
              title="Force-fetch fresh DD from stockanalysis.com"
            >
              {ddLoading ? 'Loading DD…' : 'Refresh DD'}
            </button>
          )}
          <button
            onClick={handleRunAnalysis}
            disabled={analyzing || plan.length === 0}
            className="px-3 py-1.5 rounded-lg text-sm font-medium bg-violet-700 hover:bg-violet-600 text-slate-200 disabled:opacity-40 transition-colors flex items-center gap-1.5"
            title="Run Claude AI analysis on picks"
          >
            {analyzing && <span className="inline-block w-3 h-3 border-2 border-slate-300 border-t-transparent rounded-full animate-spin" />}
            {analyzing ? 'Analyzing…' : 'AI Analysis'}
          </button>
          <button
            onClick={handleExportWatchlist}
            disabled={syncing || plan.length === 0}
            className="px-3 py-1.5 rounded-lg text-sm font-medium bg-slate-700 hover:bg-slate-600 text-slate-200 disabled:opacity-40 transition-colors"
            title="Download weekly_picks.txt — import in TradingView: Watchlists → ⋮ → Import list from file"
          >
            {syncing ? 'Exporting…' : '↓ TV Watchlist'}
          </button>

          {/* Dropdown-style split run button */}
          <div className="flex rounded-lg overflow-hidden border border-accent/30">
            <button
              onClick={handleRunScreener}
              disabled={running}
              className="px-3 py-1.5 text-sm font-medium bg-accent hover:bg-indigo-500 text-white disabled:opacity-50 transition-colors flex items-center gap-2"
              title="Run both screeners"
            >
              {running && <span className="inline-block w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              {running ? 'Scanning…' : 'Run Both'}
            </button>
            <button
              onClick={async () => { setRunning(true); setMsg(null); setElapsed(0); try { await runMinerviniScreener(); refetchStatus() } catch(e) { setRunning(false); setMsg(e?.response?.data?.detail || 'Failed'); setMsgType('error') } }}
              disabled={running}
              className="px-2 py-1.5 text-xs font-medium bg-accent/70 hover:bg-indigo-600 text-white/80 disabled:opacity-50 transition-colors border-l border-white/10"
              title="Run Minervini only"
            >MIN</button>
            <button
              onClick={async () => { setRunning(true); setMsg(null); setElapsed(0); try { await runPullbackScreener(); refetchStatus() } catch(e) { setRunning(false); setMsg(e?.response?.data?.detail || 'Failed'); setMsgType('error') } }}
              disabled={running}
              className="px-2 py-1.5 text-xs font-medium bg-cyan-700/80 hover:bg-cyan-600 text-white/80 disabled:opacity-50 transition-colors border-l border-white/10"
              title="Run Pullback-to-MA only"
            >PB</button>
          </div>
        </div>
      </div>

      {/* Running progress panel */}
      {running && (
        <div className="card border border-indigo-500/20 bg-indigo-500/5 px-4 py-3 space-y-1.5">
          <div className="flex items-center gap-2.5">
            <span className="inline-block w-3.5 h-3.5 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin flex-shrink-0" />
            <span className="text-sm font-medium text-indigo-300">
              {status?.phase || 'Starting…'}
            </span>
            <span className="ml-auto text-xs text-slate-500 num tabular-nums">
              {elapsed > 0 ? `${elapsed}s` : ''}
            </span>
          </div>
          {elapsed > 120 && (
            <p className="text-xs text-amber-400/80 pl-6">
              Taking longer than usual — yfinance PPST checks can be slow. Hang tight.
            </p>
          )}
        </div>
      )}

      {/* Error / success message */}
      {msg && !running && (
        <div className={`border rounded-xl px-4 py-2.5 text-sm ${
          msgType === 'error'
            ? 'bg-red-500/10 border-red-500/30 text-red-300'
            : 'bg-indigo-500/10 border-indigo-500/30 text-indigo-300'
        }`}>
          {msg}
        </div>
      )}

      {/* Persistent error from status (e.g. timed out) */}
      {!running && status?.status === 'error' && status?.error && !msg && (
        <div className="border rounded-xl px-4 py-2.5 text-sm bg-red-500/10 border-red-500/30 text-red-300">
          Last run failed: {status.error}
        </div>
      )}

      {isLoading ? (
        <div className="space-y-3">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="bg-card border border-border rounded-xl h-20 animate-pulse" />
          ))}
        </div>
      ) : isError ? (
        <div className="bg-card border border-red-500/30 rounded-xl p-10 text-center text-red-400 text-sm">
          Failed to load weekly plan — check backend logs.
        </div>
      ) : plan.length === 0 ? (
        <div className="bg-card border border-border rounded-xl p-12 text-center space-y-2 text-slate-500">
          {running ? (
            <>
              <p className="font-medium text-slate-300">Screener running…</p>
              <p className="text-xs">Analyzing stocks — results will appear automatically when done.</p>
            </>
          ) : (
            <>
              <p className="font-medium">No weekly plan yet.</p>
              <p className="text-xs">{status?.last_run_summary || 'Click "Run Screener" to scan stocks.'}</p>
              {status?.error && <p className="text-xs text-red-400 mt-2">Last error: {status.error}</p>}
            </>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          {plan.map(row => (
            <PlanCard
              key={row.symbol}
              row={row}
              dd={ddMap[row.symbol]}
              ddLoading={ddLoading}
              onStatusChange={handleStatus}
              tvLayoutId={tvLayoutId}
            />
          ))}
        </div>
      )}

      {analyses.length > 0 && (
        <div className="card overflow-hidden">
          {/* Collapsible header */}
          <button
            onClick={() => setLogOpen(o => !o)}
            className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/[0.02] transition-colors"
          >
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold text-slate-400 uppercase tracking-wider">AI Analysis Log</span>
              <span className="text-[10px] font-medium px-1.5 py-0.5 rounded-full bg-violet-500/15 text-violet-400 border border-violet-500/20">
                {analyses.length}
              </span>
            </div>
            <span className={`text-slate-600 text-xs transition-transform duration-200 ${logOpen ? 'rotate-180' : ''}`}>▼</span>
          </button>

          {/* Log entries */}
          {logOpen && (
            <div className="border-t border-white/5 divide-y divide-white/[0.04]">
              {analyses.map(a => (
                <div key={a.id} className="px-4 py-3 space-y-2">
                  <div className="flex items-center gap-2 text-xs text-slate-500">
                    <span className="bg-violet-500/20 text-violet-300 px-1.5 py-0.5 rounded capitalize">{a.trigger}</span>
                    {a.symbol && <span className="font-medium text-slate-300">{a.symbol}</span>}
                    <span className="ml-auto">{new Date(a.created_at).toLocaleString()}</span>
                  </div>
                  <pre className="text-xs text-slate-300 leading-relaxed whitespace-pre-wrap font-sans">{a.analysis}</pre>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const AI_DECISION_META = {
  EXECUTE: { label: 'Execute', cls: 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30' },
  WAIT:    { label: 'Wait',    cls: 'bg-yellow-500/20  text-yellow-300  border border-yellow-500/30'  },
  SKIP:    { label: 'Skip',    cls: 'bg-red-500/20     text-red-300     border border-red-500/30'     },
}

function PlanCard({ row, dd, ddLoading, onStatusChange, tvLayoutId }) {
  const [expanded, setExpanded] = useState(false)
  const [ddOpen, setDdOpen]     = useState(false)

  const tvUrl = tvLayoutId
    ? `https://www.tradingview.com/chart/${tvLayoutId}/?symbol=${row.symbol}`
    : `https://www.tradingview.com/chart/?symbol=${row.symbol}`

  const signalCls = SIGNAL_STYLE[row.signal] || SIGNAL_STYLE.STAGE2_WATCH
  const statusCls = STATUS_STYLE[row.status] || STATUS_STYLE.PENDING
  const rr = row.target1 && row.entry_price && row.stop_price
    ? ((row.target1 - row.entry_price) / (row.entry_price - row.stop_price)).toFixed(1)
    : '—'

  return (
    <div className={`bg-card border border-border rounded-xl overflow-hidden ${row.status === 'SKIPPED' ? 'opacity-50' : ''}`}>
      <div
        className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-white/5"
        onClick={() => setExpanded(e => !e)}
      >
        <span className="w-6 h-6 rounded-full bg-slate-700 text-slate-300 text-xs flex items-center justify-center font-bold flex-shrink-0">
          {row.rank}
        </span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-semibold text-slate-100">{row.symbol}</span>
            <span className={`text-xs px-1.5 py-0.5 rounded-md ${signalCls}`}>{row.signal}</span>
            {row.screener_type && SCREENER_BADGE[row.screener_type] && (
              <span className={`text-[10px] font-semibold px-1.5 py-0.5 rounded-md ${SCREENER_BADGE[row.screener_type].cls}`}>
                {SCREENER_BADGE[row.screener_type].label}
              </span>
            )}
            {row.ai_analysis?.decision && AI_DECISION_META[row.ai_analysis.decision] && (
              <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded-md ${AI_DECISION_META[row.ai_analysis.decision].cls}`}>
                AI: {AI_DECISION_META[row.ai_analysis.decision].label}
              </span>
            )}
            {dd && !dd.error && dd.sector && (
              <span className="text-xs px-1.5 py-0.5 rounded-md bg-slate-700/60 text-slate-400 hidden sm:inline">
                {dd.sector}
              </span>
            )}
          </div>
          <div className="flex items-center gap-3 mt-0.5 text-xs text-slate-400">
            {row.screener_type === 'rs_momentum' ? (
              <span>RS <strong className="text-slate-200">{row.score}th</strong></span>
            ) : (
              <span>Score <strong className="text-slate-200">{row.score}/6</strong></span>
            )}
            <span>Entry <strong className="text-slate-200">${Number(row.entry_price).toFixed(2)}</strong></span>
            <span>Stop <strong className="text-red-400">${Number(row.stop_price).toFixed(2)}</strong></span>
            <span>R:R <strong className="text-emerald-400">{rr}x</strong></span>
          </div>
        </div>
        <a
          href={tvUrl}
          target="_blank"
          rel="noopener noreferrer"
          onClick={e => e.stopPropagation()}
          className="flex-shrink-0 text-xs px-2 py-1 rounded-md bg-sky-500/15 text-sky-400 border border-sky-500/20 hover:bg-sky-500/25 transition-colors whitespace-nowrap"
          title="Open chart in TradingView"
        >
          📊 Chart
        </a>
        <div className="text-right flex-shrink-0 space-y-1">
          <div className="text-sm font-medium text-slate-200">{row.position_size} sh</div>
          <span className={`text-xs px-2 py-0.5 rounded-full ${statusCls}`}>{row.status}</span>
        </div>
      </div>

      {expanded && (
        <div className="border-t border-border px-4 py-3 space-y-3">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            <Stat label="Entry"         value={`$${Number(row.entry_price).toFixed(2)}`} />
            <Stat label="Stop"          value={`$${Number(row.stop_price).toFixed(2)}`}  color="text-red-400" />
            <Stat label="Target 1 (2R)" value={`$${Number(row.target1).toFixed(2)}`}    color="text-emerald-400" />
            <Stat label="Target 2 (3R)" value={`$${Number(row.target2).toFixed(2)}`}    color="text-emerald-300" />
            <Stat label="Shares"        value={row.position_size} />
            <Stat label="Risk $"        value={`$${Number(row.risk_amount).toFixed(0)}`} />
            <Stat label="Mode"          value={row.mode?.toUpperCase()} />
            <Stat label="R:R"           value={`${rr}x`} />
          </div>

          {row.rationale && (
            <p className="text-xs text-slate-400 leading-relaxed">{row.rationale}</p>
          )}

          <div className="flex gap-2 pt-1">
            {['PENDING', 'EXECUTED', 'PARTIAL', 'SKIPPED'].map(s => (
              <button
                key={s}
                onClick={() => onStatusChange(row.symbol, s)}
                className={`text-xs px-2 py-1 rounded-md transition-colors ${
                  row.status === s ? 'bg-accent text-white' : 'bg-slate-700 text-slate-400 hover:bg-slate-600'
                }`}
              >
                {s}
              </button>
            ))}
          </div>

          <button
            onClick={() => setDdOpen(o => !o)}
            className="flex items-center gap-1.5 text-xs text-indigo-400 hover:text-indigo-300"
          >
            <span className={`transition-transform inline-block ${ddOpen ? 'rotate-90' : ''}`}>▶</span>
            {ddOpen ? 'Hide' : 'Show'} Due Diligence
          </button>

          {ddOpen && <DDPanel dd={dd} loading={ddLoading} symbol={row.symbol} />}

          {/* Structured AI analysis */}
          <AiAnalysisBlock ai={row.ai_analysis} />
        </div>
      )}
    </div>
  )
}

function DDPanel({ dd, loading, symbol }) {
  if (loading) {
    return (
      <div className="bg-slate-800/60 rounded-xl p-4 text-xs text-slate-400 animate-pulse">
        Loading due-diligence data for {symbol}…
      </div>
    )
  }
  if (!dd) {
    return (
      <div className="bg-slate-800/60 rounded-xl p-4 text-xs text-slate-500">
        DD not yet loaded — click "Refresh DD" in the header.
      </div>
    )
  }
  if (dd.error) {
    return (
      <div className="bg-slate-800/60 rounded-xl p-4 text-xs text-red-400">
        DD error: {dd.error}
      </div>
    )
  }

  const metrics = [
    { label: 'Market Cap',    value: fmtCap(dd.market_cap) },
    { label: 'P/E TTM',       value: dd.pe_ttm?.toFixed(1)      ?? '—' },
    { label: 'Fwd P/E',       value: dd.forward_pe?.toFixed(1)   ?? '—' },
    { label: 'EPS TTM',       value: dd.eps_ttm != null ? `$${dd.eps_ttm.toFixed(2)}` : '—' },
    { label: 'Rev Growth',    value: fmtPct(dd.revenue_growth),  color: pctColor(dd.revenue_growth)  },
    { label: 'EPS Growth',    value: fmtPct(dd.earnings_growth), color: pctColor(dd.earnings_growth) },
    { label: 'Gross Margin',  value: dd.gross_margin != null ? `${(dd.gross_margin * 100).toFixed(1)}%` : '—' },
    { label: 'Net Margin',    value: dd.net_margin   != null ? `${(dd.net_margin   * 100).toFixed(1)}%` : '—' },
    { label: 'ROE',           value: dd.roe          != null ? `${(dd.roe          * 100).toFixed(1)}%` : '—' },
    { label: 'Debt / Equity', value: dd.debt_to_equity?.toFixed(1) ?? '—' },
  ]

  return (
    <div className="bg-slate-800/60 border border-slate-700/50 rounded-xl p-4 space-y-3">
      <div>
        <p className="text-sm font-semibold text-slate-100">{dd.name}</p>
        <p className="text-xs text-slate-400 mt-0.5">
          {[dd.sector, dd.industry].filter(Boolean).join(' · ')}
        </p>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-5 gap-2">
        {metrics.map(m => (
          <div key={m.label} className="bg-slate-900/50 rounded-lg px-2 py-1.5">
            <div className="text-slate-500 text-[10px] leading-tight mb-0.5">{m.label}</div>
            <div className={`text-xs font-medium ${m.color || 'text-slate-200'}`}>{m.value}</div>
          </div>
        ))}
      </div>

      {/* Analyst consensus + price targets */}
      {(dd.analyst_label && dd.analyst_label !== 'N/A') || dd.target_mean ? (
        <div className="border-t border-slate-700/50 pt-3 space-y-2">
          {dd.analyst_label && dd.analyst_label !== 'N/A' && (
            <div className="flex items-center gap-2 text-xs">
              <span className="text-slate-500">Analyst consensus:</span>
              <span className={`font-semibold ${dd.analyst_css || 'text-slate-300'}`}>
                {dd.analyst_label}
              </span>
              {dd.analyst_count && (
                <span className="text-slate-600">({dd.analyst_count} analysts)</span>
              )}
            </div>
          )}
          {dd.target_mean != null && (
            <div className="flex items-center gap-4 text-xs flex-wrap">
              <div>
                <span className="text-slate-500">Price target — </span>
                <span className="text-slate-300 font-medium">Mean: <strong className="text-slate-100">${dd.target_mean.toFixed(2)}</strong></span>
              </div>
              {dd.target_high != null && (
                <span className="text-emerald-400">High: ${dd.target_high.toFixed(2)}</span>
              )}
              {dd.target_low != null && (
                <span className="text-red-400">Low: ${dd.target_low.toFixed(2)}</span>
              )}
            </div>
          )}
        </div>
      ) : null}

      {/* AI analyst summary */}
      {dd.analyst_summary && (
        <div className="bg-violet-500/5 border border-violet-500/20 rounded-lg px-3 py-2">
          <p className="text-[10px] text-violet-400 uppercase tracking-wider mb-1 font-semibold">AI Summary</p>
          <p className="text-xs text-slate-300 leading-relaxed">{dd.analyst_summary}</p>
        </div>
      )}

      {dd.description && (
        <p className="text-xs text-slate-500 leading-relaxed border-t border-slate-700/50 pt-2">
          {dd.description}{dd.description.length >= 500 ? '…' : ''}
        </p>
      )}
    </div>
  )
}

function Stat({ label, value, color = 'text-slate-200' }) {
  return (
    <div>
      <div className="text-slate-500 mb-0.5">{label}</div>
      <div className={`font-medium ${color}`}>{value}</div>
    </div>
  )
}

function AiAnalysisBlock({ ai }) {
  if (!ai) return null
  const meta = AI_DECISION_META[ai.decision] || AI_DECISION_META.WAIT

  return (
    <div className="bg-violet-500/5 border border-violet-500/20 rounded-xl p-3 space-y-2">
      <div className="flex items-center gap-2">
        <span className="text-[10px] text-violet-400 uppercase tracking-wider font-semibold">AI Analysis</span>
        <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${meta.cls}`}>
          {meta.label}
        </span>
      </div>

      {ai.rationale && (
        <p className="text-xs text-slate-300 italic leading-relaxed">{ai.rationale}</p>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
        {ai.entry_zone && (
          <div className="bg-slate-900/50 rounded-lg p-2">
            <div className="text-[10px] text-emerald-500 uppercase tracking-wider mb-1 font-semibold">Entry zone</div>
            <p className="text-xs text-slate-300 leading-snug">{ai.entry_zone}</p>
          </div>
        )}
        {ai.exit_strategy && (
          <div className="bg-slate-900/50 rounded-lg p-2">
            <div className="text-[10px] text-sky-500 uppercase tracking-wider mb-1 font-semibold">Exit strategy</div>
            <p className="text-xs text-slate-300 leading-snug">{ai.exit_strategy}</p>
          </div>
        )}
        {ai.guardrails && (
          <div className="bg-slate-900/50 rounded-lg p-2">
            <div className="text-[10px] text-orange-500 uppercase tracking-wider mb-1 font-semibold">Guardrails</div>
            <p className="text-xs text-slate-300 leading-snug">{ai.guardrails}</p>
          </div>
        )}
      </div>
    </div>
  )
}