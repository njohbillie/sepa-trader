/**
 * Dual Momentum (GEM) Strategy Tab — polished UI
 */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from 'react-query'
import {
  fetchMarketEnvironment,
  fetchDMSignal,
  evaluateDualMomentum,
  executeDualMomentum,
  fetchDMPosition,
  fetchDMHistory,
  fetchDMConfig,
  updateDMConfig,
} from '../api/client'

// ── tiny helpers ──────────────────────────────────────────────────────────────
function pct(v, alreadyPercent = false) {
  if (v == null || v === '') return '—'
  const n = alreadyPercent ? parseFloat(v) : parseFloat(v) * 100
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}
function fmt(v, d = 2) { return v != null ? parseFloat(v).toFixed(d) : '—' }
function currency(v)   { return v != null ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(v) : '—' }

// ── colour maps ───────────────────────────────────────────────────────────────
const ENV_CFG = {
  BULL:          { label: 'Bull',          bg: 'bg-emerald-500/12', text: 'text-emerald-400', border: 'border-emerald-500/25', dot: 'bg-emerald-400', glow: 'shadow-[0_0_20px_rgba(16,185,129,0.12)]' },
  BULL_VOLATILE: { label: 'Bull Volatile', bg: 'bg-amber-500/12',   text: 'text-amber-400',   border: 'border-amber-500/25',   dot: 'bg-amber-400',   glow: 'shadow-[0_0_20px_rgba(245,158,11,0.12)]' },
  CORRECTION:    { label: 'Correction',    bg: 'bg-orange-500/12',  text: 'text-orange-400',  border: 'border-orange-500/25',  dot: 'bg-orange-400',  glow: '' },
  BEAR:          { label: 'Bear',          bg: 'bg-red-500/12',     text: 'text-red-400',     border: 'border-red-500/25',     dot: 'bg-red-400',     glow: 'shadow-[0_0_20px_rgba(239,68,68,0.10)]' },
  TRANSITIONAL:  { label: 'Transitional',  bg: 'bg-sky-500/12',    text: 'text-sky-400',     border: 'border-sky-500/25',     dot: 'bg-sky-400',     glow: '' },
  UNKNOWN:       { label: 'Unknown',       bg: 'bg-slate-500/12',   text: 'text-slate-400',   border: 'border-slate-500/25',   dot: 'bg-slate-500',   glow: '' },
}

const DECISION_CFG = {
  EXECUTE: { label: 'Execute', bg: 'bg-emerald-500/8', border: 'border-emerald-500/20', text: 'text-emerald-400', glow: 'shadow-[0_0_30px_rgba(16,185,129,0.12)]' },
  HOLD:    { label: 'Hold',    bg: 'bg-amber-500/8',   border: 'border-amber-500/20',   text: 'text-amber-400',   glow: '' },
  WAIT:    { label: 'Wait',    bg: 'bg-white/[0.02]',  border: 'border-white/[0.06]',   text: 'text-slate-400',   glow: '' },
}

// ── shared bits ───────────────────────────────────────────────────────────────
function Skeleton({ className = 'h-4 w-24' }) {
  return <div className={`${className} bg-white/5 rounded-lg animate-pulse`} />
}

function SectionTitle({ children, action }) {
  return (
    <div className="flex items-center justify-between mb-3">
      <h3 className="label">{children}</h3>
      {action}
    </div>
  )
}

function EnvBadge({ env }) {
  const c = ENV_CFG[env] || ENV_CFG.UNKNOWN
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide px-2.5 py-1 rounded-lg border ${c.bg} ${c.text} ${c.border}`}>
      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${c.dot}`} />
      {c.label}
    </span>
  )
}

// ── Market Environment ────────────────────────────────────────────────────────
function MarketEnvCard({ env, loading }) {
  if (loading) return (
    <div className="card p-4">
      <Skeleton className="h-4 w-32 mb-5" />
      <div className="grid grid-cols-2 gap-3">
        {[...Array(4)].map((_, i) => <Skeleton key={i} className="h-14 w-full" />)}
      </div>
    </div>
  )
  if (!env) return null

  const c          = ENV_CFG[env.environment] || ENV_CFG.UNKNOWN
  const vixColor   = env.vix > 30 ? 'text-red-400' : env.vix > 20 ? 'text-amber-400' : 'text-emerald-400'
  const spy20Color = env.spy_20d_return >= 0 ? 'text-emerald-400' : 'text-red-400'

  return (
    <div className={`card p-5 ${c.glow}`}>
      <SectionTitle>
        <span>Market Environment</span>
      </SectionTitle>

      <div className="flex items-center justify-between mb-4">
        <EnvBadge env={env.environment} />
        <span className="text-xs text-slate-500 italic max-w-[200px] text-right leading-tight">{env.description}</span>
      </div>

      <div className="grid grid-cols-2 gap-2.5">
        <div className="stat-card">
          <div className="label mb-1">SPY Price</div>
          <div className="text-sm font-bold text-slate-100 num">${fmt(env.spy_price)}</div>
          <div className="text-[10px] text-slate-600 num mt-0.5">200SMA ${fmt(env.spy_200sma)}</div>
        </div>
        <div className="stat-card">
          <div className="label mb-1">SPY vs 200SMA</div>
          <div className={`text-sm font-bold ${env.spy_above_200 ? 'text-emerald-400' : 'text-red-400'}`}>
            {env.spy_above_200 ? '▲ Above' : '▼ Below'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label mb-1">SPY 20d Return</div>
          <div className={`text-sm font-bold num ${spy20Color}`}>{pct(env.spy_20d_return, true)}</div>
        </div>
        <div className="stat-card">
          <div className="label mb-1">VIX</div>
          <div className={`text-sm font-bold num ${vixColor}`}>{fmt(env.vix)}</div>
          {env.vix > 30 && <div className="text-[10px] text-red-400/70 mt-0.5">Elevated volatility</div>}
        </div>
      </div>
    </div>
  )
}

// ── AI Decision ───────────────────────────────────────────────────────────────
function AiDecisionCard({ signal, loading }) {
  if (loading) return (
    <div className="card p-4">
      <Skeleton className="h-4 w-28 mb-4" />
      <Skeleton className="h-10 w-48 mb-3" />
      <Skeleton className="h-4 w-full" />
      <Skeleton className="h-4 w-3/4 mt-2" />
    </div>
  )
  if (!signal) return (
    <div className="card p-4 flex flex-col items-center justify-center text-center gap-3 min-h-[180px]">
      <span className="text-4xl opacity-20">⟳</span>
      <p className="text-slate-500 text-sm">No signal yet</p>
      <p className="text-slate-600 text-xs">Run an evaluation to generate the first signal</p>
    </div>
  )

  const decision = signal.ai_verdict || 'WAIT'
  const c        = DECISION_CFG[decision] || DECISION_CFG.WAIT

  return (
    <div className={`card p-5 border ${c.border} ${c.bg} ${c.glow}`}>
      <SectionTitle>AI Decision</SectionTitle>

      <div className="flex items-end gap-4 mb-4">
        <div className={`text-5xl font-black tracking-tight ${c.text}`}>{decision}</div>
        {signal.recommended_symbol && (
          <div className="pb-1">
            <div className="text-2xl font-bold text-slate-100 num">{signal.recommended_symbol}</div>
            <div className="text-xs text-slate-500 uppercase tracking-wide">{signal.mode || 'paper'} mode</div>
          </div>
        )}
      </div>

      {signal.ai_reasoning && (
        <p className="text-sm text-slate-400 leading-relaxed italic mb-3">"{signal.ai_reasoning}"</p>
      )}

      <div className="flex items-center gap-3 flex-wrap">
        {signal.created_at && (
          <span className="text-xs text-slate-600">
            {new Date(signal.created_at).toLocaleDateString()} {new Date(signal.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </span>
        )}
        {signal.executed && (
          <span className="inline-flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            ✓ Executed
          </span>
        )}
      </div>
    </div>
  )
}

// ── Momentum bars ─────────────────────────────────────────────────────────────
const ASSET_LABELS = { SPY: 'US Equities (SPY)', EFA: 'Intl Equities (EFA)', AGG: 'Agg Bonds (AGG)', BIL: 'T-Bills (BIL)' }

function MomentumBars({ momentum }) {
  if (!momentum) return null
  const assets = ['SPY', 'EFA', 'AGG', 'BIL']
  const values = assets.map(k => ({ key: k, val: momentum[k] ?? 0 }))
  const max    = Math.max(...values.map(v => Math.abs(v.val)), 0.001)

  return (
    <div className="card p-4">
      <SectionTitle>12-Month Momentum</SectionTitle>
      <div className="space-y-3">
        {values.map(({ key, val }) => {
          const w   = Math.round((Math.abs(val) / max) * 100)
          const pos = val >= 0
          return (
            <div key={key}>
              <div className="flex justify-between items-baseline mb-1.5">
                <span className="text-xs text-slate-400 font-medium">{ASSET_LABELS[key]}</span>
                <span className={`text-xs font-bold num ${pos ? 'text-emerald-400' : 'text-red-400'}`}>{pct(val)}</span>
              </div>
              <div className="h-2 bg-white/[0.04] rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-700 ${
                    pos ? 'bg-gradient-to-r from-emerald-600 to-emerald-400' : 'bg-gradient-to-r from-red-700 to-red-500'
                  }`}
                  style={{ width: `${w}%` }}
                />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Position list ─────────────────────────────────────────────────────────────
function PositionsList({ positions, loading }) {
  if (loading) return (
    <div className="card p-4">
      <Skeleton className="h-4 w-32 mb-4" />
      <Skeleton className="h-16 w-full" />
    </div>
  )
  return (
    <div className="card p-4">
      <SectionTitle>Current Position</SectionTitle>
      {(!positions || positions.length === 0) ? (
        <div className="text-center py-6">
          <p className="text-slate-600 text-sm">No open positions in this strategy account</p>
        </div>
      ) : (
        <div className="space-y-2.5">
          {positions.map(p => (
            <div key={p.symbol} className="flex items-center justify-between stat-card">
              <div>
                <div className="text-sm font-bold text-slate-100 num">{p.symbol}</div>
                <div className="text-xs text-slate-500 num mt-0.5">{p.qty} sh @ {currency(p.entry_price)}</div>
              </div>
              <div className="text-right">
                <div className="text-sm font-bold text-slate-100 num">{currency(p.market_value)}</div>
                <div className={`text-xs font-medium num ${p.unrealized_pl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {p.unrealized_pl >= 0 ? '+' : ''}{currency(p.unrealized_pl)} ({fmt(p.unrealized_plpc)}%)
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Signal history ────────────────────────────────────────────────────────────
const VERDICT_COLORS = {
  EXECUTE: 'bg-emerald-500/12 text-emerald-400 border-emerald-500/25',
  HOLD:    'bg-amber-500/12   text-amber-400   border-amber-500/25',
  WAIT:    'bg-slate-500/10   text-slate-500   border-slate-500/20',
}

function HistoryTable({ history }) {
  return (
    <div className="card p-4">
      <SectionTitle>Signal History</SectionTitle>
      {(!history || history.length === 0) ? (
        <div className="text-center py-6">
          <p className="text-slate-600 text-sm">No signals yet</p>
        </div>
      ) : (
        <div className="overflow-x-auto -mx-1">
          <table className="w-full text-xs">
            <thead>
              <tr>
                {['Date', 'Symbol', 'AI Verdict', 'Mode', ''].map(h => (
                  <th key={h} className="label text-left pb-3 pr-4 first:pl-1">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {history.map((row, i) => (
                <tr key={row.id} className={`border-t ${i === 0 ? 'border-white/5' : 'border-white/[0.03]'} hover:bg-white/[0.02] transition-colors`}>
                  <td className="py-2.5 pr-4 pl-1 text-slate-500 num">
                    {new Date(row.created_at).toLocaleDateString()} <span className="text-slate-700">{new Date(row.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>
                  </td>
                  <td className="py-2.5 pr-4 font-bold text-slate-200 num">{row.recommended_symbol || '—'}</td>
                  <td className="py-2.5 pr-4">
                    <span className={`inline-block text-[10px] font-bold uppercase tracking-wide px-2 py-0.5 rounded-lg border ${VERDICT_COLORS[row.ai_verdict] || VERDICT_COLORS.WAIT}`}>
                      {row.ai_verdict || 'WAIT'}
                    </span>
                  </td>
                  <td className="py-2.5 pr-4 text-slate-600 uppercase">{row.mode}</td>
                  <td className="py-2.5 text-center">
                    {row.executed
                      ? <span className="text-emerald-500 text-xs">✓</span>
                      : <span className="text-slate-700 text-xs">—</span>
                    }
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

// ── Settings accordion ────────────────────────────────────────────────────────
function StrategySettings({ config, onSave, saving }) {
  const [open, setOpen] = useState(false)
  const [form, setForm] = useState(null)

  if (config && !form) {
    setForm({
      trading_mode:           config.trading_mode                    || 'paper',
      is_active:              config.is_active                       || false,
      auto_execute:           config.auto_execute                    || false,
      lookback_months:        config.settings?.lookback_months       || 12,
      eval_day:               config.settings?.eval_day              || 1,
      eval_frequency:         config.settings?.eval_frequency        || 'monthly',
      vix_threshold:          config.settings?.vix_threshold         || 30,
      spy_drawdown_threshold: config.settings?.spy_drawdown_threshold || 10,
      alpaca_paper_key:       config.alpaca_paper_key                || '',
      alpaca_paper_secret:    config.alpaca_paper_secret             || '',
      alpaca_live_key:        config.alpaca_live_key                 || '',
      alpaca_live_secret:     config.alpaca_live_secret              || '',
    })
  }

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))
  const handleSave = () => form && onSave({
    ...form,
    lookback_months:        parseInt(form.lookback_months)        || 12,
    eval_day:               parseInt(form.eval_day)               || 1,
    vix_threshold:          parseFloat(form.vix_threshold)        || 30,
    spy_drawdown_threshold: parseFloat(form.spy_drawdown_threshold) || 10,
  })

  // Detect if dedicated keys are configured (masked value means set, empty means using shared account)
  const hasDedicatedKeys = form && (
    (form.trading_mode === 'paper' && form.alpaca_paper_key && form.alpaca_paper_key.includes('•')) ||
    (form.trading_mode === 'live'  && form.alpaca_live_key  && form.alpaca_live_key.includes('•'))
  )
  const sharedAccountWarning = form?.auto_execute && !hasDedicatedKeys

  return (
    <div className="card overflow-hidden">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/[0.02] transition-colors"
      >
        <span className="label">Strategy Settings</span>
        <span className={`text-slate-600 text-xs transition-transform duration-200 ${open ? 'rotate-180' : ''}`}>▼</span>
      </button>

      {open && form && (
        <div className="px-4 pb-4 space-y-4 border-t border-white/5 pt-4 animate-slide-up">

          {/* Shared account warning */}
          {sharedAccountWarning && (
            <div className="flex gap-3 bg-amber-500/8 border border-amber-500/25 rounded-xl px-4 py-3">
              <span className="text-amber-400 text-base flex-shrink-0 mt-0.5">⚠</span>
              <div className="space-y-1">
                <p className="text-amber-300 text-xs font-semibold">Auto-Execute shares your screener account</p>
                <p className="text-amber-400/70 text-xs leading-relaxed">
                  No dedicated Alpaca keys are set for Dual Momentum. When auto-executed, it will deploy
                  into the same account as your Minervini positions — leaving no cash buffer and undersizing
                  the ETF allocation. Set separate Alpaca keys below to fully isolate the two strategies.
                </p>
              </div>
            </div>
          )}

          {/* Toggles row */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            {[
              { label: 'Strategy Active', key: 'is_active' },
              { label: 'Auto-Execute Signals', key: 'auto_execute' },
            ].map(({ label, key }) => (
              <label key={key} className="flex items-center gap-3 cursor-pointer group">
                <div
                  onClick={() => set(key, !form[key])}
                  className={`relative w-10 h-5 rounded-full transition-all cursor-pointer flex-shrink-0 ${form[key] ? 'bg-indigo-500' : 'bg-white/10'}`}
                >
                  <div className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-all duration-200 ${form[key] ? 'left-5' : 'left-0.5'}`} />
                </div>
                <span className="text-sm text-slate-400 group-hover:text-slate-300 transition-colors">{label}</span>
              </label>
            ))}

            <div className="flex items-center gap-3">
              <label className="label whitespace-nowrap">Trading Mode</label>
              <select
                value={form.trading_mode}
                onChange={e => set('trading_mode', e.target.value)}
                className="flex-1 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-sm text-slate-200 outline-none focus:border-indigo-500/50"
              >
                <option value="paper">Paper</option>
                <option value="live">Live</option>
              </select>
            </div>
          </div>

          {/* Evaluation schedule */}
          <div className="space-y-3">
            <p className="label">Evaluation Schedule</p>
            <div className="flex flex-wrap gap-4">
              <div className="flex items-center gap-3">
                <label className="text-xs text-slate-400 whitespace-nowrap">Frequency</label>
                <select
                  value={form.eval_frequency}
                  onChange={e => set('eval_frequency', e.target.value)}
                  className="bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-sm text-slate-200 outline-none focus:border-indigo-500/50"
                >
                  <option value="monthly">Monthly</option>
                  <option value="biweekly">Bi-weekly (every 2 weeks)</option>
                  <option value="weekly">Weekly</option>
                </select>
              </div>
              {form.eval_frequency === 'monthly' && (
                <div className="flex items-center gap-3">
                  <label className="text-xs text-slate-400 whitespace-nowrap">Eval Day</label>
                  <input
                    type="number" min={1} max={28}
                    value={form.eval_day}
                    onChange={e => set('eval_day', e.target.value)}
                    className="w-16 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-sm text-slate-200 outline-none focus:border-indigo-500/50 num text-center"
                  />
                  <span className="text-xs text-slate-600">day of month (1–28)</span>
                </div>
              )}
              <div className="flex items-center gap-3">
                <label className="text-xs text-slate-400 whitespace-nowrap">Lookback</label>
                <input
                  type="number" min={1} max={24}
                  value={form.lookback_months}
                  onChange={e => set('lookback_months', e.target.value)}
                  className="w-16 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-sm text-slate-200 outline-none focus:border-indigo-500/50 num text-center"
                />
                <span className="text-xs text-slate-600">months (default 12)</span>
              </div>
            </div>
            <p className="text-xs text-slate-700">Evaluations fire at 4:30 PM ET on weekdays.</p>
          </div>

          {/* Circuit breakers */}
          <div className="space-y-3">
            <div>
              <p className="label">Volatility Circuit Breakers</p>
              <p className="text-xs text-slate-700 mt-1">
                Trigger an out-of-schedule evaluation when markets spike — useful in volatile political environments.
                Fires at most once per day to prevent spam.
              </p>
            </div>
            <div className="flex flex-wrap gap-4">
              <div className="flex items-center gap-3">
                <label className="text-xs text-slate-400 whitespace-nowrap">VIX threshold</label>
                <input
                  type="number" min={15} max={80} step={1}
                  value={form.vix_threshold}
                  onChange={e => set('vix_threshold', e.target.value)}
                  className="w-20 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-sm text-slate-200 outline-none focus:border-indigo-500/50 num text-center"
                />
                <span className="text-xs text-slate-600">trigger if VIX ≥ this (default 30)</span>
              </div>
              <div className="flex items-center gap-3">
                <label className="text-xs text-slate-400 whitespace-nowrap">SPY drawdown %</label>
                <input
                  type="number" min={3} max={30} step={1}
                  value={form.spy_drawdown_threshold}
                  onChange={e => set('spy_drawdown_threshold', e.target.value)}
                  className="w-20 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-1.5 text-sm text-slate-200 outline-none focus:border-indigo-500/50 num text-center"
                />
                <span className="text-xs text-slate-600">trigger if SPY drops this % from 20-day high (default 10)</span>
              </div>
            </div>
          </div>

          {/* Alpaca keys */}
          <div>
            <p className="text-xs text-slate-600 mb-1">
              Dedicated Alpaca account for this strategy (strongly recommended).
            </p>
            <p className="text-xs text-slate-700 mb-3">
              Dual Momentum deploys 100% of buying power into a single ETF. Running it on the same
              account as your Minervini positions will consume all remaining cash and leave no buffer.
              Use a separate Alpaca paper or live account to fully isolate the two strategies.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              {[
                ['alpaca_paper_key',    'Paper API Key'],
                ['alpaca_paper_secret', 'Paper Secret'],
                ['alpaca_live_key',     'Live API Key'],
                ['alpaca_live_secret',  'Live Secret'],
              ].map(([field, label]) => (
                <div key={field}>
                  <label className="label block mb-1.5">{label}</label>
                  <input
                    type="password"
                    className="input font-mono text-xs"
                    placeholder="••••••••"
                    value={form[field]}
                    onChange={e => set(field, e.target.value)}
                  />
                </div>
              ))}
            </div>
          </div>

          <button onClick={handleSave} disabled={saving} className="btn-primary">
            {saving ? 'Saving…' : 'Save Settings'}
          </button>
        </div>
      )}
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────
export default function DualMomentumTab() {
  const qc = useQueryClient()

  const { data: env,       isLoading: envLoading } = useQuery('market-env',  fetchMarketEnvironment, { staleTime: 60_000 })
  const { data: signal,    isLoading: sigLoading } = useQuery('dm-signal',   fetchDMSignal,          { staleTime: 30_000 })
  const { data: positions, isLoading: posLoading } = useQuery('dm-position', fetchDMPosition,        { staleTime: 10_000, retry: false })
  const { data: history                          } = useQuery('dm-history',  () => fetchDMHistory(24), { staleTime: 30_000 })
  const { data: config                           } = useQuery('dm-config',   fetchDMConfig,          { staleTime: 60_000 })

  const [toast,      setToast]      = useState(null)
  const [evalResult, setEvalResult] = useState(null)

  function showToast(msg, type = 'success') {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 4000)
  }

  const { mutate: runEvaluate, isLoading: evaluating } = useMutation(evaluateDualMomentum, {
    onSuccess: data => {
      setEvalResult(data)
      qc.invalidateQueries('dm-signal')
      qc.invalidateQueries('dm-history')
      showToast('Signal evaluated successfully')
    },
    onError: err => showToast(err?.response?.data?.detail || 'Evaluation failed', 'error'),
  })

  const { mutate: runExecute, isLoading: executing } = useMutation(executeDualMomentum, {
    onSuccess: data => {
      qc.invalidateQueries('dm-position')
      qc.invalidateQueries('dm-signal')
      qc.invalidateQueries('dm-history')
      showToast(`Executed: bought ${data.symbol} [${data.mode}]`)
    },
    onError: err => showToast(err?.response?.data?.detail || 'Execution failed', 'error'),
  })

  const { mutate: saveConfig, isLoading: saving } = useMutation(updateDMConfig, {
    onSuccess: () => { qc.invalidateQueries('dm-config'); showToast('Settings saved') },
    onError:   err => showToast(err?.response?.data?.detail || 'Save failed', 'error'),
  })

  const displaySignal = evalResult
    ? { ai_verdict: evalResult.ai_decision?.decision, ai_reasoning: evalResult.ai_decision?.reasoning, recommended_symbol: evalResult.signal?.recommended_symbol, mode: config?.trading_mode || 'paper' }
    : signal

  const momentum = evalResult?.signal?.momentum || signal?.data?.momentum
  const envData  = evalResult?.market_env       || env
  const gemReason = evalResult?.signal?.reasoning || signal?.data?.reasoning

  return (
    <div className="space-y-3 animate-fade-in">

      {/* Toast */}
      {toast && (
        <div className={`fixed top-20 right-5 z-50 flex items-center gap-2.5 px-5 py-3 rounded-2xl text-sm font-medium shadow-2xl border animate-slide-up
          ${toast.type === 'error'
            ? 'bg-red-950/90 backdrop-blur border-red-500/25 text-red-300'
            : 'bg-emerald-950/90 backdrop-blur border-emerald-500/25 text-emerald-300'}`}
        >
          <span>{toast.type === 'error' ? '⚠' : '✓'}</span>
          {toast.msg}
        </div>
      )}

      {/* Header */}
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-lg font-bold text-slate-100 tracking-tight">
            Dual Momentum
            <span className="ml-2 text-sm font-normal text-slate-600">GEM · Antonacci</span>
          </h2>
          <p className="text-xs text-slate-600 mt-0.5">SPY · EFA · AGG · BIL · 12-month momentum rotation</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => runExecute()}
            disabled={executing || !signal}
            className="btn-ghost"
          >
            {executing ? (
              <span className="flex items-center gap-1.5">
                <span className="w-3.5 h-3.5 border border-slate-400 border-t-transparent rounded-full animate-spin" />
                Executing…
              </span>
            ) : 'Execute Signal'}
          </button>
          <button onClick={() => runEvaluate()} disabled={evaluating} className="btn-primary">
            {evaluating ? (
              <span className="flex items-center gap-2">
                <span className="w-3.5 h-3.5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Running…
              </span>
            ) : 'Run Signal'}
          </button>
        </div>
      </div>

      {/* Top row: market env + AI decision */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <MarketEnvCard env={envData} loading={envLoading && !evalResult} />
        <AiDecisionCard signal={displaySignal} loading={sigLoading && !evalResult} />
      </div>

      {/* GEM reasoning */}
      {gemReason && (
        <div className="card p-4 border-l-2 border-indigo-500/30">
          <div className="label mb-2">GEM Signal Reasoning</div>
          <p className="text-sm text-slate-300 leading-relaxed">{gemReason}</p>
        </div>
      )}

      {/* Momentum bars */}
      <MomentumBars momentum={momentum} />

      {/* Position + history side by side on large screens */}
      <div className="grid grid-cols-1 xl:grid-cols-5 gap-4">
        <div className="xl:col-span-2">
          <PositionsList positions={positions} loading={posLoading} />
        </div>
        <div className="xl:col-span-3">
          <HistoryTable history={history} />
        </div>
      </div>

      {/* Settings */}
      <StrategySettings config={config} onSave={saveConfig} saving={saving} />
    </div>
  )
}
