import { useState } from 'react'
import { useQuery, useQueryClient, useMutation } from 'react-query'
import { fetchSettings, updateSetting, fetchMe, fetchTvScreeners } from '../api/client'
import TwoFactorSetup from './TwoFactorSetup'

const DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

const SECTIONS = [
  {
    title: 'Trading',
    fields: [
      { key: 'trading_mode', label: 'Trading Mode', type: 'select',
        options: [{ value: 'paper', label: 'Paper' }, { value: 'live', label: 'Live' }] },
    ],
  },
  {
    title: 'Screener — Selection',
    fields: [
      { key: 'screener_universe',  label: 'Universe (CSV — leave blank for default 96)',  type: 'text',   span: true },
      { key: 'screener_top_n',     label: 'Stocks to select (0 = auto from position cap)', type: 'number' },
      { key: 'screener_min_score', label: 'Min score (0 = adaptive)',                     type: 'number' },
      { key: 'screener_price_min', label: 'Min price $ (0 = off)',                        type: 'number' },
      { key: 'screener_price_max', label: 'Max price $ (0 = off)',                        type: 'number' },
    ],
  },
  {
    title: 'Pullback Screener (PPST + EMA)',
    fields: [
      { key: 'pb_tv_screener_name',  label: 'TradingView Screener name (leave blank to use app filters below)', type: 'tv_screener', span: true },
      { key: 'pb_price_min',         label: 'Min price $ (default 10)',             type: 'number' },
      { key: 'pb_price_max',         label: 'Max price $ (default 200)',            type: 'number' },
      { key: 'pb_rsi_min',           label: 'RSI min (reset zone, default 40)',     type: 'number' },
      { key: 'pb_rsi_max',           label: 'RSI max (reset zone, default 60)',     type: 'number' },
      { key: 'pb_avg_vol_min',       label: 'Avg 10D volume min (default 1000000)', type: 'number' },
      { key: 'pb_rel_vol_min',       label: 'Relative volume min (default 0.75)',   type: 'number' },
      { key: 'pb_market_cap_min',    label: 'Min market cap $ (default 500000000)', type: 'number' },
      { key: 'pb_week_change_min',   label: '1-week change min % (default -3)',     type: 'number' },
      { key: 'pb_ema50_proximity',   label: 'Max % from EMA50 (default 8)',         type: 'number' },
      { key: 'pb_beta_max',          label: 'Max beta (default 2.5)',               type: 'number' },
      { key: 'pb_earnings_days_min',       label: 'Min days to earnings (default 15)',                          type: 'number' },
      { key: 'pb_ema_spread_min',   label: 'Min EMA20/50 spread % — rejects flat EMA structures (default 1)', type: 'number' },
      { key: 'pb_adx_min',          label: 'Min ADX — trend strength gate (default 20, 0 = off)',              type: 'number' },
      { key: 'pb_52w_high_pct_max', label: 'Max % below 52-week high — Stage 2 guard (default 30)',            type: 'number' },
      { key: 'pb_3m_perf_min',      label: 'Min 3-month performance % (default -5, e.g. -10 = lenient)',       type: 'number' },
      { key: 'pb_block_unknown_earnings', label: 'Block stocks with unknown earnings date (recommended)',        type: 'toggle', defaultValue: 'true' },
      { key: 'pb_top_n',                  label: 'Top N from pullback screener (default 5)',                    type: 'number' },
      { key: 'pb_price_above_ema20',   label: 'Require price > EMA20',    type: 'toggle', defaultValue: 'true' },
      { key: 'pb_ema20_above_ema50',   label: 'Require EMA20 > EMA50',    type: 'toggle', defaultValue: 'true' },
      { key: 'pb_ema50_above_ema100',  label: 'Require EMA50 > EMA100',   type: 'toggle', defaultValue: 'true' },
      { key: 'pb_ema100_above_ema200', label: 'Require EMA100 > EMA200',  type: 'toggle', defaultValue: 'true' },
      { key: 'pb_ppst_required',       label: 'Require PPST bullish confirmation', type: 'toggle', defaultValue: 'true' },
      { key: 'pb_ppst_pivot_period',   label: 'PPST — Pivot Point Period (TV default 2)',  type: 'number' },
      { key: 'pb_ppst_multiplier',     label: 'PPST — ATR Factor (TV default 3)',          type: 'number' },
      { key: 'pb_ppst_period',         label: 'PPST — ATR Period (TV default 10)',         type: 'number' },
    ],
  },
  {
    title: 'Screener — Signal Filters',
    fields: [
      { key: 'screener_vol_surge_pct', label: 'Volume surge threshold % above avg (e.g. 40 = 1.4×)', type: 'number' },
      { key: 'screener_ema20_pct',     label: 'EMA20 proximity band %',                              type: 'number' },
      { key: 'screener_ema50_pct',     label: 'EMA50 proximity band %',                              type: 'number' },
    ],
  },
  {
    title: 'Monitor',
    fields: [
      { key: 'monitor_enabled',     label: 'Monitor enabled (auto-place exits & manage positions)', type: 'toggle', defaultValue: 'true' },
      { key: 'auto_execute',        label: 'Auto-execute new entries on Monday open',               type: 'toggle', defaultValue: 'true' },
      { key: 'risk_pct',            label: 'Risk per trade %',                                     type: 'number' },
      { key: 'stop_loss_pct',       label: 'Default stop loss %',                                  type: 'number' },
      { key: 'max_position_pct',    label: 'Max position size % (hard cap)',                       type: 'number' },
      { key: 'max_positions',       label: 'Max simultaneous positions',                           type: 'number' },
    ],
  },
  {
    title: 'Screener — Schedule (ET)',
    fields: [
      { key: 'screener_auto_run',       label: 'Auto-run enabled',                type: 'toggle',    defaultValue: 'true' },
      { key: 'screener_schedule_days',  label: 'Days to run (click to toggle)',   type: 'day_picker', span: true },
      { key: 'screener_schedule_times', label: 'Run times (24h ET, e.g. 20:00)', type: 'time_list',  span: true },
    ],
  },
  {
    title: 'Integrations',
    fields: [
      { key: 'tv_chart_layout_id', label: 'TradingView chart layout ID (paste from chart URL — optional)', type: 'text', span: true },
      { key: 'tv_username',    label: 'TradingView Username', type: 'text'     },
      { key: 'tv_password',    label: 'TradingView Password', type: 'password' },
      { key: 'watchlist',      label: 'Monitor Watchlist (CSV)',  type: 'text', span: true },
      { key: 'webhook_secret', label: 'Webhook Secret',       type: 'password' },
    ],
  },
  {
    title: 'Alpaca Credentials',
    fields: [
      { key: 'alpaca_paper_key',    label: 'Paper API Key',    type: 'password', span: true },
      { key: 'alpaca_paper_secret', label: 'Paper API Secret', type: 'password', span: true },
      { key: 'alpaca_live_key',     label: 'Live API Key',     type: 'password', span: true },
      { key: 'alpaca_live_secret',  label: 'Live API Secret',  type: 'password', span: true },
    ],
  },
  {
    title: 'AI Analysis',
    fields: [
      {
        key: 'ai_provider', label: 'Provider', type: 'select',
        options: [
          { value: 'anthropic',         label: 'Anthropic (Claude)' },
          { value: 'openai',            label: 'OpenAI (GPT-4 / o-series)' },
          { value: 'openai_compatible', label: 'OpenAI-compatible (xAI, DeepSeek, Mistral, Groq…)' },
        ],
      },
      { key: 'ai_api_key',  label: 'API Key',                              type: 'password', span: true },
      { key: 'ai_model',    label: 'Model ID (leave blank for provider default)', type: 'text' },
      { key: 'ai_base_url', label: 'Base URL (OpenAI-compatible only)',    type: 'text', span: true },
    ],
  },
]

export default function SettingsPanel() {
  const qc            = useQueryClient()
  const { data = {} } = useQuery('settings', fetchSettings)
  const { data: me, refetch: refetchMe } = useQuery('me', fetchMe, { staleTime: 60000 })
  const [saving, setSaving]   = useState(null)
  const [tvOpen, setTvOpen]   = useState(false)
  const [tvList, setTvList]   = useState([])
  const [tvLoading, setTvLoading] = useState(false)
  const [tvError, setTvError] = useState('')

  async function loadTvScreeners() {
    setTvLoading(true); setTvError('')
    try {
      const res = await fetchTvScreeners()
      setTvList(res.screeners || [])
      setTvOpen(true)
    } catch (e) {
      setTvError(e?.response?.data?.detail || 'Could not fetch screeners — check TradingView credentials in Settings → Integrations.')
    } finally {
      setTvLoading(false) }
  }

  async function save(key, value) {
    setSaving(key)
    try {
      await updateSetting(key, value)
      qc.invalidateQueries('settings')
      qc.invalidateQueries('account')
    } finally { setSaving(null) }
  }

  return (
    <div className="space-y-3">

      {/* Account & Security */}
      <div className="bg-card border border-border rounded-xl p-4 space-y-3">
        <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Account & Security</h3>
        {me && (
          <div className="flex items-center gap-3 pb-3 border-b border-border">
            <div className="w-9 h-9 rounded-full bg-accent/20 text-accent flex items-center justify-center font-bold">
              {me.username[0].toUpperCase()}
            </div>
            <div>
              <p className="text-sm text-slate-200 font-medium">{me.username}</p>
              <p className="text-xs text-slate-500">{me.email} · <span className="capitalize">{me.role}</span></p>
            </div>
          </div>
        )}
        <TwoFactorSetup enabled={me?.totp_enabled ?? false} onChanged={refetchMe} />
      </div>

      {SECTIONS.map(section => (
        <div key={section.title} className="bg-card border border-border rounded-xl p-4">
          <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">
            {section.title}
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {section.fields.map(f => (
              <div key={f.key} className={f.span ? 'sm:col-span-2' : ''}>
                <Field
                  field={f}
                  value={data[f.key] ?? ''}
                  saving={saving === f.key}
                  onSave={val => save(f.key, val)}
                  tvScreeners={f.type === 'tv_screener' ? tvList : undefined}
                  tvLoading={f.type === 'tv_screener' ? tvLoading : undefined}
                  tvError={f.type === 'tv_screener' ? tvError : undefined}
                  tvOpen={f.type === 'tv_screener' ? tvOpen : undefined}
                  onBrowseTv={f.type === 'tv_screener' ? loadTvScreeners : undefined}
                  onCloseTv={f.type === 'tv_screener' ? () => setTvOpen(false) : undefined}
                />
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

function Field({ field, value, saving, onSave,
                 tvScreeners, tvLoading, tvError, tvOpen, onBrowseTv, onCloseTv }) {
  const [local, setLocal] = useState(null)
  // Use local (optimistic) → DB value → field default → empty string
  const current = local ?? (value !== '' && value !== undefined ? value : (field.defaultValue ?? ''))

  if (field.type === 'tv_screener') {
    return (
      <div className="bg-surface rounded-lg p-3">
        <label className="text-xs text-slate-400 block mb-1">{field.label}</label>
        <div className="flex gap-2 items-center">
          <input
            type="text"
            value={local ?? value ?? ''}
            onChange={e => setLocal(e.target.value)}
            placeholder="e.g. My Pullback Screener"
            className="flex-1 bg-transparent text-slate-200 text-sm outline-none border-b border-border focus:border-accent"
          />
          {(local !== null && local !== value) && (
            <button
              onClick={() => { onSave(local); setLocal(null) }}
              disabled={saving}
              className="text-xs text-accent hover:text-indigo-300 disabled:opacity-50 flex-shrink-0"
            >{saving ? '…' : 'Save'}</button>
          )}
          <button
            onClick={onBrowseTv}
            disabled={tvLoading}
            className="text-xs bg-accent/20 text-accent hover:bg-accent/30 rounded px-2 py-1 flex-shrink-0 disabled:opacity-50"
          >{tvLoading ? 'Loading…' : 'Browse'}</button>
        </div>
        {tvError && <p className="text-xs text-red-400 mt-1">{tvError}</p>}
        {(local ?? value) && (
          <p className="text-xs text-emerald-400 mt-1">
            ✓ Using TV screener — app filters below are bypassed
          </p>
        )}
        {!(local ?? value) && (
          <p className="text-xs text-slate-500 mt-1">
            Blank = use app filters below (Option A — server-side TV scan)
          </p>
        )}
        {tvOpen && (
          <TvScreenerPicker
            screeners={tvScreeners}
            onSelect={name => { setLocal(name); onSave(name); onCloseTv() }}
            onClose={onCloseTv}
          />
        )}
      </div>
    )
  }

  if (field.type === 'toggle') {
    const on = current === 'true'
    return (
      <div className="flex items-center justify-between bg-surface rounded-lg p-3 h-full">
        <span className="text-sm text-slate-300">{field.label}</span>
        <button
          onClick={() => {
            const next = on ? 'false' : 'true'
            setLocal(next)   // optimistic — shows instantly
            onSave(next)
          }}
          disabled={saving}
          className={`relative w-11 h-6 rounded-full transition-colors flex-shrink-0 ${on ? 'bg-accent' : 'bg-slate-700'} disabled:opacity-50`}
        >
          <span className={`absolute top-1 w-4 h-4 bg-white rounded-full transition-all ${on ? 'left-6' : 'left-1'}`} />
        </button>
      </div>
    )
  }

  if (field.type === 'day_picker') {
    const DAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    const selected   = new Set(
      (current || '').split(',').map(d => d.trim()).filter(Boolean).map(Number)
    )
    function toggleDay(idx) {
      const next = new Set(selected)
      if (next.has(idx)) next.delete(idx); else next.add(idx)
      const val = [...next].sort((a, b) => a - b).join(',')
      setLocal(val)
      onSave(val)
    }
    return (
      <div className="bg-surface rounded-lg p-3">
        <label className="text-xs text-slate-400 block mb-2">{field.label}</label>
        <div className="flex gap-1.5 flex-wrap">
          {DAY_LABELS.map((name, idx) => (
            <button
              key={idx}
              onClick={() => toggleDay(idx)}
              disabled={saving}
              className={`px-2.5 py-1 rounded text-xs font-medium transition-colors disabled:opacity-50 ${
                selected.has(idx)
                  ? 'bg-accent text-white'
                  : 'bg-slate-700 text-slate-400 hover:bg-slate-600'
              }`}
            >{name}</button>
          ))}
          {selected.size === 0 && (
            <span className="text-xs text-slate-500 self-center ml-1">No days selected — won't auto-run</span>
          )}
        </div>
      </div>
    )
  }

  if (field.type === 'time_list') {
    const rawVal = local ?? current ?? ''
    const times  = rawVal.split(',').map(t => t.trim()).filter(Boolean)

    function saveTimes(newTimes) {
      const val = newTimes.filter(Boolean).join(',')
      setLocal(val)
      onSave(val)
    }

    return (
      <div className="bg-surface rounded-lg p-3">
        <label className="text-xs text-slate-400 block mb-2">{field.label}</label>
        <div className="space-y-2">
          {times.map((t, idx) => (
            <div key={`${t}-${idx}`} className="flex gap-2 items-center">
              <input
                type="time"
                defaultValue={t}
                onBlur={e => {
                  if (e.target.value !== t) {
                    const next = [...times]; next[idx] = e.target.value; saveTimes(next)
                  }
                }}
                className="flex-1 bg-transparent text-slate-200 text-sm outline-none border-b border-border focus:border-accent"
              />
              <button
                onClick={() => saveTimes(times.filter((_, i) => i !== idx))}
                className="text-slate-500 hover:text-red-400 text-base leading-none flex-shrink-0"
              >×</button>
            </div>
          ))}
          {times.length === 0 && (
            <p className="text-xs text-slate-500">No times set — screener won't auto-run.</p>
          )}
        </div>
        <button
          onClick={() => saveTimes([...times, '20:00'])}
          className="mt-2 text-xs text-accent hover:text-indigo-300"
        >+ Add time</button>
      </div>
    )
  }

  if (field.type === 'select') {
    return (
      <div className="bg-surface rounded-lg p-3">
        <label className="text-xs text-slate-400 block mb-1">{field.label}</label>
        <select
          value={current}
          onChange={e => { setLocal(e.target.value); onSave(e.target.value) }}
          disabled={saving}
          className="w-full bg-transparent text-slate-200 text-sm outline-none border-b border-border focus:border-accent cursor-pointer"
        >
          {field.options.map(o => (
            <option key={o.value} value={o.value} className="bg-slate-800">{o.label}</option>
          ))}
        </select>
      </div>
    )
  }

  const isDirty = local !== null && local !== value

  return (
    <div className="bg-surface rounded-lg p-3">
      <label className="text-xs text-slate-400 block mb-1">{field.label}</label>
      <div className="flex gap-2 items-center">
        <input
          type={field.type === 'number' ? 'number' : field.type === 'password' ? 'password' : field.type === 'time' ? 'time' : 'text'}
          value={current}
          onChange={e => setLocal(e.target.value)}
          onBlur={() => { if (field.type === 'time' && isDirty) { onSave(local); setLocal(null) } }}
          className="flex-1 bg-transparent text-slate-200 text-sm outline-none border-b border-border focus:border-accent"
        />
        {isDirty && field.type !== 'time' && (
          <button
            onClick={() => { onSave(local); setLocal(null) }}
            disabled={saving}
            className="text-xs text-accent hover:text-indigo-300 disabled:opacity-50 flex-shrink-0"
          >
            {saving ? '…' : 'Save'}
          </button>
        )}
      </div>
    </div>
  )
}

function TvScreenerPicker({ screeners, onSelect, onClose }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
         onClick={onClose}>
      <div className="bg-card border border-border rounded-xl p-4 w-80 max-h-96 flex flex-col shadow-2xl"
           onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-semibold text-slate-200">Your TradingView Screeners</h3>
          <button onClick={onClose} className="text-slate-500 hover:text-slate-200 text-lg leading-none">×</button>
        </div>
        {screeners.length === 0 ? (
          <p className="text-sm text-slate-400 py-4 text-center">
            No saved screeners found.<br />
            <span className="text-xs text-slate-500">Create and save a screener in TradingView first.</span>
          </p>
        ) : (
          <ul className="overflow-y-auto space-y-1">
            {screeners.map(s => (
              <li key={s.id}>
                <button
                  onClick={() => onSelect(s.name)}
                  className="w-full text-left px-3 py-2 rounded-lg hover:bg-accent/20 text-sm text-slate-200 flex items-center justify-between group"
                >
                  <span>{s.name}</span>
                  {s.symbol_count != null && (
                    <span className="text-xs text-slate-500 group-hover:text-slate-300">
                      {s.symbol_count} stocks
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
        <p className="text-xs text-slate-500 mt-3 border-t border-border pt-2">
          Selecting a screener will use its exact TV filter set. App filters below will be bypassed.
        </p>
      </div>
    </div>
  )
}
