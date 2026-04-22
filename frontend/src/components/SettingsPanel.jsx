import { useState } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import { fetchSettings, updateSetting, fetchMe } from '../api/client'
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
      { key: 'pb_earnings_days_min', label: 'Min days to earnings (default 15)',    type: 'number' },
      { key: 'pb_top_n',             label: 'Top N from pullback screener (default 5)', type: 'number' },
      { key: 'pb_ema_alignment',     label: 'Require EMA20 > EMA50 > EMA200',      type: 'toggle' },
      { key: 'pb_price_above_ema20', label: 'Require price > EMA20',               type: 'toggle' },
      { key: 'pb_ppst_required',     label: 'Require PPST bullish confirmation',   type: 'toggle' },
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
      { key: 'monitor_enabled',     label: 'Monitor enabled (auto-place exits & manage positions)', type: 'toggle' },
      { key: 'auto_execute',        label: 'Auto-execute new entries on Monday open',               type: 'toggle' },
      { key: 'risk_pct',            label: 'Risk per trade %',                                     type: 'number' },
      { key: 'stop_loss_pct',       label: 'Default stop loss %',                                  type: 'number' },
      { key: 'max_position_pct',    label: 'Max position size % (hard cap)',                       type: 'number' },
      { key: 'max_positions',       label: 'Max simultaneous positions',                           type: 'number' },
    ],
  },
  {
    title: 'Screener — Schedule (ET)',
    fields: [
      { key: 'screener_auto_run',      label: 'Auto-run enabled',   type: 'toggle' },
      { key: 'screener_schedule_day',  label: 'Day of week',        type: 'select', options: DAYS.map((d, i) => ({ value: String(i), label: d })) },
      { key: 'screener_schedule_time', label: 'Time (HH:MM, 24h)',  type: 'time'   },
    ],
  },
  {
    title: 'Integrations',
    fields: [
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
  const [saving, setSaving] = useState(null)

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
                />
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

function Field({ field, value, saving, onSave }) {
  const [local, setLocal] = useState(null)
  const current = local ?? value

  if (field.type === 'toggle') {
    const on = current === 'true'
    return (
      <div className="flex items-center justify-between bg-surface rounded-lg p-3 h-full">
        <span className="text-sm text-slate-300">{field.label}</span>
        <button
          onClick={() => onSave(on ? 'false' : 'true')}
          disabled={saving}
          className={`relative w-11 h-6 rounded-full transition-colors flex-shrink-0 ${on ? 'bg-accent' : 'bg-slate-700'} disabled:opacity-50`}
        >
          <span className={`absolute top-1 w-4 h-4 bg-white rounded-full transition-all ${on ? 'left-6' : 'left-1'}`} />
        </button>
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
