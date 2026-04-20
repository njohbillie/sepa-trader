import { useState } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import { fetchSettings, updateSetting } from '../api/client'

const DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

const SECTIONS = [
  {
    title: 'Trading',
    fields: [
      { key: 'auto_execute',  label: 'Auto Execute',       type: 'toggle'  },
      { key: 'risk_pct',      label: 'Risk per Trade %',   type: 'number'  },
      { key: 'stop_loss_pct', label: 'Stop Loss %',        type: 'number'  },
      { key: 'max_positions', label: 'Max Positions',      type: 'number'  },
    ],
  },
  {
    title: 'Screener — Selection',
    fields: [
      { key: 'screener_universe',  label: 'Universe (CSV — leave blank for default 96)',  type: 'text',   span: true },
      { key: 'screener_top_n',     label: 'Stocks to select',                             type: 'number' },
      { key: 'screener_min_score', label: 'Min score (0 = adaptive)',                     type: 'number' },
      { key: 'screener_price_min', label: 'Min price $ (0 = off)',                        type: 'number' },
      { key: 'screener_price_max', label: 'Max price $ (0 = off)',                        type: 'number' },
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
]

export default function SettingsPanel() {
  const qc            = useQueryClient()
  const { data = {} } = useQuery('settings', fetchSettings)
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
    <div className="space-y-6">
      {SECTIONS.map(section => (
        <div key={section.title} className="bg-card border border-border rounded-xl p-5">
          <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-4">
            {section.title}
          </h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
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
