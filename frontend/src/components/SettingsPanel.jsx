import { useState } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import { fetchSettings, updateSetting } from '../api/client'

const FIELDS = [
  { key: 'auto_execute',      label: 'Auto Execute',            type: 'toggle'   },
  { key: 'risk_pct',          label: 'Risk per Trade %',        type: 'number'   },
  { key: 'stop_loss_pct',     label: 'Stop Loss %',             type: 'number'   },
  { key: 'max_positions',     label: 'Max Positions',           type: 'number'   },
  { key: 'watchlist',         label: 'Watchlist (CSV)',         type: 'text'     },
  { key: 'screener_universe', label: 'Screener Universe (CSV)', type: 'text'     },
  { key: 'webhook_secret',    label: 'Webhook Secret',          type: 'password' },
  { key: 'tv_username',       label: 'TradingView Username',    type: 'text'     },
  { key: 'tv_password',       label: 'TradingView Password',    type: 'password' },
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
    <div className="bg-card border border-border rounded-xl p-5">
      <h3 className="text-base font-semibold text-slate-100 mb-5">Settings</h3>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {FIELDS.map(f => (
          <Field
            key={f.key}
            field={f}
            value={data[f.key] ?? ''}
            saving={saving === f.key}
            onSave={val => save(f.key, val)}
          />
        ))}
      </div>
    </div>
  )
}

function Field({ field, value, saving, onSave }) {
  const [local, setLocal] = useState(null)
  const current = local ?? value

  if (field.type === 'toggle') {
    const on = current === 'true'
    return (
      <div className="flex items-center justify-between bg-surface rounded-lg p-3">
        <span className="text-sm text-slate-300">{field.label}</span>
        <button
          onClick={() => onSave(on ? 'false' : 'true')}
          disabled={saving}
          className={`relative w-11 h-6 rounded-full transition-colors ${on ? 'bg-accent' : 'bg-slate-700'} disabled:opacity-50`}
        >
          <span className={`absolute top-1 w-4 h-4 bg-white rounded-full transition-all ${on ? 'left-6' : 'left-1'}`} />
        </button>
      </div>
    )
  }

  return (
    <div className="bg-surface rounded-lg p-3">
      <label className="text-xs text-slate-400 block mb-1">{field.label}</label>
      <div className="flex gap-2">
        <input
          type={field.type === 'number' ? 'number' : field.type === 'password' ? 'password' : 'text'}
          value={current}
          onChange={e => setLocal(e.target.value)}
          className="flex-1 bg-transparent text-slate-200 text-sm outline-none border-b border-border focus:border-accent"
        />
        {local !== null && local !== value && (
          <button
            onClick={() => { onSave(local); setLocal(null) }}
            disabled={saving}
            className="text-xs text-accent hover:text-indigo-300 disabled:opacity-50"
          >
            {saving ? '…' : 'Save'}
          </button>
        )}
      </div>
    </div>
  )
}
