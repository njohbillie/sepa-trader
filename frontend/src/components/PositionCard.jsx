import { useState } from 'react'
import SignalBadge from './SignalBadge'
import { closePosition } from '../api/client'
import { useQueryClient } from 'react-query'
import axios from 'axios'

function pct(n) { return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%` }
function usd(n, sign = false) {
  const prefix = sign ? (n >= 0 ? '+$' : '-$') : '$'
  return `${prefix}${Math.abs(n).toLocaleString('en-US', { minimumFractionDigits: 2 })}`
}
function fmt(n) { return n != null ? Number(n).toFixed(2) : '' }

export default function PositionCard({ pos }) {
  const qc = useQueryClient()

  const [closing,   setClosing]   = useState(false)
  const [editExits, setEditExits] = useState(false)
  const [stop,      setStop]      = useState('')
  const [target,    setTarget]    = useState('')
  const [saving,    setSaving]    = useState(false)
  const [placing,   setPlacing]   = useState(false)
  const [exitMsg,   setExitMsg]   = useState(null)
  const [execMode,  setExecMode]  = useState('auto')

  const isProfit      = pos.unrealized_pl >= 0
  const plColor       = isProfit ? 'text-emerald-400' : 'text-red-400'
  const urgent        = pos.signal === 'NO_SETUP'
  const breakout      = pos.signal === 'BREAKOUT'
  const hasPlanLevels = pos.stop_price || pos.target1

  const isHistoricalPlan = (() => {
    if (!pos.plan_week) return false
    const planDate = new Date(pos.plan_week)
    const monday   = new Date()
    monday.setDate(monday.getDate() - ((monday.getDay() + 6) % 7))
    monday.setHours(0, 0, 0, 0)
    return planDate < monday
  })()

  const planWeekLabel = pos.plan_week
    ? new Date(pos.plan_week).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' })
    : null

  function openExitForm() {
    if (!editExits) { setStop(fmt(pos.stop_price)); setTarget(fmt(pos.target1)) }
    setEditExits(e => !e)
    setExitMsg(null)
  }

  function validate() {
    const s = parseFloat(stop), t = parseFloat(target)
    if (!s || !t || s <= 0 || t <= 0) { setExitMsg({ type: 'error', text: 'Enter valid stop and target prices.' }); return null }
    if (t <= s)                        { setExitMsg({ type: 'error', text: 'Target must be above stop.' }); return null }
    if (s >= pos.entry_price)          { setExitMsg({ type: 'error', text: 'Stop must be below entry.' }); return null }
    return { s, t }
  }

  async function handleSaveOnly() {
    const vals = validate(); if (!vals) return
    setSaving(true); setExitMsg(null)
    try {
      await axios.patch(`/api/positions/${pos.symbol}/exits?stop=${vals.s}&target=${vals.t}`)
      setExitMsg({ type: 'ok', text: 'Saved — OCO will be placed on next monitor cycle.' })
      setEditExits(false); qc.invalidateQueries('positions')
    } catch (err) {
      setExitMsg({ type: 'error', text: err?.response?.data?.detail || 'Failed to save.' })
    } finally { setSaving(false) }
  }

  async function handlePlaceNow() {
    const vals = validate(); if (!vals) return
    setPlacing(true); setExitMsg(null)
    try {
      await axios.post(`/api/positions/${pos.symbol}/place-exits?stop=${vals.s}&target=${vals.t}`)
      setExitMsg({ type: 'ok', text: `OCO placed — stop $${vals.s.toFixed(2)}, target $${vals.t.toFixed(2)}.` })
      setEditExits(false); qc.invalidateQueries('positions')
    } catch (err) {
      setExitMsg({ type: 'error', text: err?.response?.data?.detail || 'Failed to place OCO.' })
    } finally { setPlacing(false) }
  }

  async function handleClose() {
    if (!confirm(`Close ${pos.symbol}?`)) return
    setClosing(true)
    try { await closePosition(pos.symbol) } finally { setClosing(false); qc.invalidateQueries('positions') }
  }

  const stopVal   = parseFloat(stop)
  const targetVal = parseFloat(target)
  const rr = stop && target && pos.entry_price && stopVal < pos.entry_price
    ? ((targetVal - pos.entry_price) / (pos.entry_price - stopVal)).toFixed(1)
    : null

  const glowClass = urgent
    ? 'shadow-[0_0_0_1px_rgba(239,68,68,0.25),0_8px_32px_rgba(239,68,68,0.08)]'
    : breakout
      ? 'shadow-[0_0_0_1px_rgba(16,185,129,0.25),0_8px_32px_rgba(16,185,129,0.08)]'
      : ''

  return (
    <div className={`card p-4 flex flex-col gap-3 glass-hover ${glowClass}`}>

      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="text-xl font-bold text-slate-100 tracking-tight num">{pos.symbol}</div>
          <div className="text-xs text-slate-500 mt-0.5 num">{pos.qty} sh @ {usd(pos.entry_price)}</div>
        </div>
        <SignalBadge signal={pos.signal} />
      </div>

      {/* P&L row */}
      <div className="grid grid-cols-3 gap-2">
        <div className="stat-card">
          <div className="label mb-1">Current Price</div>
          <div className="text-sm font-bold text-slate-100 num">
            {pos.current_price != null ? usd(pos.current_price) : '—'}
          </div>
        </div>
        <div className="stat-card">
          <div className="label mb-1">Market Value</div>
          <div className="text-sm font-bold text-slate-100 num">{usd(pos.market_value)}</div>
        </div>
        <div className={`stat-card ${isProfit ? 'border-emerald-500/15' : 'border-red-500/15'}`}>
          <div className="label mb-1">Unrealized P&L</div>
          <div className={`text-sm font-bold num ${plColor}`}>
            {usd(pos.unrealized_pl, true)}
          </div>
          <div className={`text-xs num ${plColor} opacity-70`}>{pct(pos.unrealized_plpc)}</div>
        </div>
      </div>

      {/* Plan levels */}
      {hasPlanLevels && (
        <div>
          <div className="grid grid-cols-3 gap-2">
            {pos.stop_price && (
              <div className="stat-card border-red-500/10">
                <div className="label mb-1">Stop</div>
                <div className="text-xs font-semibold text-red-400 num">${pos.stop_price.toFixed(2)}</div>
              </div>
            )}
            {pos.target1 && (
              <div className="stat-card border-emerald-500/10">
                <div className="label mb-1">Target 1</div>
                <div className="text-xs font-semibold text-emerald-400 num">${pos.target1.toFixed(2)}</div>
              </div>
            )}
            {pos.target2 && (
              <div className="stat-card border-emerald-500/10">
                <div className="label mb-1">Target 2</div>
                <div className="text-xs font-semibold text-emerald-300 num">${pos.target2.toFixed(2)}</div>
              </div>
            )}
          </div>
          {isHistoricalPlan && planWeekLabel && (
            <p className="text-[10px] text-amber-500/70 mt-1.5">
              ⚠ Levels from {planWeekLabel} — verify before placing orders
            </p>
          )}
        </div>
      )}

      {/* EMA levels */}
      <div className="grid grid-cols-3 gap-2">
        <EmaRow label="EMA 20" value={pos.ema20}       current={pos.current_price} />
        <EmaRow label="EMA 50" value={pos.ema50}       current={pos.current_price} />
        <EmaRow label="52W Hi" value={pos.week52_high} current={pos.current_price} noColor />
      </div>

      {/* Score bar */}
      <div>
        <div className="flex justify-between label mb-2">
          <span>Stage 2 Score</span>
          <span className="text-slate-400 font-semibold">{pos.score}<span className="text-slate-600">/8</span></span>
        </div>
        <div className="h-1.5 bg-white/5 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-700 ${
              pos.score >= 7 ? 'bg-gradient-to-r from-emerald-500 to-emerald-400' :
              pos.score >= 5 ? 'bg-gradient-to-r from-amber-500 to-yellow-400' :
                               'bg-gradient-to-r from-red-600 to-red-500'
            }`}
            style={{ width: `${(pos.score / 8) * 100}%` }}
          />
        </div>
      </div>

      {/* Edit exits */}
      <div className="border-t border-white/5 pt-2 space-y-2">
        <button
          onClick={openExitForm}
          className="flex items-center gap-1.5 text-xs text-amber-400/80 hover:text-amber-300 transition-colors"
        >
          <span className={`inline-block transition-transform duration-200 ${editExits ? 'rotate-90' : ''}`}>▶</span>
          {editExits ? 'Cancel' : hasPlanLevels ? 'Edit Stop / Target' : 'Set Stop / Target'}
        </button>

        {editExits && (
          <div className="space-y-3 animate-slide-up">
            {/* Mode toggle */}
            <div className="flex gap-1 bg-white/[0.03] rounded-lg p-1 w-fit border border-white/5">
              {['auto', 'manual'].map(m => (
                <button
                  key={m}
                  onClick={() => setExecMode(m)}
                  className={`px-3 py-1 text-xs rounded-md transition-all font-medium ${
                    execMode === m
                      ? m === 'auto'
                        ? 'bg-white/10 text-slate-200'
                        : 'bg-indigo-500/20 text-indigo-300 border border-indigo-500/30'
                      : 'text-slate-500 hover:text-slate-400'
                  }`}
                >
                  {m === 'auto' ? 'Auto (next cycle)' : 'Place Now'}
                </button>
              ))}
            </div>

            <p className="text-[10px] text-slate-600 leading-relaxed">
              {execMode === 'auto'
                ? 'Saves levels — exit guard places OCO on the next monitor cycle.'
                : 'Immediately cancels orphaned orders and places a live OCO.'}
            </p>

            <div className="flex gap-2 items-end flex-wrap">
              {[
                { label: 'Stop Price', val: stop, set: setStop, focus: 'focus:border-red-400/50' },
                { label: 'Target (T1)', val: target, set: setTarget, focus: 'focus:border-emerald-400/50' },
              ].map(({ label, val, set, focus }) => (
                <div key={label} className="flex flex-col gap-1">
                  <label className="label">{label}</label>
                  <input
                    type="number" step="0.01" placeholder="0.00"
                    value={val} onChange={e => set(e.target.value)}
                    className={`w-28 px-2.5 py-1.5 text-xs rounded-lg bg-white/[0.04] text-slate-200 border border-white/10 outline-none transition-colors num ${focus}`}
                  />
                </div>
              ))}

              {rr !== null && (
                <div className="flex flex-col gap-1 pb-0.5">
                  <label className="label">R:R</label>
                  <span className={`text-sm font-bold num pb-1 ${parseFloat(rr) >= 2 ? 'text-emerald-400' : 'text-amber-400'}`}>
                    {rr}×
                  </span>
                </div>
              )}

              {execMode === 'auto' ? (
                <button onClick={handleSaveOnly} disabled={saving} className="btn-ghost text-xs px-3 py-1.5">
                  {saving ? 'Saving…' : 'Save'}
                </button>
              ) : (
                <button onClick={handlePlaceNow} disabled={placing} className="btn-primary text-xs px-3 py-1.5">
                  {placing ? 'Placing…' : 'Place OCO'}
                </button>
              )}
            </div>

            {pos.target2 && (
              <p className="text-[10px] text-slate-600">
                T2 from plan: <span className="text-emerald-400 num">${pos.target2.toFixed(2)}</span> — use for scaled exit
              </p>
            )}
          </div>
        )}

        {exitMsg && (
          <p className={`text-xs ${exitMsg.type === 'error' ? 'text-red-400' : 'text-emerald-400'}`}>
            {exitMsg.text}
          </p>
        )}
      </div>

      {urgent && (
        <button
          onClick={handleClose}
          disabled={closing}
          className="btn-danger w-full"
        >
          {closing ? 'Closing…' : 'Close Position'}
        </button>
      )}
    </div>
  )
}

function EmaRow({ label, value, current, noColor }) {
  if (!value) return (
    <div className="stat-card">
      <div className="label mb-1">{label}</div>
      <div className="text-slate-600 text-xs">—</div>
    </div>
  )
  const above = current > value
  const color = noColor ? 'text-slate-300' : above ? 'text-emerald-400' : 'text-red-400'
  return (
    <div className="stat-card">
      <div className="label mb-1">{label}</div>
      <div className={`text-xs font-semibold num ${color}`}>${value.toFixed(2)}</div>
    </div>
  )
}
