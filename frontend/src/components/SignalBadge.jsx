const SIGNAL_STYLES = {
  BREAKOUT:        'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40',
  PULLBACK_EMA20:  'bg-blue-500/20   text-blue-400   border border-blue-500/40',
  PULLBACK_EMA50:  'bg-indigo-500/20 text-indigo-400 border border-indigo-500/40',
  STAGE2_WATCH:    'bg-yellow-500/20 text-yellow-400 border border-yellow-500/40',
  NO_SETUP:        'bg-red-500/20    text-red-400    border border-red-500/40',
  INSUFFICIENT_DATA:'bg-gray-500/20  text-gray-400   border border-gray-500/40',
}

export default function SignalBadge({ signal }) {
  const style = SIGNAL_STYLES[signal] || 'bg-gray-700 text-gray-300'
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${style}`}>
      {signal?.replace(/_/g, ' ') || 'N/A'}
    </span>
  )
}
