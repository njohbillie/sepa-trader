import { useState } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import Navbar from './components/Navbar'
import AccountSummary from './components/AccountSummary'
import PositionCard from './components/PositionCard'
import { OpenOrdersTable, TradeHistoryTable } from './components/OrdersTable'
import SettingsPanel from './components/SettingsPanel'
import { fetchPositions, updateSetting } from './api/client'

const TABS = ['Positions', 'Orders', 'History', 'Settings']

export default function App() {
  const [tab, setTab]     = useState('Positions')
  const [lastRun]         = useState(new Date())
  const qc                = useQueryClient()

  const { data: positions = [], isLoading } = useQuery('positions', fetchPositions)

  async function handleModeChange() {
    const { data: settings } = await import('./api/client').then(m => ({ data: null }))
    const current = positions.length ? 'paper' : 'paper'
    const next    = current === 'paper' ? 'live' : 'paper'
    if (!confirm(`Switch to ${next.toUpperCase()} trading mode?`)) return
    await updateSetting('trading_mode', next)
    qc.invalidateQueries()
  }

  const urgent    = positions.filter(p => p.signal === 'NO_SETUP')
  const breakouts = positions.filter(p => p.signal === 'BREAKOUT')

  return (
    <div className="min-h-screen bg-surface">
      <Navbar lastRun={lastRun} />

      <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {/* Alert banners */}
        {urgent.length > 0 && (
          <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-3 flex items-center gap-3">
            <span className="text-red-400 font-bold text-sm">URGENT</span>
            <span className="text-red-300 text-sm">
              Stage 2 lost: {urgent.map(p => p.symbol).join(', ')} — positions should be closed
            </span>
          </div>
        )}
        {breakouts.length > 0 && (
          <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl px-5 py-3 flex items-center gap-3">
            <span className="text-emerald-400 font-bold text-sm">BREAKOUT</span>
            <span className="text-emerald-300 text-sm">
              {breakouts.map(p => p.symbol).join(', ')} breaking out on volume
            </span>
          </div>
        )}

        <AccountSummary onModeChange={handleModeChange} />

        {/* Tabs */}
        <div className="flex gap-1 bg-card border border-border rounded-xl p-1 w-fit">
          {TABS.map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                tab === t ? 'bg-accent text-white' : 'text-slate-400 hover:text-slate-200'
              }`}
            >
              {t}
              {t === 'Positions' && positions.length > 0 && (
                <span className="ml-1.5 bg-slate-700 text-slate-300 text-xs px-1.5 py-0.5 rounded-full">
                  {positions.length}
                </span>
              )}
            </button>
          ))}
        </div>

        {/* Tab content */}
        {tab === 'Positions' && (
          <div>
            {isLoading ? (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                {[...Array(3)].map((_, i) => (
                  <div key={i} className="bg-card border border-border rounded-xl h-52 animate-pulse" />
                ))}
              </div>
            ) : positions.length === 0 ? (
              <div className="bg-card border border-border rounded-xl p-12 text-center text-slate-500">
                No open positions.
              </div>
            ) : (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                {positions.map(p => <PositionCard key={p.symbol} pos={p} />)}
              </div>
            )}
          </div>
        )}

        {tab === 'Orders'   && <OpenOrdersTable />}
        {tab === 'History'  && <TradeHistoryTable />}
        {tab === 'Settings' && <SettingsPanel />}
      </main>
    </div>
  )
}
