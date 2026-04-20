import { useState } from 'react'
import { useQuery, useQueryClient } from 'react-query'
import Navbar from './components/Navbar'
import AccountSummary from './components/AccountSummary'
import PositionCard from './components/PositionCard'
import { OpenOrdersTable, AlpacaHistoryTable } from './components/OrdersTable'
import SettingsPanel from './components/SettingsPanel'
import WeeklyPlan from './components/WeeklyPlan'
import { fetchPositions, updateSetting } from './api/client'

const TABS = ['Positions', 'Orders', 'History', 'Weekly Plan', 'Settings']

// Refresh intervals — aggressive during market hours, relaxed otherwise
const POSITIONS_INTERVAL = 5000   // 5s — near real-time position P&L
const ACCOUNT_INTERVAL   = 5000   // 5s — buying power / cash stays current

export default function App() {
  const [tab, setTab]             = useState('Positions')
  const [switching, setSwitching] = useState(false)
  const qc                        = useQueryClient()

  const { data: positions = [], isLoading, isError: posError } = useQuery(
    'positions',
    () => fetchPositions(),
    {
      refetchInterval:            POSITIONS_INTERVAL,
      refetchIntervalInBackground: true,   // keep refreshing even if tab is backgrounded
      staleTime:                  2000,
    }
  )

  async function handleModeChange(newMode) {
    if (switching) return
    if (newMode === 'live') {
      const confirmed = window.confirm(
        '⚠️ Switch to LIVE trading?\n\n' +
        'Real money will be used. Ensure your live Alpaca credentials are set in .env ' +
        'and that you have reviewed your positions and exit orders.\n\nPress OK to confirm.'
      )
      if (!confirmed) return
    }
    setSwitching(true)
    try {
      await updateSetting('trading_mode', newMode)
      await qc.invalidateQueries()
    } catch (err) {
      alert(`Failed to switch mode: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setSwitching(false)
    }
  }

  const urgent    = positions.filter(p => p.signal === 'NO_SETUP')
  const breakouts = positions.filter(p => p.signal === 'BREAKOUT')

  return (
    <div className="min-h-screen bg-surface">
      <Navbar onModeChange={handleModeChange} />

      {/* Mode-switch overlay */}
      {switching && (
        <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center">
          <div className="bg-card border border-border rounded-xl px-8 py-6 text-center space-y-2">
            <div className="w-6 h-6 border-2 border-accent border-t-transparent rounded-full animate-spin mx-auto" />
            <p className="text-slate-200 text-sm font-medium">Switching trading mode…</p>
            <p className="text-slate-500 text-xs">Refreshing all data for the new account</p>
          </div>
        </div>
      )}

      <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">

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

        <AccountSummary
          onModeChange={handleModeChange}
          refetchInterval={ACCOUNT_INTERVAL}
        />

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

        {tab === 'Positions' && (
          <div>
            {isLoading ? (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                {[...Array(3)].map((_, i) => (
                  <div key={i} className="bg-card border border-border rounded-xl h-52 animate-pulse" />
                ))}
              </div>
            ) : posError ? (
              <div className="bg-card border border-red-500/30 rounded-xl p-12 text-center text-red-400 text-sm">
                Failed to load positions — check backend logs.
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

        {tab === 'Orders'      && <div className="space-y-6"><OpenOrdersTable /><AlpacaHistoryTable /></div>}
        {tab === 'History'     && <AlpacaHistoryTable />}
        {tab === 'Weekly Plan' && <WeeklyPlan />}
        {tab === 'Settings'    && <SettingsPanel />}
      </main>
    </div>
  )
}