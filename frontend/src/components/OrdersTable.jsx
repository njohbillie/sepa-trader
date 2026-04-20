import { useQuery } from 'react-query'
import { fetchOpenOrders, fetchAlpacaHistory } from '../api/client'

const STATUS_CSS = {
  filled:           'text-emerald-400',
  partially_filled: 'text-yellow-400',
  canceled:         'text-slate-500',
  expired:          'text-slate-500',
  pending_new:      'text-blue-400',
  new:              'text-blue-400',
}

export function OpenOrdersTable() {
  const { data = [], isLoading } = useQuery(
    'openOrders',
    () => fetchOpenOrders(),          // ← arrow wrapper
    { refetchInterval: 15000 }
  )
  return (
    <Section title={`Open Orders (${data.length})`} loading={isLoading}>
      {data.length === 0
        ? <p className="text-slate-500 text-sm">No open orders.</p>
        : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  {['Symbol','Side','Qty','Status','Type','Submitted'].map(h => (
                    <th key={h} className="text-left py-2 px-3 text-xs text-slate-400 uppercase tracking-wider font-medium">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map(o => (
                  <tr key={o.id} className="border-b border-border/50 hover:bg-surface/50 transition-colors">
                    <td className="py-2 px-3 text-slate-200 font-medium">{o.symbol}</td>
                    <td className={`py-2 px-3 font-medium ${o.side?.includes('buy') ? 'text-emerald-400' : 'text-red-400'}`}>
                      {o.side?.replace('OrderSide.','').toUpperCase()}
                    </td>
                    <td className="py-2 px-3 text-slate-300">{o.qty}</td>
                    <td className="py-2 px-3 text-slate-300">{o.status?.replace('OrderStatus.','')}</td>
                    <td className="py-2 px-3 text-slate-400">{o.type?.replace('OrderType.','')}</td>
                    <td className="py-2 px-3 text-slate-500 text-xs">
                      {o.submitted_at ? new Date(o.submitted_at).toLocaleString() : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      }
    </Section>
  )
}

export function AlpacaHistoryTable() {
  const { data = [], isLoading } = useQuery(
    'alpacaHistory',
    () => fetchAlpacaHistory(),       // ← arrow wrapper, uses default limit=100
    { staleTime: 60000 }
  )
  return (
    <Section title={`Alpaca Order History (${data.length})`} loading={isLoading}>
      {data.length === 0
        ? <p className="text-slate-500 text-sm">No orders found.</p>
        : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  {['Symbol','Side','Qty','Filled','Avg Price','Status','Submitted','Filled At'].map(h => (
                    <th key={h} className="text-left py-2 px-3 text-xs text-slate-400 uppercase tracking-wider font-medium">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map(o => {
                  const side   = o.side?.replace('OrderSide.','').toLowerCase()
                  const status = o.status?.replace('OrderStatus.','').toLowerCase()
                  return (
                    <tr key={o.id} className="border-b border-border/50 hover:bg-surface/50 transition-colors">
                      <td className="py-2 px-3 text-slate-200 font-medium">{o.symbol}</td>
                      <td className={`py-2 px-3 font-medium ${side === 'buy' ? 'text-emerald-400' : 'text-red-400'}`}>
                        {side?.toUpperCase()}
                      </td>
                      <td className="py-2 px-3 text-slate-300">{o.qty}</td>
                      <td className="py-2 px-3 text-slate-300">{o.filled_qty || '—'}</td>
                      <td className="py-2 px-3 text-slate-300">
                        {o.filled_avg ? `$${Number(o.filled_avg).toFixed(2)}` : '—'}
                      </td>
                      <td className={`py-2 px-3 text-xs font-medium ${STATUS_CSS[status] || 'text-slate-400'}`}>
                        {status}
                      </td>
                      <td className="py-2 px-3 text-slate-500 text-xs">
                        {o.submitted_at ? new Date(o.submitted_at).toLocaleString() : '—'}
                      </td>
                      <td className="py-2 px-3 text-slate-500 text-xs">
                        {o.filled_at ? new Date(o.filled_at).toLocaleString() : '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )
      }
    </Section>
  )
}

function Section({ title, loading, children }) {
  return (
    <div className="bg-card border border-border rounded-xl p-5">
      <h3 className="text-base font-semibold text-slate-100 mb-4">{title}</h3>
      {loading ? <div className="animate-pulse h-16 bg-surface rounded" /> : children}
    </div>
  )
}
