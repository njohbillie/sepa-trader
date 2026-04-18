import { useQuery } from 'react-query'
import { fetchOpenOrders, fetchTradeHistory } from '../api/client'

export function OpenOrdersTable() {
  const { data = [], isLoading } = useQuery('openOrders', fetchOpenOrders)
  return (
    <Section title={`Open Orders (${data.length})`} loading={isLoading}>
      {data.length === 0
        ? <p className="text-slate-500 text-sm">No open orders.</p>
        : <Table rows={data} cols={['symbol','side','qty','status','type']} />
      }
    </Section>
  )
}

export function TradeHistoryTable() {
  const { data = [], isLoading } = useQuery('tradeHistory', fetchTradeHistory)
  return (
    <Section title="Trade History" loading={isLoading}>
      {data.length === 0
        ? <p className="text-slate-500 text-sm">No trades recorded.</p>
        : <Table
            rows={data.map(r => ({
              ...r,
              price: `$${Number(r.price).toFixed(2)}`,
              timestamp: new Date(r.timestamp).toLocaleString(),
            }))}
            cols={['symbol','action','qty','price','trigger','mode','timestamp']}
          />
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

function Table({ rows, cols }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border">
            {cols.map(c => (
              <th key={c} className="text-left py-2 px-3 text-xs text-slate-400 uppercase tracking-wider font-medium">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-border/50 hover:bg-surface/50 transition-colors">
              {cols.map(c => (
                <td key={c} className={`py-2 px-3 ${
                  c === 'action' && r[c] === 'BUY'  ? 'text-emerald-400' :
                  c === 'action' && r[c] === 'SELL' ? 'text-red-400' :
                  c === 'mode'   && r[c] === 'live' ? 'text-orange-400' : 'text-slate-300'
                }`}>
                  {r[c] ?? '—'}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
