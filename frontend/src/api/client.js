import axios from 'axios'

const api = axios.create({ baseURL: '/api' })

export const fetchAccount      = () => api.get('/account').then(r => r.data)
export const fetchPositions    = () => api.get('/positions').then(r => r.data)
export const fetchOpenOrders   = () => api.get('/orders/open').then(r => r.data)
export const fetchTradeHistory = (limit=50) => api.get(`/orders/history?limit=${limit}`).then(r => r.data)
export const fetchSettings     = () => api.get('/settings').then(r => r.data)
export const updateSetting     = (key, value) => api.patch(`/settings/${key}`, { value }).then(r => r.data)
export const runMonitor        = () => api.post('/signals/run-monitor').then(r => r.data)
export const closePosition     = (sym) => api.delete(`/positions/${sym}`).then(r => r.data)
export const analyzeSymbol     = (sym) => api.get(`/signals/analyze/${sym}`).then(r => r.data)
export const fetchWeeklyPlan    = () => api.get('/screener/weekly-plan').then(r => r.data)
export const fetchScreenerStatus= () => api.get('/screener/status').then(r => r.data)
export const runScreener        = () => api.post('/screener/run').then(r => r.data)
export const syncTradingView    = () => api.post('/screener/sync-tradingview').then(r => r.data)
export const updatePlanStatus   = (symbol, status) => api.patch(`/screener/weekly-plan/${symbol}/status`, { status }).then(r => r.data)
