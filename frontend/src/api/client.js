import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  withCredentials: true,   // send httpOnly cookies automatically
})

// ── Silent token refresh ──────────────────────────────────────────────────────
// On 401: try /auth/refresh once (uses the httpOnly refresh-token cookie).
// If refresh succeeds, replay the original request transparently.
// Only redirect to /login if refresh also fails (truly logged out).

let _refreshing      = false
let _refreshWaiters  = []   // pending requests queued while refresh is in flight

function _onRefreshDone(error) {
  _refreshWaiters.forEach(cb => cb(error))
  _refreshWaiters = []
}

api.interceptors.response.use(
  res => res,
  async err => {
    const original = err.config

    // Don't intercept: non-401, the refresh call itself, or already retried
    if (
      err.response?.status !== 401 ||
      original._retried ||
      original.url?.includes('/auth/refresh') ||
      original.url?.includes('/auth/login')
    ) {
      if (err.response?.status === 401 && !window.location.pathname.includes('/login')) {
        window.location.href = '/login'
      }
      return Promise.reject(err)
    }

    // If a refresh is already in flight, queue this request
    if (_refreshing) {
      return new Promise((resolve, reject) => {
        _refreshWaiters.push(refreshErr => {
          if (refreshErr) reject(err)
          else resolve(api({ ...original, _retried: true }))
        })
      })
    }

    original._retried = true
    _refreshing = true

    try {
      await api.post('/auth/refresh')
      _onRefreshDone(null)
      _refreshing = false
      return api({ ...original, _retried: true })
    } catch (refreshErr) {
      _onRefreshDone(refreshErr)
      _refreshing = false
      if (!window.location.pathname.includes('/login')) {
        window.location.href = '/login'
      }
      return Promise.reject(err)
    }
  }
)

// ── Auth ──────────────────────────────────────────────────────────────────────
export const login          = (email, password)          => api.post('/auth/login',    { email, password }).then(r => r.data)
export const loginWith2fa   = (temp_token, code)         => api.post('/auth/login/2fa', { temp_token, code }).then(r => r.data)
export const register       = (email, username, password) => api.post('/auth/register', { email, username, password }).then(r => r.data)
export const logout         = ()                         => api.post('/auth/logout').then(r => r.data)
export const fetchMe        = ()                         => api.get('/auth/me').then(r => r.data)
export const changePassword = (current_password, new_password) =>
  api.patch('/auth/password', { current_password, new_password }).then(r => r.data)

// ── 2FA ───────────────────────────────────────────────────────────────────────
export const setup2fa   = ()       => api.post('/auth/2fa/setup').then(r => r.data)
export const enable2fa  = (code)   => api.post('/auth/2fa/enable',  { code }).then(r => r.data)
export const disable2fa = (password) => api.post('/auth/2fa/disable', { password }).then(r => r.data)

// ── Admin ─────────────────────────────────────────────────────────────────────
export const fetchAdminUsers       = ()                             => api.get('/admin/users').then(r => r.data)
export const updateAdminUser       = (id, data)                     => api.patch(`/admin/users/${id}`, data).then(r => r.data)
export const deleteAdminUser       = (id)                           => api.delete(`/admin/users/${id}`).then(r => r.data)
export const resetAdminUserPassword = (id)                          => api.post(`/admin/users/${id}/reset-password`).then(r => r.data)
export const fetchAppHealth        = ()                             => api.get('/admin/health').then(r => r.data)

// ── Trading ───────────────────────────────────────────────────────────────────
export const fetchAccount         = () => api.get('/account').then(r => r.data)
export const fetchAccountsOverview = () => api.get('/account/overview').then(r => r.data)
export const fetchPositions    = () => api.get('/positions').then(r => r.data)
export const fetchOpenOrders   = () => api.get('/orders/open').then(r => r.data)
export const fetchTradeHistory = (limit=50) => api.get(`/orders/history?limit=${limit}`).then(r => r.data)
export const fetchSettings     = () => api.get('/settings').then(r => r.data)
export const updateSetting     = (key, value) => api.patch(`/settings/${key}`, { value }).then(r => r.data)
export const runMonitor        = () => api.post('/signals/run-monitor').then(r => r.data)
export const closePosition     = (sym) => api.delete(`/positions/${sym}`).then(r => r.data)
export const analyzeSymbol     = (sym) => api.get(`/signals/analyze/${sym}`).then(r => r.data)
export const fetchWeeklyPlan    = () => api.get('/screener/weekly-plan').then(r => r.data)
export const fetchWeeklyDD      = () => api.get('/screener/dd').then(r => r.data)
export const forceRefreshDD     = () => api.get('/screener/dd?refresh=true').then(r => r.data)
export const fetchScreenerStatus = () => api.get('/screener/status').then(r => r.data)
export const runScreener           = () => api.post('/screener/run').then(r => r.data)
export const runMinerviniScreener  = () => api.post('/screener/run-minervini').then(r => r.data)
export const runPullbackScreener   = () => api.post('/screener/run-pullback').then(r => r.data)
export const fetchPullbackSettings = () => api.get('/screener/pullback-settings').then(r => r.data)
export const syncTradingView    = () => api.post('/screener/sync-tradingview').then(r => r.data)
export const updatePlanStatus   = (symbol, status) => api.patch(`/screener/weekly-plan/${symbol}/status`, { status }).then(r => r.data)
export const fetchAlpacaHistory = (limit=100) => api.get(`/orders/alpaca-history?limit=${limit}`).then(r => r.data)
export const fetchAnalyses      = (limit=20) => api.get(`/screener/analysis?limit=${limit}`).then(r => r.data)
export const runAnalysis        = () => api.post('/screener/analysis/run').then(r => r.data)

// ── Strategies ────────────────────────────────────────────────────────────────
export const fetchMarketEnvironment = () => api.get('/strategies/market-environment').then(r => r.data)
export const fetchDMSignal          = () => api.get('/strategies/dual-momentum/signal').then(r => r.data)
export const evaluateDualMomentum   = () => api.post('/strategies/dual-momentum/evaluate').then(r => r.data)
export const executeDualMomentum    = () => api.post('/strategies/dual-momentum/execute').then(r => r.data)
export const fetchDMPosition        = () => api.get('/strategies/dual-momentum/position').then(r => r.data)
export const fetchDMHistory         = (limit = 24) => api.get(`/strategies/dual-momentum/history?limit=${limit}`).then(r => r.data)
export const fetchDMConfig          = () => api.get('/strategies/dual-momentum/config').then(r => r.data)
export const updateDMConfig         = (data) => api.patch('/strategies/dual-momentum/config', data).then(r => r.data)

// ── Market tape check ─────────────────────────────────────────────────────────
export const fetchTapeCheck         = () => api.get('/market/tape-check').then(r => r.data)
export const refreshTapeCheck       = () => api.delete('/market/tape-check').then(r => r.data)
