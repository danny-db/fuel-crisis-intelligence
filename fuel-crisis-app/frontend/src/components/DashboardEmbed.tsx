import { useEffect, useState } from 'react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, ComposedChart,
} from 'recharts'
import type { Metrics } from '../types'

interface ForecastPoint {
  year: number
  actual_ulp?: number
  actual_diesel?: number
  forecast_ulp?: number
  forecast_diesel?: number
  upper_ulp?: number
  lower_ulp?: number
  upper_diesel?: number
  lower_diesel?: number
}

const DASHBOARD_URL =
  'https://fe-vm-vdm-serverless-dw-demo.cloud.databricks.com/sql/dashboardsv3/01f1335192f81b3289c716714dab810d'





export default function DashboardEmbed() {
  const [metrics, setMetrics] = useState<Metrics | null>(null)
  const [forecastData, setForecastData] = useState<ForecastPoint[]>([])
  const [brentData, setBrentData] = useState<{year: number; actual?: number; forecast?: number; upper?: number; lower?: number}[]>([])
  const [forecastLoading, setForecastLoading] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function fetchData() {
      setLoading(true)
      setError(null)
      try {
        const metricsResp = await fetch('/api/metrics')
        if (metricsResp.ok) setMetrics(await metricsResp.json())
        else setError('Failed to load metrics.')
      } catch {
        setError('Network error')
      } finally {
        setLoading(false)
      }
    }
    fetchData()

    // Fetch forecast in background (takes longer due to ai_forecast)
    async function fetchForecast() {
      setForecastLoading(true)
      try {
        const resp = await fetch('/api/forecast/fuel-prices')
        if (resp.ok) {
          const data = await resp.json()
          const pointMap: Record<string, ForecastPoint> = {}

          // Historical quarterly data
          for (const row of data.historical || []) {
            const key = row.ds?.substring(0, 10) || row.period
            if (!pointMap[key]) pointMap[key] = { year: new Date(row.ds).getTime() }
            pointMap[key].actual_ulp = row.ulp_price
            pointMap[key].actual_diesel = row.diesel_price
          }

          // Forecast data
          for (const row of data.forecast || []) {
            const key = row.ds?.substring(0, 10)
            if (!pointMap[key]) pointMap[key] = { year: new Date(row.ds).getTime() }
            pointMap[key].forecast_ulp = row.ulp_forecast
            pointMap[key].forecast_diesel = row.diesel_forecast
            pointMap[key].upper_ulp = row.ulp_upper
            pointMap[key].lower_ulp = row.ulp_lower
            pointMap[key].upper_diesel = row.diesel_upper
            pointMap[key].lower_diesel = row.diesel_lower
          }

          // Bridge: last actual gets forecast value for line continuity
          const sorted = Object.values(pointMap).sort((a, b) => a.year - b.year)
          const lastActual = sorted.filter(p => p.actual_ulp != null).pop()
          if (lastActual) {
            lastActual.forecast_ulp = lastActual.actual_ulp
            lastActual.forecast_diesel = lastActual.actual_diesel
          }
          // Show last ~8 years of history + forecast
          const cutoff = new Date('2018-01-01').getTime()
          setForecastData(sorted.filter(p => p.year >= cutoff))

          // Build Brent crude chart data
          const brentMap: Record<string, {year: number; actual?: number; forecast?: number; upper?: number; lower?: number}> = {}
          for (const row of data.brent_historical || []) {
            const ds = row.ds?.substring(0, 10)
            if (ds) {
              brentMap[ds] = { year: new Date(ds).getTime(), actual: row.brent_price }
            }
          }
          for (const row of data.brent_forecast || []) {
            const ds = row.ds?.substring(0, 10)
            if (ds) {
              if (!brentMap[ds]) brentMap[ds] = { year: new Date(ds).getTime() }
              brentMap[ds].forecast = row.brent_forecast
              brentMap[ds].upper = row.brent_upper
              brentMap[ds].lower = row.brent_lower
            }
          }
          const brentSorted = Object.values(brentMap).sort((a, b) => a.year - b.year)
          // Bridge last actual to forecast
          const lastBrentActual = brentSorted.filter(p => p.actual != null).pop()
          if (lastBrentActual) lastBrentActual.forecast = lastBrentActual.actual
          // Show last 5 years + forecast
          const brentCutoff = new Date('2021-01-01').getTime()
          setBrentData(brentSorted.filter(p => p.year >= brentCutoff))
        }
      } catch {}
      setForecastLoading(false)
    }
    fetchForecast()
  }, [])

  /* ------------------------------------------------------------------ */
  /*  KPI cards                                                          */
  /* ------------------------------------------------------------------ */
  const kpis: {
    label: string
    value: string
    sub: string
    color: string
    border: string
  }[] = [
    {
      label: 'Brent Crude',
      value: metrics?.brent_price != null ? `$${metrics.brent_price.toFixed(2)}` : '---',
      sub: 'USD / barrel',
      color: 'text-red-400',
      border: 'border-red-500/40',
    },
    {
      label: 'AUD / USD',
      value: metrics?.aud_usd != null ? `$${metrics.aud_usd.toFixed(4)}` : '---',
      sub: 'Exchange rate',
      color: 'text-amber-400',
      border: 'border-amber-500/40',
    },
    {
      label: 'IEA Reserves',
      value: metrics?.iea_days != null ? `${metrics.iea_days}` : '---',
      sub: 'Records',
      color: 'text-orange-400',
      border: 'border-orange-500/40',
    },
    {
      label: 'Refineries',
      value: metrics?.operational_refineries != null ? `${metrics.operational_refineries}` : '---',
      sub: 'Operational globally',
      color: 'text-emerald-400',
      border: 'border-emerald-500/40',
    },
    {
      label: 'Iran % of OPEC',
      value: metrics?.iran_opec_pct != null ? `${metrics.iran_opec_pct}%` : '---',
      sub: 'Supply share',
      color: 'text-red-500',
      border: 'border-red-600/40',
    },
  ]

  /* ------------------------------------------------------------------ */
  /*  Loading state                                                      */
  /* ------------------------------------------------------------------ */
  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-gray-400">
        <div className="w-8 h-8 border-2 border-red-500/30 border-t-red-500 rounded-full animate-spin mb-4" />
        <div className="text-sm">Loading crisis dashboard...</div>
      </div>
    )
  }

  /* ------------------------------------------------------------------ */
  /*  Render                                                             */
  /* ------------------------------------------------------------------ */
  return (
    <div className="flex flex-col h-full overflow-y-auto p-6 gap-6">
      {/* ---- Header row ---- */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-gray-200">Crisis Intelligence Dashboard</h2>
          <p className="text-sm text-gray-500 mt-1">
            Real-time metrics and AI-powered forecasts
          </p>
        </div>
        <a
          href={DASHBOARD_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="bg-red-600 hover:bg-red-700 text-white px-5 py-2.5 rounded-lg text-sm font-medium transition-colors flex items-center gap-2 shrink-0"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="w-4 h-4"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
            />
          </svg>
          Open Full AI/BI Dashboard
        </a>
      </div>

      {/* ---- Error banner ---- */}
      {error && (
        <div className="bg-red-950/50 border border-red-800 rounded-lg px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {/* ---- KPI Cards ---- */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
        {kpis.map((kpi) => (
          <div
            key={kpi.label}
            className={`bg-gray-900/80 border ${kpi.border} rounded-xl p-5 flex flex-col gap-1`}
          >
            <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">
              {kpi.label}
            </span>
            <span className={`text-2xl font-bold tabular-nums ${kpi.color}`}>{kpi.value}</span>
            <span className="text-xs text-gray-600">{kpi.sub}</span>
          </div>
        ))}
      </div>

      {/* ---- Brent Crude Oil Forecast Chart ---- */}
      {brentData.length > 0 && (
        <div className="flex flex-col gap-3">
          <div>
            <h3 className="text-lg font-semibold text-gray-300">
              Brent Crude Oil — Monthly Forecast
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Real FRED data (solid) + <span className="text-blue-400">AI_FORECAST()</span> projection (dashed) with 95% confidence interval
            </p>
          </div>
          <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-4">
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart data={brentData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="year" stroke="#6b7280" fontSize={11} type="number" domain={['dataMin', 'dataMax']}
                  tickFormatter={(v) => new Date(v).toLocaleDateString('en-AU', { month: 'short', year: 'numeric' })} />
                <YAxis stroke="#6b7280" fontSize={12} label={{ value: 'USD/bbl', angle: -90, position: 'insideLeft', style: { fill: '#6b7280' } }} />
                <Tooltip
                  contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: '8px', fontSize: '12px', color: '#e5e7eb' }}
                  labelFormatter={(v) => new Date(v).toLocaleDateString('en-AU', { month: 'long', year: 'numeric' })}
                  formatter={(value, name) => [`$${Number(value).toFixed(2)}`, String(name)]}
                  itemStyle={{ color: '#d1d5db' }}
                />
                <Legend wrapperStyle={{ fontSize: '12px', color: '#9ca3af' }} />
                <Area type="monotone" dataKey="upper" stroke="none" fill="#ef4444" fillOpacity={0.1} legendType="none" />
                <Area type="monotone" dataKey="lower" stroke="none" fill="#0a0a0f" fillOpacity={1} legendType="none" />
                <Line type="monotone" dataKey="actual" stroke="#ef4444" strokeWidth={2} dot={false} name="Brent (Actual)" />
                <Line type="monotone" dataKey="forecast" stroke="#ef4444" strokeWidth={2} strokeDasharray="8 4" dot={{ r: 3 }} name="Brent (Forecast)" connectNulls={false} />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="text-center text-xs text-gray-600 mt-2">
              Source: Federal Reserve Economic Data (FRED) — Forecast: Databricks <code className="text-blue-400">AI_FORECAST()</code>
            </div>
          </div>
        </div>
      )}

      {/* Scenario table removed — using AI_FORECAST on real data */}


      {/* ---- AI Fuel Price Forecast Chart ---- */}
      <div className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-300">
              Adelaide Fuel Price Forecast
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Historical prices (solid) + <span className="text-blue-400">ai_forecast()</span> projections (dashed) with 95% confidence interval
            </p>
          </div>
          {forecastLoading && (
            <span className="flex items-center gap-2 text-xs text-gray-500">
              <span className="w-3 h-3 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
              Running ai_forecast()...
            </span>
          )}
        </div>

        {forecastData.length > 0 ? (
          <div className="bg-gray-900/60 border border-gray-800 rounded-lg p-4">
            <ResponsiveContainer width="100%" height={350}>
              <ComposedChart data={forecastData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="year" stroke="#6b7280" fontSize={11} type="number" domain={['dataMin', 'dataMax']}
                  tickFormatter={(v) => { const d = new Date(v); return `${d.getFullYear()} Q${Math.ceil((d.getMonth()+1)/3)}`; }} />
                <YAxis stroke="#6b7280" fontSize={12} label={{ value: 'c/L', angle: -90, position: 'insideLeft', style: { fill: '#6b7280' } }} />
                <Tooltip
                  contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: '8px', fontSize: '12px', color: '#e5e7eb' }}
                  labelStyle={{ color: '#e5e7eb' }}
                  itemStyle={{ color: '#d1d5db' }}
                  labelFormatter={(v) => { const d = new Date(v); return `${d.getFullYear()} Q${Math.ceil((d.getMonth()+1)/3)} (${d.toLocaleDateString('en-AU', { month: 'short', year: 'numeric' })})`; }}
                  formatter={(value, name) => [`${Number(value).toFixed(1)} c/L`, String(name)]}
                />
                <Legend wrapperStyle={{ fontSize: '12px', color: '#9ca3af' }} />

                {/* Confidence intervals as areas */}
                <Area type="monotone" dataKey="upper_ulp" stroke="none" fill="#f59e0b" fillOpacity={0.1} name="ULP 95% CI" legendType="none" />
                <Area type="monotone" dataKey="lower_ulp" stroke="none" fill="#0a0a0f" fillOpacity={1} legendType="none" />
                <Area type="monotone" dataKey="upper_diesel" stroke="none" fill="#3b82f6" fillOpacity={0.1} name="Diesel 95% CI" legendType="none" />
                <Area type="monotone" dataKey="lower_diesel" stroke="none" fill="#0a0a0f" fillOpacity={1} legendType="none" />

                {/* Historical actuals — solid lines */}
                <Line type="monotone" dataKey="actual_ulp" stroke="#f59e0b" strokeWidth={2} dot={{ r: 2 }} name="ULP (Actual)" connectNulls={false} />
                <Line type="monotone" dataKey="actual_diesel" stroke="#3b82f6" strokeWidth={2} dot={{ r: 2 }} name="Diesel (Actual)" connectNulls={false} />

                {/* Forecast — dashed lines */}
                <Line type="monotone" dataKey="forecast_ulp" stroke="#f59e0b" strokeWidth={2} strokeDasharray="8 4" dot={{ r: 3, strokeWidth: 2 }} name="ULP (Forecast)" connectNulls={false} />
                <Line type="monotone" dataKey="forecast_diesel" stroke="#3b82f6" strokeWidth={2} strokeDasharray="8 4" dot={{ r: 3, strokeWidth: 2 }} name="Diesel (Forecast)" connectNulls={false} />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="text-center text-xs text-gray-600 mt-2">
              Powered by Databricks <code className="text-blue-400">AI_FORECAST()</code> — automated ML time series forecasting
            </div>
          </div>
        ) : forecastLoading ? null : (
          <div className="bg-gray-900/40 border border-gray-800 rounded-lg p-8 text-center text-gray-500 text-sm">
            Forecast data unavailable
          </div>
        )}
      </div>

      {/* ---- Footer ---- */}
      <div className="text-center text-xs text-gray-600 pb-2">
        Data sourced from Databricks Lakehouse &mdash; forecasts powered by AI_FORECAST()
      </div>
    </div>
  )
}
