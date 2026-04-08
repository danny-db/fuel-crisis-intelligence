import { useEffect, useState } from 'react'
import type { Metrics } from '../types'

export default function MetricsTicker() {
  const [metrics, setMetrics] = useState<Metrics | null>(null)

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const ctrl = new AbortController()
        const timer = setTimeout(() => ctrl.abort(), 10000) // 10s timeout
        const resp = await fetch('/api/metrics', { signal: ctrl.signal })
        clearTimeout(timer)
        if (resp.ok) setMetrics(await resp.json())
      } catch {}
    }
    fetchMetrics()
    const interval = setInterval(fetchMetrics, 60000) // refresh every 60s
    return () => clearInterval(interval)
  }, [])

  const items = [
    {
      label: 'BRENT CRUDE',
      value: metrics?.brent_price ? `$${metrics.brent_price}` : '---',
      unit: 'USD/bbl',
      color: 'text-red-400',
    },
    {
      label: 'AUD/USD',
      value: metrics?.aud_usd ? `$${metrics.aud_usd}` : '---',
      unit: '',
      color: 'text-amber-400',
    },
    {
      label: 'IEA RESERVES',
      value: metrics?.iea_days ?? '---',
      unit: 'records',
      color: 'text-orange-400',
    },
    {
      label: 'REFINERIES',
      value: metrics?.operational_refineries ?? '---',
      unit: 'operational',
      color: 'text-emerald-400',
    },
    {
      label: 'IRAN % OPEC',
      value: metrics?.iran_opec_pct ? `${metrics.iran_opec_pct}%` : '---',
      unit: '',
      color: 'text-red-500',
    },
  ]

  return (
    <div className="bg-gray-900 border-b border-gray-800 px-4 py-2">
      <div className="flex items-center gap-8 overflow-x-auto">
        <div className="flex items-center gap-2 shrink-0">
          <span className="text-red-500 font-bold text-xs tracking-widest">FUEL CRISIS INTELLIGENCE</span>
          <span className="text-gray-600">|</span>
          <span className="text-gray-500 text-xs">COMMAND CENTRE</span>
        </div>
        {items.map((item) => (
          <div key={item.label} className="flex items-center gap-2 shrink-0">
            <span className="text-gray-500 text-xs font-medium">{item.label}</span>
            <span className={`text-lg font-bold tabular-nums ${item.color}`}>
              {item.value}
            </span>
            {item.unit && <span className="text-gray-600 text-xs">{item.unit}</span>}
          </div>
        ))}
      </div>
    </div>
  )
}
