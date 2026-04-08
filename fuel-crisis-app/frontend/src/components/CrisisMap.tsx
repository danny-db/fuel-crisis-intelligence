import { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import type { GeoJSONCollection } from '../types'

// Strait of Hormuz approximate route
const HORMUZ_ROUTE: [number, number][] = [
  [56.0, 26.6], [56.2, 26.5], [56.4, 26.3], [56.5, 26.1],
  [56.3, 25.8], [56.0, 25.5], [55.5, 25.3], [55.0, 25.1],
  [54.5, 25.0], [54.0, 25.2], [53.5, 25.8], [53.0, 26.3],
]

const LAYER_CONFIGS = [
  { id: 'refineries', label: 'Refineries', color: '#22c55e', icon: true },
  { id: 'terminals', label: 'Terminals', color: '#3b82f6', icon: false },
  { id: 'depots', label: 'Depots', color: '#a855f7', icon: false },
  { id: 'fuelwatch', label: 'WA Fuel Stations', color: '#f59e0b', icon: false },
  { id: 'minerals', label: 'Mineral Deposits', color: '#ef4444', icon: false },
]

export default function CrisisMap() {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const [layers, setLayers] = useState<Record<string, boolean>>({
    refineries: true,
    terminals: true,
    depots: false,
    fuelwatch: true,
    minerals: false,
    hormuz: true,
  })
  const [useTiles, setUseTiles] = useState(true)

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          'carto-dark': {
            type: 'raster',
            tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'],
            tileSize: 256,
            attribution: '&copy; CARTO, &copy; OpenStreetMap',
          },
        },
        layers: [
          {
            id: 'carto-dark-layer',
            type: 'raster',
            source: 'carto-dark',
            minzoom: 0,
            maxzoom: 20,
          },
        ],
      },
      center: [134.0, -28.0], // Australia centre
      zoom: 4,
    })

    map.addControl(new maplibregl.NavigationControl(), 'top-right')

    map.on('load', () => {
      // Strait of Hormuz route
      map.addSource('hormuz-route', {
        type: 'geojson',
        data: {
          type: 'Feature',
          geometry: { type: 'LineString', coordinates: HORMUZ_ROUTE },
          properties: { name: 'Strait of Hormuz' },
        },
      })
      map.addLayer({
        id: 'hormuz-line',
        type: 'line',
        source: 'hormuz-route',
        paint: {
          'line-color': '#ef4444',
          'line-width': 3,
          'line-dasharray': [4, 3],
          'line-opacity': 0.8,
        },
      })

      // Try to add MVT tile sources first, fall back to GeoJSON
      addDataLayers(map)
    })

    mapRef.current = map
    return () => { map.remove(); mapRef.current = null }
  }, [])

  async function addDataLayers(map: maplibregl.Map) {
    // Go straight to GeoJSON — Lakebase MVT tiles loaded separately if configured
    setUseTiles(false)
    await addGeoJSONLayers(map)
  }

  function addMVTLayers(map: maplibregl.Map) {
    const origin = window.location.origin

    // Refineries (MVT)
    map.addSource('refineries-src', {
      type: 'vector',
      tiles: [`${origin}/api/tiles/refineries/{z}/{x}/{y}.pbf`],
      minzoom: 0, maxzoom: 16,
    })
    map.addLayer({
      id: 'refineries-layer',
      type: 'circle',
      source: 'refineries-src',
      'source-layer': 'refineries',
      paint: {
        'circle-radius': 10,
        'circle-color': [
          'match', ['get', 'status'],
          'Operational', '#22c55e',
          '#ef4444',
        ],
        'circle-stroke-width': 2,
        'circle-stroke-color': '#fff',
      },
    })

    // Terminals (MVT)
    map.addSource('terminals-src', {
      type: 'vector',
      tiles: [`${origin}/api/tiles/terminals/{z}/{x}/{y}.pbf`],
      minzoom: 0, maxzoom: 16,
    })
    map.addLayer({
      id: 'terminals-layer',
      type: 'circle',
      source: 'terminals-src',
      'source-layer': 'terminals',
      paint: {
        'circle-radius': 6,
        'circle-color': '#3b82f6',
        'circle-stroke-width': 1,
        'circle-stroke-color': '#1e40af',
      },
    })

    // Depots (MVT)
    map.addSource('depots-src', {
      type: 'vector',
      tiles: [`${origin}/api/tiles/depots/{z}/{x}/{y}.pbf`],
      minzoom: 0, maxzoom: 16,
    })
    map.addLayer({
      id: 'depots-layer',
      type: 'circle',
      source: 'depots-src',
      'source-layer': 'depots',
      paint: {
        'circle-radius': 5,
        'circle-color': '#a855f7',
        'circle-stroke-width': 1,
        'circle-stroke-color': '#7c3aed',
      },
      layout: { visibility: 'none' },
    })

    // FuelWatch (MVT)
    map.addSource('fuelwatch-src', {
      type: 'vector',
      tiles: [`${origin}/api/tiles/fuelwatch/{z}/{x}/{y}.pbf`],
      minzoom: 4, maxzoom: 16,
    })
    map.addLayer({
      id: 'fuelwatch-layer',
      type: 'circle',
      source: 'fuelwatch-src',
      'source-layer': 'fuelwatch',
      paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 3, 12, 7],
        'circle-color': [
          'interpolate', ['linear'], ['get', 'price'],
          140, '#22c55e',  // cheap = green
          170, '#f59e0b',  // mid = amber
          200, '#ef4444',  // expensive = red
        ],
        'circle-opacity': 0.8,
      },
    })

    // Minerals (MVT)
    map.addSource('minerals-src', {
      type: 'vector',
      tiles: [`${origin}/api/tiles/minerals/{z}/{x}/{y}.pbf`],
      minzoom: 0, maxzoom: 16,
    })
    map.addLayer({
      id: 'minerals-layer',
      type: 'circle',
      source: 'minerals-src',
      'source-layer': 'minerals',
      paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 1.5, 8, 4],
        'circle-color': '#ef4444',
        'circle-opacity': 0.5,
      },
      layout: { visibility: 'none' },
    })

    // Click handlers for popups
    for (const layer of ['refineries-layer', 'terminals-layer', 'fuelwatch-layer', 'minerals-layer']) {
      map.on('click', layer, (e) => {
        if (!e.features?.length) return
        const props = e.features[0].properties
        const html = Object.entries(props)
          .map(([k, v]) => `<div><span class="font-semibold">${k}:</span> ${v}</div>`)
          .join('')
        new maplibregl.Popup({ offset: 10 })
          .setLngLat(e.lngLat)
          .setHTML(`<div class="text-xs space-y-0.5">${html}</div>`)
          .addTo(map)
      })
      map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer' })
      map.on('mouseleave', layer, () => { map.getCanvas().style.cursor = '' })
    }
  }

  async function addGeoJSONLayers(map: maplibregl.Map) {
    // Fallback: load from GeoJSON endpoints
    const sources: { id: string; url: string; color: string; radius: number }[] = [
      { id: 'refineries', url: '/api/infrastructure/refineries', color: '#22c55e', radius: 10 },
      { id: 'terminals', url: '/api/infrastructure/terminals', color: '#3b82f6', radius: 6 },
      { id: 'depots', url: '/api/infrastructure/depots', color: '#a855f7', radius: 5 },
      { id: 'fuelwatch', url: '/api/fuelwatch', color: '#f59e0b', radius: 4 },
      { id: 'minerals', url: '/api/minerals', color: '#ef4444', radius: 2 },
    ]

    for (const src of sources) {
      try {
        const ctrl = new AbortController()
        const timer = setTimeout(() => ctrl.abort(), 15000) // 15s timeout per layer
        const resp = await fetch(src.url, { signal: ctrl.signal })
        clearTimeout(timer)
        if (!resp.ok) continue
        const data: GeoJSONCollection = await resp.json()

        map.addSource(`${src.id}-src`, { type: 'geojson', data: data as any })
        map.addLayer({
          id: `${src.id}-layer`,
          type: 'circle',
          source: `${src.id}-src`,
          paint: {
            'circle-radius': src.radius,
            'circle-color': src.color,
            'circle-stroke-width': src.id === 'refineries' ? 2 : 1,
            'circle-stroke-color': '#fff',
            'circle-opacity': src.id === 'minerals' ? 0.5 : 0.8,
          },
          layout: {
            visibility: (src.id === 'depots' || src.id === 'minerals') ? 'none' : 'visible',
          },
        })

        // Click popup
        map.on('click', `${src.id}-layer`, (e: any) => {
          if (!e.features?.length) return
          const props = e.features[0].properties
          const html = Object.entries(props)
            .filter(([k]) => k !== 'undefined')
            .map(([k, v]) => `<div><span class="font-semibold">${k}:</span> ${v}</div>`)
            .join('')
          new maplibregl.Popup({ offset: 10 })
            .setLngLat(e.lngLat)
            .setHTML(`<div class="text-xs space-y-0.5">${html}</div>`)
            .addTo(map)
        })
      } catch {}
    }
  }

  // Toggle layer visibility
  function toggleLayer(id: string) {
    const map = mapRef.current
    if (!map) return
    const newState = !layers[id]
    setLayers((prev) => ({ ...prev, [id]: newState }))

    if (id === 'hormuz') {
      map.setLayoutProperty('hormuz-line', 'visibility', newState ? 'visible' : 'none')
    } else {
      const layerId = `${id}-layer`
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, 'visibility', newState ? 'visible' : 'none')
      }
    }
  }

  return (
    <div className="relative w-full h-full">
      <div ref={containerRef} className="w-full h-full" />

      {/* Layer Control Panel */}
      <div className="absolute top-4 left-4 bg-gray-900/90 backdrop-blur border border-gray-700 rounded-lg p-3 z-10">
        <div className="text-xs font-bold text-gray-400 mb-2 tracking-wider">LAYERS</div>
        {LAYER_CONFIGS.map((layer) => (
          <label
            key={layer.id}
            className="flex items-center gap-2 py-1 cursor-pointer hover:bg-gray-800/50 rounded px-1"
          >
            <input
              type="checkbox"
              checked={layers[layer.id]}
              onChange={() => toggleLayer(layer.id)}
              className="accent-red-500"
            />
            <span
              className="w-3 h-3 rounded-full shrink-0"
              style={{ backgroundColor: layer.color }}
            />
            <span className="text-xs text-gray-300">{layer.label}</span>
          </label>
        ))}
        <label className="flex items-center gap-2 py-1 cursor-pointer hover:bg-gray-800/50 rounded px-1">
          <input
            type="checkbox"
            checked={layers.hormuz}
            onChange={() => toggleLayer('hormuz')}
            className="accent-red-500"
          />
          <span className="w-3 h-3 rounded-full shrink-0 bg-red-500" />
          <span className="text-xs text-gray-300">Strait of Hormuz</span>
        </label>
        <div className="mt-2 pt-2 border-t border-gray-700 text-[10px] text-gray-500">
          {useTiles ? 'MVT Vector Tiles (Lakebase)' : 'GeoJSON (Databricks SQL)'}
        </div>
      </div>
    </div>
  )
}
