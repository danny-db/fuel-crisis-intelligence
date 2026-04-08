export interface Metrics {
  brent_price: number | null
  aud_usd: number | null
  iea_days: number | null
  operational_refineries: number | null
  iran_opec_pct: number | null
}

export interface GenieResponse {
  answer: string | null
  sql: string | null
  error?: string
  status?: string
  conversation_id?: string
}

export interface BriefingResponse {
  briefing: string
  model: string
}

export interface GeoJSONFeature {
  type: 'Feature'
  geometry: {
    type: string
    coordinates: number[]
  }
  properties: Record<string, unknown>
}

export interface GeoJSONCollection {
  type: 'FeatureCollection'
  features: GeoJSONFeature[]
}

export type TabId = 'map' | 'dashboard' | 'genie' | 'briefing'
