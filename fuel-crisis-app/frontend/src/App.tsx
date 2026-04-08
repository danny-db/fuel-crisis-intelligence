import { useState } from 'react'
import MetricsTicker from './components/MetricsTicker'
import CrisisMap from './components/CrisisMap'
import GenieChat from './components/GenieChat'
import CrisisBriefing from './components/CrisisBriefing'
import DashboardEmbed from './components/DashboardEmbed'
import type { TabId } from './types'

const TABS: { id: TabId; label: string }[] = [
  { id: 'map', label: 'Map' },
  { id: 'dashboard', label: 'Dashboard' },
  { id: 'genie', label: 'Genie' },
  { id: 'briefing', label: 'AI Briefing' },
]

export default function App() {
  const [activeTab, setActiveTab] = useState<TabId>('map')

  return (
    <div className="flex flex-col h-screen bg-gray-950">
      {/* Metrics Ticker */}
      <MetricsTicker />

      {/* Tab Bar */}
      <div className="flex border-b border-gray-800 bg-gray-900/80 px-4">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-6 py-3 text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? 'text-red-400 border-b-2 border-red-500'
                : 'text-gray-400 hover:text-gray-200'
            }`}
          >
            {tab.label}
          </button>
        ))}
        <div className="flex-1" />
        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span className="inline-block w-2 h-2 rounded-full bg-red-500 animate-pulse" />
          CRISIS ACTIVE
        </div>
      </div>

      {/* Tab Content — all tabs stay mounted to preserve state */}
      <div className="flex-1 overflow-hidden relative">
        <div className={`absolute inset-0 ${activeTab === 'map' ? '' : 'hidden'}`}><CrisisMap /></div>
        <div className={`absolute inset-0 ${activeTab === 'dashboard' ? '' : 'hidden'}`}><DashboardEmbed /></div>
        <div className={`absolute inset-0 ${activeTab === 'genie' ? '' : 'hidden'}`}><GenieChat /></div>
        <div className={`absolute inset-0 ${activeTab === 'briefing' ? '' : 'hidden'}`}><CrisisBriefing /></div>
      </div>
    </div>
  )
}
