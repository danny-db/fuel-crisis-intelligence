import { useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type { Components } from 'react-markdown'

/**
 * Detect paragraphs that contain "DATA INTEGRITY" or similar notices
 * and render them with a distinct warning style.
 */
function isNoticeParagraph(children: React.ReactNode): boolean {
  const text = extractText(children)
  return /data integrity|notice|caveat|disclaimer/i.test(text)
}

function extractText(node: React.ReactNode): string {
  if (typeof node === 'string') return node
  if (Array.isArray(node)) return node.map(extractText).join('')
  if (node && typeof node === 'object' && 'props' in node) {
    return extractText((node as React.ReactElement).props.children)
  }
  return ''
}

const markdownComponents: Components = {
  // Section headings get red accent borders via CSS; no override needed.
  // Tables are handled by remark-gfm + CSS.

  // Paragraphs — detect warning/notice text
  p({ children }) {
    if (isNoticeParagraph(children)) {
      return <div className="briefing-notice">{children}</div>
    }
    return <p>{children}</p>
  },

  // Blockquotes — already styled via CSS, but add role for semantics
  blockquote({ children }) {
    return <blockquote role="note">{children}</blockquote>
  },
}

export default function CrisisBriefing() {
  const [briefing, setBriefing] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [model, setModel] = useState<string>('')

  async function generateBriefing() {
    setLoading(true)
    setBriefing(null)
    try {
      const resp = await fetch('/api/briefing/generate', { method: 'POST' })
      if (!resp.ok) {
        const errText = await resp.text()
        setBriefing(`**Error ${resp.status}:** ${errText}`)
        return
      }
      const data = await resp.json()
      setBriefing(data.briefing)
      setModel(data.model || '')
    } catch (err) {
      setBriefing(`**Network error:** ${err}`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex flex-col h-full p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-bold text-gray-200">AI Crisis Briefing</h2>
          <p className="text-sm text-gray-500 mt-1">
            Generate a classified PM briefing note using real-time intelligence data
          </p>
        </div>
        <button
          onClick={generateBriefing}
          disabled={loading}
          className="bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white px-6 py-3 rounded-lg font-medium transition-colors flex items-center gap-2"
        >
          {loading ? (
            <>
              <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              Generating...
            </>
          ) : (
            'Generate PM Briefing'
          )}
        </button>
      </div>

      {/* Briefing Content */}
      <div className="flex-1 overflow-y-auto">
        {!briefing && !loading && (
          <div className="text-center text-gray-500 mt-20">
            <div className="text-6xl mb-4">&#x1F4CB;</div>
            <div className="text-lg">No briefing generated yet</div>
            <div className="text-sm mt-2">
              Click "Generate PM Briefing" to create a real-time intelligence briefing
            </div>
          </div>
        )}

        {loading && (
          <div className="flex flex-col items-center justify-center mt-20 text-gray-400">
            <div className="w-8 h-8 border-2 border-red-500/30 border-t-red-500 rounded-full animate-spin mb-4" />
            <div className="text-sm">Gathering intelligence data and generating briefing...</div>
            <div className="text-xs text-gray-600 mt-1">This may take 15-30 seconds</div>
          </div>
        )}

        {briefing && (
          <div className="max-w-4xl mx-auto">
            <div className="bg-gray-900/60 border border-gray-700 rounded-lg">
              {/* Classified Header Banner */}
              <div className="classified-banner rounded-t-lg border-b border-gray-700 mt-[-1px] mx-[-1px] rounded-b-none">
                <div className="classification">TOP SECRET // AUSTEO</div>
                <div className="subtitle">Prime Minister's Fuel Security Briefing</div>
                <div className="meta">
                  Generated {new Date().toLocaleString('en-AU', {
                    weekday: 'long',
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric',
                    hour: '2-digit',
                    minute: '2-digit',
                    timeZoneName: 'short',
                  })}
                  {model && ` | Model: ${model}`}
                </div>
              </div>

              {/* Markdown Content */}
              <div className="briefing-content px-8 py-6">
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={markdownComponents}
                >
                  {briefing}
                </ReactMarkdown>
              </div>

              {/* Classified Footer Banner */}
              <div className="classified-banner rounded-b-lg border-t border-gray-700 mb-[-1px] mx-[-1px] rounded-t-none">
                <div className="classification">TOP SECRET // AUSTEO</div>
                <div className="meta">
                  Distribution: PM, NSC, DISER, Defence, Home Affairs — Eyes Only
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
