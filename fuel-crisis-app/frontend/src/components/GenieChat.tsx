import { useState, useRef } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import type { GenieResponse } from '../types'

const SAMPLE_QUESTIONS = [
  'How much crude oil does Australia import from the Middle East?',
  'If Iran goes offline, what % of OPEC is lost?',
  'How many operational refineries does Australia have?',
  'Current Brent crude oil price and trend?',
  'Which states consume the most diesel?',
  'What are today\'s WA fuel station prices?',
  'Compare Adelaide vs Sydney fuel prices',
  'What is the trade balance impact of rising oil imports?',
]

export default function GenieChat() {
  const [question, setQuestion] = useState('')
  const [messages, setMessages] = useState<
    { role: 'user' | 'genie'; content: string; sql?: string | null }[]
  >([])
  const [loading, setLoading] = useState(false)
  const [showSql, setShowSql] = useState<number | null>(null)
  // Track conversation ID for follow-up messages
  const conversationId = useRef<string | null>(null)

  async function askQuestion(q: string) {
    if (!q.trim()) return
    const userQ = q.trim()
    setQuestion('')
    setMessages((prev) => [...prev, { role: 'user', content: userQ }])
    setLoading(true)

    try {
      const resp = await fetch('/api/genie/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: userQ,
          conversation_id: conversationId.current,
        }),
      })
      if (!resp.ok) {
        const errBody = await resp.text()
        setMessages((prev) => [
          ...prev,
          { role: 'genie', content: `Error ${resp.status}: ${errBody}` },
        ])
        return
      }
      const data: GenieResponse = await resp.json()
      // Track conversation for follow-ups
      if (data.conversation_id) {
        conversationId.current = data.conversation_id
      }
      setMessages((prev) => [
        ...prev,
        {
          role: 'genie',
          content: data.answer || data.error || 'Genie returned no answer',
          sql: data.sql,
        },
      ])
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: 'genie', content: `Network error: ${err}` },
      ])
    } finally {
      setLoading(false)
    }
  }

  function clearChat() {
    setMessages([])
    conversationId.current = null
  }

  return (
    <div className="flex h-full">
      {/* Chat Panel */}
      <div className="flex-1 flex flex-col">
        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-6 space-y-4">
          {messages.length === 0 && (
            <div className="text-center text-gray-500 mt-20">
              <div className="text-4xl mb-4">&#x1F50D;</div>
              <div className="text-lg font-semibold">Ask the Crisis Intelligence Engine</div>
              <div className="text-sm mt-2">
                Natural language questions about Australia's fuel security
              </div>
            </div>
          )}
          {messages.map((msg, i) => (
            <div
              key={i}
              className={`max-w-3xl ${msg.role === 'user' ? 'ml-auto' : 'mr-auto'}`}
            >
              <div
                className={`rounded-lg px-4 py-3 ${
                  msg.role === 'user'
                    ? 'bg-red-900/30 border border-red-800/50 text-gray-200'
                    : 'bg-gray-800/60 border border-gray-700/50 text-gray-300'
                }`}
              >
                <div className="text-xs text-gray-500 mb-1 font-medium">
                  {msg.role === 'user' ? 'YOU' : 'GENIE'}
                </div>
                {msg.role === 'genie' ? (
                  <div className="text-sm briefing-content">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
                  </div>
                ) : (
                  <div className="text-sm whitespace-pre-wrap">{msg.content}</div>
                )}
                {msg.sql && (
                  <button
                    onClick={() => setShowSql(showSql === i ? null : i)}
                    className="text-xs text-blue-400 hover:text-blue-300 mt-2"
                  >
                    {showSql === i ? 'Hide SQL' : 'Show SQL'}
                  </button>
                )}
                {showSql === i && msg.sql && (
                  <pre className="mt-2 p-3 bg-gray-900 rounded text-xs text-green-400 overflow-x-auto">
                    {msg.sql}
                  </pre>
                )}
              </div>
            </div>
          ))}
          {loading && (
            <div className="flex items-center gap-2 text-gray-500 text-sm">
              <div className="w-2 h-2 bg-red-500 rounded-full animate-pulse" />
              Genie is thinking...
            </div>
          )}
        </div>

        {/* Input */}
        <div className="border-t border-gray-800 p-4">
          <div className="flex gap-3 max-w-3xl mx-auto">
            <input
              type="text"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && !loading && askQuestion(question)}
              placeholder="Ask a question about Australia's fuel security..."
              className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-4 py-3 text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-red-500"
              disabled={loading}
            />
            <button
              onClick={() => askQuestion(question)}
              disabled={loading || !question.trim()}
              className="bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white px-6 py-3 rounded-lg text-sm font-medium transition-colors"
            >
              Ask
            </button>
            {messages.length > 0 && (
              <button
                onClick={clearChat}
                className="text-gray-500 hover:text-gray-300 px-3 py-3 rounded-lg text-xs transition-colors"
                title="Clear chat"
              >
                Clear
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Sample Questions Sidebar */}
      <div className="w-72 border-l border-gray-800 bg-gray-900/40 p-4 overflow-y-auto">
        <div className="text-xs font-bold text-gray-400 mb-3 tracking-wider">
          SAMPLE QUESTIONS
        </div>
        <div className="space-y-2">
          {SAMPLE_QUESTIONS.map((q, i) => (
            <button
              key={i}
              onClick={() => askQuestion(q)}
              disabled={loading}
              className="w-full text-left text-xs text-gray-400 hover:text-gray-200 bg-gray-800/50 hover:bg-gray-800 rounded-lg px-3 py-2 transition-colors disabled:opacity-50"
            >
              {q}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
