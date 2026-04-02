import { useState } from 'react';
import styles from './HetaMem.module.css';

function CodeBlock({ lang, code }: { lang: string; code: string }) {
  const [copied, setCopied] = useState(false);
  function copy() {
    navigator.clipboard.writeText(code).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }
  return (
    <div className={styles.codeBlock}>
      <div className={styles.codeHeader}>
        <span className={styles.codeLang}>{lang}</span>
        <button className={styles.copyBtn} onClick={copy}>{copied ? 'copied' : 'copy'}</button>
      </div>
      <pre className={styles.code}>{code}</pre>
    </div>
  );
}

// ── API base (port 8000 unified entry, same as main.py) ──────────────────────
const BASE = 'http://localhost:8000';

// ── MemoryKB examples ────────────────────────────────────────────────────────
const KB_INSERT = `curl -X POST ${BASE}/api/v1/hetamem/kb/insert \\
  -F "query=Sprint review: velocity 42 pts, shipped auth module." \\
  -F "image=@screenshot.png"        # optional — auto-captioned`;

const KB_QUERY = `curl -X POST ${BASE}/api/v1/hetamem/kb/query \\
  -H "Content-Type: application/json" \\
  -d '{
    "query": "What was decided in the last sprint review?",
    "mode": "hybrid",   // local | global | hybrid | naive
    "use_pm": false     // true to check parametric memory first
  }'

// Response
{
  "query": "...",
  "mode": "hybrid",
  "pm_used": false,
  "pm_relevant": false,
  "rag_memory": "<retrieved context>",
  "final_answer": "In the last sprint review..."
}`;

// ── MemoryVG examples ────────────────────────────────────────────────────────
const VG_ADD = `curl -X POST ${BASE}/api/v1/hetamem/vg/add \\
  -H "Content-Type: application/json" \\
  -d '{
    "messages": [
      {"role": "user",      "content": "I prefer dark mode and I am allergic to peanuts."},
      {"role": "assistant", "content": "Got it, I will remember that."}
    ],
    "user_id": "alice"   // at least one of user_id / agent_id / run_id required
  }'

// Response — LLM extracts facts and deduplicates against existing memories
{ "results": [
    {"id": "uuid-1", "memory": "Prefers dark mode",       "event": "ADD"},
    {"id": "uuid-2", "memory": "Allergic to peanuts",     "event": "ADD"}
] }`;

const VG_SEARCH = `curl -X POST ${BASE}/api/v1/hetamem/vg/search \\
  -H "Content-Type: application/json" \\
  -d '{
    "query": "dietary restrictions",
    "user_id": "alice",
    "limit": 5,
    "threshold": 0.6   // optional minimum similarity score
  }'`;

// ── MCP configs ──────────────────────────────────────────────────────────────
const MCP_URL = 'http://localhost:8011/mcp/';

const MCP_CLAUDE_DESKTOP = `// macOS: ~/Library/Application Support/Claude/claude_desktop_config.json
// Windows: %APPDATA%\\Claude\\claude_desktop_config.json
{
  "mcpServers": {
    "hetamem": {
      "type": "http",
      "url": "${MCP_URL}"
    }
  }
}`;

const MCP_CLAUDE_CODE = `// ~/.claude.json  — or run /mcp inside Claude Code to add interactively
{
  "mcpServers": {
    "hetamem": {
      "type": "http",
      "url": "${MCP_URL}"
    }
  }
}`;

const MCP_CURSOR = `// Global: ~/.cursor/mcp.json
// Project: .cursor/mcp.json  (takes precedence)
{
  "mcpServers": {
    "hetamem": {
      "url": "${MCP_URL}"
    }
  }
}`;

const MCP_CLINE = `// VSCode settings.json  (Cline extension)
{
  "cline.mcpServers": {
    "hetamem": {
      "type": "streamable-http",
      "url": "${MCP_URL}"
    }
  }
}`;

const MCP_RAW = `// Raw JSON-RPC 2.0 over HTTP — list available tools
curl -X POST ${MCP_URL} \\
  -H "Content-Type: application/json" \\
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

// Call a tool directly
curl -X POST ${MCP_URL} \\
  -H "Content-Type: application/json" \\
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
      "name": "kb_query",
      "arguments": {"query": "what did we ship last sprint?", "mode": "hybrid"}
    }
  }'`;

// ── Standalone startup ───────────────────────────────────────────────────────
const STANDALONE_CMD = `# HetaMem is started automatically with src/main.py (port 8000).
# To run HetaMem standalone (port 8003):
cd /path/to/Heta
python -m hetamem.api.main

# MCP server can also be started independently (port 8011):
HETAMEM_BASE_URL=http://localhost:8003 python src/hetamem/mcp/server.py`;

const MCP_CLIENTS = [
  { id: 'claude-desktop', label: 'Claude Desktop', code: MCP_CLAUDE_DESKTOP },
  { id: 'claude-code',    label: 'Claude Code',    code: MCP_CLAUDE_CODE    },
  { id: 'cursor',         label: 'Cursor',         code: MCP_CURSOR         },
  { id: 'cline',          label: 'Cline',          code: MCP_CLINE          },
  { id: 'raw',            label: 'Raw HTTP',       code: MCP_RAW            },
];

const MCP_TOOLS = [
  { name: 'kb_insert',    args: 'query: str',                                              desc: 'Store a piece of knowledge. Accepts text plus optional image, audio, or video — all automatically processed and indexed.' },
  { name: 'kb_query',     args: 'query: str, mode: str = "hybrid", use_pm: bool = False',  desc: 'Ask a question and get an answer grounded in stored knowledge. Modes: local / global / hybrid / naive.' },
  { name: 'vg_add',       args: 'messages: list, user_id?, agent_id?, run_id?, metadata?', desc: 'Save what matters from a conversation. Automatically extracts key facts and avoids creating duplicate memories.' },
  { name: 'vg_search',    args: 'query: str, user_id?, agent_id?, run_id?, limit=10, threshold?', desc: 'Find memories relevant to the current context, matched by meaning rather than keywords.' },
  { name: 'vg_list',      args: 'user_id?, agent_id?, run_id?, limit=100',                 desc: 'See everything remembered about a user or session.' },
  { name: 'vg_get',       args: 'memory_id: str',                                          desc: 'Look up one specific memory by ID.' },
  { name: 'vg_history',   args: 'memory_id: str',                                          desc: 'See how a memory has changed over time — every update is tracked.' },
  { name: 'vg_update',    args: 'memory_id: str, data: str',                               desc: 'Correct or update a stored memory.' },
  { name: 'vg_delete',    args: 'memory_id: str',                                          desc: 'Remove a specific memory by ID.' },
  { name: 'vg_delete_all','args': 'user_id?, agent_id?, run_id?',                          desc: 'Clear all memories for a given user or session.' },
];

export default function HetaMemPage() {
  const [activeClient, setActiveClient] = useState('claude-desktop');

  return (
    <div className={styles.page}>
      <div className={styles.inner}>

        {/* ── Header ── */}
        <div className={styles.header}>
          <h1 className={styles.title}>HetaMem</h1>
          <p className={styles.subtitle}>
            Give your AI agents memory. <strong>MemoryKB</strong> lets agents reference shared knowledge —
            documents, notes, and past decisions — across every conversation.
            <strong> MemoryVG</strong> lets agents remember individual users —
            their preferences, context, and history — so every interaction feels personal.
          </p>
          <div className={styles.badgeRow}>
            <span className={styles.badge}>REST API · port 8000</span>
            <span className={styles.badge}>MCP server · port 8011</span>
            <span className={`${styles.badge} ${styles.badgeAccent}`}>MemoryKB</span>
            <span className={`${styles.badge} ${styles.badgeAccent}`}>MemoryVG</span>
          </div>
        </div>

        {/* ── When to use which ── */}
        <div className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionIcon}>◈</span>
            Which one do I need?
          </h2>
          <div className={styles.compareGrid}>
            <div className={styles.compareCard}>
              <div className={styles.compareLabel}>MemoryKB</div>
              <div className={styles.compareSub}>Long-term structured memory</div>
              <ul className={styles.compareList}>
                <li>Store documents, reports, meeting notes, or any reference material</li>
                <li>Supports images, audio, and video alongside text</li>
                <li>Great for questions like <em>"What did we decide about X?"</em> or <em>"Summarise all notes on Y"</em></li>
                <li>Knowledge is shared — not tied to any single user or session</li>
                <li>Best when answers require connecting information across multiple sources</li>
              </ul>
            </div>
            <div className={styles.compareCard}>
              <div className={styles.compareLabel}>MemoryVG</div>
              <div className={styles.compareSub}>Personal memory — for each user or session</div>
              <ul className={styles.compareList}>
                <li>Remember what a specific user has told you — preferences, habits, past decisions</li>
                <li>Automatically extracts what matters from conversation history</li>
                <li>Scoped per person, agent, or session — memories never bleed across users</li>
                <li>Great for questions like <em>"What does Alice prefer?"</em> or <em>"What did we discuss last time?"</em></li>
                <li>Every change is tracked — you can always see how a memory evolved</li>
              </ul>
            </div>
          </div>
        </div>

        {/* ── MemoryKB API ── */}
        <div className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionIcon}>⬡</span>
            MemoryKB API
          </h2>
          <div className={styles.cards}>
            <div className={styles.card}>
              <div className={styles.cardTitle}>
                <span className={`${styles.method} ${styles.methodPost}`}>POST</span>
                <span className={styles.cardPath}>/api/v1/hetamem/kb/insert</span>
              </div>
              <p className={styles.cardDesc}>
                Save a piece of knowledge. Send text via <code>multipart/form-data</code> with optional <code>image</code>, <code>audio</code>, or <code>video</code> attachments — each is automatically transcribed or captioned and merged into the stored entry.
              </p>
            </div>
            <div className={styles.card}>
              <div className={styles.cardTitle}>
                <span className={`${styles.method} ${styles.methodPost}`}>POST</span>
                <span className={styles.cardPath}>/api/v1/hetamem/kb/query</span>
              </div>
              <p className={styles.cardDesc}>
                Ask a question and get an answer grounded in everything you've stored. Body: <code>query</code> (str), <code>mode</code> (local · global · hybrid · naive, default <code>hybrid</code>), <code>use_pm</code> (bool, default false — if true, tries to answer from the model's own knowledge first before searching stored content).
              </p>
            </div>
          </div>
          <table className={styles.table}>
            <thead><tr><th>mode</th><th>How it works</th><th>Best for</th></tr></thead>
            <tbody>
              <tr><td className={styles.mono}>local</td><td>Follows connections between related topics</td><td>Specific, focused questions</td></tr>
              <tr><td className={styles.mono}>global</td><td>Looks at the big picture across all stored knowledge</td><td>Broad or thematic questions</td></tr>
              <tr><td className={styles.mono}>hybrid</td><td>Balances depth and breadth</td><td>General use (recommended)</td></tr>
              <tr><td className={styles.mono}>naive</td><td>Simple similarity match, no reasoning across sources</td><td>Direct lookups, fastest response</td></tr>
            </tbody>
          </table>
          <CodeBlock lang="bash — insert with image" code={KB_INSERT} />
          <CodeBlock lang="bash — query + response shape" code={KB_QUERY} />
        </div>

        {/* ── MemoryVG API ── */}
        <div className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionIcon}>◎</span>
            MemoryVG API
          </h2>
          <p className={styles.sectionDesc}>
            Memories are always scoped — every operation requires at least one of <code>user_id</code>, <code>agent_id</code>, or <code>run_id</code>.
            This ensures memories for different users or sessions never mix. Multiple scope identifiers can be combined for finer-grained targeting.
          </p>
          <div className={styles.cards}>
            {([
              { m: 'POST',   p: '/api/v1/hetamem/vg/add',                 d: 'Automatically extract what matters from a conversation and save it. Handles deduplication — if a similar memory already exists, it updates rather than duplicates.' },
              { m: 'POST',   p: '/api/v1/hetamem/vg/search',              d: 'Find memories relevant to the current context. Body: query (str), scope ids, limit (default 10), threshold (optional 0–1 minimum similarity).' },
              { m: 'GET',    p: '/api/v1/hetamem/vg',                     d: 'See everything remembered about a user or session. Query params: user_id / agent_id / run_id (optional), limit (default 100).' },
              { m: 'GET',    p: '/api/v1/hetamem/vg/{memory_id}',         d: 'Look up one specific memory by its ID.' },
              { m: 'GET',    p: '/api/v1/hetamem/vg/{memory_id}/history', d: 'See how a memory has changed over time — every update is tracked with timestamps.' },
              { m: 'PUT',    p: '/api/v1/hetamem/vg/{memory_id}',         d: 'Correct or update a stored memory. Body: { "data": "updated content" }.' },
              { m: 'DELETE', p: '/api/v1/hetamem/vg/{memory_id}',         d: 'Remove a specific memory by ID.' },
              { m: 'DELETE', p: '/api/v1/hetamem/vg',                     d: 'Clear all memories for a given user or session (query params: user_id / agent_id / run_id).' },
            ] as { m: string; p: string; d: string }[]).map(({ m, p, d }) => (
              <div key={p} className={styles.card}>
                <div className={styles.cardTitle}>
                  <span className={`${styles.method} ${
                    m === 'POST' ? styles.methodPost :
                    m === 'GET'  ? styles.methodGet  :
                    m === 'PUT'  ? styles.methodPut  : styles.methodDel
                  }`}>{m}</span>
                  <span className={styles.cardPath}>{p}</span>
                </div>
                <p className={styles.cardDesc}>{d}</p>
              </div>
            ))}
          </div>
          <CodeBlock lang="bash — add memories from conversation" code={VG_ADD} />
          <CodeBlock lang="bash — search memories" code={VG_SEARCH} />
        </div>

        {/* ── MCP Server ── */}
        <div className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionIcon}>▸</span>
            MCP server
          </h2>
          <p className={styles.sectionDesc}>
            The MCP server starts automatically alongside <code>src/main.py</code> and listens
            on <strong>port 8011</strong> using the <code>streamable-http</code> transport.
            Endpoint: <code>{MCP_URL}</code>
          </p>

          {/* Client tabs */}
          <div className={styles.clientTabs}>
            {MCP_CLIENTS.map((c) => (
              <button
                key={c.id}
                className={[styles.clientTab, activeClient === c.id ? styles.clientTabActive : ''].join(' ')}
                onClick={() => setActiveClient(c.id)}
              >
                {c.label}
              </button>
            ))}
          </div>
          {MCP_CLIENTS.map((c) => activeClient === c.id && (
            <CodeBlock key={c.id} lang={c.label} code={c.code} />
          ))}
        </div>

        {/* ── MCP Tools ── */}
        <div className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionIcon}>◈</span>
            MCP tools
          </h2>
          <table className={styles.table}>
            <thead><tr><th>Tool</th><th>Arguments</th><th>Description</th></tr></thead>
            <tbody>
              {MCP_TOOLS.map((t) => (
                <tr key={t.name}>
                  <td className={styles.mono} style={{ whiteSpace: 'nowrap' }}>{t.name}</td>
                  <td className={styles.monoSm}>{t.args}</td>
                  <td>{t.desc}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* ── Standalone startup ── */}
        <div className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionIcon}>▸</span>
            Standalone startup
          </h2>
          <p className={styles.sectionDesc}>
            HetaMem and the MCP server launch automatically when <code>src/main.py</code> starts.
            Use the commands below only if you need to run HetaMem independently.
          </p>
          <CodeBlock lang="bash" code={STANDALONE_CMD} />
        </div>

      </div>
    </div>
  );
}
