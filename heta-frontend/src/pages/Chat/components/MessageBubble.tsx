import { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { File, FileText, Sheet, Globe } from 'lucide-react';
import type { QueryResult, Citation } from '../../../api/services/chat';
import styles from './MessageBubble.module.css';

export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  query_mode?: string;
  mode_label?: string;
  kb_id?: string;
  sources?: QueryResult[];
  citations?: Citation[];
  error?: boolean;
}

interface Props {
  message: Message;
}

/** Replace [N] markers in LLM response with markdown links when file_url exists. */
function injectCitationLinks(content: string, citations: Citation[]): string {
  if (!citations.length) return content;
  const urlMap: Record<number, string | null> = {};
  citations.forEach((c) => { urlMap[c.index] = c.file_url; });
  return content.replace(/\[(\d+)\]/g, (match, num) => {
    const url = urlMap[parseInt(num)];
    return url ? `[[${num}]](${url})` : match;
  });
}

function truncateExcerpt(text: string, maxLen = 120): string {
  if (text.length <= maxLen) return text;
  const cut = text.lastIndexOf(' ', maxLen);
  return (cut > 0 ? text.slice(0, cut) : text.slice(0, maxLen)) + '…';
}

function FileIcon({ filename }: { filename: string }) {
  const ext = filename.split('.').pop()?.toLowerCase() ?? '';
  const props = { size: 12, className: styles.fileIcon };
  if (ext === 'pdf' || ext === 'md' || ext === 'txt') return <FileText {...props} />;
  if (['csv', 'xlsx', 'xls'].includes(ext)) return <Sheet {...props} />;
  if (ext === 'html' || ext === 'htm') return <Globe {...props} />;
  return <File {...props} />;
}

export default function MessageBubble({ message }: Props) {
  const [sourcesOpen, setSourcesOpen] = useState(false);
  const citations = message.citations ?? [];
  const sources = message.sources ?? [];
  const hasCitations = citations.length > 0;
  const hasSources = sources.length > 0;
  const hasAny = hasCitations || hasSources;

  // Map source_file → first matching chunk content for excerpts
  const excerptMap: Record<string, string> = {};
  for (const s of sources) {
    if (s.kb_name && !excerptMap[s.kb_name] && s.content) {
      excerptMap[s.kb_name] = s.content;
    }
  }

  if (message.role === 'user') {
    return (
      <div className={styles.userRow}>
        <div className={styles.userMeta}>
          {(message.kb_id || message.mode_label) && (
            <span className={styles.metaBadge}>
              {message.kb_id && (
                <span className={styles.metaKb} title={message.kb_id}>
                  {message.kb_id}
                </span>
              )}
              {message.kb_id && message.mode_label && (
                <span className={styles.metaSep}>·</span>
              )}
              {message.mode_label && (
                <span className={styles.metaMode}>{message.mode_label}</span>
              )}
            </span>
          )}
        </div>
        <div className={styles.userBubble}>{message.content}</div>
      </div>
    );
  }

  return (
    <div className={styles.assistantRow}>
      <div className={styles.assistantContent}>
        <div className={[styles.assistantBubble, message.error ? styles.errorBubble : ''].join(' ')}>
          <div className={styles.markdown}>
            <ReactMarkdown>{injectCitationLinks(message.content, citations)}</ReactMarkdown>
          </div>
        </div>

        {hasAny && (
          <div className={styles.sources}>
            <button
              className={styles.sourcesToggle}
              onClick={() => setSourcesOpen((o) => !o)}
            >
              {sourcesOpen ? '▾' : '▸'}{' '}
              {hasCitations
                ? `${citations.length} source${citations.length !== 1 ? 's' : ''}`
                : `${sources.length} chunk${sources.length !== 1 ? 's' : ''}`}
            </button>

            {sourcesOpen && (
              <div className={styles.sourceList}>
                {hasCitations
                  ? citations.map((c) => {
                      const excerpt = excerptMap[c.source_file];
                      return (
                        <div key={c.index} className={styles.sourceCard}>
                          <div className={styles.sourceCardHeader}>
                            <span className={styles.sourceIndex}>[{c.index}]</span>
                            {c.file_url ? (
                              <a
                                href={c.file_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className={styles.sourceFilename}
                                title={c.source_file}
                              >
                                <FileIcon filename={c.source_file} /> {c.source_file}
                              </a>
                            ) : (
                              <span className={styles.sourceFilename} title={c.source_file}>
                                <FileIcon filename={c.source_file} /> {c.source_file}
                              </span>
                            )}
                            <span className={styles.sourceDataset}>{c.dataset}</span>
                          </div>
                          {excerpt && (
                            <p className={styles.sourceExcerpt}>{truncateExcerpt(excerpt)}</p>
                          )}
                        </div>
                      );
                    })
                  : sources.map((s, i) => (
                      <div key={i} className={styles.sourceCard}>
                        <div className={styles.sourceCardHeader}>
                          <span className={styles.sourceFilename}>{s.kb_name || s.kb_id || '—'}</span>
                        </div>
                        {s.content && (
                          <p className={styles.sourceExcerpt}>{truncateExcerpt(s.content)}</p>
                        )}
                      </div>
                    ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
