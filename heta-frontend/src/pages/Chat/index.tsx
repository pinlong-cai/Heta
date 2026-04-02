import { useEffect, useRef, useState } from 'react';
import { useKBList, useKBDetail } from '../../hooks/useKnowledgeBase';
import { sendMessage } from '../../api/services/chat';
import Dropdown from '../../components/ui/Dropdown';
import MessageBubble, { type Message } from './components/MessageBubble';
import ChatInput from './components/ChatInput';
import Spinner from './components/Spinner';
import styles from './Chat.module.css';

const SESSION_USER_ID = crypto.randomUUID();

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [selectedKb, setSelectedKb] = useState<string>('');
  const [selectedMode, setSelectedMode] = useState<string>('naive');

  const bottomRef = useRef<HTMLDivElement>(null);
  const { data: kbs = [] } = useKBList();
  const { data: kbDetail } = useKBDetail(selectedKb || null);

  const modeOptions = kbDetail?.available_query_modes ?? [];

  const kbOptions = kbs.map((kb) => ({ value: kb.name, label: kb.name }));

  useEffect(() => {
    if (kbs.length > 0 && !selectedKb) setSelectedKb(kbs[0].name);
  }, [kbs]);

  // Reset mode to first available when KB changes and modes are loaded
  useEffect(() => {
    if (modeOptions.length > 0 && !modeOptions.some((m) => m.id === selectedMode)) {
      setSelectedMode(modeOptions[0].id);
    }
  }, [modeOptions]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  async function handleSubmit() {
    const query = input.trim();
    if (!query || loading) return;
    if (!selectedKb) { alert('Please select a knowledge base first.'); return; }

    const modeAtSend = selectedMode;
    const modeLabelAtSend = modeOptions.find((m) => m.id === modeAtSend)?.label ?? modeAtSend;

    setMessages((prev) => [...prev, {
      id: crypto.randomUUID(),
      role: 'user',
      content: query,
      query_mode: modeAtSend,
      mode_label: modeLabelAtSend,
      kb_id: selectedKb,
    }]);
    setInput('');
    setLoading(true);

    try {
      const res = await sendMessage({
        query,
        kb_id: selectedKb,
        user_id: SESSION_USER_ID,
        query_mode: modeAtSend,
      });

      setMessages((prev) => [...prev, {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: res.response ?? res.message ?? 'No response.',
        sources: res.data,
        citations: res.citations ?? undefined,
        error: !res.success,
      }]);
    } catch (err) {
      setMessages((prev) => [...prev, {
        id: crypto.randomUUID(),
        role: 'assistant',
        content: (err as Error).message ?? 'Request failed.',
        error: true,
      }]);
    } finally {
      setLoading(false);
    }
  }

  const isEmpty = messages.length === 0 && !loading;

  return (
    <div className={styles.page}>
      <div className={styles.topbar}>
        <h1 className={styles.title}>Chat</h1>

        <div className={styles.controls}>
          <div className={styles.kbSelector}>
            <span className={styles.controlLabel}>KB</span>
            <Dropdown
              options={kbOptions}
              value={selectedKb}
              onChange={setSelectedKb}
              placeholder="No knowledge bases"
              disabled={loading}
            />
          </div>

          <div className={styles.modeTabs}>
            {modeOptions.map((opt) => (
              <button
                key={opt.id}
                className={[styles.modeTab, selectedMode === opt.id ? styles.modeTabActive : ''].join(' ')}
                onClick={() => setSelectedMode(opt.id)}
                disabled={loading}
                title={opt.desc}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className={styles.messageList}>
        {isEmpty && (
          <div className={styles.empty}>
            <p className={styles.emptyTitle}>What would you like to know?</p>
            <p className={styles.emptyHint}>Select a knowledge base and mode, then start asking.</p>
          </div>
        )}
        <div className={styles.messages}>
          {messages.map((msg) => (
            <MessageBubble key={msg.id} message={msg} />
          ))}
          {loading && (
            <div className={styles.spinnerRow}>
              <Spinner />
            </div>
          )}
          <div ref={bottomRef} />
        </div>
      </div>

      <ChatInput
        value={input}
        onChange={setInput}
        onSubmit={handleSubmit}
        disabled={loading || !selectedKb}
        placeholder={
          !selectedKb
            ? 'Select a knowledge base to start…'
            : 'Ask anything… (Enter to send, Shift+Enter for newline)'
        }
      />
    </div>
  );
}
