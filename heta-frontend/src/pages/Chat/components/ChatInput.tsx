import { useRef, useEffect } from 'react';
import styles from './ChatInput.module.css';

interface Props {
  value: string;
  onChange: (v: string) => void;
  onSubmit: () => void;
  disabled?: boolean;
  placeholder?: string;
}

export default function ChatInput({ value, onChange, onSubmit, disabled, placeholder }: Props) {
  const ref = useRef<HTMLTextAreaElement>(null);

  // Auto-resize textarea up to a max height
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  }, [value]);

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (!disabled && value.trim()) onSubmit();
    }
  }

  return (
    <div className={styles.root}>
      <div className={styles.box}>
        <textarea
          ref={ref}
          className={styles.textarea}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder ?? 'Ask anything… (Enter to send, Shift+Enter for newline)'}
          disabled={disabled}
          rows={1}
        />
        <button
          className={styles.send}
          onClick={onSubmit}
          disabled={disabled || !value.trim()}
          aria-label="Send"
        >
          ↑
        </button>
      </div>
      <p className={styles.hint}>Enter to send · Shift+Enter for new line</p>
    </div>
  );
}
