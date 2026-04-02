import { useState } from 'react';
import { LibraryBig, Trash2 } from 'lucide-react';
import styles from './KBCard.module.css';

interface Props {
  name: string;
  createdAt: string | null;
  status?: string;
  selected: boolean;
  activeTasks: number;
  onSelect: () => void;
  onDelete: () => void;
}

export default function KBCard({ name, createdAt, status, selected, activeTasks, onSelect, onDelete }: Props) {
  const [confirming, setConfirming] = useState(false);

  if (status === 'deleting') {
    return (
      <div className={[styles.card, styles.deleting].join(' ')}>
        <div className={styles.info}>
          <LibraryBig className={styles.icon} size={16} strokeWidth={1.75} />
          <div className={styles.text}>
            <span className={styles.name}>{name}</span>
            <span className={styles.date}>Deleting…</span>
          </div>
        </div>
      </div>
    );
  }

  const confirmText = activeTasks > 0
    ? `Parsing ${activeTasks} dataset${activeTasks > 1 ? 's' : ''}. Delete will cancel them?`
    : 'Delete?';

  const date = createdAt
    ? new Date(createdAt).toLocaleDateString('zh-CN')
    : null;

  return (
    <div
      className={[styles.card, selected ? styles.selected : ''].join(' ')}
      onClick={onSelect}
    >
      <div className={styles.info}>
        <LibraryBig className={styles.icon} size={16} strokeWidth={1.75} />
        <div className={styles.text}>
          <span className={styles.name}>{name}</span>
          {date && <span className={styles.date}>{date}</span>}
        </div>
      </div>

      <div className={styles.controls} onClick={(e) => e.stopPropagation()}>
        {confirming ? (
          <div className={styles.confirm}>
            <span className={styles.confirmText}>{confirmText}</span>
            <button className={styles.confirmYes} onClick={onDelete}>Delete</button>
            <button className={styles.confirmNo} onClick={() => setConfirming(false)}>Cancel</button>
          </div>
        ) : (
          <button
            className={styles.deleteBtn}
            onClick={(e) => { e.stopPropagation(); setConfirming(true); }}
            aria-label="Delete KB"
          >
            <Trash2 size={14} strokeWidth={1.75} />
          </button>
        )}
      </div>
    </div>
  );
}
