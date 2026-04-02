import { useState } from 'react';
import { Database, Trash2 } from 'lucide-react';
import { useDeleteDataset } from '../../../hooks/useDataset';
import styles from './DatasetCard.module.css';

interface Props {
  name: string;
  selected: boolean;
  onSelect: () => void;
  onDelete?: () => void;
}

export default function DatasetCard({ name, selected, onSelect, onDelete }: Props) {
  const [confirming, setConfirming] = useState(false);
  const { mutate: deleteDataset, isPending } = useDeleteDataset();

  function handleDelete(e: React.MouseEvent) {
    e.stopPropagation();
    if (!confirming) { setConfirming(true); return; }
    deleteDataset(name);
    onDelete?.();
  }

  function handleCancelDelete(e: React.MouseEvent) {
    e.stopPropagation();
    setConfirming(false);
  }

  return (
    <div
      className={[styles.card, selected ? styles.selected : ''].join(' ')}
      onClick={onSelect}
    >
      <div className={styles.info}>
        <Database className={styles.icon} size={16} strokeWidth={1.75} />
        <span className={styles.name}>{name}</span>
      </div>

      <div className={styles.controls} onClick={(e) => e.stopPropagation()}>
        {confirming ? (
          <div className={styles.confirm}>
            <span className={styles.confirmText}>Delete?</span>
            <button className={styles.confirmYes} onClick={handleDelete} disabled={isPending}>
              {isPending ? '…' : 'Yes'}
            </button>
            <button className={styles.confirmNo} onClick={handleCancelDelete}>No</button>
          </div>
        ) : (
          <button className={styles.deleteBtn} onClick={handleDelete} aria-label="Delete dataset">
            <Trash2 size={14} strokeWidth={1.75} />
          </button>
        )}
      </div>
    </div>
  );
}
