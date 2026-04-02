import { useState } from 'react';
import Modal from '../../../components/ui/Modal';
import Button from '../../../components/ui/Button';
import { useCreateDataset } from '../../../hooks/useDataset';
import styles from './CreateDatasetModal.module.css';

interface Props {
  open: boolean;
  onClose: () => void;
}

export default function CreateDatasetModal({ open, onClose }: Props) {
  const [name, setName] = useState('');
  const { mutate, isPending, error } = useCreateDataset();

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    mutate(trimmed, {
      onSuccess: () => {
        setName('');
        onClose();
      },
    });
  }

  function handleClose() {
    setName('');
    onClose();
  }

  return (
    <Modal open={open} title="New Dataset" onClose={handleClose}>
      <form onSubmit={handleSubmit} className={styles.form}>
        <div className={styles.labelRow}>
          <label className={styles.label}>Dataset name</label>
          <span className={styles.charCount}>{name.length}/64</span>
        </div>
        <input
          className={styles.input}
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="e.g. research_papers"
          maxLength={64}
          autoFocus
          spellCheck={false}
        />
        {error && <p className={styles.error}>{error.message}</p>}
        <div className={styles.actions}>
          <Button type="button" variant="ghost" onClick={handleClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={isPending} disabled={!name.trim()}>
            Create
          </Button>
        </div>
      </form>
    </Modal>
  );
}
