import { useState } from 'react';
import Modal from '../../../components/ui/Modal';
import Button from '../../../components/ui/Button';
import { useDatasets } from '../../../hooks/useDataset';
import { useParseKB } from '../../../hooks/useKnowledgeBase';
import { ParseConflictError } from '../../../api/services/kb';
import type { ParseTaskRef, ConflictInfo } from '../../../api/services/kb';
import styles from './ParseModal.module.css';

const MODE_OPTIONS = [
  {
    value: 0,
    label: 'Standard',
    desc: 'Recommended. Deep analysis of your documents for richer, more accurate answers.',
    disabled: false,
  },
  {
    value: 1,
    label: 'Quick Index',
    desc: 'Semantic search only, no knowledge graph. Faster but less precise.',
    disabled: true,
  },
];

interface Props {
  open: boolean;
  kbName: string;
  onClose: () => void;
  onTasksStarted: (tasks: ParseTaskRef[]) => void;
}

export default function ParseModal({ open, kbName, onClose, onTasksStarted }: Props) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [mode, setMode] = useState(0);
  const [conflicts, setConflicts] = useState<ConflictInfo[] | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const { data: datasets = [], isLoading } = useDatasets();
  const { mutateAsync: parse, isPending } = useParseKB(kbName);

  function toggle(name: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  }

  function onParseSuccess(tasks: ParseTaskRef[]) {
    setSelected(new Set());
    setConflicts(null);
    onClose();
    onTasksStarted(tasks);
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!selected.size) return;
    setSubmitError(null);
    setConflicts(null);
    try {
      onParseSuccess(await parse({ datasets: Array.from(selected), mode }));
    } catch (err) {
      if (err instanceof ParseConflictError) {
        setConflicts(err.conflicts);
      } else {
        setSubmitError(err instanceof Error ? err.message : 'Unknown error');
      }
    }
  }

  async function handleForceConfirm() {
    setSubmitError(null);
    try {
      onParseSuccess(await parse({ datasets: Array.from(selected), mode, force: true }));
    } catch (err) {
      setConflicts(null);
      setSubmitError(err instanceof Error ? err.message : 'Unknown error');
    }
  }

  function handleClose() {
    setSelected(new Set());
    setConflicts(null);
    setSubmitError(null);
    onClose();
  }

  const newDatasetCount = selected.size - (conflicts?.length ?? 0);

  return (
    <Modal open={open} title={`Parse into "${kbName}"`} onClose={handleClose}>
      {conflicts !== null ? (
        <div className={styles.form}>
          <div className={styles.conflictBox}>
            <p className={styles.conflictWarn}>
              {conflicts.length} dataset{conflicts.length !== 1 ? 's' : ''} below{' '}
              {conflicts.length !== 1 ? 'have' : 'has'} already been parsed in "{kbName}".
              Proceeding will delete and re-process their existing data.
            </p>
            <ul className={styles.conflictList}>
              {conflicts.map((c) => (
                <li key={c.dataset} className={styles.conflictItem}>
                  <span className={styles.dsName}>{c.dataset}</span>
                  {c.parsed_at && (
                    <span className={styles.hint}>
                      Last parsed: {new Date(c.parsed_at).toLocaleString()}
                    </span>
                  )}
                </li>
              ))}
            </ul>
            {newDatasetCount > 0 && (
              <p className={styles.hint}>
                {newDatasetCount} new dataset{newDatasetCount !== 1 ? 's' : ''} will be parsed normally.
              </p>
            )}
          </div>
          {submitError && <p className={styles.error}>{submitError}</p>}
          <div className={styles.actions}>
            <Button type="button" variant="ghost" onClick={() => setConflicts(null)}>
              Go back
            </Button>
            <Button
              type="button"
              variant="primary"
              loading={isPending}
              onClick={handleForceConfirm}
            >
              Overwrite and re-parse
            </Button>
          </div>
        </div>
      ) : (
        <form onSubmit={handleSubmit} className={styles.form}>
          <div className={styles.section}>
            <p className={styles.label}>Select datasets</p>
            {isLoading && <p className={styles.hint}>Loading…</p>}
            {!isLoading && datasets.length === 0 && (
              <p className={styles.hint}>No datasets available. Upload files first.</p>
            )}
            <div className={styles.list}>
              {datasets.map((name) => (
                <label key={name} className={styles.checkRow}>
                  <input
                    type="checkbox"
                    className={styles.checkbox}
                    checked={selected.has(name)}
                    onChange={() => toggle(name)}
                  />
                  <span className={styles.dsName}>{name}</span>
                </label>
              ))}
            </div>
          </div>

          <div className={styles.section}>
            <p className={styles.label}>Processing mode</p>
            <div className={styles.modeList}>
              {MODE_OPTIONS.map((opt) => (
                <label
                  key={opt.value}
                  className={[styles.radioRow, opt.disabled ? styles.radioDisabled : ''].join(' ')}
                >
                  <input
                    type="radio"
                    name="mode"
                    value={opt.value}
                    checked={mode === opt.value}
                    disabled={opt.disabled}
                    onChange={() => !opt.disabled && setMode(opt.value)}
                  />
                  <span className={styles.modeText}>
                    <span className={styles.modeLabel}>{opt.label}</span>
                    <span className={styles.modeDesc}>{opt.desc}</span>
                  </span>
                </label>
              ))}
            </div>
          </div>

          {submitError && <p className={styles.error}>{submitError}</p>}

          <div className={styles.actions}>
            <Button type="button" variant="ghost" onClick={handleClose}>Cancel</Button>
            <Button
              type="submit"
              variant="primary"
              loading={isPending}
              disabled={selected.size === 0}
            >
              Start parsing ({selected.size})
            </Button>
          </div>
        </form>
      )}
    </Modal>
  );
}
