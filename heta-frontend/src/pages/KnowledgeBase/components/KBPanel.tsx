import { useState } from 'react';
import { LibraryBig, X } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useKBDetail } from '../../../hooks/useKnowledgeBase';
import type { ParseTaskRef } from '../../../api/services/kb';
import { TERMINAL_STATUSES } from '../../../types';
import Button from '../../../components/ui/Button';
import ParseModal from './ParseModal';
import TaskRow from './TaskRow';
import styles from './KBPanel.module.css';

interface Props {
  kbName: string;
  onClose: () => void;
}

export default function KBPanel({ kbName, onClose }: Props) {
  const [parseOpen, setParseOpen] = useState(false);
  const [activeTasks, setActiveTasks] = useState<ParseTaskRef[]>([]);

  const qc = useQueryClient();
  const { data: kb, isLoading, error } = useKBDetail(kbName);

  function handleTasksStarted(tasks: ParseTaskRef[]) {
    setActiveTasks((prev) => [...prev, ...tasks]);
  }

  // Refresh KB detail only when all active tasks have reached a terminal state.
  function handleTaskSettled() {
    const allDone = activeTasks.every((t) => {
      const cached = qc.getQueryData<{ status: string }>(['tasks', t.task_id]);
      return cached && TERMINAL_STATUSES.includes(cached.status as typeof TERMINAL_STATUSES[number]);
    });
    if (allDone && activeTasks.length > 0) {
      qc.invalidateQueries({ queryKey: ['kbs', kbName] });
    }
  }

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.title}>
          <LibraryBig className={styles.icon} size={16} strokeWidth={1.75} />
          <span>{kbName}</span>
        </div>
        <button className={styles.close} onClick={onClose} aria-label="Close panel">
          <X size={16} strokeWidth={1.75} />
        </button>
      </div>

      <div className={styles.toolbar}>
        <span className={styles.subtitle}>Datasets</span>
        <Button size="sm" variant="primary" onClick={() => setParseOpen(true)}>
          Parse Datasets
        </Button>
      </div>

      <div className={styles.body}>
        {isLoading && <p className={styles.placeholder}>Loading…</p>}
        {error && <p className={styles.errorMsg}>{(error as Error).message}</p>}

        {kb && kb.datasets.length === 0 && (
          <p className={styles.placeholder}>No datasets parsed into this KB yet.</p>
        )}

        {kb?.datasets.map((ds) => (
          <div key={ds.name} className={styles.dsRow}>
            <div className={styles.dsInfo}>
              <span className={styles.dsName}>{ds.name}</span>
              {ds.parsed_at && (
                <span className={styles.dsMeta}>
                  {new Date(ds.parsed_at).toLocaleString('zh-CN', {
                    month: '2-digit', day: '2-digit',
                    hour: '2-digit', minute: '2-digit',
                  })}
                </span>
              )}
            </div>
            <span className={[styles.parsedBadge, ds.parsed ? styles.parsed : styles.unparsed].join(' ')}>
              {ds.parsed ? 'Parsed' : 'Not parsed'}
            </span>
          </div>
        ))}

        {activeTasks.length > 0 && (
          <div className={styles.tasksSection}>
            <p className={styles.tasksLabel}>Processing</p>
            {activeTasks.map((t) => (
              <TaskRow
                key={t.task_id}
                taskId={t.task_id}
                datasetName={t.dataset}
                onSettled={handleTaskSettled}
              />
            ))}
          </div>
        )}
      </div>

      <ParseModal
        open={parseOpen}
        kbName={kbName}
        onClose={() => setParseOpen(false)}
        onTasksStarted={handleTasksStarted}
      />
    </div>
  );
}
