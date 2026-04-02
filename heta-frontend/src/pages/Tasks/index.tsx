import { useState } from 'react';
import { useTaskList, useCancelTask } from '../../hooks/useKnowledgeBase';
import type { Task, TaskStatus } from '../../types';
import { TERMINAL_STATUSES } from '../../types';
import styles from './Tasks.module.css';

const STATUS_LABEL: Record<TaskStatus, string> = {
  pending:    'Pending',
  running:    'Running',
  cancelling: 'Cancelling',
  completed:  'Done',
  failed:     'Failed',
  cancelled:  'Cancelled',
};

const FILTERS: { value: string | undefined; label: string }[] = [
  { value: undefined,     label: 'All'        },
  { value: 'running',     label: 'Running'    },
  { value: 'cancelling',  label: 'Cancelling' },
  { value: 'pending',     label: 'Pending'    },
  { value: 'completed',   label: 'Completed'  },
  { value: 'failed',      label: 'Failed'     },
];

function formatTime(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

function TaskItem({ task }: { task: Task }) {
  const { mutate: cancel, isPending } = useCancelTask();
  const [confirmCancel, setConfirmCancel] = useState(false);
  const meta = task.metadata as Record<string, string>;
  const progressPct = Math.min(100, (task.progress ?? 0) * 100);

  // pending: immediate cancel; running: needs confirmation
  const canCancel = task.status === 'pending' || task.status === 'running';

  function handleCancelClick() {
    if (task.status === 'running') {
      setConfirmCancel(true);
    } else {
      cancel(task.task_id);
    }
  }

  return (
    <div className={styles.item}>
      <div className={styles.itemTop}>
        <div className={styles.itemMeta}>
          <span className={styles.itemDataset}>{meta.dataset ?? '—'}</span>
          {meta.kb_name && <span className={styles.itemKb}>→ {meta.kb_name}</span>}
          {meta.mode !== undefined && <span className={styles.itemMode}>mode {meta.mode}</span>}
        </div>
        <div className={styles.itemRight}>
          <span className={[styles.badge, styles[task.status]].join(' ')}>
            {STATUS_LABEL[task.status]}
          </span>

          {confirmCancel ? (
            <div className={styles.confirmRow}>
              <span className={styles.confirmText}>Stop this task?</span>
              <button
                className={styles.confirmYes}
                onClick={() => { cancel(task.task_id); setConfirmCancel(false); }}
                disabled={isPending}
              >
                {isPending ? '…' : 'Yes'}
              </button>
              <button className={styles.confirmNo} onClick={() => setConfirmCancel(false)}>
                No
              </button>
            </div>
          ) : canCancel && (
            <button
              className={styles.cancelBtn}
              onClick={handleCancelClick}
              disabled={isPending}
            >
              Cancel
            </button>
          )}
        </div>
      </div>

      <div className={styles.barTrack}>
        <div
          className={[styles.barFill, styles[task.status]].join(' ')}
          style={{ width: `${progressPct}%` }}
        />
      </div>

      <div className={styles.itemBottom}>
        <span className={styles.itemMsg}>{task.error ?? task.message ?? ''}</span>
        <span className={styles.itemTime}>
          {TERMINAL_STATUSES.includes(task.status)
            ? formatTime(task.completed_at)
            : formatTime(task.started_at ?? task.created_at)}
        </span>
      </div>
    </div>
  );
}

export default function TasksPage() {
  const [filter, setFilter] = useState<string | undefined>(undefined);
  const { data: tasks = [], isLoading } = useTaskList(filter);

  return (
    <div className={styles.page}>
      <div className={styles.inner}>
        <div className={styles.header}>
          <h1 className={styles.title}>Tasks</h1>
          <p className={styles.subtitle}>Processing jobs submitted to the HetaDB pipeline.</p>
        </div>

        <div className={styles.toolbar}>
          <div className={styles.filters}>
            {FILTERS.map((f) => (
              <button
                key={f.label}
                className={[styles.filterBtn, filter === f.value ? styles.filterActive : ''].join(' ')}
                onClick={() => setFilter(f.value)}
              >
                {f.label}
              </button>
            ))}
          </div>
          <span className={styles.count}>{tasks.length} task{tasks.length !== 1 ? 's' : ''}</span>
        </div>

        {isLoading ? (
          <p className={styles.empty}>Loading…</p>
        ) : tasks.length === 0 ? (
          <p className={styles.empty}>No tasks found.</p>
        ) : (
          <div className={styles.list}>
            {tasks.map((t) => <TaskItem key={t.task_id} task={t} />)}
          </div>
        )}
      </div>
    </div>
  );
}
