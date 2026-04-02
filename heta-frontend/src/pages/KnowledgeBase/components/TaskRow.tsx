// Displays a single parse task with live-polled status, progress bar, and message.

import { useEffect } from 'react';
import { useTaskPoll } from '../../../hooks/useKnowledgeBase';
import type { TaskStatus } from '../../../types';
import { TERMINAL_STATUSES } from '../../../types';
import styles from './TaskRow.module.css';

const STATUS_LABEL: Record<TaskStatus, string> = {
  pending:    'Pending',
  running:    'Running',
  cancelling: 'Cancelling',
  completed:  'Done',
  failed:     'Failed',
  cancelled:  'Cancelled',
};

interface Props {
  taskId: string;
  datasetName: string;
  onSettled?: () => void;
}

export default function TaskRow({ taskId, datasetName, onSettled }: Props) {
  const { data: task, isLoading } = useTaskPoll(taskId);

  const status = task?.status ?? 'pending';
  // Backend progress is 0.0–1.0, convert to percentage for display
  const progressPct = Math.min(100, (task?.progress ?? 0) * 100);
  const message = task?.message || task?.error || '';

  useEffect(() => {
    if (task && TERMINAL_STATUSES.includes(task.status)) {
      onSettled?.();
    }
  }, [task?.status]);

  return (
    <div className={styles.row}>
      <div className={styles.header}>
        <span className={styles.name}>{datasetName}</span>
        <span className={[styles.badge, styles[status]].join(' ')}>
          {isLoading ? '…' : STATUS_LABEL[status]}
        </span>
      </div>

      <div className={styles.barTrack}>
        <div
          className={[styles.barFill, styles[status]].join(' ')}
          style={{ width: `${progressPct}%` }}
        />
      </div>

      {message && <p className={styles.message}>{message}</p>}
    </div>
  );
}
