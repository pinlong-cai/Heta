import { useMemo, useState } from 'react';
import PageShell from '../../components/layout/PageShell';
import Button from '../../components/ui/Button';
import KBCard from './components/KBCard';
import CreateKBModal from './components/CreateKBModal';
import KBPanel from './components/KBPanel';
import Pagination from '../Dataset/components/Pagination';
import { useKBList, useTaskList } from '../../hooks/useKnowledgeBase';
import * as kbService from '../../api/services/kb';
import { useQueryClient } from '@tanstack/react-query';
import { TERMINAL_STATUSES } from '../../types';
import styles from './KnowledgeBase.module.css';

const PAGE_SIZE = 15;

export default function KnowledgeBasePage() {
  const [modalOpen, setModalOpen] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const { data: kbs = [], isLoading, error } = useKBList();
  const { data: tasks = [] } = useTaskList();
  const qc = useQueryClient();

  const activeTasksByKB = useMemo(() => {
    const map: Record<string, number> = {};
    for (const t of tasks) {
      if (TERMINAL_STATUSES.includes(t.status)) continue;
      const kb = t.metadata?.kb_name as string | undefined;
      if (kb) map[kb] = (map[kb] ?? 0) + 1;
    }
    return map;
  }, [tasks]);

  async function handleDeleteKB(name: string) {
    if (selected === name) setSelected(null);
    await kbService.deleteKB(name, true);
    qc.invalidateQueries({ queryKey: ['kbs'] });
  }

  const totalPages = Math.max(1, Math.ceil(kbs.length / PAGE_SIZE));
  const pageKBs = kbs.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  function handleSelect(name: string) {
    setSelected((prev) => (prev === name ? null : name));
  }

  function handlePageChange(next: number) {
    setPage(next);
    setSelected(null);
  }

  return (
    <div className={styles.layout}>
      <div className={styles.main}>
        <PageShell
          title="Knowledge Bases"
          actions={
            <Button variant="primary" onClick={() => setModalOpen(true)}>
              + New Knowledge Base
            </Button>
          }
        >
          {isLoading && <p className={styles.placeholder}>Loading…</p>}
          {error && <p className={styles.error}>{(error as Error).message}</p>}
          {!isLoading && !error && kbs.length === 0 && (
            <p className={styles.placeholder}>
              No knowledge bases yet. Create one to get started.
            </p>
          )}
          <div className={styles.grid}>
            {pageKBs.map((kb) => (
              <KBCard
                key={kb.name}
                name={kb.name}
                createdAt={kb.created_at}
                status={kb.status}
                selected={selected === kb.name}
                activeTasks={activeTasksByKB[kb.name] ?? 0}
                onSelect={() => handleSelect(kb.name)}
                onDelete={() => handleDeleteKB(kb.name)}
              />
            ))}
          </div>
          <Pagination page={page} totalPages={totalPages} onChange={handlePageChange} />
        </PageShell>
      </div>

      {selected && (
        <KBPanel kbName={selected} onClose={() => setSelected(null)} />
      )}

      <CreateKBModal open={modalOpen} onClose={() => setModalOpen(false)} />
    </div>
  );
}
