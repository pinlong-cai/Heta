import { useState } from 'react';
import PageShell from '../../components/layout/PageShell';
import Button from '../../components/ui/Button';
import DatasetCard from './components/DatasetCard';
import CreateDatasetModal from './components/CreateDatasetModal';
import FilePanel from './components/FilePanel';
import Pagination from './components/Pagination';
import { useDatasets } from '../../hooks/useDataset';
import styles from './Dataset.module.css';

const PAGE_SIZE = 15;

export default function DatasetPage() {
  const [modalOpen, setModalOpen] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const { data: datasets = [], isLoading, error } = useDatasets();

  const totalPages = Math.max(1, Math.ceil(datasets.length / PAGE_SIZE));
  const pageDatasets = datasets.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

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
          title="Datasets"
          actions={
            <Button variant="primary" onClick={() => setModalOpen(true)}>
              + New Dataset
            </Button>
          }
        >
          {isLoading && <p className={styles.placeholder}>Loading…</p>}
          {error && <p className={styles.error}>{(error as Error).message}</p>}
          {!isLoading && !error && datasets.length === 0 && (
            <p className={styles.placeholder}>
              No datasets yet. Create one to get started.
            </p>
          )}
          <div className={styles.grid}>
            {pageDatasets.map((name) => (
              <DatasetCard
                key={name}
                name={name}
                selected={selected === name}
                onSelect={() => handleSelect(name)}
                onDelete={() => { if (selected === name) setSelected(null); }}
              />
            ))}
          </div>
          <Pagination page={page} totalPages={totalPages} onChange={handlePageChange} />
        </PageShell>
      </div>

      {selected && (
        <FilePanel dataset={selected} onClose={() => setSelected(null)} />
      )}

      <CreateDatasetModal open={modalOpen} onClose={() => setModalOpen(false)} />
    </div>
  );
}
