import { useRef, useState } from 'react';
import { Database, X, Trash2, ChevronDown, ChevronUp } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useDatasetFiles, useDeleteFile } from '../../../hooks/useDataset';
import { chunkedUpload } from '../../../api/services/dataset';
import Pagination from './Pagination';
import Button from '../../../components/ui/Button';
import styles from './FilePanel.module.css';

interface UploadItem {
  id: string;
  name: string;
  progress: number;
  status: 'uploading' | 'done' | 'error';
  error?: string;
}

const PAGE_SIZE = 20;

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
}

interface Props {
  dataset: string;
  onClose: () => void;
}

export default function FilePanel({ dataset, onClose }: Props) {
  const [page, setPage] = useState(1);
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [bulkOpen, setBulkOpen] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const qc = useQueryClient();

  const { data: files = [], isLoading, error } = useDatasetFiles(dataset);
  const { mutate: deleteFile } = useDeleteFile(dataset);

  const totalPages = Math.max(1, Math.ceil(files.length / PAGE_SIZE));
  const pageFiles = files.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);
  const isUploading = uploads.some((u) => u.status === 'uploading');

  function updateUpload(id: string, patch: Partial<UploadItem>) {
    setUploads((prev) => prev.map((u) => (u.id === id ? { ...u, ...patch } : u)));
  }

  async function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const selected = Array.from(e.target.files ?? []);
    e.target.value = '';
    if (!selected.length) return;

    const items: UploadItem[] = selected.map((f) => ({
      id: crypto.randomUUID(),
      name: f.name,
      progress: 0,
      status: 'uploading',
    }));
    setUploads((prev) => [...prev, ...items]);

    await Promise.all(
      selected.map((file, i) => {
        const item = items[i];
        return chunkedUpload(dataset, file, (pct) => updateUpload(item.id, { progress: pct }))
          .then(() => {
            updateUpload(item.id, { status: 'done', progress: 100 });
            qc.invalidateQueries({ queryKey: ['datasets', dataset, 'files'] });
            // Auto-remove successful entries after 2s
            setTimeout(() => setUploads((prev) => prev.filter((u) => u.id !== item.id)), 2000);
          })
          .catch((err: Error) => {
            updateUpload(item.id, { status: 'error', error: err.message });
          });
      }),
    );
    setPage(1);
  }

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.title}>
          <Database className={styles.icon} size={16} strokeWidth={1.75} />
          <span>{dataset}</span>
        </div>
        <button className={styles.close} onClick={onClose} aria-label="Close">
          <X size={16} strokeWidth={1.75} />
        </button>
      </div>

      <div className={styles.toolbar}>
        <span className={styles.count}>
          {isLoading ? '…' : `${files.length} file${files.length !== 1 ? 's' : ''}`}
        </span>
        <Button
          size="sm"
          variant="primary"
          loading={isUploading}
          onClick={() => fileInput.current?.click()}
        >
          Upload
        </Button>
        <input ref={fileInput} type="file" multiple hidden onChange={handleFileChange} />
      </div>

      {uploads.length > 0 && (
        <div className={styles.uploadQueue}>
          {uploads.map((u) => (
            <div key={u.id} className={styles.uploadItem}>
              <div className={styles.uploadHeader}>
                <span className={styles.uploadName}>{u.name}</span>
                <span className={[styles.uploadBadge, styles[u.status]].join(' ')}>
                  {u.status === 'uploading' ? `${u.progress}%` : u.status === 'done' ? 'Done' : 'Failed'}
                </span>
                {u.status === 'error' && (
                  <button
                    className={styles.uploadDismiss}
                    onClick={() => setUploads((prev) => prev.filter((x) => x.id !== u.id))}
                  >
                    <X size={12} strokeWidth={2} />
                  </button>
                )}
              </div>
              <div className={styles.uploadTrack}>
                <div
                  className={[styles.uploadFill, styles[u.status]].join(' ')}
                  style={{ width: `${u.progress}%` }}
                />
              </div>
              {u.error && <p className={styles.uploadError}>{u.error}</p>}
            </div>
          ))}
        </div>
      )}

      <div className={styles.body}>
        {isLoading && <p className={styles.placeholder}>Loading…</p>}
        {error && <p className={styles.errorMsg}>{(error as Error).message}</p>}
        {!isLoading && !error && files.length === 0 && (
          <p className={styles.placeholder}>No files yet. Upload to get started.</p>
        )}
        {pageFiles.map((file) => (
          <div key={file.name} className={styles.row}>
            <div className={styles.fileInfo}>
              <span className={styles.fileName}>{file.name}</span>
              <span className={styles.fileMeta}>
                {formatBytes(file.size)} · {formatDate(file.modified_time)}
              </span>
            </div>
            <button
              className={styles.deleteFile}
              onClick={() => deleteFile(file.name)}
              aria-label={`Delete ${file.name}`}
            >
              <Trash2 size={14} strokeWidth={1.75} />
            </button>
          </div>
        ))}
      </div>

      <div className={styles.bulkHint}>
        <button className={styles.bulkToggle} onClick={() => setBulkOpen((o) => !o)}>
          <span>Uploading a large folder?</span>
          {bulkOpen
            ? <ChevronUp size={12} strokeWidth={2} />
            : <ChevronDown size={12} strokeWidth={2} />}
        </button>
        {bulkOpen && (
          <div className={styles.bulkCard}>
            <p className={styles.bulkDesc}>
              For bulk folder imports, copy files directly to the server instead of uploading through the browser.
            </p>
            <div className={styles.bulkStep}>
              <span className={styles.bulkLabel}>Target path</span>
              <code className={styles.bulkCode}>workspace/raw_files/{dataset}/</code>
            </div>
            <div className={styles.bulkStep}>
              <span className={styles.bulkLabel}>Example</span>
              <code className={styles.bulkCode}>cp -r ./my_folder/ workspace/raw_files/{dataset}/</code>
            </div>
            <p className={styles.bulkNote}>
              After copying, refresh the file list to see the new files.
            </p>
          </div>
        )}
      </div>

      <div className={styles.footer}>
        <Pagination page={page} totalPages={totalPages} onChange={setPage} />
      </div>
    </div>
  );
}
