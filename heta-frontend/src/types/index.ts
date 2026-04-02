// Shared TypeScript types

// --- Dataset ---
export interface RawFile {
  name: string;
  size: number;        // bytes
  modified_time: string;
}

export interface DatasetFilesResponse {
  success: boolean;
  dataset: string;
  files: RawFile[];
}

// --- Task ---
export interface Task {
  task_id: string;
  task_type: string;
  status: TaskStatus;
  progress: number;        // 0–100
  message: string;
  error: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export type TaskStatus = 'pending' | 'running' | 'cancelling' | 'completed' | 'failed' | 'cancelled';

export const TERMINAL_STATUSES: TaskStatus[] = ['completed', 'failed', 'cancelled'];

// --- Knowledge Base ---
export interface KnowledgeBase {
  name: string;
  created_at: string | null;
  status?: string;  // 'ready' | 'deleting'
}

export interface KBDataset {
  name: string;
  parsed: boolean;
  process_mode: number | null;
  parsed_at: string | null;
}

export interface QueryMode {
  id: string;
  label: string;
  desc: string;
}

export interface KnowledgeBaseDetail {
  name: string;
  created_at: string | null;
  datasets: KBDataset[];
  available_query_modes: QueryMode[];
}

// --- Generic API envelope ---
export interface ApiResponse<T = unknown> {
  success: boolean;
  message: string;
  data: T;
}
