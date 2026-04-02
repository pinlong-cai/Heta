// Knowledge base and task HTTP request functions

import client from '../client';
import { API } from '../endpoints';
import type {
  ApiResponse,
  KnowledgeBase,
  KnowledgeBaseDetail,
  Task,
} from '../../types';

// --- Knowledge bases ---

export async function listKBs(): Promise<KnowledgeBase[]> {
  const res = await client.get<{ success: boolean; data: KnowledgeBase[] }>(API.kb.list);
  return res.data.data;
}

export async function createKB(name: string): Promise<void> {
  await client.post<ApiResponse>(API.kb.create, { name });
}

export async function getKBDetail(name: string): Promise<KnowledgeBaseDetail> {
  const res = await client.get<KnowledgeBaseDetail & { success: boolean }>(API.kb.detail(name));
  return res.data;
}

export async function deleteKB(name: string, purgeDb = true): Promise<void> {
  await client.delete(API.kb.delete(name), { params: { purge_db: purgeDb } });
}

export interface ParseRequest {
  datasets: string[];
  mode: number;
  schema_name?: string;
  force?: boolean;
}

export interface ParseTaskRef {
  task_id: string;
  dataset: string;
}

export interface ConflictInfo {
  dataset: string;
  parsed_at: string | null;
  process_mode: number | null;
}

export class ParseConflictError extends Error {
  readonly conflicts: ConflictInfo[];
  constructor(message: string, conflicts: ConflictInfo[]) {
    super(message);
    this.name = 'ParseConflictError';
    this.conflicts = conflicts;
  }
}

export async function parseKB(
  kbName: string,
  req: ParseRequest,
): Promise<ParseTaskRef[]> {
  // Allow 409 through so we can inspect the conflict payload before throwing.
  const res = await client.post<ApiResponse<{ tasks: ParseTaskRef[]; mode: number }>>(
    API.kb.parse(kbName),
    req,
    { validateStatus: (status) => status < 500 },
  );
  if (res.status === 409) {
    const detail = (res.data as unknown as { detail: { message: string; conflicts: ConflictInfo[] } }).detail;
    throw new ParseConflictError(
      detail?.message ?? 'Some datasets have already been parsed.',
      detail?.conflicts ?? [],
    );
  }
  return res.data.data.tasks;
}

// --- Tasks ---

export async function listTasks(status?: string, limit = 100): Promise<Task[]> {
  const res = await client.get<Task[]>(API.task.list, { params: { status, limit } });
  return res.data;
}

export async function getTask(taskId: string): Promise<Task> {
  const res = await client.get<Task>(API.task.detail(taskId));
  return res.data;
}

export async function cancelTask(taskId: string): Promise<void> {
  await client.post(API.task.cancel(taskId));
}
