// Knowledge base and task state management

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import * as kbService from '../api/services/kb';
import type { ParseRequest } from '../api/services/kb';
import { TERMINAL_STATUSES } from '../types';

const KEYS = {
  kbs: ['kbs'] as const,
  kbDetail: (name: string) => ['kbs', name] as const,
  task: (id: string) => ['tasks', id] as const,
};

// --- KB list ---

export function useKBList() {
  return useQuery({
    queryKey: KEYS.kbs,
    queryFn: kbService.listKBs,
    refetchInterval: (query) => {
      const kbs = query.state.data ?? [];
      return kbs.some((kb) => kb.status === 'deleting') ? 3000 : false;
    },
  });
}

export function useCreateKB() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => kbService.createKB(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEYS.kbs }),
  });
}

// --- KB detail ---

export function useKBDetail(name: string | null) {
  return useQuery({
    queryKey: KEYS.kbDetail(name ?? ''),
    queryFn: () => kbService.getKBDetail(name!),
    enabled: name !== null,
  });
}

// --- Parse ---

export function useParseKB(kbName: string) {
  return useMutation<kbService.ParseTaskRef[], Error, ParseRequest>({
    mutationFn: (req: ParseRequest) => kbService.parseKB(kbName, req),
  });
}

// --- Task list ---
// Polls the full task list every 3s when any task is still active.

export function useTaskList(status?: string) {
  return useQuery({
    queryKey: ['tasks', 'list', status ?? 'all'],
    queryFn: () => kbService.listTasks(status),
    refetchInterval: (query) => {
      const tasks = query.state.data ?? [];
      const hasActive = tasks.some((t) => !TERMINAL_STATUSES.includes(t.status));
      return hasActive ? 3000 : 10000;
    },
  });
}

export function useCancelTask() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (taskId: string) => kbService.cancelTask(taskId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tasks'] }),
  });
}

// --- Task polling ---
// Polls a single task every 2s until it reaches a terminal state.

export function useTaskPoll(taskId: string | null) {
  return useQuery({
    queryKey: KEYS.task(taskId ?? ''),
    queryFn: () => kbService.getTask(taskId!),
    enabled: taskId !== null,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (!status) return 2000;
      return TERMINAL_STATUSES.includes(status) ? false : 2000;
    },
  });
}
