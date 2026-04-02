// Dataset state management — wraps react-query and dataset service calls

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import * as datasetService from '../api/services/dataset';

const KEYS = {
  datasets: ['datasets'] as const,
  files: (name: string) => ['datasets', name, 'files'] as const,
};

// --- Dataset list ---

export function useDatasets() {
  return useQuery({
    queryKey: KEYS.datasets,
    queryFn: datasetService.listDatasets,
  });
}

export function useCreateDataset() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => datasetService.createDataset(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEYS.datasets }),
  });
}

export function useDeleteDataset() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => datasetService.deleteDataset(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEYS.datasets }),
  });
}

// --- Files ---

export function useDatasetFiles(dataset: string | null) {
  return useQuery({
    queryKey: KEYS.files(dataset ?? ''),
    queryFn: () => datasetService.listFiles(dataset!),
    enabled: dataset !== null,
  });
}

export function useDeleteFile(dataset: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (filename: string) => datasetService.deleteFile(dataset, filename),
    onSuccess: () => qc.invalidateQueries({ queryKey: KEYS.files(dataset) }),
  });
}
