// Raw dataset and file HTTP request functions

import client from '../client';
import { API } from '../endpoints';
import type { ApiResponse, DatasetFilesResponse, RawFile } from '../../types';

export async function listDatasets(): Promise<string[]> {
  const res = await client.get<{ success: boolean; data: string[] }>(API.dataset.list);
  return res.data.data;
}

export async function createDataset(name: string): Promise<void> {
  await client.post<ApiResponse>(API.dataset.create, { name });
}

export async function deleteDataset(name: string): Promise<void> {
  await client.delete(API.dataset.delete(name));
}

export async function listFiles(dataset: string): Promise<RawFile[]> {
  const res = await client.get<DatasetFilesResponse>(API.dataset.listFiles(dataset));
  return res.data.files;
}

export async function uploadFiles(dataset: string, files: File[]): Promise<void> {
  const form = new FormData();
  files.forEach((f) => form.append('files', f));
  await client.post(API.dataset.uploadFiles(dataset), form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
}

// ---------------------------------------------------------------------------
// Chunked upload
// ---------------------------------------------------------------------------

const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB per chunk

async function initUpload(dataset: string, filename: string, totalChunks: number, totalSize: number): Promise<string> {
  const res = await client.post<{ upload_id: string }>(
    API.dataset.uploadInit(dataset),
    { filename, total_chunks: totalChunks, total_size: totalSize },
  );
  return res.data.upload_id;
}

async function sendChunk(dataset: string, uploadId: string, chunkIndex: number, chunk: Blob): Promise<void> {
  await client.post(
    API.dataset.uploadChunk(dataset, uploadId),
    chunk,
    {
      params: { chunk_index: chunkIndex },
      headers: { 'Content-Type': 'application/octet-stream' },
      timeout: 0, // no timeout for chunk transfers
    },
  );
}

async function completeUpload(dataset: string, uploadId: string): Promise<void> {
  await client.post(API.dataset.uploadComplete(dataset, uploadId));
}

async function abortUpload(dataset: string, uploadId: string): Promise<void> {
  await client.delete(API.dataset.uploadAbort(dataset, uploadId));
}

/** Upload a single file in 5 MB chunks. Calls onProgress(0–100) after each chunk. */
export async function chunkedUpload(
  dataset: string,
  file: File,
  onProgress: (pct: number) => void,
): Promise<void> {
  const totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE));
  const uploadId = await initUpload(dataset, file.name, totalChunks, file.size);
  try {
    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const chunk = file.slice(start, Math.min(start + CHUNK_SIZE, file.size));
      await sendChunk(dataset, uploadId, i, chunk);
      onProgress(Math.round(((i + 1) / totalChunks) * 100));
    }
    await completeUpload(dataset, uploadId);
  } catch (e) {
    await abortUpload(dataset, uploadId).catch(() => {}); // best-effort cleanup
    throw e;
  }
}

export async function deleteFile(dataset: string, filename: string): Promise<void> {
  await client.delete(API.dataset.deleteFile(dataset, filename));
}
