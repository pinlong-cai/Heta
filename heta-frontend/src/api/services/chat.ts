// Chat HTTP request functions

import client from '../client';
import { API } from '../endpoints';

export interface QueryResult {
  kb_id: string;
  kb_name: string;
  score: number;
  content: string;
  text: string;
  source_id: string[];
}

export interface ChatRequest {
  query: string;
  kb_id: string;
  user_id: string;
  max_results?: number;
  query_mode?: string;
}

export interface Citation {
  index: number;
  source_file: string;
  dataset: string;
  file_url: string | null;
}

export interface ChatResponse {
  success: boolean;
  message: string;
  data: QueryResult[];
  total_count: number;
  query_info: Record<string, unknown>;
  request_id: string;
  code: number;
  response: string | null;
  citations: Citation[] | null;
}

export async function sendMessage(req: ChatRequest): Promise<ChatResponse> {
  const res = await client.post<ChatResponse>(API.chat, {
    query: req.query,
    kb_id: req.kb_id,
    user_id: req.user_id,
    max_results: req.max_results ?? 20,
    query_mode: req.query_mode ?? 'naive',
  });
  return res.data;
}
