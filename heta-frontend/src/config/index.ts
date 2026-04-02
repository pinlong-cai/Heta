// Central app configuration — edit apiBaseUrl for production deployment

const config = {
  // Dev: leave empty, Vite proxy forwards /api → localhost:8000
  // Prod: set to backend URL, e.g. 'http://10.0.0.1:8000'
  apiBaseUrl: '',
  apiTimeout: 300_000, // 5 min — allow time for CPU reranker inference
} as const;

export default config;
