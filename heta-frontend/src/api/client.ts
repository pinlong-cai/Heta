// Axios instance — base config and interceptors

import axios from 'axios';
import config from '../config';

const client = axios.create({
  baseURL: config.apiBaseUrl,
  timeout: config.apiTimeout,
  headers: { 'Content-Type': 'application/json' },
});

// Response interceptor: unwrap data or surface error message
client.interceptors.response.use(
  (res) => res,
  (err) => {
    const message = err.response?.data?.message ?? err.message ?? 'Unknown error';
    return Promise.reject(new Error(message));
  },
);

export default client;
