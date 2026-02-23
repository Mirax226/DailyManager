import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { Pool } from 'pg';
import { config } from './config';
import { type Database } from './types/supabase';

let supabaseClient: SupabaseClient<Database> | null = null;
let pgPool: Pool | null = null;

type PromiseResult<T extends PromiseLike<unknown>> = T extends PromiseLike<infer R> ? R : never;

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const withTimeout = async <T>(promise: PromiseLike<T>, ms: number): Promise<T> => {
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new Error(`Operation timed out after ${ms}ms`));
    }, ms);
  });

  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  }
};

export function getSupabaseClient(): SupabaseClient<Database> {
  if (supabaseClient) {
    return supabaseClient;
  }

  const { url, serviceRoleKey } = config.supabase;

  if (!url || !serviceRoleKey) {
    throw new Error('Supabase configuration is missing URL or service role key');
  }

  supabaseClient = createClient<Database>(url, serviceRoleKey);
  return supabaseClient;
}

export async function pingSupabase(): Promise<void> {
  const client = getSupabaseClient();
  const maxAttempts = 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const query = client.from('users').select('id', { count: 'exact', head: true }).limit(1);
      const result = await withTimeout<PromiseResult<typeof query>>(query, 3000);
      if (result.error) {
        throw result.error;
      }
      return;
    } catch (error) {
      if (attempt >= maxAttempts) {
        throw error;
      }
      await wait(250 * attempt);
    }
  }
}

export function getDbPool(): Pool {
  if (pgPool) {
    return pgPool;
  }

  const connectionString = config.db.connectionString;

  if (!connectionString) {
    throw new Error('Database connection string is missing');
  }

  pgPool = new Pool({ connectionString });
  return pgPool;
}

export async function queryDb<T = any>(
  sql: string,
  params: unknown[] = [],
): Promise<{ rows: T[] }> {
  const pool = getDbPool();
  const result = await pool.query(sql, params);
  return { rows: result.rows as T[] };
}
