import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { Pool } from 'pg';
import { config } from './config';
import { type Database } from './types/supabase';

let supabaseClient: SupabaseClient<Database> | null = null;
let pgPool: Pool | null = null;

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const withTimeout = async <T>(ms: number, run: (signal: AbortSignal) => Promise<T>): Promise<T> => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), ms);
  try {
    return await run(controller.signal);
  } finally {
    clearTimeout(timeout);
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
      await withTimeout(3000, async (signal) => {
        const { error } = await client.from('users').select('id', { count: 'exact', head: true }).limit(1).abortSignal(signal);
        if (error) {
          throw error;
        }
      });
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
