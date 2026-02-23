import type { PostgrestError, SupabaseClient } from '@supabase/supabase-js';
import { config } from '../config';
import { getSupabaseClient } from '../db';
import type { Database } from '../types/supabase';
import { incrementMetricCounter, logError, logInfo } from '../utils/logger';

export type UserRecord = Database['public']['Tables']['users']['Row'];

const USERS_TABLE = 'users';
const USERS_SELECT_FIELDS =
  'id, telegram_id, username, timezone, home_chat_id, home_message_id, settings_json, created_at, updated_at';
const USER_FETCH_TIMEOUT_MS = 5000;
const USER_FETCH_MAX_RETRIES = 2;

export class UserServiceUnavailableError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'UserServiceUnavailableError';
    (this as Error & { cause?: unknown }).cause = cause;
  }
}

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

const getCauseMeta = (error: unknown): Record<string, unknown> => {
  const cause = (error as { cause?: Record<string, unknown> } | null)?.cause;
  if (!cause || typeof cause !== 'object') {
    return {};
  }

  return {
    cause_code: typeof cause.code === 'string' ? cause.code : undefined,
    cause_errno: typeof cause.errno === 'string' || typeof cause.errno === 'number' ? cause.errno : undefined,
    cause_syscall: typeof cause.syscall === 'string' ? cause.syscall : undefined,
    cause_hostname: typeof cause.hostname === 'string' ? cause.hostname : undefined,
    cause_address: typeof cause.address === 'string' ? cause.address : undefined
  };
};

const isTransientNetworkError = (error: unknown): boolean => {
  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  const causeCode = String((error as { cause?: { code?: unknown } } | null)?.cause?.code ?? '').toUpperCase();

  if (causeCode && ['ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'ECONNREFUSED'].includes(causeCode)) {
    return true;
  }

  return (
    message.includes('fetch failed') ||
    message.includes('network') ||
    message.includes('timeout') ||
    message.includes('abort')
  );
};

const handleSupabaseError = (error: unknown, action: string, reqId?: string): never => {
  const metric = incrementMetricCounter('supabase_fetch_failed_count');
  const causeMeta = getCauseMeta(error);
  const errorName = error instanceof Error ? error.name : 'UnknownError';
  const errorMessage = error instanceof Error ? error.message : String(error);

  logError('Supabase users service failure', {
    scope: 'services/users',
    event: 'supabase_error',
    action,
    error_name: errorName,
    error_message: errorMessage,
    reqId,
    supabase_host: config.supabase.host,
    supabase_fetch_failed_count: metric,
    ...causeMeta
  });

  const wrapped = new UserServiceUnavailableError(`Failed to ${action}: ${errorMessage}`, error);
  throw wrapped;
};

const runWithRetry = async <T>(action: string, operation: () => Promise<T>, reqId?: string): Promise<T> => {
  const maxAttempts = USER_FETCH_MAX_RETRIES + 1;
  let lastError: unknown;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt >= maxAttempts || !isTransientNetworkError(error)) {
        break;
      }

      const delayMs = 150 * attempt + Math.floor(Math.random() * 75);
      logInfo('Retrying transient Supabase users request', {
        scope: 'services/users',
        action,
        attempt,
        delay_ms: delayMs,
        reqId,
        supabase_host: config.supabase.host
      });
      await wait(delayMs);
    }
  }

  handleSupabaseError(lastError, action, reqId);
  throw new UserServiceUnavailableError(`Failed to ${action}`);
};

export async function getUserByTelegramId(
  telegramId: string,
  supabaseClient: SupabaseClient<Database> = getSupabaseClient(),
  reqId?: string
): Promise<UserRecord | null> {
  return runWithRetry('fetch user by telegram_id', async () => {
    try {
      const { data, error } = await withTimeout(USER_FETCH_TIMEOUT_MS, async (signal) =>
        supabaseClient
          .from(USERS_TABLE)
          .select(USERS_SELECT_FIELDS)
          .eq('telegram_id', telegramId)
          .maybeSingle()
          .abortSignal(signal)
      );

      if (error) {
        throw error;
      }

      return data ?? null;
    } catch (error) {
      throw error;
    }
  }, reqId);
}

export async function createUser(
  params: { telegramId: string; username?: string | null },
  supabaseClient: SupabaseClient<Database> = getSupabaseClient()
): Promise<UserRecord> {
  const { data, error } = await supabaseClient
    .from(USERS_TABLE)
    .insert({
      telegram_id: params.telegramId,
      username: params.username ?? null
    })
    .select(USERS_SELECT_FIELDS)
    .single();

  if (error) {
    handleSupabaseError(error, 'create user');
  }

  if (!data) {
    logError('Missing data after user creation', { scope: 'services/users', event: 'user_create_missing_data', telegramId: params.telegramId });
    throw new Error('Failed to create user: no data returned');
  }

  logInfo('User created', { scope: 'services/users', event: 'user_created', telegramId: params.telegramId, username: params.username ?? null });
  return data as UserRecord;
}

export async function ensureUser(
  params: { telegramId: string; username?: string | null },
  supabaseClient: SupabaseClient<Database> = getSupabaseClient(),
  reqId?: string
): Promise<UserRecord> {
  const telegramId = params.telegramId;
  if (!telegramId) {
    throw new Error('telegramId is required');
  }

  const existing = await getUserByTelegramId(telegramId, supabaseClient, reqId);

  if (!existing) {
    return runWithRetry('create user', () => createUser({ telegramId, username: params.username ?? null }, supabaseClient), reqId);
  }

  const shouldUpdateUsername = typeof params.username !== 'undefined' && params.username !== existing.username;

  if (!shouldUpdateUsername) {
    logInfo('User found', { scope: 'services/users', event: 'user_found', telegramId });
    return existing;
  }

  const updated = await runWithRetry('update user username', async () => {
    const { data, error } = await withTimeout(USER_FETCH_TIMEOUT_MS, async (signal) =>
      supabaseClient
        .from(USERS_TABLE)
        .update({ username: params.username ?? null, updated_at: new Date().toISOString() })
        .eq('id', existing.id)
        .select(USERS_SELECT_FIELDS)
        .single()
        .abortSignal(signal)
    );

    if (error) {
      throw error;
    }

    return data;
  }, reqId);

  if (!updated) {
    logError('Missing data after user update', { scope: 'services/users', event: 'user_update_missing_data', telegramId });
    throw new Error('Failed to reload user after update: no data returned');
  }

  logInfo('User username updated', { scope: 'services/users', event: 'user_username_updated', telegramId, username: params.username ?? null });
  return updated as UserRecord;
}

export async function updateUserSettings(
  userId: string,
  settings: Record<string, unknown>,
  supabaseClient: SupabaseClient<Database> = getSupabaseClient()
): Promise<UserRecord | null> {
  const { data, error } = await supabaseClient
    .from(USERS_TABLE)
    .update({ settings_json: settings, updated_at: new Date().toISOString() })
    .eq('id', userId)
    .select(USERS_SELECT_FIELDS)
    .maybeSingle();

  if (error) {
    handleSupabaseError(error, 'update user settings');
  }

  return (data as UserRecord | null) ?? null;
}

export type UserSettingsRecord = Database['public']['Tables']['user_settings']['Row'];

export async function ensureUserSettings(
  userId: string,
  supabaseClient: SupabaseClient<Database> = getSupabaseClient()
): Promise<UserSettingsRecord> {
  const { data, error } = await supabaseClient.from('user_settings').upsert({ user_id: userId }, { onConflict: 'user_id' }).select('*').single();
  if (error || !data) {
    handleSupabaseError(error as PostgrestError, 'ensure user settings');
  }
  return data as UserSettingsRecord;
}

export async function updateUserSettingsJson(
  userId: string,
  settingsJson: Record<string, unknown>,
  supabaseClient: SupabaseClient<Database> = getSupabaseClient()
): Promise<UserSettingsRecord> {
  const { data, error } = await supabaseClient
    .from('user_settings')
    .update({ settings_json: settingsJson, updated_at: new Date().toISOString() })
    .eq('user_id', userId)
    .select('*')
    .single();

  if (error || !data) {
    handleSupabaseError(error as PostgrestError, 'update user settings json');
  }

  return data as UserSettingsRecord;
}
