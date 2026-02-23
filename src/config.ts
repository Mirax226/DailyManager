import 'dotenv/config';
import { z } from 'zod';

const envSchema = z.object({
  HOST: z.string().default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().default(3000),
  DEV_POLLING: z.coerce.boolean().default(false),
  TELEGRAM_BOT_TOKEN: z.string().min(1, 'TELEGRAM_BOT_TOKEN is required'),
  TELEGRAM_ADMIN_ID: z.string().optional(),
  ADMIN_TELEGRAM_ID: z.string().optional(),
  TELEGRAM_WEBHOOK_URL: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().url().optional()),
  CRON_SECRET: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().regex(/^[A-Za-z0-9_-]+$/, 'CRON_SECRET must be URL-safe').optional()),
  CRON_MAX_BATCH: z.coerce.number().int().positive().default(20),
  CRON_MAX_RUNTIME_MS: z.coerce.number().int().positive().default(20000),
  TELEGRAM_SEND_DELAY_MS: z.coerce.number().int().nonnegative().default(0),
  SUPABASE_URL: z
    .string()
    .url('SUPABASE_URL must be a valid URL')
    .refine((value) => value.startsWith('https://'), 'SUPABASE_URL must start with https://'),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1, 'SUPABASE_SERVICE_ROLE_KEY is required'),
  SUPABASE_DB_CONNECTION: z.string().optional(),
  SUPABASE_DB_CONNECTION_STRING: z.string().optional(),
  SUPABASE_DSN_DAILY_SYSTEM: z.string().optional(),
  DEFAULT_TIMEZONE: z.string().min(1, 'DEFAULT_TIMEZONE is required'),
  ARCHIVE_ENABLED: z.coerce.boolean().default(true),
  ARCHIVE_MODE: z.string().default('channel'),
  ARCHIVE_CHANNEL_ID: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().optional()),
  ARCHIVE_CHAT_ID: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().optional()),
  ARCHIVE_MAX_CHUNK: z.coerce.number().int().positive().default(3500),
  LOG_REPORTER_ENABLED: z.coerce.boolean().default(true),
  LOG_LEVELS: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().default('error')),
  PATH_APPLIER_LOG_INGEST_URL: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().url().optional()),
  PATH_APPLIER_LOG_INGEST_KEY: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().optional()),
  PROJECT_ID: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().default('daily-system')),
  SERVICE_NAME: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().optional()),
  APP_ENV: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().default('production')),
  RENDER_SERVICE_NAME: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().optional()),
  RENDER_SERVICE_ID: z
    .preprocess((value: unknown) => {
      if (typeof value !== 'string') return undefined;
      const trimmed = value.trim();
      return trimmed.length === 0 ? undefined : trimmed;
    }, z.string().optional()),
  UI_EMOJI_ENABLED: z.coerce.boolean().default(true),
  DB_MIGRATIONS_ENABLED: z.coerce.boolean().default(true),
  RUN_BACKFILL: z.coerce.boolean().default(false)
});

const env = envSchema.parse(process.env);
const supabaseUrl = new URL(env.SUPABASE_URL);

export const config = {
  server: {
    host: env.HOST,
    port: env.PORT
  },
  telegram: {
    botToken: env.TELEGRAM_BOT_TOKEN,
    adminId: env.ADMIN_TELEGRAM_ID ?? env.TELEGRAM_ADMIN_ID,
    webhookUrl: env.TELEGRAM_WEBHOOK_URL,
    devPolling: env.DEV_POLLING
  },
  cron: {
    secret: env.CRON_SECRET,
    maxBatch: env.CRON_MAX_BATCH,
    maxRuntimeMs: env.CRON_MAX_RUNTIME_MS,
    telegramSendDelayMs: env.TELEGRAM_SEND_DELAY_MS
  },
  supabase: {
    url: supabaseUrl.toString(),
    host: supabaseUrl.host,
    serviceRoleKey: env.SUPABASE_SERVICE_ROLE_KEY
  },
  db: {
    connectionString:
      env.SUPABASE_DB_CONNECTION ?? env.SUPABASE_DB_CONNECTION_STRING ?? env.SUPABASE_DSN_DAILY_SYSTEM,
    migrationsEnabled: env.DB_MIGRATIONS_ENABLED,
    runBackfill: env.RUN_BACKFILL
  },
  defaultTimezone: env.DEFAULT_TIMEZONE,
  archive: {
    enabled: env.ARCHIVE_ENABLED,
    mode: env.ARCHIVE_MODE,
    channelId: env.ARCHIVE_CHANNEL_ID ?? env.ARCHIVE_CHAT_ID,
    maxChunk: env.ARCHIVE_MAX_CHUNK
  },
  logReporter: {
    enabled: env.LOG_REPORTER_ENABLED,
    levels: env.LOG_LEVELS,
    ingestUrl: env.PATH_APPLIER_LOG_INGEST_URL,
    ingestKey: env.PATH_APPLIER_LOG_INGEST_KEY,
    projectId: env.PROJECT_ID,
    serviceName:
      env.SERVICE_NAME ??
      env.RENDER_SERVICE_NAME ??
      env.RENDER_SERVICE_ID ??
      process.env.npm_package_name ??
      'daily-system-bot-v2',
    env: env.APP_ENV
  },
  ui: {
    emojiEnabled: env.UI_EMOJI_ENABLED
  },
  build: {
    version: process.env.npm_package_version ?? 'unknown'
  }
};
