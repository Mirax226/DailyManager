import Fastify, { type FastifyReply, type FastifyRequest } from 'fastify';
import { GrammyError, webhookCallback } from 'grammy';
import { bot } from './bot';
import { config } from './config';
import { getDbPool, getSupabaseClient, pingSupabase } from './db';
import { schemaSync } from './db/schemaSync';
import { resolveArchiveChatId, setArchiveRuntimeStatus, validateArchiveChannel } from './services/archive';
import { getCronHealth, isCronAuthorized, runCronTick } from './services/cron.service';
import { initLogReporter } from './services/log_reporter';
import { backfillReminderAttachmentFileIds } from './services/reminders';
import { logError } from './utils/logger';

const server = Fastify({ logger: true });
const logReporter = initLogReporter();

server.setErrorHandler((err, request, reply) => {
  try {
    logError('HTTP request error', {
      method: request.method,
      url: request.url,
      statusCode: reply.statusCode,
      error: err.message,
      stack: err.stack
    });
  } catch (logErrorErr) {
    request.log.error({ err: logErrorErr }, 'Failed to log HTTP request error.');
  }

  reply.status(500).send({ ok: false });
});

server.addHook('onError', async (request, reply, error) => {
  await logReporter.report('error', 'Fastify request error', {
    stack: error instanceof Error ? error.stack : undefined,
    context: {
      reqId: request.id,
      route: request.routerPath ?? request.url,
      method: request.method,
      statusCode: reply.statusCode
    }
  });
});

process.on('unhandledRejection', (reason) => {
  logError('Unhandled promise rejection', { reason });
  void logReporter.report('error', 'Unhandled promise rejection', {
    context: { reason: reason instanceof Error ? reason.message : String(reason) },
    stack: reason instanceof Error ? reason.stack : undefined
  });
});

process.on('uncaughtException', (err) => {
  logError('Uncaught exception', { error: err.message, stack: err.stack });
  void logReporter.report('error', 'Uncaught exception', {
    context: { error: err.message },
    stack: err.stack
  });
});

server.get('/health', async () => {
  return { ok: true };
});

server.get('/', async () => {
  return { ok: true, service: 'daily-system', ts: new Date().toISOString() };
});

const telegramWebhookHandler = webhookCallback(bot, 'fastify');

server.post('/webhook', async (request, reply) => {
  try {
    await telegramWebhookHandler(request, reply);
  } catch (error) {
    const reqId = request.id;
    logError('Telegram webhook processing failed', {
      reqId,
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined
    });

    await logReporter.report('error', 'Telegram webhook processing failed', {
      stack: error instanceof Error ? error.stack : undefined,
      context: {
        reqId,
        method: request.method,
        route: request.routerPath ?? request.url
      }
    });

    if (!reply.sent) {
      reply.code(200).send({ ok: true });
    }
  }
});

server.get(
  '/cron/tick',
  async (
    request: FastifyRequest<{ Querystring: { key?: string } }>,
    reply: FastifyReply
  ) => {
    if (!isCronAuthorized(request.query.key)) {
      reply.code(401);
      return { ok: false, error: 'unauthorized', time: new Date().toISOString() };
    }

    const time = new Date().toISOString();
    try {
      const result = await runCronTick({ key: request.query.key, botClient: bot });
      reply.code(result.ok ? 200 : 500);
      return {
        ok: result.ok,
        processed: result.ok ? result.claimed : 0,
        sent: result.ok ? result.sent : 0,
        failed: result.ok ? result.failed : 0,
        duration_ms: result.ok ? result.duration_ms : 0,
        version: config.build.version,
        time,
        error: result.ok ? undefined : result.error
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      reply.code(500);
      return {
        ok: false,
        processed: 0,
        sent: 0,
        failed: 1,
        duration_ms: 0,
        version: config.build.version,
        time,
        error: message
      };
    }
  }
);

server.get('/cron/health', async () => {
  const health = await getCronHealth();
  return { ok: true, ...health };
});

const isTelegramTooManyRequests = (error: unknown): boolean => {
  if (error instanceof GrammyError) {
    return error.error_code === 429;
  }

  if (typeof error === 'object' && error !== null && 'error_code' in error) {
    const errorCode = (error as { error_code?: number }).error_code;
    return errorCode === 429;
  }

  return false;
};

const logArchiveStatus = (logger: typeof server.log): void => {
  const enabled = config.archive.enabled;
  const chatId = resolveArchiveChatId();
  const looksValid = Boolean(chatId && /^-100\\d+$/.test(String(chatId)));
  if (enabled && looksValid) {
    logger.info('[archive] channel configured');
    return;
  }
  if (enabled) {
    logger.warn('[archive] channel missing, archive disabled');
    return;
  }
  logger.info('[archive] disabled');
};

const start = async () => {
  try {
    server.log.info('Init start');
    server.log.info({
      app_env: config.logReporter.env,
      supabase_host: config.supabase.host,
      archive_chat_id_present: Boolean(config.archive.channelId),
      webhook_url_present: Boolean(config.telegram.webhookUrl)
    }, 'Startup config summary (sanitized)');
    getDbPool();
    getSupabaseClient();
    await pingSupabase();
    server.log.info({ supabase_host: config.supabase.host }, 'Supabase connectivity check passed');
    const migrationSummary = await schemaSync();
    server.log.info(
      {
        applied: migrationSummary.appliedCount,
        skipped: migrationSummary.skippedCount,
        duration_ms: migrationSummary.durationMs
      },
      'Schema sync complete'
    );

    if (config.db.runBackfill) {
      const summary = await backfillReminderAttachmentFileIds();
      server.log.info(
        {
          updated: summary.updated,
          skipped: summary.skipped,
          needs_manual_fix: summary.needsManualFix,
          duration_ms: summary.durationMs
        },
        'Reminder attachment backfill complete'
      );
    } else {
      server.log.info('Reminder attachment backfill skipped (RUN_BACKFILL=false)');
    }

    logArchiveStatus(server.log);
    if (config.archive.enabled) {
      const validation = await validateArchiveChannel(bot.api);
      if (!validation.ok) {
        setArchiveRuntimeStatus(false);
        server.log.warn({ reason: validation.reason }, '[archive] disabled: invalid channel config');
      }
    }

    const port = Number(config.server.port);
    const host = config.server.host;
    await server.listen({ host, port });
    server.log.info({ host, port }, 'HTTP server is listening');

    if (config.telegram.devPolling) {
      server.log.info('Running in DEV_POLLING mode: starting bot via long polling.');
      await bot.start();
    } else {
      if (config.telegram.webhookUrl) {
        try {
          await bot.api.setWebhook(config.telegram.webhookUrl);
          server.log.info('Webhook registered with Telegram.');
        } catch (error) {
          if (isTelegramTooManyRequests(error)) {
            server.log.warn(
              { err: error, scope: 'telegram/webhook' },
              'Telegram setWebhook rate limited.'
            );
          } else {
            server.log.error(
              { err: error },
              'Failed to set Telegram webhook (non-fatal).'
            );
          }
        }
      }

      server.log.info(
        'Running in WEBHOOK mode: NOT calling bot.start(), updates come via /webhook.'
      );
    }

    server.log.info('Init done');
  } catch (error) {
    server.log.error({ err: error }, 'Failed to start application.');
    process.exit(1);
  }
};

void start();
