import { config } from '../config';

export type LogLevel = 'info' | 'warn' | 'error';

export interface LogMeta {
  [key: string]: unknown;
}

export interface LogEvent {
  level: LogLevel;
  message: string;
  meta?: LogMeta;
  timestamp: string;
  projectId: string;
  env?: string;
}

const projectId = config.logReporter.projectId ?? 'daily-system';
const endpoint = (() => {
  if (!config.logReporter.ingestUrl) return null;
  if (!config.logReporter.ingestKey) return config.logReporter.ingestUrl;
  const url = new URL(config.logReporter.ingestUrl);
  url.searchParams.set('key', config.logReporter.ingestKey);
  return url.toString();
})();
const levelsRaw = config.logReporter.levels;

const isLogLevel = (value: string | undefined): value is LogLevel => {
  return value === 'info' || value === 'warn' || value === 'error';
};

const parseLevels = (value: string | undefined): Set<LogLevel> => {
  if (!value) return new Set(['error']);
  const tokens = value
    .split(',')
    .map((token) => token.trim().toLowerCase())
    .filter(Boolean);
  const parsed = tokens.filter((token): token is LogLevel => isLogLevel(token));
  return parsed.length ? new Set(parsed) : new Set(['error']);
};

const enabledLevels = parseLevels(levelsRaw);
const metricCounters = new Map<string, number>();

export const levelToNumber = (level: LogLevel): number => {
  switch (level) {
    case 'info':
      return 10;
    case 'warn':
      return 20;
    case 'error':
      return 30;
  }
};

const minLevelNumber = Math.min(...[...enabledLevels].map(levelToNumber));

const sendRemoteLog = async (event: LogEvent): Promise<void> => {
  if (!endpoint) return;

  try {
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event)
    });

    if (res.status >= 400) {
      console.error(`[LOGGER] Failed to send log to Path Applier: ${res.status} ${res.statusText}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[LOGGER] Failed to send log to Path Applier:', message);
  }
};

const logWithLevel = (level: LogLevel, message: string, meta?: LogMeta): void => {
  const prefix = level.toUpperCase();
  const logMessage = `[${prefix}] ${message}`;

  if (meta) {
    if (level === 'info') {
      console.log(logMessage, meta);
    } else if (level === 'warn') {
      console.warn(logMessage, meta);
    } else {
      console.error(logMessage, meta);
    }
  } else {
    if (level === 'info') {
      console.log(logMessage);
    } else if (level === 'warn') {
      console.warn(logMessage);
    } else {
      console.error(logMessage);
    }
  }

  if (!config.logReporter.enabled) return;
  if (!endpoint) return;
  if (!enabledLevels.has(level)) return;
  if (levelToNumber(level) < minLevelNumber) return;

  const event: LogEvent = {
    level,
    message,
    meta,
    timestamp: new Date().toISOString(),
    projectId,
    env: config.logReporter.env
  };

  void sendRemoteLog(event);
};

export const logInfo = (message: string, meta?: LogMeta): void => {
  logWithLevel('info', message, meta);
};

export const logWarn = (message: string, meta?: LogMeta): void => {
  logWithLevel('warn', message, meta);
};

export const logError = (message: string, meta?: LogMeta): void => {
  logWithLevel('error', message, meta);
};

export const incrementMetricCounter = (name: string): number => {
  const nextValue = (metricCounters.get(name) ?? 0) + 1;
  metricCounters.set(name, nextValue);
  return nextValue;
};
