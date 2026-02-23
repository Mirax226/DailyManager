import { InlineKeyboard } from 'grammy';
import type { Context } from 'grammy';
import { ensureUser, UserServiceUnavailableError } from '../services/users';
import { getOrCreateUserSettings } from '../services/userSettings';
import { resolveLocale, t, type Locale } from '../i18n';
import type { UserRecord } from '../services/users';
import type { UserSettingsRow } from '../types/supabase';
import { safeTruncate } from '../utils/safe_truncate';
import { safePlain } from './text';

export type RenderScreenParams = {
  titleKey?: string;
  title?: string;
  bodyLines?: string[];
  inlineKeyboard?: InlineKeyboard;
};

type CachedUserContext = { user: UserRecord; settings: UserSettingsRow; locale: Locale };

const getCachedUserContext = (ctx: Context): CachedUserContext | null =>
  ((ctx as unknown as { _userContext?: CachedUserContext })._userContext as CachedUserContext | undefined) ?? null;

const setCachedUserContext = (ctx: Context, payload: CachedUserContext): void => {
  (ctx as unknown as { _userContext?: CachedUserContext })._userContext = payload;
};

export const updateCachedUserContext = (ctx: Context, patch: Partial<CachedUserContext>): void => {
  const existing = getCachedUserContext(ctx);
  if (!existing) return;
  setCachedUserContext(ctx, { ...existing, ...patch });
};

const renderDbUnavailableNotice = async (ctx: Context): Promise<void> => {
  const retryKeyboard = new InlineKeyboard().text('🔄 Retry', 'system.retry');
  const text = 'Service temporarily unavailable. Try again.';

  if (ctx.callbackQuery?.message) {
    const { chat, message_id: messageId } = ctx.callbackQuery.message;
    await ctx.api.editMessageText(chat.id, messageId, text, { reply_markup: retryKeyboard });
    return;
  }

  await ctx.reply(text, { reply_markup: retryKeyboard });
};

export const ensureUserAndSettings = async (ctx: Context) => {
  const cached = getCachedUserContext(ctx);
  if (cached) return cached;

  if (!ctx.from) throw new Error('User not found in context');
  const telegramId = String(ctx.from.id);
  const username = ctx.from.username ?? null;

  try {
    const user = await ensureUser({ telegramId, username }, undefined, ctx.update?.update_id ? String(ctx.update.update_id) : undefined);
    const settings = await getOrCreateUserSettings(user.id);
    const locale = resolveLocale(((settings.settings_json ?? {}) as { language_code?: string | null }).language_code ?? null);
    const payload: CachedUserContext = { user, settings, locale };
    setCachedUserContext(ctx, payload);
    return payload;
  } catch (error) {
    if (error instanceof UserServiceUnavailableError) {
      await renderDbUnavailableNotice(ctx);
    }
    throw error;
  }
};

export const renderScreen = async (ctx: Context, params: RenderScreenParams): Promise<void> => {
  await ensureUserAndSettings(ctx);
  const maxLength = 3500;

  const resolvedTitle =
    params.title ??
    (params.titleKey && (params.titleKey.startsWith('screens.') || params.titleKey.startsWith('errors.'))
      ? t(params.titleKey)
      : params.titleKey) ??
    '';

  const resolvedLines =
    params.bodyLines?.map((line) => {
      if (typeof line === 'string' && (line.startsWith('screens.') || line.startsWith('errors.'))) {
        return t(line);
      }
      return line;
    }) ?? [];

  const normalizedLines = [...resolvedLines];
  if (resolvedTitle && normalizedLines[0] === resolvedTitle) {
    normalizedLines.shift();
  }

  const parts: string[] = [];
  if (resolvedTitle) {
    parts.push(resolvedTitle);
  }
  if (normalizedLines.length > 0) {
    if (resolvedTitle) {
      parts.push('');
    }
    parts.push(...normalizedLines);
  }

  const text = safeTruncate(safePlain(parts.join('\n')), maxLength);
  const replyMarkup = params.inlineKeyboard;

  if (ctx.callbackQuery?.message) {
    const { chat, message_id: messageId } = ctx.callbackQuery.message;
    await ctx.api.editMessageText(chat.id, messageId, text, {
      reply_markup: replyMarkup
    });
    return;
  }

  await ctx.reply(text, {
    reply_markup: replyMarkup
  });
};
