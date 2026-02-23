/* eslint-disable no-console */
import { Bot, InlineKeyboard, GrammyError } from 'grammy';
import type { BotError, Context } from 'grammy';

import { config } from './config';

import { ensureUser, UserServiceUnavailableError } from './services/users';
import { getOrCreateUserSettings, setUserLanguageCode, setUserOnboarded, updateUserSettingsJson } from './services/userSettings';

import {
  seedDefaultRewardsIfEmpty,
  listRewards,
  getRewardById,
  purchaseReward,
  listRewardsForEdit,
  createReward,
  updateReward,
  deleteReward
} from './services/rewards';

import { addXpDelta, getXpBalance, getXpSummary } from './services/xpLedger';

import {
  ensureDefaultItems,
  ensureDefaultTemplate,
  upsertItem,
  listItems,
  listAllItems,
  listUserTemplates,
  setActiveTemplate,
  deleteTemplate,
  duplicateTemplate,
  getTemplateById,
  getItemById,
  updateItem,
  setItemEnabled,
  moveItem,
  deleteItem,
  createUserTemplate,
  updateTemplateTitle
} from './services/reportTemplates';
import { listRoutines, getRoutineById, createRoutine, updateRoutine, deleteRoutine } from './services/routines';
import {
  listRoutineTasks,
  listRoutineTasksByRoutineIds,
  getRoutineTaskById,
  createRoutineTask,
  updateRoutineTask,
  deleteRoutineTask
} from './services/routineTasks';

import {
  getOrCreateReportDay,
  getReportDayByDate,
  getReportDayById,
  listCompletionStatus,
  saveValue,
  lockReportDay,
  unlockReportDay,
  listRecentReportDays
} from './services/dailyReport';
import { getTodayDateString } from './services/dailyLogs';
import {
  createNote,
  createNoteAttachment,
  clearPendingNoteAttachmentsByKinds,
  clearPendingNoteAttachments,
  deleteNote,
  getNoteAttachmentById,
  getNoteById,
  listNoteAttachments,
  listNoteAttachmentKinds,
  listNoteAttachmentsByKinds,
  listNoteDateSummaries,
  listNotesByDate,
  listNotesByDatePage,
  listPendingNoteAttachments,
  listUnarchivedNoteAttachments,
  updateNote,
  updateNoteAttachmentArchiveInfo,
} from './services/notes';

import { consumeCallbackToken } from './services/callbackTokens';
import { getRecentTelemetryEvents, isTelemetryEnabled, logTelemetryEvent } from './services/telemetry';
import { getErrorReportByCode, logErrorReport } from './services/errorReports';
import { escapeMarkdown } from './utils/markdown';
import {
  computeNextRunAt,
  createReminder,
  createReminderDraft,
  createReminderAttachment,
  deleteReminder,
  getReminderById,
  listReminderAttachmentCounts,
  listReminderAttachments,
  listRemindersForUser,
  toggleReminderEnabled,
  updateReminder
} from './services/reminders';
import type { ReminderScheduleType } from './services/reminders';

import {
  copyArchiveGroupToUser,
  getArchiveItemByEntity,
  markArchiveItemStatus,
  resolveArchiveChatId,
  sendArchiveItemToChannel,
  upsertArchiveItem,
  type ArchiveMediaType,
  type ArchiveMediaSummary
} from './services/archive';

import { makeActionButton } from './ui/inlineButtons';
import { renderScreen, ensureUserAndSettings as renderEnsureUserAndSettings, updateCachedUserContext } from './ui/renderScreen';
import { aiEnabledForUser, sendMainMenu } from './ui/mainMenu';
import { labels } from './ui/labels';
import { emoji, isEmojiEnabled, runWithEmojiSetting } from './ui/emoji';
import { getUserBooleanSetting, setUserSetting } from './services/settings';
import { renderSettingsScreen } from './ui/settingsScreen';

import { formatInstantToLocal, formatLocalTime, localDateTimeToUtcIso } from './utils/time';
import { gregorianToJalali, isValidJalaliDate, jalaliToGregorian } from './utils/jalali';
import { logError } from './utils/logger';
import { sendAttachments, type StoredAttachment } from './services/telegram-media';
import { resolveLocale, t, withLocale, type Locale } from './i18n';
import { initLogReporter } from './services/log_reporter';

import type { NoteAttachmentRow, NoteRow, ReportItemRow, ReportDayRow, RewardRow, RoutineRow, RoutineTaskRow } from './types/supabase';

export const bot = new Bot<Context>(config.telegram.botToken);
const logReporter = initLogReporter();

bot.use(async (ctx: Context, next: () => Promise<void>) => {
  if (!ctx.from) {
    await next();
    return;
  }
  const { user } = await ensureUserAndSettings(ctx);
  const emojiEnabled = await getUserBooleanSetting(user.id, EMOJI_SETTING_KEY, config.ui.emojiEnabled);
  await runWithEmojiSetting(emojiEnabled, next);
});

/**
 * Per-user in-memory state (ephemeral).
 * IMPORTANT: Render free-tier can restart; state should be considered best-effort.
 */
type TemplateItemFlow = {
  mode: 'create' | 'edit';
  templateId: string;
  itemId?: string;
  step: 'label' | 'key' | 'type' | 'category' | 'category_custom' | 'xp_mode' | 'xp_value' | 'xp_max' | 'summary';
  draft: {
    label?: string;
    itemKey?: string;
    itemType?: string;
    category?: string | null;
    xpMode?: 'none' | 'fixed' | 'per_minute' | 'per_number' | 'time' | null;
    xpValue?: number | null;
    xpMaxPerDay?: number | null;
    optionsJson?: Record<string, unknown> | null;
  };
};

type RoutineFlow = {
  mode: 'create' | 'edit';
  routineId?: string;
  step: 'title' | 'description' | 'type' | 'xp_mode' | 'xp_value' | 'xp_max' | 'confirm';
  draft: {
    title?: string;
    description?: string | null;
    routineType?: 'boolean' | 'duration_minutes' | 'number';
    xpMode?: 'fixed' | 'per_minute' | 'per_number' | 'none';
    xpValue?: number | null;
    xpMaxPerDay?: number | null;
    isActive?: boolean;
  };
};

type RoutineTaskFlow = {
  mode: 'create' | 'edit';
  routineId: string;
  taskId?: string;
  step: 'title' | 'description' | 'type' | 'xp_mode' | 'xp_value' | 'xp_max' | 'confirm';
  draft: {
    title?: string;
    description?: string | null;
    itemType?: 'boolean' | 'duration_minutes' | 'number';
    xpMode?: 'fixed' | 'per_minute' | 'per_number' | 'none';
    xpValue?: number | null;
    xpMaxPerDay?: number | null;
    optionsJson?: Record<string, unknown> | null;
  };
};

type CategoryFlow =
  | { mode: 'create'; step: 'name' | 'emoji'; draft: { name?: string; emoji?: string } }
  | { mode: 'rename'; categoryId: string; step: 'name' }
  | { mode: 'emoji'; categoryId: string; step: 'emoji' };

type TemplateRenameFlow = { templateId: string };
type TemplateCreateFlow = { step: 'title' };

type BuilderNavigationState = {
  active: true;
  templateId: string;
  step: string;
  returnStep?: string;
};

type AwaitingValueState = { reportDayId: string; itemId: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' };

type NumericDraftState = { reportDayId: string; itemId: string; value: number; unit?: 'minutes' | 'seconds' };

type TimeDraftState = {
  reportDayId: string;
  itemId: string;
  hour12: number;
  minuteTens: number;
  minuteOnes: number;
  ampm: 'AM' | 'PM';
  mode?: 'single' | 'start_end';
  phase?: 'start' | 'end';
  startValue?: { hhmm: string; minutesTotal: number };
};

type ReminderDraft = {
  localDate?: string;
  localTime?: string;
  timeMinutes?: number;
  title?: string | null;
  description?: string | null;
  descGroupKey?: string | null;
  scheduleType?: ReminderScheduleType;
  intervalMinutes?: number;
  atTime?: string;
  byWeekday?: number;
  byMonthday?: number;
  byMonth?: number;
  attachments?: ReminderAttachmentDraft[];
  descriptionAttachments?: ArchiveAttachmentDraft[];
  dateMode?: 'gregorian' | 'jalali';
  year?: number;
  month?: number;
  day?: number;
  dateSource?: 'today' | 'tomorrow' | 'weekend' | 'custom';
};

type ReminderFlow =
  | {
      mode: 'create';
      reminderId?: string;
      step:
        | 'title'
        | 'description'
        | 'attachments'
        | 'caption_choice'
        | 'caption_all'
        | 'caption_category'
        | 'schedule_type'
        | 'date_select'
        | 'custom_date'
        | 'time'
        | 'time_manual'
        | 'interval_minutes'
        | 'daily_time'
        | 'weekly_day'
        | 'weekly_time'
        | 'monthly_day'
        | 'monthly_time'
        | 'yearly_month'
        | 'yearly_day'
        | 'yearly_time';
      draft: ReminderDraft;
      captionCategories?: ReminderCaptionCategory[];
      currentCategory?: ReminderCaptionCategory;
    }
  | {
      mode: 'edit';
      reminderId: string;
      step:
        | 'title'
        | 'description'
        | 'attachments'
        | 'caption_choice'
        | 'caption_all'
        | 'caption_category'
        | 'schedule_type'
        | 'date_select'
        | 'custom_date'
        | 'time'
        | 'time_manual'
        | 'interval_minutes'
        | 'daily_time'
        | 'weekly_day'
        | 'weekly_time'
        | 'monthly_day'
        | 'monthly_time'
        | 'yearly_month'
        | 'yearly_day'
        | 'yearly_time';
      draft: ReminderDraft;
      captionCategories?: ReminderCaptionCategory[];
      currentCategory?: ReminderCaptionCategory;
    };

type NotesFlow =
  | {
      mode: 'create';
      step: 'title' | 'body';
      draft: { title?: string | null; noteDate: string };
    }
  | {
      mode: 'create';
      step: 'attachments';
      noteId: string;
      viewContext?: { noteDate?: string; page?: number; historyPage?: number };
    }
  | {
      mode: 'create';
      step: 'caption_choice' | 'caption_all' | 'caption_category';
      noteId: string;
      captionCategories?: NoteCaptionCategory[];
      currentCategory?: NoteCaptionCategory;
      viewContext?: { noteDate?: string; page?: number; historyPage?: number };
    }
  | {
      mode: 'edit';
      noteId: string;
      step: 'title' | 'body';
      viewContext?: { noteDate?: string; page?: number; historyPage?: number };
    }
  | {
      mode: 'view_date';
      date: string;
    }
  | {
      mode: 'view_note';
      noteId: string;
    }
  | {
      mode: 'clear_date';
      noteDate: string;
    };

type NoteUploadSession = {
  noteId: string;
  viewContext?: { noteDate?: string; page?: number; historyPage?: number };
  pendingKinds: Partial<Record<NoteAttachmentKind, number>>;
  lastReceivedAt: number;
  timer?: ReturnType<typeof setTimeout>;
  prompted?: boolean;
};

type ReminderUploadSession = {
  reminderId: string;
  pendingKinds: Partial<Record<ReminderAttachmentKind, number>>;
  lastReceivedAt: number;
  timer?: ReturnType<typeof setTimeout>;
  prompted?: boolean;
};

type ReminderlessState = {
  awaitingValue?: AwaitingValueState;

  settingsRoutine?: { step: 'label' | 'xp'; label?: string };

  numericDraft?: NumericDraftState;

  timeDraft?: TimeDraftState;
  reminderFlow?: ReminderFlow;
  notesFlow?: NotesFlow;
  noteUploadSession?: NoteUploadSession;
  reminderUploadSession?: ReminderUploadSession;
  statusFilter?: { reportDayId: string; filter: 'all' | 'not_filled' | 'filled' };

  rewardEdit?: {
    mode: 'create' | 'edit';
    rewardId?: string;
    step: 'title' | 'description' | 'xp' | 'confirm_delete';
    draft: { title?: string; description?: string | null; xpCost?: number };
  };

  templateItemFlow?: TemplateItemFlow;
  routineFlow?: RoutineFlow;
  routineTaskFlow?: RoutineTaskFlow;
  categoryFlow?: CategoryFlow;
  templateRename?: TemplateRenameFlow;
  templateCreate?: TemplateCreateFlow;
  builder?: BuilderNavigationState;
};

const userStates = new Map<string, ReminderlessState>();

// Cache report context per (user,date) to reduce repeated DB fetches within same session.
const reportContextCache = new Map<string, { reportDay: ReportDayRow; items: ReportItemRow[] }>();
const clearReportContextCache = (): void => {
  reportContextCache.clear();
};
const clearTemplateItemFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.templateItemFlow;
  userStates.set(telegramId, st);
};

const setTemplateItemFlow = (telegramId: string, flow: TemplateItemFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.templateItemFlow = flow;
  userStates.set(telegramId, st);
};

const setRoutineFlow = (telegramId: string, flow: RoutineFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.routineFlow = flow;
  userStates.set(telegramId, st);
};

const clearRoutineFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.routineFlow;
  userStates.set(telegramId, st);
};

const setRoutineTaskFlow = (telegramId: string, flow: RoutineTaskFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.routineTaskFlow = flow;
  userStates.set(telegramId, st);
};

const clearRoutineTaskFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.routineTaskFlow;
  userStates.set(telegramId, st);
};

const setReminderFlow = (telegramId: string, flow: ReminderFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.reminderFlow = flow;
  userStates.set(telegramId, st);
};

const clearReminderFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.reminderFlow;
  userStates.set(telegramId, st);
};

const setNotesFlow = (telegramId: string, flow: NotesFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.notesFlow = flow;
  userStates.set(telegramId, st);
};

const clearNotesFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.notesFlow;
  userStates.set(telegramId, st);
};

const setNoteUploadSession = (telegramId: string, session: NoteUploadSession | undefined): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  if (session) {
    st.noteUploadSession = session;
  } else {
    delete st.noteUploadSession;
  }
  userStates.set(telegramId, st);
};

const setReminderUploadSession = (telegramId: string, session: ReminderUploadSession | undefined): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  if (session) {
    st.reminderUploadSession = session;
  } else {
    delete st.reminderUploadSession;
  }
  userStates.set(telegramId, st);
};

const setCategoryFlow = (telegramId: string, flow: CategoryFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.categoryFlow = flow;
  userStates.set(telegramId, st);
};

const clearCategoryFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.categoryFlow;
  userStates.set(telegramId, st);
};

const setTemplateRenameFlow = (telegramId: string, flow: TemplateRenameFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.templateRename = flow;
  userStates.set(telegramId, st);
};

const clearTemplateRenameFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.templateRename;
  userStates.set(telegramId, st);
};

const setTemplateCreateFlow = (telegramId: string, flow: TemplateCreateFlow): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  st.templateCreate = flow;
  userStates.set(telegramId, st);
};

const clearTemplateCreateFlow = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.templateCreate;
  userStates.set(telegramId, st);
};

const clearBuilderState = (telegramId: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  delete st.builder;
  userStates.set(telegramId, st);
};

const updateBuilderStep = (telegramId: string, templateId: string, step: string, returnStep?: string): void => {
  const st = { ...(userStates.get(telegramId) || {}) };
  const activeBuilder: BuilderNavigationState = { active: true, templateId, step, returnStep };
  st.builder = activeBuilder;
  userStates.set(telegramId, st);
};

const builderIsActiveForTemplate = (telegramId: string, templateId: string): boolean => {
  const st = userStates.get(telegramId);
  return Boolean(st?.builder?.active && st.builder.templateId === templateId);
};

const setBuilderStepForFlow = (ctx: Context, templateId: string, step: string): void => {
  const telegramId = String(ctx.from?.id ?? '');
  const st = userStates.get(telegramId);
  if (st?.templateItemFlow?.mode === 'create') {
    updateBuilderStep(telegramId, templateId, step);
  }
};

const makeBuilderBackButton = async (ctx: Context, params: { templateId: string; fallbackAction: string; fallbackData?: Record<string, unknown> }) => {
  const telegramId = String(ctx.from?.id ?? '');
  if (builderIsActiveForTemplate(telegramId, params.templateId)) {
    return makeActionButton(ctx, { label: t('buttons.back'), action: 'builder.back', data: { templateId: params.templateId } });
  }
  return makeActionButton(ctx, { label: t('buttons.back'), action: params.fallbackAction, data: params.fallbackData ?? { templateId: params.templateId } });
};

const setTemplateFlowStepFromBuilder = (telegramId: string, builderStep: string): void => {
  const st = userStates.get(telegramId);
  if (!st?.templateItemFlow || st.templateItemFlow.mode !== 'create') return;
  const map: Record<string, TemplateItemFlow['step']> = {
    'builder.enterLabel': 'label',
    'builder.chooseType': 'type',
    'builder.chooseCategory': 'category',
    'builder.configureXP': 'xp_mode',
    'builder.configureXPValue': 'xp_value',
    'builder.configureXPMax': 'xp_max'
  };
  const nextStep = map[builderStep];
  if (nextStep) {
    setTemplateItemFlow(telegramId, { ...st.templateItemFlow, step: nextStep });
  }
};

const resolvePreviousBuilderStep = (builder: BuilderNavigationState, flow?: TemplateItemFlow): string | null => {
  if (builder.returnStep) return builder.returnStep;

  switch (builder.step) {
    case 'builder.chooseType':
      return 'builder.enterLabel';
    case 'builder.chooseCategory':
      return 'builder.chooseType';
    case 'builder.configureXP':
      return 'builder.chooseCategory';
    case 'builder.configureXPValue':
      return 'builder.configureXP';
    case 'builder.configureXPMax':
      return 'builder.configureXPValue';
    case 'builder.summary': {
      const xpMode = flow?.draft.xpMode ?? 'none';
      if (xpMode === 'per_minute' || xpMode === 'per_number') return 'builder.configureXPMax';
      if (xpMode !== 'none') return 'builder.configureXPValue';
      return 'builder.configureXP';
    }
    default:
      return null;
  }
};

const renderBuilderStep = async (ctx: Context, templateId: string, step: string, flow?: TemplateItemFlow): Promise<void> => {
  switch (step) {
    case 'builder.enterLabel':
      await promptLabelInput(ctx, { templateId, backToItemId: flow?.itemId });
      return;
    case 'builder.chooseType':
      await promptTypeSelection(ctx, { templateId, itemId: flow?.itemId, backToItem: flow?.mode === 'edit' });
      return;
    case 'builder.chooseCategory':
      await promptCategorySelection(ctx, { templateId, itemId: flow?.itemId, backToItem: flow?.mode === 'edit' });
      return;
    case 'builder.configureXP':
      await promptXpModeSelection(ctx, { templateId, itemId: flow?.itemId, backToItem: flow?.mode === 'edit', itemType: flow?.draft.itemType });
      return;
    case 'builder.configureXPValue':
      await promptXpValueInput(ctx, { templateId, itemId: flow?.itemId, backToItem: flow?.mode === 'edit' });
      return;
    case 'builder.configureXPMax':
      await promptXpMaxInput(ctx, { templateId, itemId: flow?.itemId });
      return;
    case 'builder.summary':
      await renderTemplateEdit(ctx, templateId);
      return;
    default:
      await renderTemplateEdit(ctx, templateId);
  }
};

const handleBuilderBackNavigation = async (ctx: Context, templateId: string): Promise<void> => {
  const telegramId = String(ctx.from?.id ?? '');
  const st = userStates.get(telegramId);
  const builder = st?.builder;

  if (!builder || builder.templateId !== templateId) {
    await renderTemplateEdit(ctx, templateId);
    return;
  }

  if (builder.returnStep) {
    setTemplateFlowStepFromBuilder(telegramId, builder.returnStep);
    updateBuilderStep(telegramId, templateId, builder.returnStep);
    await renderBuilderStep(ctx, templateId, builder.returnStep, st?.templateItemFlow);
    return;
  }

  const prevStep = resolvePreviousBuilderStep(builder, st?.templateItemFlow);
  if (!prevStep) {
    clearBuilderState(telegramId);
    clearTemplateItemFlow(telegramId);
    await renderTemplateEdit(ctx, templateId);
    return;
  }

  setTemplateFlowStepFromBuilder(telegramId, prevStep);
  updateBuilderStep(telegramId, templateId, prevStep);
  await renderBuilderStep(ctx, templateId, prevStep, st?.templateItemFlow);
};

const greetingKeys = [
  'screens.dashboard.greeting_1',
  'screens.dashboard.greeting_2',
  'screens.dashboard.greeting_3',
  'screens.dashboard.greeting_4',
  'screens.dashboard.greeting_5'
];
const chooseGreeting = (): string => {
  const options = greetingKeys.map((key) => t(key));
  const pick = Math.floor(Math.random() * options.length);
  return options[pick] ?? t('screens.dashboard.greeting_1');
};

const LANGUAGE_OPTIONS: { code: Locale; labelKey: string }[] = [
  { code: 'en', labelKey: 'buttons.language_en' },
  { code: 'fa', labelKey: 'buttons.language_fa' }
];

type LanguageScreenOrigin = 'onboarding' | 'settings';

const readStoredLanguageCode = (settingsJson: Record<string, unknown> | null | undefined): Locale | null => {
  const code = (settingsJson as { language_code?: string | null } | null | undefined)?.language_code ?? null;
  if (code === 'fa') return 'fa';
  if (code === 'en') return 'en';
  return null;
};

const renderLanguageSelection = async (ctx: Context, params: { origin: LanguageScreenOrigin; currentLocale: Locale }): Promise<void> => {
  const kb = new InlineKeyboard();

  for (const option of LANGUAGE_OPTIONS) {
    const isActive = params.currentLocale === option.code;
    const suffix = isActive ? ' ✅' : '';
    const btn = await makeActionButton(ctx, {
      label: `${t(option.labelKey)}${suffix}`,
      action: 'language.set',
      data: { language: option.code, origin: params.origin }
    });
    kb.text(btn.text, btn.callback_data).row();
  }

  if (params.origin === 'settings') {
    const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.settings' });
    kb.text(backBtn.text, backBtn.callback_data);
  }

  const titleKey = params.origin === 'onboarding' ? 'screens.language.choose_title' : 'screens.language.change_title';
  const bodyKey = params.origin === 'onboarding' ? 'screens.language.choose_hint' : 'screens.language.change_hint';

  await renderScreen(ctx, { titleKey, bodyLines: [bodyKey], inlineKeyboard: kb });
};

const applyLanguageSelection = async (ctx: Context, language: Locale, origin: LanguageScreenOrigin): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const updatedSettings = await setUserLanguageCode(user.id, language);
  updateCachedUserContext(ctx, { settings: updatedSettings, locale: language });

  await withLocale(language, async () => {
    await sendMainMenu(ctx, aiEnabledForUser(user.settings_json as Record<string, unknown>));
    if (origin === 'settings') {
      await renderSettingsRoot(ctx);
    } else {
      await renderDashboard(ctx);
    }
  });
};

const isTooOldCallbackError = (error: unknown): error is GrammyError =>
  error instanceof GrammyError && error.error_code === 400 && error.description.toLowerCase().includes('query is too old');

const generateTraceId = (): string => `tr-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;

const getTraceId = (ctx: Context): string => {
  const existing = (ctx as unknown as { traceId?: string }).traceId;
  if (existing) return existing;
  const fresh = generateTraceId();
  (ctx as unknown as { traceId?: string }).traceId = fresh;
  return fresh;
};

const safeAnswerCallback = async (ctx: Context, params?: Parameters<Context['answerCallbackQuery']>[0]): Promise<void> => {
  try {
    await ctx.answerCallbackQuery(params);
  } catch (error) {
    if (isTooOldCallbackError(error)) {
      console.warn({
        scope: 'telegram',
        event: 'callback_query_too_old',
        callbackQueryId: ctx.callbackQuery?.id,
        userId: ctx.from?.id
      });

      if (ctx.from?.id) {
        await ctx.api.sendMessage(ctx.from.id, t('errors.session_expired_start_over'));
      }

      await renderDashboard(ctx);
      return;
    }
    throw error;
  }
};

const ensureUserAndSettings = async (ctx: Context) => {
  if (renderEnsureUserAndSettings) return renderEnsureUserAndSettings(ctx);
  if (!ctx.from) throw new Error('User not found in context');
  const telegramId = String(ctx.from.id);
  const username = ctx.from.username ?? null;
  const user = await ensureUser({ telegramId, username });
  const settings = await getOrCreateUserSettings(user.id);
  const locale = resolveLocale(((settings.settings_json ?? {}) as { language_code?: string | null }).language_code ?? null);
  return { user, settings, locale };
};

bot.use(async (ctx, next) => {
  if (!ctx.from) {
    await next();
    return;
  }
  const { locale } = await ensureUserAndSettings(ctx);
  await withLocale(locale, async () => {
    await next();
  });
});

const telemetryEnabledForUser = (userSettingsJson?: Record<string, unknown>) => isTelemetryEnabled(userSettingsJson);

const logForUser = async (params: {
  userId: string;
  ctx: Context;
  eventName: string;
  screen?: string | null;
  payload?: Record<string, unknown> | null;
  enabled: boolean;
}) =>
  logTelemetryEvent({
    userId: params.userId,
    traceId: getTraceId(params.ctx),
    eventName: params.eventName,
    screen: params.screen,
    payload: params.payload,
    enabled: params.enabled
  });

const sendErrorNotice = async (ctx: Context, errorCode: string) => {
  const btn = await makeActionButton(ctx, { label: t('buttons.send_report'), action: 'error.send_report', data: { errorCode } });
  const kb = new InlineKeyboard().text(btn.text, btn.callback_data);
  await renderScreen(ctx, {
    titleKey: 'errors.error_title',
    bodyLines: [t('errors.with_code', { code: errorCode })],
    inlineKeyboard: kb
  });
};

const handleBotError = async (ctx: Context, error: unknown, traceId: string): Promise<void> => {
  try {
    const updateType = ctx.update ? Object.keys(ctx.update)[0] : undefined;
    const chatId = typeof ctx.chat?.id === 'number' ? ctx.chat.id : undefined;
    await logReporter.report('error', 'Telegram bot middleware error', {
      stack: error instanceof Error ? error.stack : undefined,
      context: {
        traceId,
        updateId: ctx.update?.update_id,
        updateType,
        chatId
      }
    });

    const { user } = await ensureUserAndSettings(ctx);
    const enabled = telemetryEnabledForUser(user.settings_json as Record<string, unknown>);
    const errorCode = `ERR-${Math.random().toString(36).slice(2, 8)}`.toUpperCase();
    const recentEvents = await getRecentTelemetryEvents(user.id, 20);
    await logErrorReport({ userId: user.id, traceId, errorCode, error, recentEvents });
    await sendErrorNotice(ctx, errorCode);
    await logTelemetryEvent({
      userId: user.id,
      traceId,
      eventName: 'error_reported',
      payload: { error_code: errorCode },
      enabled
    });
  } catch (err) {
    console.error({ scope: 'bot', event: 'error_handler_failed', err, originalError: error, traceId });
    if (error instanceof UserServiceUnavailableError) {
      return;
    }
    try {
      await ctx.reply(t('errors.unexpected'));
    } catch {
      // ignore
    }
  }
};

bot.use(async (ctx, next) => {
  const traceId = getTraceId(ctx);
  try {
    await next();
  } catch (error) {
    await handleBotError(ctx, error, traceId);
  }
});

const buildDashboardLines = (isNew: boolean, timezone?: string | null): string[] => {
  const local = formatLocalTime(timezone ?? config.defaultTimezone);
  const lines = [chooseGreeting(), t('screens.dashboard.time', { date: local.date, time: local.time })];

  if (isNew) {
    const welcomeNew = t('screens.dashboard.welcome_new');
    lines.push('', ...(Array.isArray(welcomeNew) ? welcomeNew : [welcomeNew]));
  } else {
    lines.push('', t('screens.dashboard.welcome_back'));
  }
  return lines;
};

const buildRewardCenterKeyboard = async (ctx: Context): Promise<InlineKeyboard> => {
  const buyBtn = await makeActionButton(ctx, { label: t('buttons.rewards_buy'), action: 'rewards.buy' });
  const editBtn = await makeActionButton(ctx, { label: t('buttons.rewards_edit_store'), action: 'rewards.edit_root' });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });

  return new InlineKeyboard()
    .text(buyBtn.text, buyBtn.callback_data)
    .row()
    .text(editBtn.text, editBtn.callback_data)
    .row()
    .text(backBtn.text, backBtn.callback_data);
};

const buildReportsMenuKeyboard = async (ctx: Context): Promise<InlineKeyboard> => {
  const xpBtn = await makeActionButton(ctx, { label: t('buttons.reports_xp'), action: 'reports.xp' });
  const sleepBtn = await makeActionButton(ctx, { label: t('buttons.reports_sleep'), action: 'reports.sleep' });
  const studyBtn = await makeActionButton(ctx, { label: t('buttons.reports_study'), action: 'reports.study' });
  const tasksBtn = await makeActionButton(ctx, { label: t('buttons.reports_tasks'), action: 'reports.tasks' });
  const chartBtn = await makeActionButton(ctx, { label: t('buttons.reports_chart'), action: 'reports.chart' });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });

  return new InlineKeyboard()
    .text(xpBtn.text, xpBtn.callback_data)
    .row()
    .text(sleepBtn.text, sleepBtn.callback_data)
    .row()
    .text(studyBtn.text, studyBtn.callback_data)
    .row()
    .text(tasksBtn.text, tasksBtn.callback_data)
    .row()
    .text(chartBtn.text, chartBtn.callback_data)
    .row()
    .text(backBtn.text, backBtn.callback_data);
};

const isRoutineItem = (item: ReportItemRow): boolean => {
  const opts = (item.options_json ?? {}) as { is_routine?: boolean; routine_id?: string };
  return Boolean(opts.is_routine || item.item_key?.startsWith('routine_'));
};

const isRoutineParentItem = (item: ReportItemRow): boolean => {
  const opts = (item.options_json ?? {}) as { routine_role?: string };
  return isRoutineItem(item) && opts.routine_role === 'parent';
};

const isRoutineTaskItem = (item: ReportItemRow): boolean => {
  const opts = (item.options_json ?? {}) as { routine_role?: string; routine_task_id?: string };
  return isRoutineItem(item) && (opts.routine_role === 'task' || Boolean(opts.routine_task_id));
};

const displayItemTypeLabel = (itemType: string): string => {
  switch (itemType) {
    case 'boolean':
      return t('screens.form_builder.type_boolean_label');
    case 'number':
      return t('screens.form_builder.type_number_label');
    case 'time_hhmm':
      return t('screens.form_builder.type_time_label');
    case 'duration_minutes':
      return t('screens.form_builder.type_duration_label');
    case 'text':
      return t('screens.form_builder.type_text_label');
    default:
      return itemType;
  }
};

const formatItemLabel = (item: ReportItemRow): string => {
  const base = item.label ?? '';
  if (isRoutineTaskItem(item)) {
    const opts = (item.options_json ?? {}) as { routine_title?: string };
    const routineTitle = opts.routine_title ? `${opts.routine_title} – ` : '';
    return t('screens.daily_report.routine_task_label', { routine: routineTitle, title: base });
  }
  if (isRoutineParentItem(item)) return t('screens.daily_report.routine_label', { title: base });
  return base;
};

const filterRoutineDisplayItems = (items: ReportItemRow[]): ReportItemRow[] => items.filter((i) => !isRoutineTaskItem(i));

const routineValueState = (valueJson: Record<string, unknown> | null): 'pending' | 'done' | 'partial' | 'skipped' => {
  if (!valueJson) return 'pending';
  if ((valueJson as { skipped?: boolean }).skipped) return 'skipped';
  if ((valueJson as { completed_all?: boolean }).completed_all || (valueJson as { status?: string }).status === 'done' || valueIsTrue((valueJson as { value?: unknown }).value)) {
    return 'done';
  }
  if ((valueJson as { status?: string }).status === 'partial' || Array.isArray((valueJson as { completed_task_ids?: unknown }).completed_task_ids)) {
    return 'partial';
  }
  return 'pending';
};

const routineMetaFromItem = (item: ReportItemRow): { routineId?: string; routineTaskId?: string } => {
  const opts = (item.options_json ?? {}) as { routine_id?: string; routine_task_id?: string };
  return { routineId: opts.routine_id, routineTaskId: opts.routine_task_id };
};

const computeRoutineParentXp = (item: ReportItemRow, valueJson: Record<string, unknown> | null): number => {
  if (!isRoutineParentItem(item)) return 0;
  const xpMode = item.xp_mode;
  const xpValue = item.xp_value ?? 0;
  const checked = valueIsTrue((valueJson as { value?: unknown })?.value) || Boolean((valueJson as { completed_all?: boolean }).completed_all);
  if (!checked) return 0;
  if (xpMode === 'fixed') return Math.max(0, xpValue);
  return 0;
};

const STANDARD_CATEGORIES: { name: string; emoji: string; labelKey: string }[] = [
  { name: 'sleep', emoji: '😴', labelKey: 'screens.templates.category_sleep' },
  { name: 'routine', emoji: '🔁', labelKey: 'screens.templates.category_routine' },
  { name: 'study', emoji: '📚', labelKey: 'screens.templates.category_study' },
  { name: 'tasks', emoji: '✅', labelKey: 'screens.templates.category_tasks' },
  { name: 'health', emoji: '❤️', labelKey: 'screens.templates.category_health' },
  { name: 'mindset', emoji: '🧠', labelKey: 'screens.templates.category_mindset' },
  { name: 'other', emoji: '🏷', labelKey: 'screens.templates.category_other' }
];

const allowedXpModesForItemType = (itemType?: string): ('none' | 'fixed' | 'per_minute' | 'per_number')[] => {
  if (itemType === 'boolean') return ['none', 'fixed'];
  if (itemType === 'time_hhmm' || itemType === 'duration_minutes') return ['none', 'fixed', 'per_minute'];
  if (itemType === 'number') return ['none', 'fixed', 'per_number'];
  return ['none', 'fixed', 'per_minute', 'per_number'];
};

const allowedRoutineTaskXpModes = (itemType?: string): ('none' | 'fixed' | 'per_minute' | 'per_number')[] => {
  if (itemType === 'boolean') return ['fixed'];
  if (itemType === 'duration_minutes') return ['fixed', 'per_minute'];
  if (itemType === 'number') return ['fixed', 'per_number'];
  return ['fixed'];
};

const normalizeXpModeForItemType = (itemType: string | undefined, xpMode: string | null | undefined): 'none' | 'fixed' | 'per_minute' | 'per_number' | null => {
  const resolved = xpMode === 'time' ? 'per_minute' : xpMode;
  const allowed = allowedXpModesForItemType(itemType);
  if (resolved && allowed.includes(resolved as 'none' | 'fixed' | 'per_minute' | 'per_number')) return resolved as 'none' | 'fixed' | 'per_minute' | 'per_number';
  return 'none';
};

const getPerNumberConfig = (
  draft: Pick<TemplateItemFlow['draft'], 'optionsJson' | 'xpValue'>
): { perNumber: number; xpPerUnit: number } => {
  const opts = draft.optionsJson ?? {};
  const perNumber = Number((opts as { perNumber?: number }).perNumber ?? 1);
  const xpPerUnit = Number((opts as { xpPerUnit?: number }).xpPerUnit ?? draft.xpValue ?? 0);
  return {
    perNumber: Number.isInteger(perNumber) && perNumber > 0 ? perNumber : 1,
    xpPerUnit: Number.isInteger(xpPerUnit) && xpPerUnit > 0 ? xpPerUnit : draft.xpValue ?? 0
  };
};

const buildXpSummary = (draft: TemplateItemFlow['draft']): string => {
  const mode = normalizeXpModeForItemType(draft.itemType, draft.xpMode);
  const capText = draft.xpMaxPerDay && draft.xpMaxPerDay > 0 ? t('screens.form_builder.xp_cap', { cap: draft.xpMaxPerDay }) : '';
  if (!mode || mode === 'none') return t('screens.daily_report.ask_xp_mode_none');
  if (mode === 'fixed') return t('screens.form_builder.xp_summary_fixed', { xp: draft.xpValue ?? 0 });
  if (mode === 'per_minute') {
    return t('screens.form_builder.xp_summary_time', {
      xp: draft.xpValue ?? 0,
      cap: capText
    });
  }
  const perNumber = getPerNumberConfig(draft).perNumber;
  const xpPerUnit = getPerNumberConfig(draft).xpPerUnit;
  return t('screens.form_builder.xp_summary_number', {
    ratio: `${perNumber}:${xpPerUnit}`,
    cap: capText
  });
};

const buildFieldSummaryLines = (draft: TemplateItemFlow['draft']): string[] => {
  const lines: string[] = [
    t('screens.form_builder.summary_field', { label: draft.label ?? t('screens.daily_report.template_new_title') }),
    t('screens.form_builder.summary_type', { type: displayItemTypeLabel(draft.itemType ?? 'text') }),
    t('screens.form_builder.summary_category', { category: draft.category ?? t('screens.templates.category_other') }),
    t('screens.form_builder.summary_xp', { xp: buildXpSummary(draft) })
  ];
  return lines;
};

const itemToDraft = (item: ReportItemRow): TemplateItemFlow['draft'] => ({
  label: item.label ?? undefined,
  itemKey: item.item_key ?? undefined,
  itemType: item.item_type,
  category: item.category ?? undefined,
  xpMode: (item.xp_mode as TemplateItemFlow['draft']['xpMode']) ?? 'none',
  xpValue: item.xp_value ?? null,
  xpMaxPerDay: (item as ReportItemRow & { xp_max_per_day?: number | null }).xp_max_per_day ?? null,
  optionsJson: item.options_json ?? {}
});

const deriveXpStorage = (
  draft: TemplateItemFlow['draft']
): { xpMode: 'fixed' | 'per_minute' | 'per_number' | null; xpValue: number | null; xpMax: number | null; optionsJson: Record<string, unknown> } => {
  const normalizedXpMode = normalizeXpModeForItemType(draft.itemType, draft.xpMode ?? null);
  if (!normalizedXpMode || normalizedXpMode === 'none') {
    return { xpMode: null, xpValue: null, xpMax: null, optionsJson: {} };
  }
  if (normalizedXpMode === 'fixed') {
    return { xpMode: 'fixed', xpValue: draft.xpValue ?? 0, xpMax: null, optionsJson: {} };
  }
  if (normalizedXpMode === 'per_minute') {
    return {
      xpMode: 'per_minute',
      xpValue: draft.xpValue ?? 0,
      xpMax: draft.xpMaxPerDay ?? null,
      optionsJson: { ...(draft.optionsJson ?? {}), per: 'minute' }
    };
  }
  const perNumberCfg = getPerNumberConfig(draft);
  const perUnit = perNumberCfg.perNumber > 0 ? perNumberCfg.xpPerUnit / perNumberCfg.perNumber : draft.xpValue ?? 0;
  return {
    xpMode: 'per_number',
    xpValue: perUnit,
    xpMax: draft.xpMaxPerDay ?? null,
    optionsJson: { ...(draft.optionsJson ?? {}), perNumber: perNumberCfg.perNumber, xpPerUnit: perNumberCfg.xpPerUnit }
  };
};

const valueIsTrue = (value: unknown): boolean => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value > 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    return ['true', '1', 'yes', 'y', 'ok', 'on', '✅', '✔️'].includes(normalized);
  }
  return false;
};

const parseNonNegativeNumber = (input: string): number | null => {
  const trimmed = input.trim();
  const n = Number(trimmed);
  if (!Number.isFinite(n)) return null;
  if (n < 0) return null;
  return n;
};

const parseXpRatio = (input: string): { per: number; xp: number } | null => {
  const normalized = input.trim();
  if (!normalized) return null;
  if (normalized.includes(':')) {
    const [perStr, xpStr] = normalized.split(':').map((s) => s.trim());
    const per = Number(perStr);
    const xp = Number(xpStr);
    if (Number.isFinite(per) && Number.isFinite(xp) && per > 0 && xp >= 0) {
      return { per, xp };
    }
    return null;
  }
  const single = Number(normalized);
  if (Number.isFinite(single) && single >= 0) {
    return { per: 1, xp: single };
  }
  return null;
};

const convertToMinutes = (value: number, unit: 'minutes' | 'seconds' = 'minutes'): { minutes: number; seconds?: number } => {
  if (!Number.isFinite(value) || value < 0) return { minutes: 0 };
  if (unit === 'seconds') {
    const seconds = Math.max(0, Math.round(value));
    return { minutes: seconds / 60, seconds };
  }
  return { minutes: value };
};

const minutesFromHhmm = (hhmm: string): number | null => {
  const parsed = parseTimeHhmm(hhmm);
  if (!parsed) return null;
  return parsed.minutes;
};

const formatDurationValue = (minutes: number, seconds?: number): string => {
  const safeMinutes = Number.isFinite(minutes) ? minutes : 0;
  const totalSeconds = seconds != null && Number.isFinite(seconds) ? Math.max(0, Math.round(seconds)) : Math.max(0, Math.round(safeMinutes * 60));
  const hours = Math.floor(totalSeconds / 3600);
  const remainder = totalSeconds % 3600;
  const mins = Math.floor(remainder / 60);
  const secs = remainder % 60;
  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours}${t('screens.daily_report.duration_unit_hour')}`);
  if (mins > 0) parts.push(`${mins}${t('screens.daily_report.duration_unit_minute')}`);
  if (secs > 0 || parts.length === 0) parts.push(`${secs}${t('screens.daily_report.duration_unit_second')}`);
  return parts.join(' ');
};

const formatDisplayValue = (item: ReportItemRow, valueJson: Record<string, unknown> | null): string => {
  if (!valueJson) return '-';
  if ((valueJson as { skipped?: boolean }).skipped) return t('screens.daily_report.value_skipped');
  const value = (valueJson as { value?: unknown }).value ?? (valueJson as { minutes?: unknown }).minutes ?? (valueJson as { number?: unknown }).number;
  switch (item.item_type) {
    case 'boolean':
      return valueIsTrue(value) ? t('screens.daily_report.value_yes') : t('screens.daily_report.value_no');
    case 'time_hhmm':
      return typeof value === 'string' ? value : '-';
    case 'duration_minutes': {
      const minutes = Number((valueJson as { minutes?: number; value?: number }).minutes ?? (valueJson as { value?: number }).value ?? 0);
      const seconds = Number((valueJson as { seconds?: number }).seconds);
      const start = (valueJson as { start?: string }).start;
      const end = (valueJson as { end?: string }).end;
      const formatted = formatDurationValue(minutes, Number.isFinite(seconds) ? seconds : undefined);
      if (start && end) return `${start} → ${end} (${formatted})`;
      return formatted;
    }
    case 'number':
      return value != null ? String(value) : '-';
    default:
      return value != null ? String(value) : '-';
  }
};

const syncRoutineItemsForTemplate = async (
  templateId: string,
  userId: string,
  routines: RoutineRow[],
  items: ReportItemRow[]
): Promise<ReportItemRow[]> => {
  const routineIds = new Set(routines.map((r) => r.id));
  const tasksMap = await listRoutineTasksByRoutineIds(Array.from(routineIds));
  let sortCursor = items.reduce((max, item) => Math.max(max, item.sort_order ?? 0), 0) + 10;
  const result = [...items];
  const seenRoutineItemIds = new Set<string>();

  const taskRatio = (task: RoutineTaskRow): { per: number; xp: number } => {
    const opts = (task.options_json ?? {}) as { per?: unknown; xp?: unknown; perNumber?: unknown; xpPerUnit?: unknown };
    const per = Number((opts.per as number) ?? (opts.perNumber as number));
    const xp = Number((opts.xp as number) ?? (opts.xpPerUnit as number));
    return {
      per: Number.isFinite(per) && per > 0 ? per : 1,
      xp: Number.isFinite(xp) && xp > 0 ? xp : task.xp_value ?? 0
    };
  };

  for (const routine of routines) {
    const itemKey = `routine_${routine.id}`;
    const routineTasks = tasksMap.get(routine.id) ?? [];
    const parentType: ReportItemRow['item_type'] = 'boolean';
    const existingIdx = result.findIndex((i) => i.item_key === itemKey);
    const optionsJson = {
      ...(existingIdx >= 0 ? result[existingIdx].options_json ?? {} : {}),
      is_routine: true,
      routine_id: routine.id,
      routine_type: routine.routine_type,
      routine_role: 'parent',
      routine_title: routine.title
    };

    const parentSort = routine.sort_order ?? sortCursor;
    sortCursor = parentSort;

    if (existingIdx >= 0) {
      const existing = result[existingIdx];
      const needsUpdate =
        existing.label !== routine.title ||
        existing.item_type !== parentType ||
        existing.category !== 'routine' ||
        existing.xp_mode !== routine.xp_mode ||
        existing.xp_value !== (routine.xp_value ?? null) ||
        existing.enabled !== routine.is_active ||
        (existing.sort_order ?? 0) !== parentSort ||
        (existing as ReportItemRow & { xp_max_per_day?: number | null }).xp_max_per_day !== (routine.xp_max_per_day ?? null);

      if (needsUpdate) {
        const updated = await updateItem(existing.id, {
          label: routine.title,
          item_type: parentType,
          category: 'routine',
          xp_mode: routine.xp_mode,
          xp_value: routine.xp_value ?? null,
          xp_max_per_day: routine.xp_max_per_day ?? null,
          options_json: optionsJson,
          enabled: routine.is_active,
          sort_order: parentSort
        });
        result[existingIdx] = updated;
      }
      seenRoutineItemIds.add(result[existingIdx].id);
    } else {
      sortCursor += 10;
      const inserted = await upsertItem({
        templateId,
        label: routine.title,
        itemKey,
        itemType: parentType,
        category: 'routine',
        xpMode: routine.xp_mode,
        xpValue: routine.xp_value ?? null,
        xpMaxPerDay: routine.xp_max_per_day ?? null,
        optionsJson,
        sortOrder: parentSort
      });
      result.push(inserted);
      seenRoutineItemIds.add(inserted.id);
    }

    let taskSort = parentSort + 1;
    for (const task of routineTasks) {
      const taskKey = `routine_${routine.id}_task_${task.id}`;
      const existingTaskIdx = result.findIndex((i) => i.item_key === taskKey);
      const taskOptions = {
        ...(existingTaskIdx >= 0 ? result[existingTaskIdx].options_json ?? {} : {}),
        is_routine: true,
        routine_id: routine.id,
        routine_task_id: task.id,
        routine_role: 'task',
        routine_title: routine.title,
        ...taskRatio(task)
      };
      const ratio = taskRatio(task);
      if (existingTaskIdx >= 0) {
        const existing = result[existingTaskIdx];
        const needsUpdate =
          existing.label !== task.title ||
          existing.item_type !== task.item_type ||
          existing.category !== 'routine' ||
          existing.xp_mode !== task.xp_mode ||
          existing.xp_value !== (ratio.xp ?? null) ||
          (existing as ReportItemRow & { xp_max_per_day?: number | null }).xp_max_per_day !== (task.xp_max_per_day ?? null) ||
          existing.enabled !== routine.is_active ||
          (existing.sort_order ?? 0) !== taskSort;

        if (needsUpdate) {
          const updated = await updateItem(existing.id, {
            label: task.title,
            item_type: task.item_type,
            category: 'routine',
            xp_mode: task.xp_mode,
            xp_value: ratio.xp ?? null,
            xp_max_per_day: task.xp_max_per_day ?? null,
            options_json: taskOptions,
            enabled: routine.is_active,
            sort_order: taskSort
          });
          result[existingTaskIdx] = updated;
        }
        seenRoutineItemIds.add(result[existingTaskIdx].id);
      } else {
        const insertedTask = await upsertItem({
          templateId,
          label: task.title,
          itemKey: taskKey,
          itemType: task.item_type,
          category: 'routine',
          xpMode: task.xp_mode,
          xpValue: ratio.xp ?? null,
          xpMaxPerDay: task.xp_max_per_day ?? null,
          optionsJson: taskOptions,
          sortOrder: taskSort
        });
        result.push(insertedTask);
        seenRoutineItemIds.add(insertedTask.id);
      }
      taskSort += 1;
      sortCursor = Math.max(sortCursor, taskSort);
    }
  }

  // Disable orphaned routine items
  for (const item of result) {
    const opts = (item.options_json ?? {}) as { is_routine?: boolean; routine_id?: string };
    if ((opts.is_routine || item.item_key.startsWith('routine_')) && opts.routine_id && !routineIds.has(opts.routine_id) && item.enabled) {
      const updated = await updateItem(item.id, { enabled: false });
      const idx = result.findIndex((i) => i.id === item.id);
      if (idx >= 0) result[idx] = updated;
      continue;
    }
    if ((opts.is_routine || item.item_key.startsWith('routine_')) && !seenRoutineItemIds.has(item.id) && item.enabled) {
      const updated = await updateItem(item.id, { enabled: false });
      const idx = result.findIndex((i) => i.id === item.id);
      if (idx >= 0) result[idx] = updated;
    }
  }

  return result;
};

const buildDailyReportKeyboard = async (
  ctx: Context,
  reportDay: ReportDayRow,
  options?: { items?: ReportItemRow[]; statuses?: { item: ReportItemRow; filled: boolean; skipped: boolean }[] }
): Promise<InlineKeyboard> => {
  const statusBtn = await makeActionButton(ctx, { label: t('buttons.dr_today_status'), action: 'dr.status', data: { reportDayId: reportDay.id, filter: 'all' } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.back' });

  // When locked: keep only Status + Back, plus Unlock.
  if (reportDay.locked) {
    const unlockBtn = await makeActionButton(ctx, { label: t('buttons.dr_unlock'), action: 'dr.unlock', data: { reportDayId: reportDay.id } });
    return new InlineKeyboard().text(statusBtn.text, statusBtn.callback_data).row().text(unlockBtn.text, unlockBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);
  }

  let hasPending = true;
  try {
    const items = options?.items ?? (await ensureContextByReportDayId(ctx, reportDay.id)).items;
    const statuses = (options?.statuses ?? (await listCompletionStatus(reportDay.id, items))).filter((s) => !isRoutineTaskItem(s.item));
    hasPending = statuses.some((s) => !s.filled && !s.skipped);
  } catch (error) {
    console.warn({ scope: 'daily_report', event: 'keyboard_pending_check_failed', reportDayId: reportDay.id, error });
  }

  const nextBtn = hasPending ? await makeActionButton(ctx, { label: t('buttons.dr_fill_next'), action: 'dr.next', data: { reportDayId: reportDay.id } }) : null;
  const templatesBtn = await makeActionButton(ctx, { label: t('buttons.dr_templates'), action: 'dr.templates', data: { reportDayId: reportDay.id } });
  const historyBtn = await makeActionButton(ctx, { label: t('buttons.dr_history'), action: 'dr.history', data: { reportDayId: reportDay.id } });
  const lockBtn = await makeActionButton(ctx, { label: t('buttons.dr_lock'), action: 'dr.lock', data: { reportDayId: reportDay.id } });

  const kb = new InlineKeyboard().text(statusBtn.text, statusBtn.callback_data).row();
  if (nextBtn) kb.text(nextBtn.text, nextBtn.callback_data).row();
  kb.text(templatesBtn.text, templatesBtn.callback_data)
    .row()
    .text(historyBtn.text, historyBtn.callback_data)
    .row()
    .text(lockBtn.text, lockBtn.callback_data)
    .row()
    .text(backBtn.text, backBtn.callback_data);
  return kb;
};

const ensureReportContext = async (ctx: Context): Promise<{ userId: string; reportDay: ReportDayRow; items: ReportItemRow[] }> => {
  const { user, settings } = await ensureUserAndSettings(ctx);
  const local = formatLocalTime(user.timezone ?? config.defaultTimezone);
  const cacheKey = `${user.id}:${local.date}`;

  const cached = reportContextCache.get(cacheKey);
  if (cached) return { userId: user.id, ...cached };

  const defaultTemplate = await ensureDefaultTemplate(user.id);
  const settingsJson = (settings.settings_json ?? {}) as { active_template_id?: string | null };
  const activeTemplateId = settingsJson.active_template_id ?? defaultTemplate.id;
  const activeTemplateCandidate = activeTemplateId === defaultTemplate.id ? defaultTemplate : await getTemplateById(activeTemplateId);
  const template = activeTemplateCandidate && activeTemplateCandidate.user_id === user.id ? activeTemplateCandidate : defaultTemplate;

  const baseItems = template.id === defaultTemplate.id ? await ensureDefaultItems(user.id) : await listAllItems(template.id);
  const routines = await listRoutines(user.id);
  const merged = await syncRoutineItemsForTemplate(template.id, user.id, routines, baseItems);
  const items = merged.filter((item) => item.enabled);
  const reportDay = await getOrCreateReportDay({ userId: user.id, templateId: template.id, localDate: local.date });

  reportContextCache.set(cacheKey, { reportDay, items });
  return { userId: user.id, reportDay, items };
};

const ensureSpecificReportContext = async (
  ctx: Context,
  localDate: string
): Promise<{ userId: string; reportDay: ReportDayRow; items: ReportItemRow[] }> => {
  const { user, settings } = await ensureUserAndSettings(ctx);

  const cacheKey = `${user.id}:${localDate}`;
  const cached = reportContextCache.get(cacheKey);
  if (cached) return { userId: user.id, ...cached };

  const defaultTemplate = await ensureDefaultTemplate(user.id);
  const settingsJson = (settings.settings_json ?? {}) as { active_template_id?: string | null };
  const activeTemplateId = settingsJson.active_template_id ?? defaultTemplate.id;
  const activeTemplateCandidate = activeTemplateId === defaultTemplate.id ? defaultTemplate : await getTemplateById(activeTemplateId);
  const template = activeTemplateCandidate && activeTemplateCandidate.user_id === user.id ? activeTemplateCandidate : defaultTemplate;

  const baseItems = template.id === defaultTemplate.id ? await ensureDefaultItems(user.id) : await listAllItems(template.id);
  const routines = await listRoutines(user.id);
  const merged = await syncRoutineItemsForTemplate(template.id, user.id, routines, baseItems);
  const items = merged.filter((item) => item.enabled);

  const reportDay = await getOrCreateReportDay({ userId: user.id, templateId: template.id, localDate });

  reportContextCache.set(cacheKey, { reportDay, items });
  return { userId: user.id, reportDay, items };
};

const ensureContextByReportDayId = async (ctx: Context, reportDayId: string): Promise<{ reportDay: ReportDayRow; items: ReportItemRow[] }> => {
  const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);
  if (cached) return cached;
  const reportDayRow = await getReportDayById(reportDayId);
  if (reportDayRow) {
    const context = await ensureSpecificReportContext(ctx, reportDayRow.local_date);
    return { reportDay: context.reportDay, items: context.items };
  }
  const context = await ensureReportContext(ctx);
  return { reportDay: context.reportDay, items: context.items };
};

const isLockedMessageLines = (reportDay: ReportDayRow): string[] => {
  // Only one localized line should be shown; translations handle language.
  return [t('screens.daily_report.day_locked')];
};

const renderDashboard = async (ctx: Context): Promise<void> => {
  try {
    const { user, settings, locale } = await ensureUserAndSettings(ctx);
    const storedLanguage = readStoredLanguageCode(settings.settings_json as Record<string, unknown>);
    if (!storedLanguage) {
      await renderLanguageSelection(ctx, { origin: 'onboarding', currentLocale: locale });
      return;
    }
    const isNew = !settings.onboarded;

    if (isNew) {
      try {
        await setUserOnboarded(user.id);
      } catch {
        // ignore
      }
    }

    if (isNew || ctx.message) {
      await sendMainMenu(ctx, aiEnabledForUser(user.settings_json as Record<string, unknown>));
    }

    const reportContextPromise = ensureReportContext(ctx);
    const xpBalancePromise = getXpBalance(user.id);

    const { reportDay, items } = await reportContextPromise;
    const statuses = await listCompletionStatus(reportDay.id, items);
    const completed = statuses.filter((s) => s.filled).length;
    const total = statuses.length;

    const xpBalance = await xpBalancePromise;
    const streak = (user.settings_json as { streak?: number } | undefined)?.streak ?? 0;

    const bodyLines = [
      ...buildDashboardLines(isNew, user.timezone),
      '',
      t('screens.dashboard.xp_balance', { xp: xpBalance }),
      t('screens.dashboard.today_items', { completed, total }),
      t('screens.dashboard.streak', { streak })
    ];

    const dailyReportBtn = await makeActionButton(ctx, { label: t('buttons.nav_daily_report'), action: 'nav.daily_report' });
    const reportcarBtn = await makeActionButton(ctx, { label: t('buttons.nav_reportcar'), action: 'nav.reportcar' });
    const tasksBtn = await makeActionButton(ctx, { label: t('buttons.nav_tasks'), action: 'nav.tasks' });
    const remindersBtn = await makeActionButton(ctx, { label: t('buttons.nav_reminders'), action: 'nav.reminders' });
    const rewardsBtn = await makeActionButton(ctx, { label: t('buttons.nav_rewards'), action: 'nav.rewards' });
    const reportsBtn = await makeActionButton(ctx, { label: t('buttons.nav_reports'), action: 'nav.reports' });
    const settingsBtn = await makeActionButton(ctx, { label: t('buttons.nav_settings'), action: 'nav.settings' });

    const kb = new InlineKeyboard()
      .text(dailyReportBtn.text, dailyReportBtn.callback_data)
      .row()
      .text(reportcarBtn.text, reportcarBtn.callback_data)
      .row()
      .text(tasksBtn.text, tasksBtn.callback_data)
      .row()
      .text(remindersBtn.text, remindersBtn.callback_data)
      .row()
      .text(rewardsBtn.text, rewardsBtn.callback_data)
      .row()
      .text(reportsBtn.text, reportsBtn.callback_data)
      .row()
      .text(settingsBtn.text, settingsBtn.callback_data);

    await renderScreen(ctx, { titleKey: t('screens.dashboard.title'), bodyLines, inlineKeyboard: kb });
  } catch (error) {
    console.error({ scope: 'home', event: 'render_error', error });
    const reloadBtn = await makeActionButton(ctx, { label: t('buttons.reload'), action: 'nav.dashboard' });
    await renderScreen(ctx, {
      titleKey: t('screens.dashboard.title'),
      bodyLines: [t('errors.dashboard_unavailable')],
      inlineKeyboard: new InlineKeyboard().text(reloadBtn.text, reloadBtn.callback_data)
    });
  }
};

const renderRewardCenter = async (ctx: Context): Promise<void> => {
  try {
    const { user } = await ensureUserAndSettings(ctx);
    await seedDefaultRewardsIfEmpty(user.id);
    const balance = await getXpBalance(user.id);

    const bodyLines = [t('screens.rewards.balance', { xp: balance }), '', t('screens.rewards.choose_option')];
    const kb = await buildRewardCenterKeyboard(ctx);

    await renderScreen(ctx, { titleKey: t('screens.rewards.title'), bodyLines, inlineKeyboard: kb });
  } catch (error) {
    console.error({ scope: 'rewards', event: 'render_error', error });
    const kb = await buildRewardCenterKeyboard(ctx);
    await renderScreen(ctx, {
      titleKey: t('screens.rewards.title'),
      bodyLines: [t('errors.rewards_unavailable')],
      inlineKeyboard: kb
    });
  }
};

const renderRewardBuyList = async (ctx: Context): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const rewards = await listRewards(user.id);

  if (!rewards.length) {
    const kb = await buildRewardCenterKeyboard(ctx);
    await renderScreen(ctx, { titleKey: t('screens.rewards.title'), bodyLines: [t('screens.rewards.empty')], inlineKeyboard: kb });
    return;
  }

  const kb = new InlineKeyboard();
  for (const reward of rewards) {
    const btn = await makeActionButton(ctx, { label: `${reward.title} (${reward.xp_cost} XP)`, action: 'rewards.confirm', data: { rewardId: reward.id } });
    kb.text(btn.text, btn.callback_data).row();
  }

  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.rewards' });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.rewards.title'), bodyLines: [t('screens.rewards.choose_reward')], inlineKeyboard: kb });
};

const renderRewardStoreEditorRoot = async (ctx: Context): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const rewards = await listRewardsForEdit(user.id);

  const bodyLines: string[] = [t('screens.rewards.edit_store_title'), ''];

  if (!rewards.length) {
    bodyLines.push(t('screens.rewards.edit_store_empty'));
  } else {
    bodyLines.push(t('screens.rewards.edit_store_list_header'));
    rewards.forEach((r) => {
      const status = r.is_active ? t('common.active') : t('common.inactive');
      bodyLines.push(`• ${r.title} — ${r.xp_cost} XP (${status})`);
    });
  }

  bodyLines.push('', t('screens.rewards.edit_store_hint'));

  const addBtn = await makeActionButton(ctx, { label: t('buttons.rewards_add'), action: 'rewards.add' });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.rewards' });

  const kb = new InlineKeyboard().text(addBtn.text, addBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);

  // Also add per-reward edit buttons
  if (rewards.length) {
    kb.row();
    for (const r of rewards) {
      const btn = await makeActionButton(ctx, { label: `✏ ${r.title}`, action: 'rewards.edit_open', data: { rewardId: r.id } });
      kb.text(btn.text, btn.callback_data).row();
    }
  }

  await renderScreen(ctx, { titleKey: t('screens.rewards.edit_store_title'), bodyLines, inlineKeyboard: kb });
};

const renderRewardEditMenu = async (ctx: Context, reward: RewardRow): Promise<void> => {
  const lines = [
    t('screens.rewards.editing', { title: reward.title, xp: reward.xp_cost }),
    t('screens.rewards.status_line', { status: reward.is_active ? t('common.active') : t('common.inactive') })
  ];

  const titleBtn = await makeActionButton(ctx, { label: t('buttons.rewards_edit_title'), action: 'rewards.edit_title', data: { rewardId: reward.id } });
  const descBtn = await makeActionButton(ctx, { label: t('buttons.rewards_edit_description'), action: 'rewards.edit_description', data: { rewardId: reward.id } });
  const xpBtn = await makeActionButton(ctx, { label: t('buttons.rewards_edit_xp'), action: 'rewards.edit_xp', data: { rewardId: reward.id } });
  const toggleBtn = await makeActionButton(ctx, {
    label: reward.is_active ? t('buttons.rewards_deactivate') : t('buttons.rewards_activate'),
    action: 'rewards.toggle_active',
    data: { rewardId: reward.id }
  });
  const deleteBtn = await makeActionButton(ctx, { label: t('buttons.rewards_delete'), action: 'rewards.delete', data: { rewardId: reward.id } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'rewards.edit_root' });

  const kb = new InlineKeyboard()
    .text(titleBtn.text, titleBtn.callback_data)
    .row()
    .text(descBtn.text, descBtn.callback_data)
    .row()
    .text(xpBtn.text, xpBtn.callback_data)
    .row()
    .text(toggleBtn.text, toggleBtn.callback_data)
    .row()
    .text(deleteBtn.text, deleteBtn.callback_data)
    .row()
    .text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.rewards.edit_store_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderReportsMenu = async (ctx: Context): Promise<void> => {
  const kb = await buildReportsMenuKeyboard(ctx);
  await renderScreen(ctx, { titleKey: 'screens.reports.title', bodyLines: ['screens.reports.choose_category'], inlineKeyboard: kb });
};

const NOTES_HISTORY_PAGE_SIZE = 20;
const NOTES_DATE_PAGE_SIZE = 8;
const NOTE_ATTACHMENT_KINDS = ['photo', 'video', 'voice', 'document', 'video_note', 'audio'] as const;
type NoteAttachmentKind = (typeof NOTE_ATTACHMENT_KINDS)[number];
type NoteCaptionCategory = 'photo' | 'video' | 'voice' | 'video_note' | 'files';
type ReminderCaptionCategory = NoteCaptionCategory;
const EMOJI_SETTING_KEY = 'emoji_enabled';
const NOTE_UPLOAD_IDLE_MS = 2000;
const NOTE_DETAIL_MAX_CHARS = 1500;
const NOTE_BODY_PREVIEW_LIMIT = 600;
type ReminderAttachmentKind = NoteAttachmentKind;
const REMINDER_UPLOAD_IDLE_MS = 2000;
const REMINDER_DETAIL_MAX_CHARS = 1500;
const REMINDER_DESC_PREVIEW_LIMIT = 600;
const ARCHIVE_DESCRIPTION_LIMIT = 900;
type ReminderAttachmentDraft = {
  kind: ReminderAttachmentKind;
  fileId: string;
  caption?: string | null;
  fileUniqueId?: string | null;
  mimeType?: string | null;
};
type ArchiveAttachmentDraft = {
  archiveChatId: number;
  archiveMessageId: number;
  mediaType: ArchiveMediaType;
  caption?: string | null;
};

const getNoteAttachmentKindEmoji = (kind: NoteAttachmentKind): string => {
  if (!isEmojiEnabled()) return '';
  if (kind === 'photo') return emoji('photo');
  if (kind === 'video') return emoji('video');
  if (kind === 'voice') return emoji('voice');
  if (kind === 'video_note') return emoji('video_note');
  return emoji('file');
};

const getNotesArchiveChatId = (): number | null => resolveArchiveChatId('notes');
const getRemindersArchiveChatId = (): number | null => resolveArchiveChatId('reminders');

const buildUserStatusLine = (ctx: Context): string => {
  const name = ctx.from?.first_name ?? 'User';
  const username = ctx.from?.username ? `(@${ctx.from.username})` : '';
  return `${name} ${username}`.trim();
};

const buildReminderDeleteStatusLine = (ctx: Context): string => {
  const username = ctx.from?.username ? `@${ctx.from.username}` : '@unknown';
  const userId = ctx.from?.id ?? 'unknown';
  return labels.archive.deletedBy({ user: `${username} (id:${userId})` });
};

const isReminderActive = (reminder: { enabled: boolean | null; next_run_at: string | null }): boolean =>
  Boolean(reminder.enabled) && Boolean(reminder.next_run_at);

const getNoteCaptionHeader = (category: NoteCaptionCategory): string => labels.notes.captionHeader(category);

const buildNoteCaptionPatch = (category: NoteCaptionCategory, caption: string | null) => {
  if (category === 'photo') return { notePhotoCaption: caption };
  if (category === 'video') return { noteVideoCaption: caption };
  if (category === 'voice') return { noteVoiceCaption: caption };
  if (category === 'video_note') return { noteVideoNoteCaption: caption };
  return { noteFileCaption: caption };
};

const resolveNoteCaptionForCategory = (note: NoteRow, category: NoteCaptionCategory): string | null => {
  if (category === 'photo') return note.note_photo_caption ?? null;
  if (category === 'video') return note.note_video_caption ?? null;
  if (category === 'voice') return note.note_voice_caption ?? null;
  if (category === 'video_note') return note.note_videonote_caption ?? null;
  return note.note_file_caption ?? null;
};

const resolveNoteCaptionCategory = (kind: NoteAttachmentRow['kind']): NoteCaptionCategory => {
  if (kind === 'photo' || kind === 'video' || kind === 'voice' || kind === 'video_note') {
    return kind;
  }
  return 'files';
};

const chunkItems = <T,>(items: T[], size: number): T[][] => {
  if (items.length <= size) return [items];
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
};

const markNoteArchiveDeleted = async (ctx: Context, noteId: string): Promise<void> => {
  const archiveItem = await getArchiveItemByEntity({ kind: 'note', entityId: noteId });
  if (!archiveItem) return;
  const statusLine = labels.archive.deletedBy({ user: buildUserStatusLine(ctx) });
  await markArchiveItemStatus(ctx.api, {
    item: archiveItem,
    status: 'deleted',
    statusNote: statusLine,
    statusLine
  });
};

const markReminderArchiveDeleted = async (ctx: Context, reminderId: string): Promise<void> => {
  const archiveItem = await getArchiveItemByEntity({ kind: 'reminder', entityId: reminderId });
  if (!archiveItem) return;
  const statusLine = buildReminderDeleteStatusLine(ctx);
  await markArchiveItemStatus(ctx.api, {
    item: archiveItem,
    status: 'deleted',
    statusNote: statusLine,
    statusLine
  });
};

const maybeArchiveReminderDescription = async (ctx: Context, reminderId: string, description: string | null): Promise<void> => {
  if (!description || description.length <= ARCHIVE_DESCRIPTION_LIMIT) return;
  const archiveChatId = getRemindersArchiveChatId();
  if (!archiveChatId) return;
  const reminder = await getReminderById(reminderId);
  if (!reminder) return;
  const existingItem = await getArchiveItemByEntity({ kind: 'reminder', entityId: reminderId });
  if (existingItem) return;
  const { user } = await ensureUserAndSettings(ctx);
  const telegramMeta = resolveTelegramUserMeta(ctx, user);
  const timeLabel = buildArchiveTimeLabel(reminder.created_at, user.timezone ?? config.defaultTimezone);
  const archiveResult = await sendArchiveItemToChannel(ctx.api, {
    archiveChatId,
    user: {
      firstName: telegramMeta.firstName,
      lastName: telegramMeta.lastName,
      username: telegramMeta.username,
      telegramId: telegramMeta.telegramId,
      appUserId: user.id
    },
    timeLabel,
    kindLabel: labels.archive.kindReminder(),
    title: reminder.title ?? null,
    description,
    attachments: []
  });
  const updatedItem = await upsertArchiveItem({
    existing: null,
    ownerUserId: user.id,
    kind: 'reminder',
    entityId: reminderId,
    channelId: archiveChatId,
    title: reminder.title ?? null,
    description,
    summary: emptyArchiveSummary(),
    messageIds: archiveResult.messageIds,
    messageMeta: archiveResult.messageMeta,
    meta: {
      username: telegramMeta.username ?? null,
      first_name: telegramMeta.firstName ?? null,
      last_name: telegramMeta.lastName ?? null,
      telegram_id: telegramMeta.telegramId ?? null,
      created_at: reminder.created_at,
      summary_line: buildCaptionSummaryLine(emptyArchiveSummary())
    }
  });
  await updateReminder(reminderId, { archiveItemId: updatedItem.id });
};

const buildPreviewText = (text: string, limit: number): string => {
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}…`;
};

const buildArchivedPreview = (text: string, limit: number, showNotice: boolean, notice: string): string => {
  const preview = buildPreviewText(text, limit);
  return showNotice ? `${preview}\n${notice}` : preview;
};

const buildArchiveTimeLabel = (isoUtc: string, timezone?: string | null): string => {
  const local = formatInstantToLocal(isoUtc, timezone ?? config.defaultTimezone);
  const offset = new Intl.DateTimeFormat('en-US', { timeZone: local.timezone, timeZoneName: 'shortOffset' })
    .formatToParts(new Date(isoUtc))
    .find((part) => part.type === 'timeZoneName')?.value;
  return `${local.date} ${local.time} (${offset ?? local.timezone})`;
};

const resolveTelegramUserMeta = (ctx: Context, user: { telegram_id: string; username: string | null }) => ({
  firstName: ctx.from?.first_name ?? null,
  lastName: ctx.from?.last_name ?? null,
  username: ctx.from?.username ?? user.username ?? null,
  telegramId: ctx.from?.id ?? Number(user.telegram_id)
});

const emptyArchiveSummary = (): ArchiveMediaSummary => ({
  photos: 0,
  videos: 0,
  voices: 0,
  documents: 0,
  video_notes: 0,
  audios: 0
});

const buildCaptionSummaryLine = (summary: ArchiveMediaSummary): string =>
  labels.archive.summaryLine({
    photos: summary.photos,
    videos: summary.videos,
    voices: summary.voices,
    files: summary.documents + summary.audios,
    videoNotes: summary.video_notes
  });

const buildSummaryFromNoteAttachments = (attachments: Array<{ kind: NoteAttachmentKind | string }>): ArchiveMediaSummary => {
  const summary = emptyArchiveSummary();
  for (const attachment of attachments) {
    if (attachment.kind === 'photo') summary.photos += 1;
    if (attachment.kind === 'video') summary.videos += 1;
    if (attachment.kind === 'voice') summary.voices += 1;
    if (attachment.kind === 'document') summary.documents += 1;
    if (attachment.kind === 'video_note') summary.video_notes += 1;
    if (attachment.kind === 'audio') summary.audios += 1;
  }
  return summary;
};

const splitTextForTelegram = (text: string, limit = 3800): string[] => {
  if (text.length <= limit) return [text];
  const chunks: string[] = [];
  let cursor = 0;
  while (cursor < text.length) {
    chunks.push(text.slice(cursor, cursor + limit));
    cursor += limit;
  }
  return chunks;
};

const renderNotesToday = async (ctx: Context): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const { date } = getTodayDateString(user.timezone ?? config.defaultTimezone);
  const notes = await listNotesByDate({ userId: user.id, noteDate: date });

  const lines: string[] = [];
  if (!notes.length) {
    lines.push(labels.notes.todayEmpty());
  } else {
    lines.push(labels.notes.todayHeader(), '');
    for (const note of notes) {
      const local = formatInstantToLocal(note.created_at, user.timezone ?? config.defaultTimezone);
      const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
      lines.push(labels.notes.todayItemLine({ time: local.time, title }));
    }
  }

  const kb = new InlineKeyboard();
  const addBtn = await makeActionButton(ctx, { label: labels.notesButtons.add(), action: 'notes.add' });
  const historyBtn = await makeActionButton(ctx, { label: labels.notesButtons.history(), action: 'notes.history' });
  const backBtn = await makeActionButton(ctx, { label: labels.notesButtons.back(), action: 'nav.dashboard' });

  kb.text(addBtn.text, addBtn.callback_data).row();
  kb.text(historyBtn.text, historyBtn.callback_data).row();
  if (notes.length > 0) {
    const clearBtn = await makeActionButton(ctx, { label: labels.notesButtons.clearToday(), action: 'notes.clear_today' });
    kb.text(clearBtn.text, clearBtn.callback_data).row();
  }
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.notes.title(),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderNotesHistory = async (ctx: Context, page = 0): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const offset = Math.max(0, page) * NOTES_HISTORY_PAGE_SIZE;
  const { entries, hasMore } = await listNoteDateSummaries({ userId: user.id, limit: NOTES_HISTORY_PAGE_SIZE, offset });

  const lines: string[] = [];
  if (entries.length === 0) {
    lines.push(labels.notes.historyEmpty());
  } else {
    for (const entry of entries) {
      lines.push(labels.notes.historyItemLine({ date: entry.date, count: String(entry.count) }));
    }
    lines.push('', labels.notes.historyOpenHint());
  }

  const kb = new InlineKeyboard();
  for (const entry of entries) {
    const btn = await makeActionButton(ctx, {
      label: labels.notes.historyItemLine({ date: entry.date, count: String(entry.count) }),
      action: 'notes.history_date',
      data: { date: entry.date, historyPage: page }
    });
    kb.text(btn.text, btn.callback_data).row();
  }
  if (page > 0 || hasMore) {
    const prevBtn = page > 0 ? await makeActionButton(ctx, { label: labels.notesButtons.prev(), action: 'notes.history_page', data: { page: page - 1 } }) : null;
    const nextBtn = hasMore ? await makeActionButton(ctx, { label: labels.notesButtons.next(), action: 'notes.history_page', data: { page: page + 1 } }) : null;
    if (prevBtn) kb.text(prevBtn.text, prevBtn.callback_data);
    if (nextBtn) kb.text(nextBtn.text, nextBtn.callback_data);
    kb.row();
  }
  const backBtn = await makeActionButton(ctx, { label: labels.notesButtons.back(), action: 'nav.free_text' });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.notes.historyTitle(),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderNotesDate = async (ctx: Context, noteDate: string, page = 0, historyPage = 0): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const offset = Math.max(0, page) * NOTES_DATE_PAGE_SIZE;
  const { notes, total } = await listNotesByDatePage({ userId: user.id, noteDate, limit: NOTES_DATE_PAGE_SIZE, offset });

  const lines: string[] = [];
  if (!notes.length) {
    lines.push(labels.notes.viewEmpty());
  } else {
    for (const note of notes) {
      const local = formatInstantToLocal(note.created_at, user.timezone ?? config.defaultTimezone);
      const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
      lines.push(labels.notes.dateItemLine({ time: local.time, title }));
    }
  }

  const kb = new InlineKeyboard();
  for (const note of notes) {
    const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
    const local = formatInstantToLocal(note.created_at, user.timezone ?? config.defaultTimezone);
    const btn = await makeActionButton(ctx, {
      label: labels.notes.dateItemLine({ time: local.time, title }),
      action: 'notes.view_note',
      data: { noteId: note.id, noteDate, page, historyPage }
    });
    kb.text(btn.text, btn.callback_data).row();
  }
  const hasPrev = page > 0;
  const hasNext = offset + notes.length < total;
  if (hasPrev || hasNext) {
    const prevBtn = hasPrev
      ? await makeActionButton(ctx, { label: labels.notesButtons.prev(), action: 'notes.history_date_page', data: { date: noteDate, page: page - 1, historyPage } })
      : null;
    const nextBtn = hasNext
      ? await makeActionButton(ctx, { label: labels.notesButtons.next(), action: 'notes.history_date_page', data: { date: noteDate, page: page + 1, historyPage } })
      : null;
    if (prevBtn) kb.text(prevBtn.text, prevBtn.callback_data);
    if (nextBtn) kb.text(nextBtn.text, nextBtn.callback_data);
    kb.row();
  }
  const backBtn = await makeActionButton(ctx, { label: labels.notesButtons.back(), action: 'notes.history_page', data: { page: historyPage } });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.notes.dateTitle({ date: noteDate }),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderNoteDetails = async (
  ctx: Context,
  noteId: string,
  viewContext: { noteDate?: string; page?: number; historyPage?: number } = {}
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const note = await getNoteById({ userId: user.id, id: noteId });
  if (!note) {
    await renderNotesHistory(ctx);
    return;
  }
  const noteDate = viewContext.noteDate ?? note.note_date;
  const page = viewContext.page ?? 0;
  const historyPage = viewContext.historyPage ?? 0;
  const attachmentSummary = await listNoteAttachmentKinds({ noteId: note.id });
  const local = formatInstantToLocal(note.created_at, user.timezone ?? config.defaultTimezone);
  const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
  const bodyNotice = labels.notes.detailArchivedNotice();
  const rawBody = (note.description ?? note.body ?? '').trim();
  const hasArchivedBody = Boolean(note.archive_item_id || note.content_group_key);
  const buildNoteLines = (limit: number): string[] => {
    const showNotice = hasArchivedBody || rawBody.length > limit;
    const bodyPreview = rawBody.length
      ? buildArchivedPreview(rawBody, limit, showNotice, bodyNotice)
      : labels.notes.viewEmpty();
    const lines = [
      labels.notes.detailDate({ date: note.note_date }),
      labels.notes.detailTime({ time: local.time }),
      labels.notes.detailTitle({ title }),
      '',
      bodyPreview
    ];
    lines.push('', labels.notes.attachmentsSummary({ count: String(attachmentSummary.total) }));
    return lines;
  };
  let lines = buildNoteLines(NOTE_BODY_PREVIEW_LIMIT);
  if (lines.join('\n').length > NOTE_DETAIL_MAX_CHARS) {
    lines = buildNoteLines(300);
  }
  if (lines.join('\n').length > NOTE_DETAIL_MAX_CHARS) {
    lines = buildNoteLines(150);
  }

  const kb = new InlineKeyboard();
  if (hasArchivedBody || rawBody.length > NOTE_BODY_PREVIEW_LIMIT) {
    const viewBtn = await makeActionButton(ctx, {
      label: labels.notesButtons.viewFull(),
      action: 'notes.body_view',
      data: { noteId: note.id, noteDate, page, historyPage }
    });
    kb.text(viewBtn.text, viewBtn.callback_data).row();
  }
  const editBtn = await makeActionButton(ctx, { label: labels.notesButtons.edit(), action: 'notes.edit_menu', data: { noteId: note.id, noteDate, page, historyPage } });
  const attachBtn = await makeActionButton(ctx, { label: labels.notesButtons.attach(), action: 'notes.attach_more', data: { noteId: note.id, noteDate, page, historyPage } });
  const deleteBtn = await makeActionButton(ctx, { label: labels.notesButtons.delete(), action: 'notes.delete_note', data: { noteId: note.id, noteDate, page, historyPage } });
  const viewAllBtn = await makeActionButton(ctx, { label: labels.notesButtons.viewAllItems(), action: 'notes.view_all', data: { noteId: note.id, noteDate, page, historyPage } });
  const backBtn = await makeActionButton(ctx, { label: labels.notesButtons.back(), action: 'notes.history_date', data: { date: noteDate, page, historyPage } });
  kb.text(editBtn.text, editBtn.callback_data).row();
  kb.text(attachBtn.text, attachBtn.callback_data).row();
  kb.text(deleteBtn.text, deleteBtn.callback_data).row();
  kb.text(viewAllBtn.text, viewAllBtn.callback_data).row();
  if (attachmentSummary.total > 0) {
    const { counts } = attachmentSummary;
    if (counts.photo > 0) {
      const photoBtn = await makeActionButton(ctx, {
        label: labels.notesButtons.photo(),
        action: 'notes.attachments_kind',
        data: { noteId: note.id, kind: 'photo', noteDate, page, historyPage }
      });
      kb.text(photoBtn.text, photoBtn.callback_data).row();
    }
    const videoCount = (counts.video ?? 0) + (counts.video_note ?? 0);
    if (videoCount > 0) {
      const videoBtn = await makeActionButton(ctx, {
        label: labels.notesButtons.video(),
        action: 'notes.attachments_kind',
        data: { noteId: note.id, kind: 'video', noteDate, page, historyPage }
      });
      kb.text(videoBtn.text, videoBtn.callback_data).row();
    }
    if (counts.voice > 0) {
      const voiceBtn = await makeActionButton(ctx, {
        label: labels.notesButtons.voice(),
        action: 'notes.attachments_kind',
        data: { noteId: note.id, kind: 'voice', noteDate, page, historyPage }
      });
      kb.text(voiceBtn.text, voiceBtn.callback_data).row();
    }
    const fileCount = (counts.document ?? 0) + (counts.audio ?? 0);
    if (fileCount > 0) {
      const docBtn = await makeActionButton(ctx, {
        label: labels.notesButtons.document(),
        action: 'notes.attachments_kind',
        data: { noteId: note.id, kind: 'document', noteDate, page, historyPage }
      });
      kb.text(docBtn.text, docBtn.callback_data).row();
    }
  }
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.notes.detailTitleLabel(),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderNoteEditMenu = async (
  ctx: Context,
  noteId: string,
  viewContext: { noteDate?: string; page?: number; historyPage?: number } = {}
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const note = await getNoteById({ userId: user.id, id: noteId });
  if (!note) {
    await renderNotesHistory(ctx);
    return;
  }
  const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
  const lines = [labels.notes.editing({ title })];

  const titleBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.editTitle(),
    action: 'notes.edit_title',
    data: { noteId: note.id, noteDate: viewContext.noteDate ?? note.note_date, page: viewContext.page ?? 0, historyPage: viewContext.historyPage ?? 0 }
  });
  const bodyBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.editBody(),
    action: 'notes.edit_body',
    data: { noteId: note.id, noteDate: viewContext.noteDate ?? note.note_date, page: viewContext.page ?? 0, historyPage: viewContext.historyPage ?? 0 }
  });
  const backBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.back(),
    action: 'notes.view_note',
    data: { noteId: note.id, noteDate: viewContext.noteDate ?? note.note_date, page: viewContext.page ?? 0, historyPage: viewContext.historyPage ?? 0 }
  });

  const kb = new InlineKeyboard().text(titleBtn.text, titleBtn.callback_data).row().text(bodyBtn.text, bodyBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { title: labels.notes.editMenuTitle(), bodyLines: lines, inlineKeyboard: kb });
};

const renderNoteAttachmentPrompt = async (
  ctx: Context,
  noteId: string,
  viewContext: { noteDate?: string; page?: number; historyPage?: number } = {}
): Promise<void> => {
  const doneBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.attachDone(),
    action: 'notes.attach_done',
    data: { noteId, ...viewContext }
  });
  const cancelBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.attachCancel(),
    action: 'notes.attach_cancel',
    data: { noteId, ...viewContext }
  });
  const kb = new InlineKeyboard().text(doneBtn.text, doneBtn.callback_data).row().text(cancelBtn.text, cancelBtn.callback_data);

  await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.attachmentsPrompt()], inlineKeyboard: kb });
};

const buildNoteCaptionCategories = (attachments: NoteAttachmentRow[]): NoteCaptionCategory[] => {
  const kinds = new Set(attachments.map((attachment) => attachment.kind));
  const categories: NoteCaptionCategory[] = [];
  if (kinds.has('photo')) categories.push('photo');
  if (kinds.has('video')) categories.push('video');
  if (kinds.has('voice')) categories.push('voice');
  if (kinds.has('video_note')) categories.push('video_note');
  if (kinds.has('document') || kinds.has('audio')) categories.push('files');
  return categories;
};

const renderNoteCaptionChoice = async (
  ctx: Context,
  noteId: string,
  viewContext: { noteDate?: string; page?: number; historyPage?: number },
  categories: NoteCaptionCategory[],
  summaryLine: string
): Promise<void> => {
  if (!ctx.from) return;
  const allBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.captionAll(),
    action: 'notes.caption_all',
    data: { noteId, ...viewContext }
  });
  const byCategoryBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.captionByCategory(),
    action: 'notes.caption_by_category',
    data: { noteId, ...viewContext }
  });
  const skipBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.captionSkip(),
    action: 'notes.caption_skip',
    data: { noteId, ...viewContext }
  });
  const kb = new InlineKeyboard()
    .text(allBtn.text, allBtn.callback_data)
    .row()
    .text(byCategoryBtn.text, byCategoryBtn.callback_data)
    .row()
    .text(skipBtn.text, skipBtn.callback_data);

  setNotesFlow(String(ctx.from.id), {
    mode: 'create',
    step: 'caption_choice',
    noteId,
    captionCategories: categories,
    viewContext
  });

  await renderScreen(ctx, {
    title: labels.notes.title(),
    bodyLines: [labels.notes.captionsSummary({ summary: summaryLine }), labels.notes.captionsPrompt()],
    inlineKeyboard: kb
  });
};

const promptNoteCaptionCategory = async (
  ctx: Context,
  noteId: string,
  category: NoteCaptionCategory,
  viewContext: { noteDate?: string; page?: number; historyPage?: number },
  categories: NoteCaptionCategory[]
): Promise<void> => {
  if (!ctx.from) return;
  const skipBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.skip(),
    action: 'notes.caption_category_skip',
    data: { noteId, ...viewContext }
  });
  const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data);
  setNotesFlow(String(ctx.from.id), {
    mode: 'create',
    step: 'caption_category',
    noteId,
    currentCategory: category,
    captionCategories: categories,
    viewContext
  });
  const label = labels.notes.captionLabel(category);
  await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.captionCategoryPrompt({ category: label })], inlineKeyboard: kb });
};

const startNoteCaptionFlow = async (
  ctx: Context,
  noteId: string,
  viewContext: { noteDate?: string; page?: number; historyPage?: number }
): Promise<void> => {
  if (ctx.from) {
    const stateKey = String(ctx.from.id);
    const session = userStates.get(stateKey)?.noteUploadSession;
    if (session?.timer) {
      clearTimeout(session.timer);
    }
    setNoteUploadSession(stateKey, undefined);
  }
  const pending = await listPendingNoteAttachments({ noteId });
  const unarchived = await listUnarchivedNoteAttachments({ noteId });
  if (unarchived.length === 0) {
    clearNotesFlow(String(ctx.from?.id ?? ''));
    await renderNoteDetails(ctx, noteId, viewContext);
    return;
  }
  if (pending.length === 0) {
    clearNotesFlow(String(ctx.from?.id ?? ''));
    await finalizeNoteArchive(ctx, noteId, viewContext);
    return;
  }
  const categories = buildNoteCaptionCategories(pending);
  const summaryLine = buildCaptionSummaryLine(buildSummaryFromNoteAttachments(pending));
  await renderNoteCaptionChoice(ctx, noteId, viewContext, categories, summaryLine);
};

const finalizeNoteArchive = async (
  ctx: Context,
  noteId: string,
  viewContext: { noteDate?: string; page?: number; historyPage?: number }
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const note = await getNoteById({ userId: user.id, id: noteId });
  if (!note) {
    await renderNotesHistory(ctx);
    return;
  }
  const pendingAttachments = await listUnarchivedNoteAttachments({ noteId });
  if (pendingAttachments.length === 0) {
    await renderNoteDetails(ctx, noteId, viewContext);
    return;
  }
  const archiveChatId = getNotesArchiveChatId();
  if (!archiveChatId) {
    await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.attachmentsFailed()] });
    return;
  }
  const telegramMeta = resolveTelegramUserMeta(ctx, user);

  const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
  const savingMessage = targetId
    ? await ctx.api.sendMessage(targetId, labels.notes.saving())
    : null;

  const summary = buildSummaryFromNoteAttachments(pendingAttachments);
  const attachments = pendingAttachments.map((attachment) => ({
    id: attachment.id,
    kind: attachment.kind as ArchiveMediaType,
    fileId: attachment.file_id,
    caption: attachment.caption ?? null
  }));
  const timeLabel = buildArchiveTimeLabel(note.created_at, user.timezone ?? config.defaultTimezone);
  const archiveResult = await sendArchiveItemToChannel(ctx.api, {
    archiveChatId,
    user: {
      firstName: telegramMeta.firstName,
      lastName: telegramMeta.lastName,
      username: telegramMeta.username,
      telegramId: telegramMeta.telegramId,
      appUserId: user.id
    },
    timeLabel,
    kindLabel: labels.archive.kindNote(),
    title: note.title ?? null,
    description: note.description ?? note.body ?? null,
    attachments
  });

  for (const attachment of pendingAttachments) {
    const messageId = attachment.id ? archiveResult.attachmentMessageIds.get(attachment.id) : undefined;
    if (messageId) {
      await updateNoteAttachmentArchiveInfo({
        attachmentId: attachment.id,
        archiveChatId,
        archiveMessageId: messageId
      });
    }
  }

  const existingItem = await getArchiveItemByEntity({ kind: 'note', entityId: note.id });
  const updatedItem = await upsertArchiveItem({
    existing: existingItem,
    ownerUserId: user.id,
    kind: 'note',
    entityId: note.id,
    channelId: archiveChatId,
    title: note.title ?? null,
    description: note.description ?? note.body ?? null,
    summary,
    messageIds: archiveResult.messageIds,
    messageMeta: archiveResult.messageMeta,
    meta: {
      username: telegramMeta.username ?? null,
      first_name: telegramMeta.firstName ?? null,
      last_name: telegramMeta.lastName ?? null,
      telegram_id: telegramMeta.telegramId ?? null,
      created_at: note.created_at,
      summary_line: buildCaptionSummaryLine(summary)
    }
  });

  if (!note.archive_item_id || note.archive_item_id !== updatedItem.id) {
    await updateNote({ userId: user.id, id: note.id, archiveItemId: updatedItem.id });
  }

  if (savingMessage && targetId) {
    try {
      await ctx.api.editMessageText(targetId, savingMessage.message_id, labels.notes.saved());
    } catch {
      await ctx.api.sendMessage(targetId, labels.notes.saved());
    }
  }

  await renderNoteDetails(ctx, noteId, viewContext);
};

const buildReminderCaptionCategories = (attachments: ReminderAttachmentDraft[]): ReminderCaptionCategory[] => {
  const kinds = new Set(attachments.map((attachment) => attachment.kind));
  const categories: ReminderCaptionCategory[] = [];
  if (kinds.has('photo')) categories.push('photo');
  if (kinds.has('video')) categories.push('video');
  if (kinds.has('voice')) categories.push('voice');
  if (kinds.has('video_note')) categories.push('video_note');
  if (kinds.has('document') || kinds.has('audio')) categories.push('files');
  return categories;
};

const renderReminderCaptionChoice = async (
  ctx: Context,
  reminderId: string,
  categories: ReminderCaptionCategory[],
  summaryLine: string
): Promise<void> => {
  if (!ctx.from) return;
  const currentFlow = userStates.get(String(ctx.from.id))?.reminderFlow;
  if (!currentFlow) return;
  const allBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.captionAll(),
    action: 'reminders.caption_all',
    data: { reminderId }
  });
  const byCategoryBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.captionByCategory(),
    action: 'reminders.caption_by_category',
    data: { reminderId }
  });
  const skipBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.captionSkip(),
    action: 'reminders.caption_skip',
    data: { reminderId }
  });
  const kb = new InlineKeyboard()
    .text(allBtn.text, allBtn.callback_data)
    .row()
    .text(byCategoryBtn.text, byCategoryBtn.callback_data)
    .row()
    .text(skipBtn.text, skipBtn.callback_data);

  setReminderFlow(String(ctx.from.id), {
    ...currentFlow,
    reminderId,
    step: 'caption_choice',
    captionCategories: categories
  });

  await renderScreen(ctx, {
    title: labels.reminders.newTitle(),
    bodyLines: [labels.notes.captionsSummary({ summary: summaryLine }), labels.notes.captionsPrompt()],
    inlineKeyboard: kb
  });
};

const promptReminderCaptionCategory = async (
  ctx: Context,
  reminderId: string,
  category: ReminderCaptionCategory,
  categories: ReminderCaptionCategory[]
): Promise<void> => {
  if (!ctx.from) return;
  const currentFlow = userStates.get(String(ctx.from.id))?.reminderFlow;
  if (!currentFlow) return;
  const skipBtn = await makeActionButton(ctx, { label: labels.notesButtons.skip(), action: 'reminders.caption_category_skip', data: { reminderId } });
  const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data);
  setReminderFlow(String(ctx.from.id), {
    ...currentFlow,
    reminderId,
    step: 'caption_category',
    currentCategory: category,
    captionCategories: categories
  });
  const label = labels.notes.captionLabel(category);
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.notes.captionCategoryPrompt({ category: label })], inlineKeyboard: kb });
};

const applyCaptionToReminderAttachments = (
  attachments: ReminderAttachmentDraft[],
  kinds: ReminderAttachmentKind[],
  caption: string | null
): ReminderAttachmentDraft[] =>
  attachments.map((attachment) => {
    if (!kinds.includes(attachment.kind)) return attachment;
    if (attachment.caption && attachment.caption.trim().length > 0) return attachment;
    return { ...attachment, caption };
  });

const startReminderCaptionFlow = async (ctx: Context, flow: ReminderFlow): Promise<void> => {
  if (ctx.from) {
    const stateKey = String(ctx.from.id);
    const session = userStates.get(stateKey)?.reminderUploadSession;
    if (session?.timer) {
      clearTimeout(session.timer);
    }
    setReminderUploadSession(stateKey, undefined);
  }
  const attachments = flow.draft.attachments ?? [];
  const reminderId = getReminderIdFromFlow(flow);
  if (!reminderId) {
    await renderReminders(ctx);
    return;
  }
  if (attachments.length === 0) {
    await finalizeReminderArchive(ctx, reminderId, flow);
    return;
  }
  const categories = buildReminderCaptionCategories(attachments);
  const summaryLine = buildCaptionSummaryLine(buildSummaryFromNoteAttachments(attachments));
  await renderReminderCaptionChoice(ctx, reminderId, categories, summaryLine);
};

const finalizeReminderArchive = async (
  ctx: Context,
  reminderId: string,
  flow: ReminderFlow
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const reminder = await getReminderById(reminderId);
  if (!reminder || reminder.user_id !== user.id) {
    await renderReminders(ctx);
    return;
  }
  const attachments = flow.draft.attachments ?? [];
  if (attachments.length === 0) {
    if (flow.mode === 'edit') {
      clearReminderFlow(String(ctx.from?.id ?? ''));
      await renderReminderDetails(ctx, reminderId);
      return;
    }
    setReminderFlow(String(ctx.from?.id ?? ''), { ...flow, step: 'schedule_type' });
    await renderReminderScheduleTypePrompt(ctx, flow.mode, reminderId);
    return;
  }

  const archiveChatId = getRemindersArchiveChatId();
  if (!archiveChatId) {
    await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.attachmentsFailed()] });
    return;
  }

  const telegramMeta = resolveTelegramUserMeta(ctx, user);
  const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
  const savingMessage = targetId ? await ctx.api.sendMessage(targetId, labels.reminders.saving()) : null;

  const summary = buildSummaryFromNoteAttachments(attachments);
  const archiveResult = await sendArchiveItemToChannel(ctx.api, {
    archiveChatId,
    user: {
      firstName: telegramMeta.firstName,
      lastName: telegramMeta.lastName,
      username: telegramMeta.username,
      telegramId: telegramMeta.telegramId,
      appUserId: user.id
    },
    timeLabel: buildArchiveTimeLabel(reminder.created_at, user.timezone ?? config.defaultTimezone),
    kindLabel: labels.archive.kindReminder(),
    title: reminder.title ?? null,
    description: reminder.description ?? null,
    attachments: attachments.map((attachment, index) => ({
      id: String(index),
      kind: attachment.kind,
      fileId: attachment.fileId,
      caption: attachment.caption ?? null
    }))
  });

  for (const [index, attachment] of attachments.entries()) {
    const messageId = archiveResult.attachmentMessageIds.get(String(index));
    if (!messageId) continue;
    await createReminderAttachment({
      reminderId,
      archiveChatId,
      archiveMessageId: messageId,
      kind: attachment.kind,
      fileId: attachment.fileId,
      caption: attachment.caption ?? null,
      fileUniqueId: attachment.fileUniqueId ?? null,
      mimeType: attachment.mimeType ?? null
    });
  }

  const existingItem = await getArchiveItemByEntity({ kind: 'reminder', entityId: reminderId });
  const updatedItem = await upsertArchiveItem({
    existing: existingItem,
    ownerUserId: user.id,
    kind: 'reminder',
    entityId: reminderId,
    channelId: archiveChatId,
    title: reminder.title ?? null,
    description: reminder.description ?? null,
    summary,
    messageIds: archiveResult.messageIds,
    messageMeta: archiveResult.messageMeta,
    meta: {
      username: telegramMeta.username ?? null,
      first_name: telegramMeta.firstName ?? null,
      last_name: telegramMeta.lastName ?? null,
      telegram_id: telegramMeta.telegramId ?? null,
      created_at: reminder.created_at,
      summary_line: buildCaptionSummaryLine(summary)
    }
  });

  if (!reminder.archive_item_id || reminder.archive_item_id !== updatedItem.id) {
    await updateReminder(reminderId, { archiveItemId: updatedItem.id });
  }

  if (savingMessage && targetId) {
    try {
      await ctx.api.editMessageText(targetId, savingMessage.message_id, labels.reminders.saved());
    } catch {
      await ctx.api.sendMessage(targetId, labels.reminders.saved());
    }
  }

  if (flow.mode === 'edit') {
    clearReminderFlow(String(ctx.from?.id ?? ''));
    await renderReminderDetails(ctx, reminderId);
    return;
  }

  setReminderFlow(String(ctx.from?.id ?? ''), { ...flow, step: 'schedule_type' });
  await renderReminderScheduleTypePrompt(ctx, flow.mode, reminderId);
};

const renderNoteAttachmentsList = async (
  ctx: Context,
  params: { noteId: string; kind: NoteAttachmentKind; noteDate: string; page: number; historyPage: number }
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const note = await getNoteById({ userId: user.id, id: params.noteId });
  if (!note) {
    await renderNotesHistory(ctx);
    return;
  }

  const kinds =
    params.kind === 'document'
      ? (['document', 'audio'] as NoteAttachmentKind[])
      : params.kind === 'video'
        ? (['video', 'video_note'] as NoteAttachmentKind[])
        : [params.kind];
  const attachments = await listNoteAttachmentsByKinds({ noteId: params.noteId, kinds });
  const kindLabel = labels.notes.kindLabel(params.kind);
  const noteDate = params.noteDate || note.note_date;
  const lines: string[] = [];
  if (!attachments.length) {
    lines.push(labels.notes.attachmentsEmpty({ kind: kindLabel }));
  } else {
    lines.push(labels.notes.attachmentsHeader({ kind: kindLabel }), '');
    attachments.forEach((attachment, index) => {
      const local = formatInstantToLocal(attachment.created_at, user.timezone ?? config.defaultTimezone);
      const caption = attachment.caption && attachment.caption.trim().length > 0 ? attachment.caption : labels.notes.attachmentNoCaption();
      lines.push(labels.notes.attachmentLine({ index: String(index + 1), time: local.time, caption }));
    });
  }

  const kb = new InlineKeyboard();
  for (let index = 0; index < attachments.length; index += 1) {
    const attachment = attachments[index];
    const kindEmoji = getNoteAttachmentKindEmoji(params.kind);
    const btn = await makeActionButton(ctx, {
      label: kindEmoji ? `${kindEmoji} ${index + 1}` : `${index + 1}`,
      action: 'notes.attachment_open',
      data: { noteId: params.noteId, attachmentId: attachment.id, kind: params.kind, noteDate, page: params.page, historyPage: params.historyPage }
    });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backBtn = await makeActionButton(ctx, {
    label: labels.notesButtons.back(),
    action: 'notes.view_note',
    data: { noteId: params.noteId, noteDate, page: params.page, historyPage: params.historyPage }
  });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { title: labels.notes.attachmentsTitle({ kind: kindLabel }), bodyLines: lines, inlineKeyboard: kb });
};

const buildNoteCaptionBlock = (
  note: NoteRow,
  category: NoteCaptionCategory,
  _attachments: NoteAttachmentRow[]
): string | null => {
  const header = getNoteCaptionHeader(category);
  const singleCaption = resolveNoteCaptionForCategory(note, category)?.trim();
  if (singleCaption) {
    return `${header}\n${singleCaption}`;
  }
  return null;
};

const sendNoteAttachmentsToUser = async (
  ctx: Context,
  targetId: number,
  _category: NoteCaptionCategory,
  attachments: NoteAttachmentRow[]
): Promise<void> => {
  if (attachments.length === 0) return;
  const items: StoredAttachment[] = attachments.map((attachment) => ({
    kind: attachment.kind as StoredAttachment['kind'],
    fileId: attachment.file_id,
    caption: attachment.caption ?? undefined
  }));
  await sendAttachments(ctx, targetId, items);
};

const sendNoteAttachmentsByKind = async (
  ctx: Context,
  params: { noteId: string; kind: NoteAttachmentKind; noteDate?: string; page?: number; historyPage?: number }
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const note = await getNoteById({ userId: user.id, id: params.noteId });
  if (!note) {
    await renderNotesHistory(ctx);
    return;
  }
  const kinds =
    params.kind === 'document'
      ? (['document', 'audio'] as NoteAttachmentKind[])
      : params.kind === 'video'
        ? (['video', 'video_note'] as NoteAttachmentKind[])
        : [params.kind];
  const attachments = await listNoteAttachmentsByKinds({ noteId: params.noteId, kinds });
  const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
  if (!targetId) {
    await renderNoteDetails(ctx, note.id, { noteDate: params.noteDate, page: params.page, historyPage: params.historyPage });
    return;
  }

  const category: NoteCaptionCategory = params.kind === 'document' || params.kind === 'audio' ? 'files' : params.kind;
  await sendNoteAttachmentsToUser(ctx, targetId, category, attachments);
  const captionBlock = buildNoteCaptionBlock(note, category, attachments);
  if (captionBlock) {
    await ctx.api.sendMessage(targetId, captionBlock);
  }

  await renderNoteDetails(ctx, note.id, { noteDate: params.noteDate, page: params.page, historyPage: params.historyPage });
};

const sendNoteEverything = async (
  ctx: Context,
  params: { noteId: string; noteDate?: string; page?: number; historyPage?: number }
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const note = await getNoteById({ userId: user.id, id: params.noteId });
  if (!note) {
    await renderNotesHistory(ctx);
    return;
  }
  const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
  if (!targetId) {
    await renderNoteDetails(ctx, note.id, { noteDate: params.noteDate, page: params.page, historyPage: params.historyPage });
    return;
  }
  const attachments = await listNoteAttachments({ noteId: note.id });
  const photos = attachments.filter((attachment) => attachment.kind === 'photo');
  const videos = attachments.filter((attachment) => attachment.kind === 'video' || attachment.kind === 'video_note');
  const voices = attachments.filter((attachment) => attachment.kind === 'voice');
  const files = attachments.filter((attachment) => attachment.kind === 'document' || attachment.kind === 'audio');

  const batches: Array<{ category: NoteCaptionCategory; items: NoteAttachmentRow[] }> = [
    { category: 'photo', items: photos },
    { category: 'video', items: videos },
    { category: 'voice', items: voices },
    { category: 'files', items: files }
  ];
  for (const batch of batches) {
    if (batch.items.length === 0) continue;
    await sendNoteAttachmentsToUser(ctx, targetId, batch.category, batch.items);
    const captionBlock = buildNoteCaptionBlock(note, batch.category, batch.items);
    if (captionBlock) {
      await ctx.api.sendMessage(targetId, captionBlock);
    }
  }

  const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
  const description = note.description ?? note.body ?? '';
  const detailLines = [
    labels.notes.detailTitle({ title }),
    labels.notes.descriptionLabel(),
    description.length > 0 ? description : '—'
  ];
  const chunks = splitTextForTelegram(detailLines.join('\n'));
  for (const chunk of chunks) {
    await ctx.api.sendMessage(targetId, chunk);
  }

  await renderNoteDetails(ctx, note.id, { noteDate: params.noteDate, page: params.page, historyPage: params.historyPage });
};

const handleNoteAttachmentMessage = async (
  ctx: Context,
  params: { kind: NoteAttachmentKind; fileId: string; fileUniqueId?: string | null; caption?: string | null }
): Promise<void> => {
  if (!ctx.from) return;
  const stateKey = String(ctx.from.id);
  const state = userStates.get(stateKey);
  const flow = state?.notesFlow;
  if (!flow || flow.mode !== 'create' || flow.step !== 'attachments') return;
  const { user } = await ensureUserAndSettings(ctx);

  try {
    await createNoteAttachment({
      noteId: flow.noteId,
      kind: params.kind,
      fileId: params.fileId,
      fileUniqueId: params.fileUniqueId ?? null,
      caption: params.caption ?? null,
      captionPending: !params.caption,
      archiveChatId: null,
      archiveMessageId: null
    });
    const now = Date.now();
    const existingSession = state?.noteUploadSession;
    const pendingKinds = { ...(existingSession?.pendingKinds ?? {}) };
    pendingKinds[params.kind] = (pendingKinds[params.kind] ?? 0) + 1;

    if (existingSession?.timer) {
      clearTimeout(existingSession.timer);
    }

    const timer = setTimeout(async () => {
      const latestState = userStates.get(stateKey);
      const session = latestState?.noteUploadSession;
      if (!session || session.noteId !== flow.noteId) return;
      if (session.prompted) return;
      const idleFor = Date.now() - session.lastReceivedAt;
      if (idleFor < NOTE_UPLOAD_IDLE_MS) return;
      const saveBtn = await makeActionButton(ctx, {
        label: labels.notesButtons.saveNow(),
        action: 'notes.attachments_save',
        data: { noteId: session.noteId, ...(session.viewContext ?? {}) }
      });
      const continueBtn = await makeActionButton(ctx, {
        label: labels.notesButtons.continue(),
        action: 'notes.attachments_continue',
        data: { noteId: session.noteId, ...(session.viewContext ?? {}) }
      });
      const kb = new InlineKeyboard().text(saveBtn.text, saveBtn.callback_data).row().text(continueBtn.text, continueBtn.callback_data);
      setNoteUploadSession(stateKey, { ...session, prompted: true });
      await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.attachmentsIdlePrompt()], inlineKeyboard: kb });
    }, NOTE_UPLOAD_IDLE_MS);

    setNoteUploadSession(stateKey, {
      noteId: flow.noteId,
      viewContext: flow.viewContext,
      pendingKinds,
      lastReceivedAt: now,
      prompted: false,
      timer
    });
  } catch (error) {
    console.error({ scope: 'notes', event: 'attachment_save_failed', error, kind: params.kind });
    await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.attachmentsFailed()] });
  }
};

const handleReminderAttachmentMessage = async (
  ctx: Context,
  params: { kind: ReminderAttachmentKind; fileId: string; fileUniqueId?: string | null; caption?: string | null; mimeType?: string | null }
): Promise<void> => {
  if (!ctx.from) return;
  const stateKey = String(ctx.from.id);
  const flow = userStates.get(stateKey)?.reminderFlow;
  if (!flow || (flow.step !== 'attachments' && flow.step !== 'description')) return;
  const reminderId = getReminderIdFromFlow(flow);
  if (!reminderId) {
    await renderReminders(ctx);
    return;
  }

  const attachments = [
    ...(flow.draft.attachments ?? []),
    {
      kind: params.kind,
      fileId: params.fileId,
      caption: params.caption ?? null,
      fileUniqueId: params.fileUniqueId ?? null,
      mimeType: params.mimeType ?? null
    }
  ];

  const now = Date.now();
  const existingSession = userStates.get(stateKey)?.reminderUploadSession;
  const pendingKinds = { ...(existingSession?.pendingKinds ?? {}) };
  pendingKinds[params.kind] = (pendingKinds[params.kind] ?? 0) + 1;

  if (existingSession?.timer) {
    clearTimeout(existingSession.timer);
  }

  const timer = setTimeout(async () => {
    const latestState = userStates.get(stateKey);
    const session = latestState?.reminderUploadSession;
    if (!session || session.reminderId !== reminderId) return;
    if (session.prompted) return;
    const idleFor = Date.now() - session.lastReceivedAt;
    if (idleFor < REMINDER_UPLOAD_IDLE_MS) return;
    const saveBtn = await makeActionButton(ctx, {
      label: labels.notesButtons.saveNow(),
      action: 'reminders.attachments_save',
      data: { reminderId }
    });
    const continueBtn = await makeActionButton(ctx, {
      label: labels.notesButtons.continue(),
      action: 'reminders.attachments_continue',
      data: { reminderId }
    });
    const kb = new InlineKeyboard().text(saveBtn.text, saveBtn.callback_data).row().text(continueBtn.text, continueBtn.callback_data);
    setReminderUploadSession(stateKey, { ...session, prompted: true });
    await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.attachmentsIdlePrompt()], inlineKeyboard: kb });
  }, REMINDER_UPLOAD_IDLE_MS);

  setReminderFlow(stateKey, { ...flow, step: 'attachments', draft: { ...flow.draft, attachments } });
  setReminderUploadSession(stateKey, {
    reminderId,
    pendingKinds,
    lastReceivedAt: now,
    prompted: false,
    timer
  });

};

const renderXpSummary = async (ctx: Context): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const summary = await getXpSummary(user.id);

  const lines = [
    t('screens.reports.xp_earned', { earned: summary.earned }),
    t('screens.reports.xp_spent', { spent: summary.spent }),
    t('screens.reports.xp_net', { net: summary.net })
  ];

  const kb = await buildReportsMenuKeyboard(ctx);
  await renderScreen(ctx, { titleKey: 'screens.reports.xp_title', bodyLines: lines, inlineKeyboard: kb });
};

const renderReportcar = async (ctx: Context): Promise<void> => {
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  await renderScreen(ctx, { titleKey: t('screens.reportcar.title'), bodyLines: [t('screens.reportcar.coming_soon')], inlineKeyboard: new InlineKeyboard().text(back.text, back.callback_data) });
};

const routineTypeLabel = (routine: RoutineRow): string => {
  if (routine.routine_type === 'duration_minutes') return t('screens.routines.type_duration');
  if (routine.routine_type === 'number') return t('screens.routines.type_number');
  return t('screens.routines.type_boolean');
};

const routineXpLabel = (routine: RoutineRow): string => {
  if (routine.xp_mode === 'fixed') return t('screens.routines.xp_fixed', { xp: routine.xp_value ?? 0 });
  if (routine.xp_mode === 'per_minute' || routine.xp_mode === 'per_number') {
    return t('screens.routines.xp_per_unit', { xp: routine.xp_value ?? 0 });
  }
  return t('screens.routines.xp_none');
};

const routineTaskXpLabel = (task: RoutineTaskRow): string => {
  const opts = (task.options_json ?? {}) as { per?: unknown; xp?: unknown; perNumber?: unknown; xpPerUnit?: unknown };
  const per = Number((opts.per as number) ?? (opts.perNumber as number));
  const xp = Number((opts.xp as number) ?? (opts.xpPerUnit as number) ?? task.xp_value ?? 0);
  const ratioPer = Number.isFinite(per) && per > 0 ? per : 1;
  if (task.xp_mode === 'fixed') return t('screens.routines.xp_fixed', { xp: task.xp_value ?? 0 });
  if (task.xp_mode === 'per_minute') {
    const maxPart = task.xp_max_per_day && task.xp_max_per_day > 0 ? t('screens.routines.xp_max_suffix', { xp: task.xp_max_per_day }) : '';
    return t('screens.routines.xp_per_minute_ratio', { ratio: `${ratioPer}:${xp}`, max: maxPart });
  }
  if (task.xp_mode === 'per_number') {
    const maxPart = task.xp_max_per_day && task.xp_max_per_day > 0 ? t('screens.routines.xp_max_suffix', { xp: task.xp_max_per_day }) : '';
    return t('screens.routines.xp_per_number_ratio', { ratio: `${ratioPer}:${xp}`, max: maxPart });
  }
  return t('screens.routines.xp_none');
};

const renderRoutinesRoot = async (ctx: Context, flash?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const routines = await listRoutines(user.id);
  const lines: string[] = [];
  if (flash) lines.push(flash);
  if (routines.length === 0) {
    lines.push(t('screens.routines.empty'));
  } else {
    lines.push(t('screens.routines.tap_to_edit'));
  }

  const kb = new InlineKeyboard();
  const addBtn = await makeActionButton(ctx, { label: t('buttons.routines_add'), action: 'routines.add' });
  kb.text(addBtn.text, addBtn.callback_data).row();
  for (const routine of routines) {
    const btn = await makeActionButton(ctx, { label: `🧩 ${routine.title}`, action: 'routines.view', data: { routineId: routine.id } });
    kb.text(btn.text, btn.callback_data).row();
  }
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  kb.text(back.text, back.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderRoutineDetails = async (ctx: Context, routineId: string, flash?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const routine = await getRoutineById(routineId);
  if (!routine || routine.user_id !== user.id) {
    await renderRoutinesRoot(ctx);
    return;
  }
  const tasks = await listRoutineTasks(routineId);
  const primaryTaskTitle = tasks[0]?.title ?? t('screens.routine_tasks.list_empty');
  const lines: string[] = [
    t('screens.routines.detail_title', { title: routine.title }),
    primaryTaskTitle,
    routineTypeLabel(routine),
    routineXpLabel(routine)
  ];
  if (flash) lines.push(flash);

  const kb = new InlineKeyboard();
  const toggleBtn = await makeActionButton(ctx, {
    label: t('buttons.routines_toggle_active'),
    action: 'routines.toggle',
    data: { routineId }
  });
  const editTitleBtn = await makeActionButton(ctx, { label: t('buttons.routines_edit_title'), action: 'routines.edit_title', data: { routineId } });
  const editDescBtn = await makeActionButton(ctx, { label: t('buttons.routines_edit_description'), action: 'routines.edit_description', data: { routineId } });
  const editTypeBtn = await makeActionButton(ctx, { label: t('buttons.routines_edit_type'), action: 'routines.edit_type', data: { routineId } });
  const editXpBtn = await makeActionButton(ctx, { label: t('buttons.routines_edit_xp'), action: 'routines.edit_xp_mode', data: { routineId } });
  const editTasksBtn = await makeActionButton(ctx, { label: t('buttons.routine_tasks_manage'), action: 'routines.tasks', data: { routineId } });
  const deleteBtn = await makeActionButton(ctx, { label: t('buttons.routines_delete'), action: 'routines.delete_confirm', data: { routineId } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.root' });

  kb.text(toggleBtn.text, toggleBtn.callback_data)
    .row()
    .text(editTitleBtn.text, editTitleBtn.callback_data)
    .row()
    .text(editDescBtn.text, editDescBtn.callback_data)
    .row()
    .text(editTypeBtn.text, editTypeBtn.callback_data)
    .row()
    .text(editXpBtn.text, editXpBtn.callback_data)
    .row()
    .text(editTasksBtn.text, editTasksBtn.callback_data)
    .row()
    .text(deleteBtn.text, deleteBtn.callback_data)
    .row()
    .text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderRoutineDeleteConfirm = async (ctx: Context, routineId: string): Promise<void> => {
  const yesBtn = await makeActionButton(ctx, { label: t('buttons.tpl_yes'), action: 'routines.delete', data: { routineId } });
  const noBtn = await makeActionButton(ctx, { label: t('buttons.tpl_no'), action: 'routines.view', data: { routineId } });
  const kb = new InlineKeyboard().text(yesBtn.text, yesBtn.callback_data).row().text(noBtn.text, noBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.delete_confirm')], inlineKeyboard: kb });
};

const renderRoutineTasks = async (ctx: Context, routineId: string, flash?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const routine = await getRoutineById(routineId);
  if (!routine || routine.user_id !== user.id) {
    await renderRoutinesRoot(ctx);
    return;
  }
  const tasks = await listRoutineTasks(routineId);
  const lines: string[] = [];
  if (flash) lines.push(flash);
  if (tasks.length === 0) {
    lines.push(t('screens.routine_tasks.list_empty'));
  } else {
    tasks.forEach((task, idx) => {
      const typeIcon = task.item_type === 'boolean' ? '⭐' : task.item_type === 'duration_minutes' ? '⏱' : '🔢';
      lines.push(`${idx + 1}) ${task.title} — ${typeIcon} ${routineTaskXpLabel(task)}`);
    });
  }

  const kb = new InlineKeyboard();
  const addBtn = await makeActionButton(ctx, { label: t('buttons.routine_tasks_add'), action: 'routines.task_add', data: { routineId } });
  kb.text(addBtn.text, addBtn.callback_data).row();
  for (const task of tasks) {
    const editBtn = await makeActionButton(ctx, { label: `✏️ ${task.title}`, action: 'routines.task_edit', data: { routineId, taskId: task.id } });
    const delBtn = await makeActionButton(ctx, { label: t('buttons.routine_tasks_delete'), action: 'routines.task_delete_confirm', data: { routineId, taskId: task.id } });
    kb.text(editBtn.text, editBtn.callback_data).text(delBtn.text, delBtn.callback_data).row();
  }
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.view', data: { routineId } });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { title: t('screens.routine_tasks.title', { title: routine.title }), bodyLines: lines, inlineKeyboard: kb });
};

const promptRoutineTaskTitle = async (ctx: Context, params: { routineId: string; taskId?: string }) => {
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.tasks', data: { routineId: params.routineId } });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: params.taskId ? t('screens.routine_tasks.edit_title') : t('screens.routine_tasks.add_title'),
    bodyLines: [t('screens.routine_tasks.add_prompt')],
    inlineKeyboard: kb
  });
};

const promptRoutineTaskDescription = async (ctx: Context, params: { routineId: string; taskId?: string }) => {
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.tasks', data: { routineId: params.routineId } });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: params.taskId ? t('screens.routine_tasks.edit_title') : t('screens.routine_tasks.add_title'),
    bodyLines: [t('screens.routine_tasks.add_description')],
    inlineKeyboard: kb
  });
};

const promptRoutineTaskType = async (ctx: Context, params: { routineId: string; taskId?: string }) => {
  const kb = new InlineKeyboard();
  const types: { key: RoutineTaskRow['item_type']; label: string }[] = [
    { key: 'boolean', label: t('screens.form_builder.type_boolean_label') },
    { key: 'duration_minutes', label: t('screens.form_builder.type_duration_label') },
    { key: 'number', label: t('screens.form_builder.type_number_label') }
  ];
  for (const type of types) {
    const btn = await makeActionButton(ctx, { label: type.label, action: 'routines.task_select_type', data: { routineId: params.routineId, taskId: params.taskId, itemType: type.key } });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.tasks', data: { routineId: params.routineId } });
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routine_tasks.title_short'), bodyLines: [t('screens.routine_tasks.choose_type')], inlineKeyboard: kb });
};

const promptRoutineTaskXpMode = async (ctx: Context, params: { routineId: string; taskId?: string; itemType?: RoutineTaskRow['item_type'] }) => {
  const allowed = allowedRoutineTaskXpModes(params.itemType);
  const modes = [
    { key: 'fixed', label: t('screens.routines.xp_mode_fixed') },
    { key: 'per_minute', label: t('screens.routines.xp_mode_time') },
    { key: 'per_number', label: t('screens.routines.xp_mode_number') }
  ].filter((m) => allowed.includes(m.key as 'none' | 'fixed' | 'per_minute' | 'per_number'));
  const kb = new InlineKeyboard();
  for (const mode of modes) {
    const btn = await makeActionButton(ctx, { label: mode.label, action: 'routines.task_select_xp_mode', data: { routineId: params.routineId, taskId: params.taskId, xpMode: mode.key } });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.tasks', data: { routineId: params.routineId } });
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routine_tasks.title_short'), bodyLines: [t('screens.routines.choose_xp_mode')], inlineKeyboard: kb });
};

const promptRoutineTaskXpValue = async (
  ctx: Context,
  params: { routineId: string; taskId?: string; xpMode: RoutineTaskRow['xp_mode']; itemType?: RoutineTaskRow['item_type'] }
) => {
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.tasks', data: { routineId: params.routineId } });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  const body =
    params.xpMode === 'fixed'
      ? t('screens.routines.ask_fixed_xp')
      : params.xpMode === 'per_number'
        ? t('screens.routines.ask_number_ratio')
        : t('screens.routines.ask_time_ratio');
  await renderScreen(ctx, { titleKey: t('screens.routine_tasks.title_short'), bodyLines: [body], inlineKeyboard: kb });
};

const promptRoutineTaskXpMax = async (ctx: Context, params: { routineId: string; taskId?: string }) => {
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.tasks', data: { routineId: params.routineId } });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routine_tasks.title_short'), bodyLines: [t('screens.routines.ask_time_xp_max')], inlineKeyboard: kb });
};

const renderRoutineTaskDeleteConfirm = async (ctx: Context, params: { routineId: string; taskId: string }) => {
  const yesBtn = await makeActionButton(ctx, { label: t('buttons.tpl_yes'), action: 'routines.task_delete', data: params });
  const noBtn = await makeActionButton(ctx, { label: t('buttons.tpl_no'), action: 'routines.tasks', data: { routineId: params.routineId } });
  const kb = new InlineKeyboard().text(yesBtn.text, yesBtn.callback_data).row().text(noBtn.text, noBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routine_tasks.title_short'), bodyLines: [t('screens.routine_tasks.delete_confirm')], inlineKeyboard: kb });
};
const renderTasks = async (ctx: Context): Promise<void> => {
  await renderRoutinesRoot(ctx);
};

const renderTodo = async (ctx: Context): Promise<void> => {
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  await renderScreen(ctx, { titleKey: t('screens.todo.title'), bodyLines: [t('screens.todo.coming_soon')], inlineKeyboard: new InlineKeyboard().text(back.text, back.callback_data) });
};

const renderPlanning = async (ctx: Context): Promise<void> => {
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  await renderScreen(ctx, { titleKey: t('screens.planning.title'), bodyLines: [t('screens.planning.coming_soon')], inlineKeyboard: new InlineKeyboard().text(back.text, back.callback_data) });
};

const renderMyDay = async (ctx: Context): Promise<void> => {
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  await renderScreen(ctx, { titleKey: t('screens.my_day.title'), bodyLines: [t('screens.my_day.coming_soon')], inlineKeyboard: new InlineKeyboard().text(back.text, back.callback_data) });
};

const renderFreeText = async (ctx: Context): Promise<void> => {
  await renderNotesToday(ctx);
};

const renderReminders = async (ctx: Context): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);

  const reminders = await listRemindersForUser(user.id);
  const attachmentCounts = await listReminderAttachmentCounts({ reminderIds: reminders.map((reminder) => reminder.id) });

  const lines: string[] = [];

  if (reminders.length === 0) {
    lines.push(labels.reminders.empty());
  } else {
    lines.push(labels.reminders.listHeader(), '');
    for (const r of reminders) {
      const status = isReminderActive(r) ? labels.reminders.statusOnLabel() : labels.reminders.statusOffLabel();
      const local = r.next_run_at ? formatInstantToLocal(r.next_run_at, user.timezone ?? config.defaultTimezone) : null;
      const timePart = local ? `${local.date} ${local.time}` : labels.reminders.noTime();
      const attachments = attachmentCounts[r.id] ?? 0;
      const title = r.title && r.title.trim().length > 0 ? r.title : labels.reminders.untitled();
      lines.push(
        labels.reminders.itemLine({
          status,
          time: timePart,
          title,
          attachments: String(attachments)
        })
      );
    }
  }

  lines.push('', labels.reminders.actionsHint());

  const kb = new InlineKeyboard();

  const newBtn = await makeActionButton(ctx, { label: labels.remindersButtons.new(), action: 'reminders.new' });
  kb.text(newBtn.text, newBtn.callback_data).row();

  for (const r of reminders) {
    const editBtn = await makeActionButton(ctx, { label: labels.remindersButtons.edit(), action: 'reminders.edit_open', data: { reminderId: r.id } });
    const toggleBtn = await makeActionButton(ctx, {
      label: isReminderActive(r) ? labels.remindersButtons.toggleOff() : labels.remindersButtons.toggleOn(),
      action: 'reminders.toggle',
      data: { reminderId: r.id }
    });
    const deleteBtn = await makeActionButton(ctx, {
      label: labels.remindersButtons.delete(),
      action: 'reminders.delete',
      data: { reminderId: r.id }
    });
    kb.text(editBtn.text, editBtn.callback_data).text(toggleBtn.text, toggleBtn.callback_data).text(deleteBtn.text, deleteBtn.callback_data).row();
  }

  const back = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: 'nav.dashboard' });
  kb.text(back.text, back.callback_data);

  await renderScreen(ctx, {
    title: labels.reminders.title(),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderReminderDetails = async (ctx: Context, reminderId: string, flash?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const reminder = await getReminderById(reminderId);
  if (!reminder || reminder.user_id !== user.id) {
    await renderReminders(ctx);
    return;
  }

  const timezone = user.timezone ?? config.defaultTimezone;
  const local = reminder.next_run_at ? formatInstantToLocal(reminder.next_run_at, timezone) : null;
  const statusLabel = isReminderActive(reminder) ? labels.reminders.statusOn() : labels.reminders.statusOff();
  const attachments = await listReminderAttachments({ reminderId: reminder.id });
  const scheduleLabel = labels.reminders.scheduleTypeLabel(reminder.schedule_type);

  const rawDescription = reminder.description?.trim() ?? '';
  const hasArchivedDescription = Boolean(reminder.archive_item_id || reminder.desc_group_key);
  const descriptionNotice = labels.reminders.detailsArchivedNotice();
  const buildReminderLines = (limit: number): string[] => {
    const showNotice = hasArchivedDescription || rawDescription.length > limit;
    const detail = rawDescription.length
      ? buildArchivedPreview(rawDescription, limit, showNotice, descriptionNotice)
      : labels.reminders.detailsEmpty();
    const title = reminder.title && reminder.title.trim().length > 0 ? reminder.title : labels.reminders.untitled();
    return [
      flash,
      labels.reminders.detailsTitleLine({ title }),
      labels.reminders.detailsDetailLine({ detail }),
      labels.reminders.detailsScheduleLine({ schedule: scheduleLabel }),
      labels.reminders.detailsScheduledLine({ scheduled: local ? `${local.date} ${local.time}` : labels.reminders.noTime() }),
      labels.reminders.detailsStatusLine({ status: statusLabel, enabled: isReminderActive(reminder) }),
      labels.reminders.detailsAttachmentsLine({ count: String(attachments.length) })
    ].filter(Boolean) as string[];
  };
  let lines = buildReminderLines(REMINDER_DESC_PREVIEW_LIMIT);
  if (lines.join('\n').length > REMINDER_DETAIL_MAX_CHARS) {
    lines = buildReminderLines(300);
  }
  if (lines.join('\n').length > REMINDER_DETAIL_MAX_CHARS) {
    lines = buildReminderLines(150);
  }

  const kb = new InlineKeyboard();
  if (hasArchivedDescription || rawDescription.length > REMINDER_DESC_PREVIEW_LIMIT) {
    const viewBtn = await makeActionButton(ctx, {
      label: labels.remindersButtons.viewFull(),
      action: 'reminders.desc_view',
      data: { reminderId }
    });
    kb.text(viewBtn.text, viewBtn.callback_data).row();
  }
  const editTitleBtn = await makeActionButton(ctx, { label: labels.remindersButtons.editTitle(), action: 'reminders.edit_title', data: { reminderId } });
  const editDetailBtn = await makeActionButton(ctx, { label: labels.remindersButtons.editDetail(), action: 'reminders.edit_detail', data: { reminderId } });
  const editScheduleBtn = await makeActionButton(ctx, { label: labels.remindersButtons.editSchedule(), action: 'reminders.edit_schedule', data: { reminderId } });
  const attachBtn = await makeActionButton(ctx, { label: labels.remindersButtons.attach(), action: 'reminders.attach', data: { reminderId } });
  const toggleBtn = await makeActionButton(ctx, {
    label: isReminderActive(reminder) ? labels.remindersButtons.toggleOff() : labels.remindersButtons.toggleOn(),
    action: 'reminders.toggle',
    data: { reminderId }
  });
  const deleteBtn = await makeActionButton(ctx, { label: labels.remindersButtons.delete(), action: 'reminders.delete', data: { reminderId } });
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: 'nav.reminders' });

  kb.text(editTitleBtn.text, editTitleBtn.callback_data).row();
  kb.text(editDetailBtn.text, editDetailBtn.callback_data).row();
  kb.text(editScheduleBtn.text, editScheduleBtn.callback_data).row();
  kb.text(attachBtn.text, attachBtn.callback_data).row();
  kb.text(toggleBtn.text, toggleBtn.callback_data).row();
  kb.text(deleteBtn.text, deleteBtn.callback_data).row();
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.reminders.detailsTitle({ time: local?.time ?? reminder.at_time }),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderReminderTitlePrompt = async (ctx: Context, mode: ReminderFlow['mode'], reminderId?: string): Promise<void> => {
  const kb = new InlineKeyboard();
  if (mode === 'create') {
    const skipBtn = await makeActionButton(ctx, { label: labels.notesButtons.skip(), action: 'reminders.skip_title' });
    kb.text(skipBtn.text, skipBtn.callback_data).row();
  }
  const backAction = mode === 'edit' ? 'reminders.edit_open' : 'nav.reminders';
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: reminderId ? { reminderId } : undefined });
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.newEnterTitle()], inlineKeyboard: kb });
};

const renderReminderDescriptionPrompt = async (
  ctx: Context,
  mode: ReminderFlow['mode'],
  reminderId?: string
): Promise<void> => {
  const kb = new InlineKeyboard();
  if (mode === 'create') {
    const skipBtn = await makeActionButton(ctx, { label: labels.notesButtons.skip(), action: 'reminders.skip_description', data: reminderId ? { reminderId } : undefined });
    kb.text(skipBtn.text, skipBtn.callback_data).row();
  }
  const doneBtn = await makeActionButton(ctx, { label: labels.remindersButtons.descriptionDone(), action: 'reminders.description_done', data: reminderId ? { reminderId } : undefined });
  const attachBtn = await makeActionButton(ctx, { label: labels.remindersButtons.attach(), action: 'reminders.attach', data: reminderId ? { reminderId } : undefined });
  const backAction = mode === 'edit' ? 'reminders.edit_open' : 'nav.reminders';
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: reminderId ? { reminderId } : undefined });
  kb.text(doneBtn.text, doneBtn.callback_data).row();
  kb.text(attachBtn.text, attachBtn.callback_data).row();
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.descriptionPrompt()], inlineKeyboard: kb });
};

const renderReminderScheduleTypePrompt = async (
  ctx: Context,
  mode: ReminderFlow['mode'],
  reminderId?: string
): Promise<void> => {
  const kb = new InlineKeyboard();
  const types: Array<{ key: ReminderScheduleType; label: string }> = [
    { key: 'once', label: labels.reminders.scheduleOnce() },
    { key: 'hourly', label: labels.reminders.scheduleHourly() },
    { key: 'daily', label: labels.reminders.scheduleDaily() },
    { key: 'weekly', label: labels.reminders.scheduleWeekly() },
    { key: 'monthly', label: labels.reminders.scheduleMonthly() },
    { key: 'yearly', label: labels.reminders.scheduleYearly() }
  ];
  for (const entry of types) {
    const btn = await makeActionButton(ctx, { label: entry.label, action: 'reminders.schedule_type', data: { scheduleType: entry.key, mode, reminderId } });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backAction = mode === 'edit' ? 'reminders.edit_open' : 'nav.reminders';
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: reminderId ? { reminderId } : undefined });
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.scheduleTypePrompt()], inlineKeyboard: kb });
};

const renderReminderAttachmentPrompt = async (
  ctx: Context,
  params: { mode: ReminderFlow['mode']; reminderId?: string }
): Promise<void> => {
  const doneBtn = await makeActionButton(ctx, { label: labels.notesButtons.attachDone(), action: 'reminders.attach_done', data: params.reminderId ? { reminderId: params.reminderId } : undefined });
  const backAction = params.mode === 'edit' ? 'reminders.edit_open' : 'reminders.schedule_back';
  const backData = params.mode === 'edit' ? { reminderId: params.reminderId } : undefined;
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: backData });
  const kb = new InlineKeyboard().text(doneBtn.text, doneBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.attachmentsPrompt()], inlineKeyboard: kb });
};

const renderReminderIntervalPrompt = async (ctx: Context): Promise<void> => {
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.intervalPrompt()] });
};

const renderReminderDailyTimePrompt = async (ctx: Context): Promise<void> => {
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.dailyTimePrompt()] });
};

const renderReminderWeeklyDayPrompt = async (
  ctx: Context,
  params: { mode: ReminderFlow['mode']; reminderId?: string }
): Promise<void> => {
  const kb = new InlineKeyboard();
  for (let idx = 0; idx < WEEKDAY_KEYS.length; idx += 1) {
    const label = getWeekdayLabel(idx);
    const btn = await makeActionButton(ctx, { label, action: 'reminders.weekly_day_set', data: { day: idx, mode: params.mode, reminderId: params.reminderId } });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backAction = params.mode === 'edit' ? 'reminders.edit_open' : 'reminders.schedule_back';
  const backData = params.mode === 'edit' ? { reminderId: params.reminderId } : undefined;
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: backData });
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.weeklyDayPrompt()], inlineKeyboard: kb });
};

const renderReminderMonthlyDayPrompt = async (ctx: Context): Promise<void> => {
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.monthlyDayPrompt()] });
};

const renderReminderYearlyMonthPrompt = async (ctx: Context): Promise<void> => {
  const kb = new InlineKeyboard();
  for (let month = 1; month <= 12; month += 1) {
    const btn = await makeActionButton(ctx, { label: month.toString(), action: 'reminders.yearly_month_set', data: { month } });
    kb.text(btn.text, btn.callback_data);
    if (month % 4 === 0) kb.row();
  }
  await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.yearlyMonthPrompt()], inlineKeyboard: kb });
};

const getReminderIdFromFlow = (flow: ReminderFlow): string | undefined => {
  return 'reminderId' in flow ? flow.reminderId : undefined;
};

const persistReminderSchedule = async (ctx: Context, flow: ReminderFlow): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const timezone = user.timezone ?? config.defaultTimezone;
  const scheduleType = flow.draft.scheduleType ?? 'once';
  let onceAt: Date | null = null;
  if (scheduleType === 'once') {
    if (!flow.draft.localDate || !flow.draft.localTime) {
      await renderReminders(ctx);
      return;
    }
    onceAt = new Date(localDateTimeToUtcIso(flow.draft.localDate, flow.draft.localTime, timezone));
  }

  const schedule = {
    scheduleType,
    timezone,
    onceAt,
    intervalMinutes: flow.draft.intervalMinutes ?? null,
    atTime: flow.draft.atTime ?? flow.draft.localTime ?? null,
    byWeekday: flow.draft.byWeekday ?? null,
    byMonthday: flow.draft.byMonthday ?? null,
    byMonth: flow.draft.byMonth ?? null
  };

  const nextRunAt = computeNextRunAt(schedule, new Date());
  const shouldEnable = Boolean(nextRunAt);
  const status = shouldEnable ? 'active' : 'inactive';

  if (flow.mode === 'create') {
    const reminderId = getReminderIdFromFlow(flow);
    const title = flow.draft.title && flow.draft.title.trim().length > 0 ? flow.draft.title : labels.reminders.untitled();
    if (reminderId) {
      await updateReminder(reminderId, {
        title,
        description: flow.draft.description ?? null,
        schedule,
        nextRunAt,
        isActive: shouldEnable,
        enabled: shouldEnable,
        status
      });
      clearReminderFlow(String(ctx.from?.id ?? ''));
      const local = nextRunAt ? formatInstantToLocal(nextRunAt.toISOString(), timezone) : null;
      await renderScreen(ctx, {
        title: labels.reminders.newTitle(),
        bodyLines: [labels.reminders.newCreated({ local_date: local?.date ?? '-', local_time: local?.time ?? '-' })]
      });
      await renderReminders(ctx);
      return;
    }
    const reminder = await createReminder({
      userId: user.id,
      title,
      description: flow.draft.description ?? null,
      descGroupKey: flow.draft.descGroupKey ?? null,
      schedule,
      nextRunAt,
      enabled: shouldEnable,
      isActive: shouldEnable,
      status
    });
    clearReminderFlow(String(ctx.from?.id ?? ''));
    const local = nextRunAt ? formatInstantToLocal(nextRunAt.toISOString(), timezone) : null;
    await renderScreen(ctx, {
      title: labels.reminders.newTitle(),
      bodyLines: [labels.reminders.newCreated({ local_date: local?.date ?? '-', local_time: local?.time ?? '-' })]
    });
    await renderReminders(ctx);
    return;
  }

  if (flow.mode === 'edit') {
    const current = await getReminderById(flow.reminderId);
    const enabled = current?.enabled ?? true;
    const isActive = Boolean(enabled) && Boolean(nextRunAt);
    await updateReminder(flow.reminderId, { schedule, nextRunAt, enabled, isActive, status: isActive ? 'active' : 'inactive' });
    clearReminderFlow(String(ctx.from?.id ?? ''));
    await renderReminderDetails(ctx, flow.reminderId, labels.reminders.editSaved());
  }
};

const renderReminderDateSelect = async (
  ctx: Context,
  params: { mode: ReminderFlow['mode']; reminderId?: string }
): Promise<void> => {
  const { user, settings } = await ensureUserAndSettings(ctx);
  const weekendDay = getWeekendDay(settings.settings_json as Record<string, unknown>);
  const weekendLabel = getWeekdayLabel(weekendDay);

  const todayBtn = await makeActionButton(ctx, { label: labels.remindersButtons.today(), action: 'reminders.date_select', data: { ...params, choice: 'today' } });
  const tomorrowBtn = await makeActionButton(ctx, { label: labels.remindersButtons.tomorrow(), action: 'reminders.date_select', data: { ...params, choice: 'tomorrow' } });
  const weekendBtn = await makeActionButton(ctx, { label: labels.remindersButtons.weekend(), action: 'reminders.date_select', data: { ...params, choice: 'weekend' } });
  const customBtn = await makeActionButton(ctx, { label: labels.remindersButtons.customDate(), action: 'reminders.date_select', data: { ...params, choice: 'custom' } });
  const weekendDayBtn = await makeActionButton(ctx, {
    label: labels.remindersButtons.weekendDay({ day: weekendLabel }),
    action: 'reminders.weekend_day',
    data: params
  });

  const kb = new InlineKeyboard()
    .text(todayBtn.text, todayBtn.callback_data)
    .text(tomorrowBtn.text, tomorrowBtn.callback_data)
    .row()
    .text(weekendBtn.text, weekendBtn.callback_data)
    .text(customBtn.text, customBtn.callback_data)
    .row()
    .text(weekendDayBtn.text, weekendDayBtn.callback_data);

  const backAction = params.mode === 'edit' ? 'reminders.edit_open' : 'reminders.schedule_back';
  const backData = params.mode === 'edit' ? { reminderId: params.reminderId } : undefined;
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: backData });
  kb.row().text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.reminders.newTitle(),
    bodyLines: [labels.reminders.newChooseDate()],
    inlineKeyboard: kb
  });
};

const renderReminderCustomDatePicker = async (
  ctx: Context,
  params: { mode: ReminderFlow['mode']; reminderId?: string; draft: ReminderDraft }
): Promise<void> => {
  const draft = clampCustomDateDraft(params.draft);
  const dateMode = draft.dateMode ?? 'gregorian';
  const year = draft.year ?? 0;
  const month = draft.month ?? 0;
  const day = draft.day ?? 0;
  const formatted = dateMode === 'jalali' ? `J${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}` : `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;

  const lines = [
    labels.reminders.customDateTitle({ mode: dateMode === 'jalali' ? t('screens.reminders.custom_date_mode_jalali') : t('screens.reminders.custom_date_mode_gregorian') }),
    labels.reminders.customDateCurrent({ date: formatted }),
    labels.reminders.customDateHint()
  ];

  const kb = new InlineKeyboard();
  const yearMinusBtn = await makeActionButton(ctx, {
    label: '−1Y',
    action: 'reminders.date_adjust',
    data: { ...params, field: 'year', delta: -1 }
  });
  const yearPlusBtn = await makeActionButton(ctx, {
    label: '+1Y',
    action: 'reminders.date_adjust',
    data: { ...params, field: 'year', delta: 1 }
  });
  const monthMinusBtn = await makeActionButton(ctx, {
    label: '−1M',
    action: 'reminders.date_adjust',
    data: { ...params, field: 'month', delta: -1 }
  });
  const monthPlusBtn = await makeActionButton(ctx, {
    label: '+1M',
    action: 'reminders.date_adjust',
    data: { ...params, field: 'month', delta: 1 }
  });
  const dayMinusBtn = await makeActionButton(ctx, {
    label: '−1D',
    action: 'reminders.date_adjust',
    data: { ...params, field: 'day', delta: -1 }
  });
  const dayPlusBtn = await makeActionButton(ctx, {
    label: '+1D',
    action: 'reminders.date_adjust',
    data: { ...params, field: 'day', delta: 1 }
  });
  kb.text(yearMinusBtn.text, yearMinusBtn.callback_data).text(yearPlusBtn.text, yearPlusBtn.callback_data).row();
  kb.text(monthMinusBtn.text, monthMinusBtn.callback_data).text(monthPlusBtn.text, monthPlusBtn.callback_data).row();
  kb.text(dayMinusBtn.text, dayMinusBtn.callback_data).text(dayPlusBtn.text, dayPlusBtn.callback_data).row();

  const toggleBtn = await makeActionButton(ctx, {
    label: dateMode === 'jalali' ? labels.remindersButtons.useGregorian() : labels.remindersButtons.useJalali(),
    action: 'reminders.date_toggle',
    data: params
  });
  const manualBtn = await makeActionButton(ctx, { label: labels.remindersButtons.typeDate(), action: 'reminders.date_manual', data: params });
  const confirmBtn = await makeActionButton(ctx, { label: labels.remindersButtons.dateConfirm(), action: 'reminders.date_confirm', data: params });
  kb.text(toggleBtn.text, toggleBtn.callback_data).row();
  kb.text(manualBtn.text, manualBtn.callback_data).row();
  kb.text(confirmBtn.text, confirmBtn.callback_data).row();

  const backAction = params.mode === 'edit' ? 'reminders.edit_open' : 'nav.reminders';
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: params.reminderId ? { reminderId: params.reminderId } : undefined });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.reminders.newTitle(),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderReminderTimePicker = async (
  ctx: Context,
  params: { mode: ReminderFlow['mode']; reminderId?: string; draft: ReminderDraft }
): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const timezone = user.timezone ?? config.defaultTimezone;
  const draft = ensureReminderTimeDraft(params.draft, timezone);
  const timeLabel = minutesToHhmm(draft.timeMinutes ?? getDefaultReminderTimeMinutes(timezone));

  const lines = [labels.reminders.timeTitle(), labels.reminders.timeCurrent({ time: timeLabel })];

  const kb = new InlineKeyboard();
  const minusHourBtn = await makeActionButton(ctx, { label: '−1h', action: 'reminders.time_adjust', data: { ...params, delta: -60 } });
  const plusHourBtn = await makeActionButton(ctx, { label: '+1h', action: 'reminders.time_adjust', data: { ...params, delta: 60 } });
  const minus15Btn = await makeActionButton(ctx, { label: '−15m', action: 'reminders.time_adjust', data: { ...params, delta: -15 } });
  const plus15Btn = await makeActionButton(ctx, { label: '+15m', action: 'reminders.time_adjust', data: { ...params, delta: 15 } });
  kb.text(minusHourBtn.text, minusHourBtn.callback_data).text(plusHourBtn.text, plusHourBtn.callback_data).row();
  kb.text(minus15Btn.text, minus15Btn.callback_data).text(plus15Btn.text, plus15Btn.callback_data).row();

  const presets = ['08:00', '12:00', '18:00', '21:00'];
  for (const preset of presets) {
    const btn = await makeActionButton(ctx, { label: preset, action: 'reminders.time_preset', data: { ...params, preset } });
    kb.text(btn.text, btn.callback_data);
  }
  kb.row();

  const manualBtn = await makeActionButton(ctx, { label: labels.remindersButtons.typeTime(), action: 'reminders.time_manual', data: params });
  const confirmBtn = await makeActionButton(ctx, { label: labels.remindersButtons.timeConfirm(), action: 'reminders.time_confirm', data: params });
  kb.text(manualBtn.text, manualBtn.callback_data).row();
  kb.text(confirmBtn.text, confirmBtn.callback_data).row();

  const backAction = params.mode === 'edit' ? 'reminders.edit_open' : 'nav.reminders';
  const backBtn = await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: backAction, data: params.reminderId ? { reminderId: params.reminderId } : undefined });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, {
    title: labels.reminders.newTitle(),
    bodyLines: lines,
    inlineKeyboard: kb
  });
};

const renderCalendarEvents = async (ctx: Context): Promise<void> => {
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  await renderScreen(ctx, { titleKey: t('screens.calendar.title'), bodyLines: [t('screens.calendar.coming_soon')], inlineKeyboard: new InlineKeyboard().text(back.text, back.callback_data) });
};

const renderAI = async (ctx: Context): Promise<void> => {
  const back = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.dashboard' });
  await renderScreen(ctx, { titleKey: t('screens.ai.title'), bodyLines: [t('screens.ai.coming_soon')], inlineKeyboard: new InlineKeyboard().text(back.text, back.callback_data) });
};

/**
 * Parsers & pickers
 */
const parseTimeHhmm = (input: string): { hhmm: string; minutes: number } | null => {
  const trimmed = input.trim();
  const parts = trimmed.split(':');
  if (parts.length !== 2) return null;

  const hours = Number(parts[0]);
  const minutes = Number(parts[1]);

  if (!Number.isInteger(hours) || !Number.isInteger(minutes)) return null;
  if (hours < 0 || hours > 23) return null;
  if (minutes < 0 || minutes > 59) return null;

  const hh = hours.toString().padStart(2, '0');
  const mm = minutes.toString().padStart(2, '0');
  const total = hours * 60 + minutes;

  return { hhmm: `${hh}:${mm}`, minutes: total };
};

const isValidLocalDate = (input: string): boolean => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(input)) return false;
  const [year, month, day] = input.split('-').map(Number);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return false;
  if (month < 1 || month > 12) return false;
  if (day < 1 || day > 31) return false;
  const probe = new Date(Date.UTC(year, month - 1, day));
  return probe.getUTCFullYear() === year && probe.getUTCMonth() === month - 1 && probe.getUTCDate() === day;
};

const WEEKDAY_KEYS = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'] as const;

const getWeekendDay = (settingsJson?: Record<string, unknown>): number => {
  const raw = (settingsJson as { weekend_day?: number | string } | undefined)?.weekend_day;
  if (typeof raw === 'number' && Number.isInteger(raw) && raw >= 0 && raw <= 6) return raw;
  if (typeof raw === 'string') {
    const idx = WEEKDAY_KEYS.indexOf(raw.toLowerCase() as (typeof WEEKDAY_KEYS)[number]);
    if (idx >= 0) return idx;
  }
  return 5;
};

const getWeekdayLabel = (dayIndex: number): string => {
  const key = WEEKDAY_KEYS[dayIndex] ?? 'fri';
  return t(`screens.reminders.weekday_${key}`);
};

const getLocalWeekdayIndex = (date: Date, timezone: string): number => {
  const label = new Intl.DateTimeFormat('en-US', { timeZone: timezone, weekday: 'short' }).format(date);
  const map: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  return map[label] ?? 0;
};

const addDaysToLocalDate = (localDate: string, days: number, timezone: string): string => {
  const utcIso = localDateTimeToUtcIso(localDate, '00:00', timezone);
  const next = new Date(new Date(utcIso).getTime() + days * 24 * 60 * 60 * 1000);
  return formatInstantToLocal(next.toISOString(), timezone).date;
};

const getNextWeekendLocalDate = (timezone: string, weekendDay: number): string => {
  const now = new Date();
  const localNow = formatInstantToLocal(now.toISOString(), timezone);
  const currentDow = getLocalWeekdayIndex(now, timezone);
  const delta = (weekendDay - currentDow + 7) % 7;
  return addDaysToLocalDate(localNow.date, delta, timezone);
};

const minutesToHhmm = (minutes: number): string => {
  const total = ((minutes % 1440) + 1440) % 1440;
  const hh = Math.floor(total / 60)
    .toString()
    .padStart(2, '0');
  const mm = Math.floor(total % 60)
    .toString()
    .padStart(2, '0');
  return `${hh}:${mm}`;
};

const normalizeTimeMinutes = (minutes: number): number => {
  const total = minutes % 1440;
  return total < 0 ? total + 1440 : total;
};

const getDefaultReminderTimeMinutes = (timezone: string): number => {
  const local = formatLocalTime(timezone);
  const parsed = parseTimeHhmm(local.time);
  if (!parsed) return 9 * 60;
  const rounded = Math.ceil(parsed.minutes / 15) * 15;
  return normalizeTimeMinutes(rounded);
};

const ensureReminderTimeDraft = (draft: ReminderDraft, timezone: string): ReminderDraft => {
  if (typeof draft.timeMinutes === 'number') return draft;
  if (draft.localTime) {
    const parsed = parseTimeHhmm(draft.localTime);
    if (parsed) return { ...draft, timeMinutes: parsed.minutes };
  }
  const minutes = getDefaultReminderTimeMinutes(timezone);
  return { ...draft, timeMinutes: minutes, localTime: minutesToHhmm(minutes) };
};

const buildCustomDateDraft = (localDate: string, mode: 'gregorian' | 'jalali'): ReminderDraft => {
  const [year, month, day] = localDate.split('-').map(Number);
  if (mode === 'jalali') {
    const jalali = gregorianToJalali(year, month, day);
    return { dateMode: 'jalali', year: jalali.year, month: jalali.month, day: jalali.day };
  }
  return { dateMode: 'gregorian', year, month, day };
};

const clampCustomDateDraft = (draft: ReminderDraft): ReminderDraft => {
  if (!draft.year || !draft.month || !draft.day || !draft.dateMode) return draft;
  if (draft.dateMode === 'gregorian') {
    const daysInMonth = new Date(Date.UTC(draft.year, draft.month, 0)).getUTCDate();
    const nextDay = Math.min(Math.max(1, draft.day), daysInMonth);
    return { ...draft, day: nextDay };
  }
  const maxDay = draft.month <= 6 ? 31 : draft.month <= 11 ? 30 : isValidJalaliDate(draft.year, 12, 30) ? 30 : 29;
  const nextDay = Math.min(Math.max(1, draft.day), maxDay);
  return { ...draft, day: nextDay };
};

const timeDraftToDisplay = (draft: {
  hour12: number;
  minuteTens: number;
  minuteOnes: number;
  ampm: 'AM' | 'PM';
}): { hhmm24: string; label: string } => {
  const hour12 = Math.min(12, Math.max(1, draft.hour12));
  const mt = Math.min(5, Math.max(0, draft.minuteTens));
  const mo = Math.min(9, Math.max(0, draft.minuteOnes));
  const minutes = mt * 10 + mo;

  const hour24 = draft.ampm === 'AM' ? hour12 % 12 : (hour12 % 12) + 12;

  const HH = hour24.toString().padStart(2, '0');
  const MM = minutes.toString().padStart(2, '0');

  const hhmm24 = `${HH}:${MM}`;
  const hh12 = hour12.toString().padStart(2, '0');
  const label = `${hh12}:${MM} ${draft.ampm}`;
  return { hhmm24, label };
};

const renderTimePicker = async (
  ctx: Context,
  reportDayId: string,
  item: ReportItemRow,
  draft: { hour12: number; minuteTens: number; minuteOnes: number; ampm: 'AM' | 'PM' },
  opts?: { title?: string; currentLabel?: string; hint?: string }
): Promise<void> => {
  const { label: timeLabel } = timeDraftToDisplay(draft);

  const lines = [
    opts?.title ?? t('screens.daily_report.time_title', { label: item.label }),
    opts?.currentLabel ?? t('screens.daily_report.time_current', { value: timeLabel }),
    opts?.hint ?? t('screens.daily_report.time_hint')
  ];

  const kb = new InlineKeyboard();

  const hourRows = [
    [1, 2, 3, 4],
    [5, 6, 7, 8],
    [9, 10, 11, 12]
  ];

  for (const row of hourRows) {
    for (const h of row) {
      const btn = await makeActionButton(ctx, { label: h.toString(), action: 'dr.time_set_hour', data: { reportDayId, itemId: item.id, hour12: h } });
      kb.text(btn.text, btn.callback_data);
    }
    kb.row();
  }

  for (let mt = 0; mt <= 5; mt++) {
    const btn = await makeActionButton(ctx, { label: `${mt}0`, action: 'dr.time_set_mtens', data: { reportDayId, itemId: item.id, minuteTens: mt } });
    kb.text(btn.text, btn.callback_data);
  }
  kb.row();

  for (let mo = 0; mo <= 9; mo++) {
    const btn = await makeActionButton(ctx, { label: mo.toString(), action: 'dr.time_set_mones', data: { reportDayId, itemId: item.id, minuteOnes: mo } });
    kb.text(btn.text, btn.callback_data);
  }
  kb.row();

  const amBtn = await makeActionButton(ctx, { label: t('buttons.am'), action: 'dr.time_set_ampm', data: { reportDayId, itemId: item.id, ampm: 'AM' } });
  const pmBtn = await makeActionButton(ctx, { label: t('buttons.pm'), action: 'dr.time_set_ampm', data: { reportDayId, itemId: item.id, ampm: 'PM' } });
  kb.text(amBtn.text, amBtn.callback_data).text(pmBtn.text, pmBtn.callback_data);
  kb.row();

  const saveBtn = await makeActionButton(ctx, { label: t('screens.daily_report.time_save'), action: 'dr.time_save', data: { reportDayId, itemId: item.id } });
  const skipBtn = await makeActionButton(ctx, { label: t('buttons.skip'), action: 'dr.skip', data: { reportDayId, itemId: item.id } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu', data: { reportDayId } });

  kb.text(saveBtn.text, saveBtn.callback_data).row();
  kb.text(skipBtn.text, skipBtn.callback_data).text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderNumericInput = async (ctx: Context, reportDayId: string, item: ReportItemRow, draft: NumericDraftState): Promise<void> => {
  const unit = draft.unit ?? 'minutes';
  const formattedValue =
    item.item_type === 'duration_minutes'
      ? formatDurationValue(convertToMinutes(draft.value, unit).minutes, unit === 'seconds' ? draft.value : undefined)
      : draft.value;
  const lines = [
    t('screens.daily_report.numeric_title', { label: item.label }),
    t('screens.daily_report.numeric_current', { value: formattedValue }),
    t('screens.daily_report.numeric_hint')
  ];
  if (item.item_type === 'duration_minutes') {
    const unitLabel = unit === 'seconds' ? t('screens.daily_report.unit_seconds') : t('screens.daily_report.unit_minutes');
    lines.splice(2, 0, t('screens.daily_report.numeric_units', { unit: unitLabel }));
  }

  const kb = new InlineKeyboard();

  if (item.item_type === 'duration_minutes') {
    const minutesBtn = await makeActionButton(ctx, {
      label: `${unit === 'minutes' ? '✅ ' : ''}${t('screens.daily_report.unit_minutes')}`,
      action: 'dr.num_unit',
      data: { reportDayId, itemId: item.id, unit: 'minutes' }
    });
    const secondsBtn = await makeActionButton(ctx, {
      label: `${unit === 'seconds' ? '✅ ' : ''}${t('screens.daily_report.unit_seconds')}`,
      action: 'dr.num_unit',
      data: { reportDayId, itemId: item.id, unit: 'seconds' }
    });
    kb.text(minutesBtn.text, minutesBtn.callback_data).text(secondsBtn.text, secondsBtn.callback_data).row();
  }

  const deltasRowPositive = [50, 10, 5, 2, 1];
  for (const delta of deltasRowPositive) {
    const btn = await makeActionButton(ctx, {
      label: `+${delta}`,
      action: 'dr.num_delta',
      data: { reportDayId, itemId: item.id, delta }
    });
    kb.text(btn.text, btn.callback_data);
  }
  kb.row();

  const deltasRowNegative = [-1, -2, -5, -10, -50];
  for (const delta of deltasRowNegative) {
    const btn = await makeActionButton(ctx, { label: `${delta}`, action: 'dr.num_delta', data: { reportDayId, itemId: item.id, delta } });
    kb.text(btn.text, btn.callback_data);
  }
  kb.row();

  const saveBtn = await makeActionButton(ctx, { label: t('screens.daily_report.numeric_save'), action: 'dr.num_save', data: { reportDayId, itemId: item.id } });
  const skipBtn = await makeActionButton(ctx, { label: t('buttons.skip'), action: 'dr.skip', data: { reportDayId, itemId: item.id } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu', data: { reportDayId } });

  kb.text(saveBtn.text, saveBtn.callback_data).row();
  kb.text(skipBtn.text, skipBtn.callback_data).text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderBooleanInput = async (ctx: Context, reportDayId: string, item: ReportItemRow): Promise<void> => {
  const labelLine = item.label ? String(item.label) : null;
  const lines = [labelLine, t('screens.daily_report.boolean_question')].filter(Boolean) as string[];
  const kb = new InlineKeyboard();

  const yesBtn = await makeActionButton(ctx, { label: t('screens.daily_report.boolean_yes'), action: 'dr.boolean', data: { reportDayId, itemId: item.id, value: true } });
  const noBtn = await makeActionButton(ctx, { label: t('screens.daily_report.boolean_no'), action: 'dr.boolean', data: { reportDayId, itemId: item.id, value: false } });
  const skipBtn = await makeActionButton(ctx, { label: t('buttons.skip'), action: 'dr.skip', data: { reportDayId, itemId: item.id } });
  const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'dr.menu', data: { reportDayId } });

  kb.text(yesBtn.text, yesBtn.callback_data).text(noBtn.text, noBtn.callback_data).row();
  kb.text(skipBtn.text, skipBtn.callback_data).text(cancelBtn.text, cancelBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderRoutineDailyTasks = async (
  ctx: Context,
  params: { reportDay: ReportDayRow; routineItem: ReportItemRow; items: ReportItemRow[]; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' }
): Promise<void> => {
  const opts = (params.routineItem.options_json ?? {}) as { routine_id?: string };
  const routineId = opts.routine_id;
  const taskItems = params.items.filter((it) => isRoutineTaskItem(it) && ((it.options_json ?? {}) as { routine_id?: string }).routine_id === routineId);
  const statuses = taskItems.length ? await listCompletionStatus(params.reportDay.id, taskItems) : [];
  const lines: string[] = [
    t('screens.daily_report.routine_tasks_title', { title: params.routineItem.label ?? '' }),
    t('screens.daily_report.routine_tasks_hint'),
    ''
  ];
  if (statuses.length === 0) {
    lines.push(t('screens.routine_tasks.list_empty'));
  } else {
    statuses.forEach((s, idx) => {
      const icon = s.filled ? '✅' : s.skipped ? '⏭' : '⬜️';
      const valueText = s.filled ? formatDisplayValue(s.item, s.value?.value_json ?? null) : '-';
      lines.push(`${icon} ${idx + 1}) ${formatItemLabel(s.item)} — ${valueText}`);
    });
  }
  const kb = new InlineKeyboard();
  for (const status of statuses) {
    const action = params.reportDay.locked ? 'noop' : 'dr.item';
    const btn = await makeActionButton(ctx, {
      label: formatItemLabel(status.item),
      action,
      data: {
        reportDayId: params.reportDay.id,
        itemId: status.item.id,
        origin: params.origin,
        statusFilter: params.statusFilter
      }
    });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backBtn = await makeActionButton(ctx, {
    label: t('buttons.back'),
    action: 'dr.routine_detail',
    data: { reportDayId: params.reportDay.id, routineId, itemId: params.routineItem.id, origin: params.origin, statusFilter: params.statusFilter }
  });
  kb.text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderRoutineDailyEntry = async (
  ctx: Context,
  reportDay: ReportDayRow,
  routineItem: ReportItemRow,
  items: ReportItemRow[],
  origin?: 'next' | 'status',
  statusFilter?: 'all' | 'not_filled' | 'filled'
): Promise<void> => {
  const opts = (routineItem.options_json ?? {}) as { routine_id?: string };
  const routineId = opts.routine_id;
  const taskItems = items.filter((it) => isRoutineTaskItem(it) && ((it.options_json ?? {}) as { routine_id?: string }).routine_id === routineId);
  const statuses = taskItems.length ? await listCompletionStatus(reportDay.id, taskItems) : [];
  const doneCount = statuses.filter((s) => s.filled && !s.skipped).length;
  const [routineStatus] = await listCompletionStatus(reportDay.id, [routineItem]);
  const state = routineValueState(routineStatus?.value?.value_json ?? null);
  const lines: string[] = [t('screens.routines.detail_title', { title: routineItem.label ?? '' })];
  if (state === 'pending') lines.push(t('screens.daily_report.routine_prompt_question'));
  lines.push(t('screens.daily_report.routine_task_progress', { completed: doneCount, total: statuses.length }));
  if (state === 'done') lines.push(t('screens.daily_report.routine_status_done'));
  if (state === 'partial') lines.push(t('screens.daily_report.routine_status_partial'));
  if (state === 'skipped') lines.push(t('screens.daily_report.routine_status_skipped'));
  if (state === 'pending') lines.push(t('screens.daily_report.routine_status_pending'));

  const kb = new InlineKeyboard();
  if (!reportDay.locked && routineId) {
    if (state === 'done') {
      const undoBtn = await makeActionButton(ctx, {
        label: t('buttons.routine_undo'),
        action: 'dr.routine_undo',
        data: { reportDayId: reportDay.id, routineId, itemId: routineItem.id, origin, statusFilter }
      });
      const detailsBtn = await makeActionButton(ctx, {
        label: t('buttons.routine_open_tasks'),
        action: 'dr.routine_open_tasks',
        data: { reportDayId: reportDay.id, routineId, itemId: routineItem.id, origin, statusFilter }
      });
      kb.text(undoBtn.text, undoBtn.callback_data).row().text(detailsBtn.text, detailsBtn.callback_data).row();
    } else {
      const doneBtn = await makeActionButton(ctx, {
        label: t('buttons.routine_mark_done'),
        action: 'dr.routine_done',
        data: { reportDayId: reportDay.id, routineId, itemId: routineItem.id, origin, statusFilter }
      });
      const partialBtn = await makeActionButton(ctx, {
        label: t('buttons.routine_mark_partial'),
        action: 'dr.routine_partial',
        data: { reportDayId: reportDay.id, routineId, itemId: routineItem.id, origin, statusFilter }
      });
      const skipBtn = await makeActionButton(ctx, {
        label: t('buttons.routine_skip'),
        action: 'dr.routine_skip',
        data: { reportDayId: reportDay.id, routineId, itemId: routineItem.id, origin, statusFilter }
      });
      kb.text(doneBtn.text, doneBtn.callback_data).row().text(partialBtn.text, partialBtn.callback_data).row().text(skipBtn.text, skipBtn.callback_data).row();
    }
  }
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu', data: { reportDayId: reportDay.id } });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: lines, inlineKeyboard: kb });
};

const promptForItem = async (
  ctx: Context,
  reportDay: ReportDayRow,
  item: ReportItemRow,
  opts?: { origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' }
) => {
  const telegramId = String(ctx.from?.id ?? '');
  const existing = userStates.get(telegramId) ?? {};
  const awaitingValue: AwaitingValueState = {
    reportDayId: reportDay.id,
    itemId: item.id,
    origin: opts?.origin,
    statusFilter: opts?.statusFilter
  };

  if (reportDay.locked) {
    await renderScreen(ctx, {
      titleKey: t('screens.daily_report.title'),
      bodyLines: isLockedMessageLines(reportDay),
      inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay)
    });
    return;
  }

  if (item.item_type === 'time_hhmm') {
    const initialDraft: TimeDraftState = {
      reportDayId: reportDay.id,
      itemId: item.id,
      hour12: 10,
      minuteTens: 0,
      minuteOnes: 0,
      ampm: 'PM',
      mode: 'single',
      phase: 'end'
    };
    userStates.set(telegramId, { ...existing, awaitingValue, timeDraft: initialDraft });
    await renderTimePicker(ctx, reportDay.id, item, initialDraft);
    return;
  }

  if (isRoutineParentItem(item)) {
    const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDay.id);
    const context = cached ?? (await ensureSpecificReportContext(ctx, reportDay.local_date));
    userStates.set(telegramId, { ...existing, awaitingValue });
    await renderRoutineDailyEntry(ctx, context.reportDay, item, context.items, opts?.origin, opts?.statusFilter);
    return;
  }

  const optionsJson = (item.options_json ?? {}) as { useStartEnd?: boolean };
  if (item.item_type === 'duration_minutes' && optionsJson.useStartEnd) {
    const initialDraft: TimeDraftState = {
      reportDayId: reportDay.id,
      itemId: item.id,
      hour12: 10,
      minuteTens: 0,
      minuteOnes: 0,
      ampm: 'AM',
      mode: 'start_end',
      phase: 'start'
    };
    userStates.set(telegramId, { ...existing, awaitingValue, timeDraft: initialDraft });
    await renderTimePicker(ctx, reportDay.id, item, initialDraft, {
      title: t('screens.daily_report.duration_start_title'),
      currentLabel: t('screens.daily_report.duration_start_current', { value: timeDraftToDisplay(initialDraft).label }),
      hint: t('screens.daily_report.duration_start_hint', { label: item.label })
    });
    return;
  }

  if (item.item_type === 'number' || item.item_type === 'duration_minutes') {
    const draftValue = 0;
    const numericDraft: NumericDraftState = {
      reportDayId: reportDay.id,
      itemId: item.id,
      value: draftValue,
      unit: item.item_type === 'duration_minutes' ? 'minutes' : undefined
    };
    userStates.set(telegramId, { ...existing, awaitingValue, numericDraft });
    await renderNumericInput(ctx, reportDay.id, item, numericDraft);
    return;
  }

  if (item.item_type === 'boolean') {
    userStates.set(telegramId, { ...existing, awaitingValue });
    await renderBooleanInput(ctx, reportDay.id, item);
    return;
  }

  userStates.set(telegramId, { ...existing, awaitingValue });

  const skipBtn = await makeActionButton(ctx, { label: t('buttons.skip'), action: 'dr.skip', data: { reportDayId: reportDay.id, itemId: item.id } });
  const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'dr.menu', data: { reportDayId: reportDay.id } });

  const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data).row().text(cancelBtn.text, cancelBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: [t('screens.daily_report.set_value_for', { label: item.label }), t('screens.daily_report.send_value_as_text')], inlineKeyboard: kb });
};

const continueFlowAfterAction = async (
  ctx: Context,
  reportDay: ReportDayRow,
  origin?: 'next' | 'status',
  statusFilter?: 'all' | 'not_filled' | 'filled'
): Promise<void> => {
  if (origin === 'next') {
    const context = await ensureContextByReportDayId(ctx, reportDay.id);
    const statuses = await listCompletionStatus(reportDay.id, filterRoutineDisplayItems(context.items));
    const next = statuses.find((s) => !s.filled && !s.skipped);
    if (next) {
      await promptForItem(ctx, context.reportDay, next.item, { origin: 'next' });
      return;
    }
    await renderDailyReportRoot(ctx, reportDay.local_date);
    return;
  }
  if (origin === 'status') {
    const telegramId = String(ctx.from?.id ?? '');
    const filter = statusFilter ?? userStates.get(telegramId)?.statusFilter?.filter ?? 'all';
    await renderDailyStatusWithFilter(ctx, reportDay.id, filter);
    return;
  }
  await renderDailyReportRoot(ctx, reportDay.local_date);
};

const renderDailyReportRoot = async (ctx: Context, localDate?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const local = formatLocalTime(user.timezone ?? config.defaultTimezone);

  const targetDate = localDate ?? local.date;
  const { reportDay, items } = await ensureSpecificReportContext(ctx, targetDate);
  const displayItems = filterRoutineDisplayItems(items);
  const statuses = await listCompletionStatus(reportDay.id, displayItems);
  const completed = statuses.filter((s) => s.filled).length;
  const total = statuses.length;

  const template = (await getTemplateById(reportDay.template_id)) ?? (await ensureDefaultTemplate(reportDay.user_id));
  const templateName = template?.title ?? t('screens.templates.default_title');

  const bodyLines: string[] = [
    t('screens.daily_report.root_header', { date: reportDay.local_date }),
    t('screens.daily_report.template_line', { template: templateName }),
    t('screens.daily_report.completion_line', { completed, total }),
    ''
  ];

  if (reportDay.locked) {
    bodyLines.push(t('screens.daily_report.today_locked_note'), '');
  }

  // After 00:00 Tehran time, show "Fill Yesterday" button only if yesterday has unfilled/unskipped items.
  // We treat server time via user timezone formatting; parse HH from "HH:MM".
  const hourNow = Number((local.time || '00:00').split(':')[0] ?? 0);
  const showYesterdayCheck = hourNow >= 0; // always true, but kept for clarity

  const kb = await buildDailyReportKeyboard(ctx, reportDay, { items: displayItems, statuses });

  if (showYesterdayCheck) {
    // Compute yesterday date (YYYY-MM-DD). We do simple date arithmetic in UTC and accept that formatLocalTime is the main source.
    const [y, m, d] = targetDate.split('-').map((x) => Number(x));
    if (Number.isInteger(y) && Number.isInteger(m) && Number.isInteger(d)) {
      const dt = new Date(Date.UTC(y, m - 1, d));
      dt.setUTCDate(dt.getUTCDate() - 1);
      const y2 = dt.getUTCFullYear();
      const m2 = String(dt.getUTCMonth() + 1).padStart(2, '0');
      const d2 = String(dt.getUTCDate()).padStart(2, '0');
      const yesterday = `${y2}-${m2}-${d2}`;

      try {
        const yd = await getReportDayByDate({ userId: user.id, templateId: template.id, localDate: yesterday });
        if (yd) {
          const ydItems = filterRoutineDisplayItems(await ensureDefaultItems(user.id));
          const ydStatuses = await listCompletionStatus(yd.id, ydItems);
          const hasPending = ydStatuses.some((s) => !s.filled && !s.skipped);
          if (hasPending) {
            const yBtn = await makeActionButton(ctx, { label: t('buttons.dr_fill_yesterday'), action: 'dr.open_date', data: { localDate: yesterday } });
            kb.row().text(yBtn.text, yBtn.callback_data);
          }
        }
      } catch (e) {
        console.warn({ scope: 'daily_report', event: 'yesterday_check_failed', e });
      }
    }
  }

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines, inlineKeyboard: kb });
};

const renderDailyStatusWithFilter = async (ctx: Context, reportDayId: string, filter: 'all' | 'not_filled' | 'filled' = 'all'): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);

  const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);
  const reportDay = cached?.reportDay ?? (await getReportDayById(reportDayId)) ?? (await getOrCreateReportDay({ userId: user.id, templateId: (await ensureDefaultTemplate(user.id)).id, localDate: formatLocalTime(user.timezone ?? config.defaultTimezone).date }));

  const telegramId = String(ctx.from?.id ?? '');
  if (telegramId) {
    const st = { ...(userStates.get(telegramId) || {}) };
    st.statusFilter = { reportDayId: reportDay.id, filter };
    userStates.set(telegramId, st);
  }

  const context = cached ?? (await ensureSpecificReportContext(ctx, reportDay.local_date));
  const items = filterRoutineDisplayItems(context.items);
  const statuses = await listCompletionStatus(reportDay.id, items);
  const decorated = statuses.map((s) => ({ ...s, routineState: isRoutineParentItem(s.item) ? routineValueState(s.value?.value_json ?? null) : null }));

  let filtered = decorated;
  if (filter === 'not_filled')
    filtered = decorated.filter((s) =>
      isRoutineParentItem(s.item) ? s.routineState !== 'done' : !s.filled && !s.skipped
    );
  if (filter === 'filled')
    filtered = decorated.filter((s) =>
      isRoutineParentItem(s.item) ? s.routineState === 'done' : s.filled
    );

  const lines: string[] = [t('screens.daily_report.root_header', { date: reportDay.local_date }), t('screens.daily_report.status_header')];

  if (filtered.length === 0) {
    lines.push(filter === 'filled' ? t('screens.daily_report.none_filled') : t('screens.daily_report.none_pending'));
  } else {
    filtered.forEach((s, idx) => {
      const icon = isRoutineParentItem(s.item)
        ? s.routineState === 'done'
          ? '✅'
          : s.routineState === 'partial'
            ? '🌓'
            : s.routineState === 'skipped'
              ? '⏭'
              : '⬜️'
        : s.filled
          ? '✅'
          : s.skipped
            ? '⏭'
            : '⬜️';
      const statusLabel = isRoutineParentItem(s.item)
        ? s.routineState === 'done'
          ? t('screens.daily_report.routine_status_done')
          : s.routineState === 'partial'
            ? t('screens.daily_report.routine_status_partial')
            : s.routineState === 'skipped'
              ? t('screens.daily_report.routine_status_skipped')
              : t('screens.daily_report.routine_status_pending')
        : formatItemLabel(s.item);
      lines.push(`${icon} ${idx + 1}) ${isRoutineParentItem(s.item) ? `${formatItemLabel(s.item)} — ${statusLabel}` : statusLabel}`);
    });
  }

  const kb = new InlineKeyboard();

  const allBtn = await makeActionButton(ctx, { label: t('screens.daily_report.filter_all'), action: 'dr.status', data: { reportDayId: reportDay.id, filter: 'all' } });
  const notFilledBtn = await makeActionButton(ctx, { label: t('screens.daily_report.filter_not_filled'), action: 'dr.status', data: { reportDayId: reportDay.id, filter: 'not_filled' } });
  const filledBtn = await makeActionButton(ctx, { label: t('screens.daily_report.filter_filled'), action: 'dr.status', data: { reportDayId: reportDay.id, filter: 'filled' } });

  kb.text(allBtn.text, allBtn.callback_data).text(notFilledBtn.text, notFilledBtn.callback_data).text(filledBtn.text, filledBtn.callback_data).row();

  // Only allow edit actions if NOT locked.
  for (const status of filtered) {
    const icon = isRoutineParentItem(status.item)
      ? status.routineState === 'done'
        ? '✅'
        : status.routineState === 'partial'
          ? '🌓'
          : status.routineState === 'skipped'
            ? '⏭'
            : '⬜️'
      : status.filled
        ? '✅'
        : status.skipped
          ? '⏭'
          : '⬜️';
    const label = `${icon} ${formatItemLabel(status.item)}`;
    const action = reportDay.locked ? 'noop' : 'dr.item';
    const btn = await makeActionButton(ctx, { label, action, data: { reportDayId: reportDay.id, itemId: status.item.id, filter } });
    kb.text(btn.text, btn.callback_data).row();
  }

  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu' });
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: lines, inlineKeyboard: kb });
};

const slugifyItemKey = (input: string): string =>
  input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\s]/g, '')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

const generateUniqueItemKey = async (templateId: string, label: string): Promise<string> => {
  const base = slugifyItemKey(label) || 'item';
  const existing = await listAllItems(templateId);
  const existingKeys = new Set(existing.map((i) => i.item_key));
  let candidate = base;
  let counter = 2;
  while (existingKeys.has(candidate)) {
    candidate = `${base}_${counter}`;
    counter += 1;
  }
  return candidate;
};

const buildTypeKeyboard = async (ctx: Context, params: { templateId: string; itemId?: string; backAction?: string; backData?: Record<string, unknown> }) => {
  const kb = new InlineKeyboard();
  const types: { key: string; label: string }[] = [
    { key: 'boolean', label: t('screens.form_builder.type_boolean_label') },
    { key: 'number', label: t('screens.form_builder.type_number_label') },
    { key: 'time_hhmm', label: t('screens.form_builder.type_time_label') },
    { key: 'duration_minutes', label: t('screens.form_builder.type_duration_label') },
    { key: 'text', label: t('screens.form_builder.type_text_label') }
  ];
  for (const type of types) {
    const btn = await makeActionButton(ctx, { label: type.label, action: 'dr.template_item_select_type', data: { templateId: params.templateId, itemId: params.itemId, itemType: type.key } });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backAction = params.backAction ?? 'dr.template_edit';
  const backBtn = await makeBuilderBackButton(ctx, {
    templateId: params.templateId,
    fallbackAction: backAction,
    fallbackData: params.backData ?? { templateId: params.templateId }
  });
  kb.text(backBtn.text, backBtn.callback_data);
  return kb;
};

const buildCategoryKeyboard = async (ctx: Context, params: { templateId: string; itemId?: string; backAction?: string; backData?: Record<string, unknown> }) => {
  const kb = new InlineKeyboard();
  for (const category of STANDARD_CATEGORIES) {
    const btn = await makeActionButton(ctx, {
      label: `${category.emoji} ${t(category.labelKey)}`,
      action: 'dr.template_item_select_category',
      data: { templateId: params.templateId, itemId: params.itemId, category: category.name }
    });
    kb.text(btn.text, btn.callback_data).row();
  }

  const customBtn = await makeActionButton(ctx, {
    label: t('buttons.category_custom'),
    action: 'dr.template_item_custom_category',
    data: { templateId: params.templateId, itemId: params.itemId }
  });
  kb.text(customBtn.text, customBtn.callback_data).row();

  const backAction = params.backAction ?? 'dr.template_edit';
  const backBtn = await makeBuilderBackButton(ctx, {
    templateId: params.templateId,
    fallbackAction: backAction,
    fallbackData: params.backData ?? { templateId: params.templateId }
  });
  kb.text(backBtn.text, backBtn.callback_data);
  return kb;
};

const buildXpModeKeyboard = async (
  ctx: Context,
  params: { templateId: string; itemId?: string; backAction?: string; backData?: Record<string, unknown>; itemType?: string }
) => {
  const allowed = allowedXpModesForItemType(params.itemType);
  const modes = [
    { key: 'fixed', label: t('screens.templates.xp_mode_fixed') },
    { key: 'per_minute', label: t('screens.templates.xp_mode_time') },
    { key: 'per_number', label: t('screens.templates.xp_mode_number') },
    { key: 'none', label: t('screens.daily_report.ask_xp_mode_none') ?? 'No XP' }
  ].filter((m) => allowed.includes(m.key as 'none' | 'fixed' | 'per_minute' | 'per_number'));
  const kb = new InlineKeyboard();
  for (const mode of modes) {
    const btn = await makeActionButton(ctx, {
      label: mode.label,
      action: 'dr.template_item_select_xp_mode',
      data: { templateId: params.templateId, itemId: params.itemId, xpMode: mode.key }
    });
    kb.text(btn.text, btn.callback_data).row();
  }
  const backAction = params.backAction ?? 'dr.template_edit';
  const backBtn = await makeBuilderBackButton(ctx, {
    templateId: params.templateId,
    fallbackAction: backAction,
    fallbackData: params.backData ?? { templateId: params.templateId }
  });
  kb.text(backBtn.text, backBtn.callback_data);
  return kb;
};

const promptLabelInput = async (ctx: Context, params: { templateId: string; backToItemId?: string }) => {
  const backAction = params.backToItemId ? 'dr.template_item_menu' : 'dr.template_edit';
  const backData = params.backToItemId ? { templateId: params.templateId, itemId: params.backToItemId } : { templateId: params.templateId };
  setBuilderStepForFlow(ctx, params.templateId, 'builder.enterLabel');
  const backBtn = await makeBuilderBackButton(ctx, { templateId: params.templateId, fallbackAction: backAction, fallbackData: backData });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.daily_report.ask_label'), t('screens.templates.label_hint')],
    inlineKeyboard: kb
  });
};

const promptKeyInput = async (ctx: Context, params: { templateId: string; itemId: string }) => {
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.template_item_menu', data: { templateId: params.templateId, itemId: params.itemId } });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.daily_report.ask_key')],
    inlineKeyboard: kb
  });
};

const promptTypeSelection = async (ctx: Context, params: { templateId: string; itemId?: string; backToItem?: boolean }) => {
  setBuilderStepForFlow(ctx, params.templateId, 'builder.chooseType');
  const kb = await buildTypeKeyboard(ctx, {
    templateId: params.templateId,
    itemId: params.itemId,
    backAction: params.backToItem ? 'dr.template_item_menu' : 'dr.template_edit',
    backData: params.backToItem ? { templateId: params.templateId, itemId: params.itemId } : { templateId: params.templateId }
  });
  const helpBtn = await makeActionButton(ctx, {
    label: t('buttons.help'),
    action: 'dr.template_help',
    data: { templateId: params.templateId, topic: 'type', itemId: params.itemId, backToItem: params.backToItem === true }
  });
  kb.row().text(helpBtn.text, helpBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.daily_report.ask_type'), t('screens.templates.type_hint')],
    inlineKeyboard: kb
  });
};

const promptCategorySelection = async (ctx: Context, params: { templateId: string; itemId?: string; backToItem?: boolean }) => {
  setBuilderStepForFlow(ctx, params.templateId, 'builder.chooseCategory');
  const kb = await buildCategoryKeyboard(ctx, {
    templateId: params.templateId,
    itemId: params.itemId,
    backAction: params.backToItem ? 'dr.template_item_menu' : 'dr.template_edit',
    backData: params.backToItem ? { templateId: params.templateId, itemId: params.itemId } : { templateId: params.templateId }
  });
  const helpBtn = await makeActionButton(ctx, {
    label: t('buttons.help'),
    action: 'dr.template_help',
    data: { templateId: params.templateId, topic: 'category', itemId: params.itemId, backToItem: params.backToItem === true }
  });
  kb.row().text(helpBtn.text, helpBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.daily_report.ask_category'), t('screens.templates.category_hint')],
    inlineKeyboard: kb
  });
};

const promptXpModeSelection = async (ctx: Context, params: { templateId: string; itemId?: string; backToItem?: boolean; itemType?: string }) => {
  setBuilderStepForFlow(ctx, params.templateId, 'builder.configureXP');
  const kb = await buildXpModeKeyboard(ctx, {
    templateId: params.templateId,
    itemId: params.itemId,
    backAction: params.backToItem ? 'dr.template_item_menu' : 'dr.template_edit',
    backData: params.backToItem ? { templateId: params.templateId, itemId: params.itemId } : { templateId: params.templateId },
    itemType: params.itemType
  });
  const helpBtn = await makeActionButton(ctx, {
    label: t('buttons.help'),
    action: 'dr.template_help',
    data: { templateId: params.templateId, topic: 'xp_mode', itemId: params.itemId, backToItem: params.backToItem === true }
  });
  kb.row().text(helpBtn.text, helpBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.templates.xp_mode_title'), t('screens.templates.xp_mode_hint')],
    inlineKeyboard: kb
  });
};

const promptXpValueInput = async (ctx: Context, params: { templateId: string; itemId?: string; backToItem?: boolean }) => {
  setBuilderStepForFlow(ctx, params.templateId, 'builder.configureXPValue');
  const backAction = params.backToItem ? 'dr.template_item_menu' : 'dr.template_edit';
  const backData = params.backToItem ? { templateId: params.templateId, itemId: params.itemId } : { templateId: params.templateId };
  const backBtn = await makeBuilderBackButton(ctx, { templateId: params.templateId, fallbackAction: backAction, fallbackData: backData });
  const helpBtn = await makeActionButton(ctx, {
    label: t('buttons.help'),
    action: 'dr.template_help',
    data: { templateId: params.templateId, topic: 'xp_value', itemId: params.itemId, backToItem: params.backToItem === true }
  });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data).row().text(helpBtn.text, helpBtn.callback_data);
  const telegramId = String(ctx.from?.id ?? '');
  const stateXpMode = userStates.get(telegramId)?.templateItemFlow?.draft.xpMode ?? 'fixed';
  const hintKey =
    stateXpMode === 'per_minute'
      ? 'screens.templates.xp_value_hint_time'
      : stateXpMode === 'per_number'
        ? 'screens.templates.xp_value_hint_number'
        : 'screens.templates.xp_value_hint_fixed';
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.daily_report.ask_xp_value'), t(hintKey)],
    inlineKeyboard: kb
  });
};

const promptXpMaxInput = async (ctx: Context, params: { templateId: string; itemId?: string }) => {
  setBuilderStepForFlow(ctx, params.templateId, 'builder.configureXPMax');
  const backBtn = await makeBuilderBackButton(ctx, {
    templateId: params.templateId,
    fallbackAction: 'dr.template_item_menu',
    fallbackData: { templateId: params.templateId, itemId: params.itemId }
  });
  const helpBtn = await makeActionButton(ctx, {
    label: t('buttons.help'),
    action: 'dr.template_help',
    data: { templateId: params.templateId, topic: 'xp_max', itemId: params.itemId, backToItem: true }
  });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data).row().text(helpBtn.text, helpBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.daily_report.ask_xp_max_per_day'), t('screens.templates.xp_max_hint')],
    inlineKeyboard: kb
  });
};

const promptCustomCategoryInput = async (ctx: Context, params: { templateId: string; itemId?: string; backAction?: string; backData?: Record<string, unknown> }) => {
  const backAction = params.backAction ?? (params.itemId ? 'dr.template_item_menu' : 'dr.template_edit');
  const backData = params.backData ?? (params.itemId ? { templateId: params.templateId, itemId: params.itemId } : { templateId: params.templateId });
  const backBtn = await makeBuilderBackButton(ctx, { templateId: params.templateId, fallbackAction: backAction, fallbackData: backData });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.daily_report.template_builder_title'),
    bodyLines: [t('screens.templates.custom_category_prompt'), t('screens.templates.category_hint')],
    inlineKeyboard: kb
  });
};

const renderStepSummary = async (
  ctx: Context,
  params: { templateId: string; stage: 'category' | 'xp'; flow: TemplateItemFlow; backToItem?: boolean }
): Promise<void> => {
  const titleKey = params.stage === 'category' ? 'screens.templates.summary_title_category' : 'screens.templates.summary_title_xp';
  const telegramId = String(ctx.from?.id ?? '');
  const returnStep =
    params.stage === 'category'
      ? 'builder.chooseCategory'
      : params.flow.draft.xpMode === 'per_minute' || params.flow.draft.xpMode === 'per_number'
        ? 'builder.configureXPMax'
        : params.flow.draft.xpMode === 'none'
          ? 'builder.configureXP'
          : 'builder.configureXPValue';
  updateBuilderStep(telegramId, params.templateId, 'builder.summary', returnStep);
  setTemplateItemFlow(telegramId, { ...params.flow, step: 'summary' });

  const confirmBtn = await makeActionButton(ctx, {
    label: t('buttons.summary_confirm'),
    action: 'builder.summary_continue',
    data: { templateId: params.templateId, stage: params.stage, backToItem: params.backToItem === true }
  });
  const editBtn = await makeActionButton(ctx, {
    label: t('buttons.summary_edit'),
    action: 'builder.summary_edit',
    data: { templateId: params.templateId, stage: params.stage, backToItem: params.backToItem === true }
  });
  const kb = new InlineKeyboard().text(confirmBtn.text, confirmBtn.callback_data).row().text(editBtn.text, editBtn.callback_data);

  await renderScreen(ctx, {
    titleKey,
    bodyLines: buildFieldSummaryLines(params.flow.draft),
    inlineKeyboard: kb
  });
};

const renderTemplateHelp = async (
  ctx: Context,
  params: { templateId: string; topic: 'type' | 'category' | 'xp_mode' | 'xp_value' | 'xp_max'; backToItem?: boolean; itemId?: string }
) => {
  const keyMap: Record<string, string> = {
    type: 'screens.templates.help_type',
    category: 'screens.templates.help_category',
    xp_mode: 'screens.templates.help_xp',
    xp_value: 'screens.templates.help_xp_value',
    xp_max: 'screens.templates.help_xp_max'
  };
  const backAction = params.backToItem ? 'dr.template_item_menu' : 'dr.template_edit';
  const backData = params.backToItem ? { templateId: params.templateId, itemId: params.itemId } : { templateId: params.templateId };
  const telegramId = String(ctx.from?.id ?? '');
  const currentBuilder = userStates.get(telegramId)?.builder;
  if (currentBuilder?.active && currentBuilder.templateId === params.templateId) {
    const currentStep = currentBuilder.step?.startsWith('builder.help') ? currentBuilder.returnStep : currentBuilder?.step;
    updateBuilderStep(telegramId, params.templateId, `builder.help.${params.topic}`, currentStep);
  }
  const backBtn = await (builderIsActiveForTemplate(telegramId, params.templateId)
    ? makeActionButton(ctx, { label: t('buttons.back'), action: 'builder.back', data: { templateId: params.templateId } })
    : makeActionButton(ctx, { label: t('buttons.back'), action: backAction, data: backData }));
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, {
    titleKey: t('screens.form_builder.help.title'),
    bodyLines: [t(keyMap[params.topic] ?? '')],
    inlineKeyboard: kb
  });
};

const buildRoutineTypeKeyboard = async (ctx: Context, routineId?: string) => {
  const kb = new InlineKeyboard();
  const booleanBtn = await makeActionButton(ctx, {
    label: t('screens.routines.type_boolean'),
    action: 'routines.select_type',
    data: { routineId, routineType: 'boolean' }
  });
  const durationBtn = await makeActionButton(ctx, {
    label: t('screens.routines.type_duration'),
    action: 'routines.select_type',
    data: { routineId, routineType: 'duration_minutes' }
  });
  const numberBtn = await makeActionButton(ctx, {
    label: t('screens.routines.type_number'),
    action: 'routines.select_type',
    data: { routineId, routineType: 'number' }
  });
  kb.text(booleanBtn.text, booleanBtn.callback_data).row().text(durationBtn.text, durationBtn.callback_data).row().text(numberBtn.text, numberBtn.callback_data);
  return kb;
};

const buildRoutineXpModeKeyboard = async (ctx: Context, routineId?: string) => {
  const kb = new InlineKeyboard();
  const modes = [
    { key: 'fixed', label: t('screens.routines.xp_mode_fixed') },
    { key: 'per_minute', label: t('screens.routines.xp_mode_time') },
    { key: 'per_number', label: t('screens.routines.xp_mode_number') },
    { key: 'none', label: t('screens.routines.xp_mode_none') }
  ];
  for (const mode of modes) {
    const btn = await makeActionButton(ctx, { label: mode.label, action: 'routines.select_xp_mode', data: { routineId, xpMode: mode.key } });
    kb.text(btn.text, btn.callback_data).row();
  }
  return kb;
};

const promptRoutineTitle = async (ctx: Context, params: { routineId?: string }) => {
  const backAction = params.routineId ? 'routines.view' : 'routines.root';
  const backData = params.routineId ? { routineId: params.routineId } : {};
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: backAction, data: backData });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.add_title')], inlineKeyboard: kb });
};

const promptRoutineDescription = async (ctx: Context, params: { routineId?: string }) => {
  const backAction = params.routineId ? 'routines.view' : 'routines.root';
  const backData = params.routineId ? { routineId: params.routineId } : {};
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: backAction, data: backData });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.add_description')], inlineKeyboard: kb });
};

const promptRoutineXpValue = async (ctx: Context, params: { routineId?: string; xpMode: 'fixed' | 'per_minute' | 'per_number' }) => {
  const backAction = params.routineId ? 'routines.view' : 'routines.root';
  const backData = params.routineId ? { routineId: params.routineId } : {};
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: backAction, data: backData });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  const body =
    params.xpMode === 'fixed'
      ? t('screens.routines.ask_fixed_xp')
      : params.xpMode === 'per_number'
        ? t('screens.routines.ask_number_xp')
        : t('screens.routines.ask_time_xp');
  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [body], inlineKeyboard: kb });
};

const promptRoutineXpMax = async (ctx: Context, params: { routineId?: string }) => {
  const backAction = params.routineId ? 'routines.view' : 'routines.root';
  const backData = params.routineId ? { routineId: params.routineId } : {};
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: backAction, data: backData });
  const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.ask_time_xp_max')], inlineKeyboard: kb });
};

const promptRoutineConfirm = async (ctx: Context, flow: RoutineFlow) => {
  const typeLabel = flow.draft.routineType === 'duration_minutes' ? t('screens.routines.type_duration') : t('screens.routines.type_boolean');
  const summary = [
    t('screens.routines.confirm_title', { title: flow.draft.title ?? '' }),
    flow.draft.description || t('screens.routines.no_description'),
    typeLabel,
    flow.draft.xpMode === 'fixed'
      ? t('screens.routines.xp_fixed', { xp: flow.draft.xpValue ?? 0 })
      : flow.draft.xpMode === 'per_minute'
        ? t('screens.routines.xp_per_minute', {
            xp: flow.draft.xpValue ?? 0,
            max: flow.draft.xpMaxPerDay && flow.draft.xpMaxPerDay > 0 ? t('screens.routines.xp_max_suffix', { xp: flow.draft.xpMaxPerDay }) : ''
          })
        : flow.draft.xpMode === 'per_number'
          ? t('screens.routines.xp_per_number', {
              xp: flow.draft.xpValue ?? 0,
              max: flow.draft.xpMaxPerDay && flow.draft.xpMaxPerDay > 0 ? t('screens.routines.xp_max_suffix', { xp: flow.draft.xpMaxPerDay }) : ''
            })
          : t('screens.routines.xp_none')
  ];

  const saveBtn = await makeActionButton(ctx, { label: t('buttons.tpl_yes'), action: 'routines.save', data: { routineId: flow.routineId } });
  const cancelBtn = await makeActionButton(ctx, { label: t('buttons.tpl_no'), action: flow.routineId ? 'routines.view' : 'routines.root', data: flow.routineId ? { routineId: flow.routineId } : {} });
  const kb = new InlineKeyboard().text(saveBtn.text, saveBtn.callback_data).row().text(cancelBtn.text, cancelBtn.callback_data);
  await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: summary, inlineKeyboard: kb });
};

const renderTemplatesScreen = async (ctx: Context, flashLine?: string): Promise<void> => {
  const { user, settings } = await ensureUserAndSettings(ctx);
  await ensureDefaultTemplate(user.id);
  const templates = await listUserTemplates(user.id);
  const settingsJson = (settings.settings_json ?? {}) as { active_template_id?: string | null };
  const fallbackTemplateId = templates[0]?.id ?? null;
  const activeTemplateId = settingsJson.active_template_id ?? fallbackTemplateId;
  const resolvedActiveId = templates.some((tpl) => tpl.id === activeTemplateId) ? activeTemplateId : fallbackTemplateId;

  const lines: string[] = [t('screens.daily_report.templates_title')];
  if (flashLine) lines.push(flashLine);
  if (templates.length === 0) {
    lines.push(t('screens.daily_report.templates_empty'));
  } else {
    templates.forEach((tpl) => {
      const isActive = tpl.id === resolvedActiveId;
      const prefix = isActive ? '⭐' : '•';
      const title = tpl.title ?? t('screens.templates.default_title');
      lines.push(`${prefix} ${title} (${tpl.itemCount ?? 0} items)`);
    });
  }

  const kb = new InlineKeyboard();

  for (const tpl of templates) {
    const setActiveBtn = await makeActionButton(ctx, { label: t('buttons.tpl_set_active'), action: 'dr.template_set_active', data: { templateId: tpl.id } });
    const editBtn = await makeActionButton(ctx, { label: t('buttons.tpl_edit_form'), action: 'dr.template_edit', data: { templateId: tpl.id } });
    const moreBtn = await makeActionButton(ctx, { label: t('buttons.tpl_more'), action: 'dr.template_actions', data: { templateId: tpl.id } });
    kb.text(setActiveBtn.text, setActiveBtn.callback_data).text(editBtn.text, editBtn.callback_data).text(moreBtn.text, moreBtn.callback_data).row();
  }

  const newBtn = await makeActionButton(ctx, { label: t('buttons.dr_template_new'), action: 'dr.template_new' });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu' });

  kb.text(newBtn.text, newBtn.callback_data).row();
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.templates_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderTemplateActions = async (ctx: Context, templateId: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const tpl = await getTemplateById(templateId);
  if (!tpl || tpl.user_id !== user.id) {
    await renderTemplatesScreen(ctx);
    return;
  }

  const items = await listAllItems(templateId);
  const lines: string[] = [
    t('screens.daily_report.template_action_title'),
    tpl.title ?? t('screens.templates.default_title'),
    t('screens.daily_report.template_details_items_line', { count: items.length })
  ];

  const kb = new InlineKeyboard();
  const renameBtn = await makeActionButton(ctx, { label: t('buttons.templates_rename'), action: 'dr.template_rename_prompt', data: { templateId } });
  const dupBtn = await makeActionButton(ctx, { label: t('buttons.tpl_actions_duplicate'), action: 'dr.template_duplicate', data: { templateId } });
  const delBtn = await makeActionButton(ctx, { label: t('buttons.tpl_actions_delete'), action: 'dr.template_delete_confirm', data: { templateId } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.tpl_actions_back'), action: 'dr.templates' });

  kb.text(renameBtn.text, renameBtn.callback_data).row();
  kb.text(dupBtn.text, dupBtn.callback_data).row();
  kb.text(delBtn.text, delBtn.callback_data).row();
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.templates_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderTemplateEdit = async (ctx: Context, templateId: string, flashLine?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const tpl = await getTemplateById(templateId);
  if (!tpl || tpl.user_id !== user.id) {
    await renderTemplatesScreen(ctx);
    return;
  }
  const telegramId = String(ctx.from?.id ?? '');
  clearTemplateItemFlow(telegramId);
  clearBuilderState(telegramId);

  const items = await listAllItems(templateId);
  const visibleItems = items.filter((item) => !isRoutineItem(item));

  const lines: string[] = [
    t('screens.daily_report.template_builder_title'),
    t('screens.daily_report.template_items_header', { title: tpl.title ?? t('screens.templates.default_title') }),
    t('screens.daily_report.template_items_count', { count: visibleItems.length })
  ];
  if (flashLine) lines.push(flashLine);
  if (visibleItems.length === 0) {
    lines.push(t('screens.daily_report.template_items_empty'));
  } else {
    visibleItems.forEach((item, idx) => {
      const statusIcon = item.enabled ? '✅' : '🚫';
      const xpSummary = buildXpSummary(itemToDraft(item));
      lines.push(`[${idx + 1}] ${statusIcon} ${item.label} (${displayItemTypeLabel(item.item_type)}, ${xpSummary})`);
    });
  }

  const kb = new InlineKeyboard();

  for (const [idx, item] of visibleItems.entries()) {
    const btn = await makeActionButton(ctx, {
      label: t('buttons.template_item_open', { index: idx + 1, label: item.label }),
      action: 'dr.template_item_menu',
      data: { templateId, itemId: item.id }
    });
    kb.text(btn.text, btn.callback_data).row();
  }

  const addBtn = await makeActionButton(ctx, { label: t('buttons.tpl_add_item'), action: 'dr.template_item_add', data: { templateId } });
  const renameBtn = await makeActionButton(ctx, { label: t('buttons.templates_rename'), action: 'dr.template_rename_prompt', data: { templateId } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.templates' });
  kb.text(addBtn.text, addBtn.callback_data).row();
  kb.text(renameBtn.text, renameBtn.callback_data).row();
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.template_builder_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderTemplateItemMenu = async (ctx: Context, templateId: string, itemId: string, flashLine?: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const tpl = await getTemplateById(templateId);
  const item = await getItemById(itemId);
  if (!tpl || tpl.user_id !== user.id || !item || item.template_id !== tpl.id) {
    await renderTemplatesScreen(ctx);
    return;
  }
  if (isRoutineItem(item)) {
    await renderTemplateEdit(ctx, templateId);
    return;
  }

  const xpSummary = buildXpSummary(itemToDraft(item));
  const lines: string[] = [
    t('screens.daily_report.item_menu_title'),
    t('screens.daily_report.item_menu_summary', {
      label: item.label,
      type: displayItemTypeLabel(item.item_type),
      category: item.category ?? '-',
      xp: xpSummary,
      enabled: item.enabled ? t('common.active') : t('common.inactive')
    })
  ];
  if (flashLine) lines.push(flashLine);

  const kb = new InlineKeyboard();
  const editLabelBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_edit_label'), action: 'dr.template_item_edit_label', data: { templateId, itemId } });
  const editKeyBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_edit_key'), action: 'dr.template_item_edit_key', data: { templateId, itemId } });
  const editTypeBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_edit_type'), action: 'dr.template_item_edit_type', data: { templateId, itemId } });
  const editCategoryBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_edit_category'), action: 'dr.template_item_edit_category', data: { templateId, itemId } });
  const editXpBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_edit_xp'), action: 'dr.template_item_edit_xp', data: { templateId, itemId } });
  const toggleBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_toggle_enabled'), action: 'dr.template_item_toggle_enabled', data: { templateId, itemId } });
  const moveUpBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_move_up'), action: 'dr.template_item_move_up', data: { templateId, itemId } });
  const moveDownBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_move_down'), action: 'dr.template_item_move_down', data: { templateId, itemId } });
  const delBtn = await makeActionButton(ctx, { label: t('buttons.tpl_item_delete'), action: 'dr.template_item_delete_confirm', data: { templateId, itemId } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.template_edit', data: { templateId } });

  kb.text(editLabelBtn.text, editLabelBtn.callback_data).text(editKeyBtn.text, editKeyBtn.callback_data).row();
  kb.text(editTypeBtn.text, editTypeBtn.callback_data).text(editCategoryBtn.text, editCategoryBtn.callback_data).row();
  kb.text(editXpBtn.text, editXpBtn.callback_data).row();
  kb.text(toggleBtn.text, toggleBtn.callback_data).row();
  kb.text(moveUpBtn.text, moveUpBtn.callback_data).text(moveDownBtn.text, moveDownBtn.callback_data).row();
  kb.text(delBtn.text, delBtn.callback_data).row();
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.item_menu_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderTemplateItemDeleteConfirm = async (ctx: Context, templateId: string, itemId: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const tpl = await getTemplateById(templateId);
  const item = await getItemById(itemId);
  if (!tpl || tpl.user_id !== user.id || !item || item.template_id !== tpl.id) {
    await renderTemplatesScreen(ctx);
    return;
  }

  const lines: string[] = [t('screens.daily_report.item_delete_confirm')];
  const yesBtn = await makeActionButton(ctx, { label: t('buttons.tpl_yes'), action: 'dr.template_item_delete', data: { templateId, itemId } });
  const noBtn = await makeActionButton(ctx, { label: t('buttons.tpl_no'), action: 'dr.template_item_menu', data: { templateId, itemId } });
  const kb = new InlineKeyboard().text(yesBtn.text, yesBtn.callback_data).row().text(noBtn.text, noBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.item_menu_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderTemplateDeleteConfirm = async (ctx: Context, templateId: string): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const tpl = await getTemplateById(templateId);
  if (!tpl || tpl.user_id !== user.id) {
    await renderTemplatesScreen(ctx);
    return;
  }

  const lines: string[] = [t('screens.daily_report.template_delete_confirm', { title: tpl.title ?? t('screens.templates.default_title') })];

  const confirmBtn = await makeActionButton(ctx, { label: t('buttons.dr_template_delete'), action: 'dr.template_delete', data: { templateId } });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.tpl_actions_back'), action: 'dr.template_actions', data: { templateId } });

  const kb = new InlineKeyboard().text(confirmBtn.text, confirmBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.templates_title'), bodyLines: lines, inlineKeyboard: kb });
};

const finalizeNewTemplateItem = async (ctx: Context, telegramId: string, flow: TemplateItemFlow): Promise<void> => {
  try {
    const items = await listAllItems(flow.templateId);
    const maxSort = items.reduce((max, item) => Math.max(max, item.sort_order ?? 0), 0);
    const sortOrder = (maxSort || items.length * 10) + 10;
    const label = flow.draft.label ?? t('screens.daily_report.template_new_title');
    const itemKey = flow.draft.itemKey ?? (await generateUniqueItemKey(flow.templateId, label));
    const itemType = flow.draft.itemType ?? 'text';
    const category = flow.draft.category && flow.draft.category !== 'none' ? flow.draft.category : null;
    const xpStorage = deriveXpStorage({ ...flow.draft, itemType });

    await upsertItem({
      templateId: flow.templateId,
      label,
      itemKey,
      itemType,
      category,
      xpMode: xpStorage.xpMode,
      xpValue: xpStorage.xpValue,
      xpMaxPerDay: xpStorage.xpMax,
      optionsJson: xpStorage.optionsJson,
      sortOrder
    });
    clearTemplateItemFlow(telegramId);
    clearBuilderState(telegramId);
    clearReportContextCache();
    await renderTemplateEdit(ctx, flow.templateId, t('screens.daily_report.item_saved'));
  } catch (error) {
    console.error({ scope: 'daily_report', event: 'template_item_finalize_failed', error, flow });
    clearTemplateItemFlow(telegramId);
    clearBuilderState(telegramId);
    await renderTemplateEdit(ctx, flow.templateId);
  }
};

const renderHistory = async (ctx: Context, range: '7d' | '30d' = '7d'): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const days = await listRecentReportDays({ userId: user.id, range });

  const lines: string[] = [
    t('screens.daily_report.history_title'),
    t('screens.daily_report.history_range', { range: range === '7d' ? t('buttons.dr_history_7d') : t('buttons.dr_history_30d') }),
    ''
  ];

  if (!days.length) {
    lines.push(t('screens.daily_report.history_no_results'));
  } else {
    lines.push(t('screens.daily_report.history_open_hint'), '');
    for (const entry of days) {
      const lockedSuffix = entry.day.locked ? t('screens.daily_report.history_locked_suffix') : '';
      lines.push(
        t('screens.daily_report.history_list_line', {
          date: entry.day.local_date,
          completed: entry.completed,
          total: entry.total,
          skipped: entry.skipped
        }) + lockedSuffix
      );
    }
  }

  const kb = new InlineKeyboard();
  const range7Btn = await makeActionButton(ctx, { label: t('buttons.dr_history_7d'), action: 'dr.history_7d' });
  const range30Btn = await makeActionButton(ctx, { label: t('buttons.dr_history_30d'), action: 'dr.history_30d' });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu' });

  kb.text(range7Btn.text, range7Btn.callback_data).text(range30Btn.text, range30Btn.callback_data).row();
  for (const entry of days) {
    const openBtn = await makeActionButton(ctx, {
      label: t('buttons.dr_history_open_day', { date: entry.day.local_date }),
      action: 'dr.history_open_day',
      data: { reportDayId: entry.day.id }
    });
    kb.text(openBtn.text, openBtn.callback_data).row();
  }
  kb.text(backBtn.text, backBtn.callback_data);

  await renderScreen(ctx, { titleKey: t('screens.daily_report.history_title'), bodyLines: lines, inlineKeyboard: kb });
};

const renderHistoryDay = async (ctx: Context, reportDayId: string): Promise<void> => {
  const reportDay = await getReportDayById(reportDayId);
  if (!reportDay) {
    await renderHistory(ctx);
    return;
  }
  const items = await listAllItems(reportDay.template_id);
  const enabledItems = items.filter((i) => i.enabled);
  const displayItems = enabledItems.filter((item) => !isRoutineTaskItem(item));
  const statuses = await listCompletionStatus(reportDay.id, displayItems);
  const template = await getTemplateById(reportDay.template_id);

  const lines: string[] = [
    t('screens.daily_report.history_detail_title', { date: reportDay.local_date, template: template?.title ?? t('screens.templates.default_title') }),
    t('screens.daily_report.history_detail_hint'),
    ''
  ];

  statuses.forEach((s, idx) => {
    const icon = s.filled ? '✅' : s.skipped ? '⏭' : '⬜️';
    const valueText = s.filled ? formatDisplayValue(s.item, s.value?.value_json ?? null) : t('screens.daily_report.value_skipped');
    lines.push(`${icon} ${idx + 1}) ${formatItemLabel(s.item)} — ${valueText}`);
  });

  const kb = new InlineKeyboard();
  const backToHistoryBtn = await makeActionButton(ctx, { label: t('buttons.dr_history_back'), action: 'dr.history' });
  const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.menu' });
  kb.text(backToHistoryBtn.text, backToHistoryBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);

  // cache warmed
  reportContextCache.set(`${reportDay.user_id}:${reportDay.local_date}`, { reportDay, items: enabledItems });

  await renderScreen(ctx, { titleKey: t('screens.daily_report.history_title'), bodyLines: lines, inlineKeyboard: kb });
};

const handleSaveValue = async (ctx: Context, text: string): Promise<void> => {
  if (!ctx.from) return;
  const stateKey = String(ctx.from.id);
  const state = userStates.get(stateKey);
  if (!state?.awaitingValue) return;

  const { reportDayId, itemId, origin, statusFilter } = state.awaitingValue;
  const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);

  let context = cached ?? null;
  if (!context) {
    const existingDay = await getReportDayById(reportDayId);
    const localDate = existingDay?.local_date ?? formatLocalTime(config.defaultTimezone).date;
    context = await ensureSpecificReportContext(ctx, localDate);
  }
  const reportDay = context.reportDay;
  const items = context.items;

  if (reportDay.id !== reportDayId) {
    userStates.delete(stateKey);
    await renderScreen(ctx, {
      titleKey: t('screens.daily_report.title'),
      bodyLines: [t('screens.daily_report.session_expired')],
      inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay)
    });
    return;
  }

  if (reportDay.locked) {
    userStates.delete(stateKey);
    await renderScreen(ctx, {
      titleKey: t('screens.daily_report.title'),
      bodyLines: isLockedMessageLines(reportDay),
      inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay)
    });
    return;
  }

  const item = items.find((i) => i.id === itemId);
  if (!item) {
    userStates.delete(stateKey);
    await renderScreen(ctx, {
      titleKey: t('screens.daily_report.title'),
      bodyLines: [t('screens.daily_report.item_not_found')],
      inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay)
    });
    return;
  }

  let valueJson: Record<string, unknown> | null = null;

  switch (item.item_type) {
    case 'time_hhmm': {
      const parsed = parseTimeHhmm(text);
      if (!parsed) {
        await renderScreen(ctx, {
          titleKey: t('screens.daily_report.title'),
          bodyLines: [t('screens.daily_report.invalid_time')],
          inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items })
        });
        return;
      }
      valueJson = { value: parsed.hhmm, minutes: parsed.minutes };
      break;
    }
    case 'duration_minutes': {
      const n = parseNonNegativeNumber(text);
      if (n === null) {
        await renderScreen(ctx, {
          titleKey: t('screens.daily_report.title'),
          bodyLines: [t('screens.daily_report.invalid_duration')],
          inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items })
        });
        return;
      }
      const unit = state.numericDraft?.unit ?? 'minutes';
      const { minutes, seconds } = convertToMinutes(n, unit);
      valueJson = { value: minutes, minutes, ...(unit === 'seconds' ? { seconds: Math.max(0, Math.round(n)) } : {}) };
      break;
    }
    case 'number': {
      const n = parseNonNegativeNumber(text);
      if (n === null) {
        await renderScreen(ctx, {
          titleKey: t('screens.daily_report.title'),
          bodyLines: [t('screens.daily_report.invalid_number')],
          inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items })
        });
        return;
      }
      const isPerMinute = ['per_minute', 'time'].includes(item.xp_mode ?? '');
      const isPerNumber = (item.xp_mode ?? '') === 'per_number';
      valueJson = { value: n, number: n, ...(isPerMinute ? { minutes: n } : {}), ...(isPerNumber ? { units: n } : {}) };
      break;
    }
    case 'boolean': {
      valueJson = { value: valueIsTrue(text) };
      break;
    }
    default:
      valueJson = { value: text };
  }

  try {
    await saveValue({ reportDayId, item, valueJson, userId: reportDay.user_id });
  } catch (error) {
    console.error({ scope: 'daily_report', event: 'save_value_failed', error, reportDayId, itemId: item.id, valueJson });
    await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: [t('screens.daily_report.save_failed')], inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
    return;
  }

  const userSettings = (await ensureUserAndSettings(ctx)).user.settings_json as Record<string, unknown>;
  await logForUser({ userId: reportDay.user_id, ctx, eventName: 'db_write', payload: { action: 'save_value', item_id: item.id }, enabled: telemetryEnabledForUser(userSettings) });

  // Clear state for that field
  const nextState = { ...(userStates.get(stateKey) ?? {}) };
  delete nextState.awaitingValue;
  delete nextState.numericDraft;
  delete nextState.timeDraft;
  userStates.set(stateKey, nextState);

  await continueFlowAfterAction(ctx, reportDay, origin, statusFilter);
};

const renderSettingsRoot = async (ctx: Context): Promise<void> => {
  const { user } = await ensureUserAndSettings(ctx);
  const emojiEnabled = await getUserBooleanSetting(user.id, EMOJI_SETTING_KEY, config.ui.emojiEnabled);
  const changeLanguageBtn = await makeActionButton(ctx, { label: t('buttons.change_language'), action: 'settings.language' });
  const speedBtn = await makeActionButton(ctx, { label: t('buttons.settings_speed_test'), action: 'settings.speed_test' });
  await renderSettingsScreen(ctx, {
    emojiEnabled,
    extraButtons: [changeLanguageBtn, speedBtn]
  });
};

/* ===== Commands ===== */

bot.command('start', async (ctx: Context) => {
  const { settings, locale } = await ensureUserAndSettings(ctx);
  const storedLanguage = readStoredLanguageCode(settings.settings_json as Record<string, unknown>);
  if (!storedLanguage) {
    await renderLanguageSelection(ctx, { origin: 'onboarding', currentLocale: locale });
    return;
  }
  await renderDashboard(ctx);
});

bot.command('home', async (ctx: Context) => {
  await renderDashboard(ctx);
});

bot.command('debug_inline', async (ctx: Context) => {
  const keyboard = new InlineKeyboard().text(t('buttons.debug_inline'), 'dbg:test');
  await ctx.reply(t('screens.debug_inline.title'), { reply_markup: keyboard });
});

bot.callbackQuery('dbg:test', async (ctx) => {
  await safeAnswerCallback(ctx, { text: t('screens.debug_inline.success') });
});

bot.callbackQuery('system.retry', async (ctx) => {
  await safeAnswerCallback(ctx);
  await renderDashboard(ctx);
});

const navButtons: Array<{ key: string; handler: (ctx: Context) => Promise<void> | void; label?: (locale: Locale) => string }> = [
  { key: 'buttons.nav_dashboard', handler: renderDashboard },
  { key: 'buttons.nav_daily_report', handler: async (ctx: Context) => renderDailyReportRoot(ctx) },
  { key: 'buttons.notes', handler: renderNotesToday, label: () => labels.nav.notes() },
  { key: 'buttons.nav_reportcar', handler: renderReportcar },
  { key: 'buttons.nav_tasks', handler: renderTasks },
  { key: 'buttons.nav_todo', handler: renderTodo },
  { key: 'buttons.nav_planning', handler: renderPlanning },
  { key: 'buttons.nav_my_day', handler: renderMyDay },
  { key: 'buttons.nav_free_text', handler: renderNotesToday, label: () => labels.nav.freeText() },
  { key: 'buttons.nav_reminders', handler: renderReminders, label: () => labels.nav.reminders() },
  { key: 'buttons.nav_rewards', handler: renderRewardCenter },
  { key: 'buttons.nav_reports', handler: renderReportsMenu },
  { key: 'buttons.nav_calendar', handler: renderCalendarEvents },
  { key: 'buttons.nav_settings', handler: renderSettingsRoot },
  { key: 'buttons.nav_ai', handler: renderAI }
];
navButtons.forEach(({ key, handler, label }) => {
  const resolveLabel = (locale: Locale) => (label ? label(locale) : t(key, undefined, locale));
  bot.hears([resolveLabel('en'), resolveLabel('fa')], handler);
});

/**
 * Tokenized callback handler
 */
bot.callbackQuery(/^[A-Za-z0-9_-]{8,12}$/, async (ctx) => {
  await safeAnswerCallback(ctx);

  const traceId = getTraceId(ctx);

  try {
    const { user } = await ensureUserAndSettings(ctx);
    const enabled = telemetryEnabledForUser(user.settings_json as Record<string, unknown>);

    await logTelemetryEvent({ userId: user.id, traceId, eventName: 'callback_token_pressed', payload: { data: ctx.callbackQuery.data }, enabled });

    const token = ctx.callbackQuery.data;
    const payload = await consumeCallbackToken(token);

    await logTelemetryEvent({ userId: user.id, traceId, eventName: 'callback_token_consumed', payload: { token, valid: Boolean(payload) }, enabled });

    const action = typeof payload === 'object' && payload ? (payload as { action?: string }).action : null;

    if (!action) {
      await ctx.answerCallbackQuery({ text: t('errors.action_expired'), show_alert: true });
      return;
    }

    switch (action) {
      case 'noop':
        return;

      /* --- Navigation --- */
      case 'nav.dashboard':
        await renderDashboard(ctx);
        return;
      case 'nav.daily_report':
        await renderDailyReportRoot(ctx);
        return;
      case 'nav.free_text':
        await renderNotesToday(ctx);
        return;
      case 'dr.back':
        await renderDashboard(ctx);
        return;
      case 'nav.reportcar':
        await renderReportcar(ctx);
        return;
      case 'nav.tasks':
        await renderTasks(ctx);
        return;
      case 'nav.reminders':
        await renderReminders(ctx);
        return;
      case 'reminders.new': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        setReminderFlow(stateKey, { mode: 'create', step: 'title', draft: {} });
        await renderReminderTitlePrompt(ctx, 'create');
        return;
      }
      case 'nav.rewards':
        await renderRewardCenter(ctx);
        return;
      case 'nav.reports':
        await renderReportsMenu(ctx);
        return;
      case 'nav.settings':
        await renderSettingsRoot(ctx);
        return;
      case 'notes.add': {
        if (!ctx.from) break;
        const { user } = await ensureUserAndSettings(ctx);
        const { date } = getTodayDateString(user.timezone ?? config.defaultTimezone);
        setNotesFlow(String(ctx.from.id), { mode: 'create', step: 'title', draft: { noteDate: date } });

        const skipBtn = await makeActionButton(ctx, { label: labels.notesButtons.skip(), action: 'notes.skip_title' });
        const cancelBtn = await makeActionButton(ctx, { label: labels.notesButtons.cancel(), action: 'notes.cancel_add' });
        const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data).row().text(cancelBtn.text, cancelBtn.callback_data);

        await renderScreen(ctx, {
          title: labels.notes.title(),
          bodyLines: [labels.notes.askTitle()],
          inlineKeyboard: kb
        });
        return;
      }
      case 'notes.skip_title': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const state = userStates.get(stateKey);
        if (state?.notesFlow && state.notesFlow.mode === 'create' && state.notesFlow.step === 'title') {
          setNotesFlow(stateKey, { mode: 'create', step: 'body', draft: { ...state.notesFlow.draft, title: null } });
        }
        await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.askBody()] });
        return;
      }
      case 'notes.cancel_add': {
        if (ctx.from) {
          const stateKey = String(ctx.from.id);
          const session = userStates.get(stateKey)?.noteUploadSession;
          if (session?.timer) {
            clearTimeout(session.timer);
          }
          setNoteUploadSession(stateKey, undefined);
          clearNotesFlow(stateKey);
        }
        await renderNotesToday(ctx);
        return;
      }
      case 'notes.clear_today': {
        if (!ctx.from) break;
        const { user } = await ensureUserAndSettings(ctx);
        const { date } = getTodayDateString(user.timezone ?? config.defaultTimezone);
        setNotesFlow(String(ctx.from.id), { mode: 'clear_date', noteDate: date });
        const confirmBtn = await makeActionButton(ctx, { label: labels.notesButtons.confirmClear(), action: 'notes.clear_confirm' });
        const cancelBtn = await makeActionButton(ctx, { label: labels.notesButtons.cancel(), action: 'notes.clear_cancel' });
        const kb = new InlineKeyboard().text(confirmBtn.text, confirmBtn.callback_data).text(cancelBtn.text, cancelBtn.callback_data);
        await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.clearConfirm()], inlineKeyboard: kb });
        return;
      }
      case 'notes.clear_confirm': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.notesFlow;
        if (flow?.mode === 'clear_date') {
          const { user } = await ensureUserAndSettings(ctx);
          const notes = await listNotesByDate({ userId: user.id, noteDate: flow.noteDate });
          for (const note of notes) {
            await markNoteArchiveDeleted(ctx, note.id);
            await deleteNote({ userId: user.id, id: note.id });
          }
        }
        clearNotesFlow(stateKey);
        await renderNotesToday(ctx);
        return;
      }
      case 'notes.clear_cancel': {
        if (ctx.from) clearNotesFlow(String(ctx.from.id));
        await renderNotesToday(ctx);
        return;
      }
      case 'notes.history':
        await renderNotesHistory(ctx, 0);
        return;
      case 'notes.history_page': {
        const page = Number((payload as { data?: { page?: number } }).data?.page ?? 0);
        await renderNotesHistory(ctx, Number.isFinite(page) ? page : 0);
        return;
      }
      case 'notes.history_date': {
        const data = (payload as { data?: { date?: string; historyPage?: number } }).data;
        const date = data?.date;
        if (!date) {
          await renderNotesHistory(ctx);
          return;
        }
        const historyPage = Number(data?.historyPage ?? 0);
        await renderNotesDate(ctx, date, 0, Number.isFinite(historyPage) ? historyPage : 0);
        return;
      }
      case 'notes.history_date_page': {
        const data = (payload as { data?: { date?: string; page?: number; historyPage?: number } }).data;
        const date = data?.date;
        if (!date) {
          await renderNotesHistory(ctx);
          return;
        }
        const page = Number(data?.page ?? 0);
        const historyPage = Number(data?.historyPage ?? 0);
        await renderNotesDate(ctx, date, Number.isFinite(page) ? page : 0, Number.isFinite(historyPage) ? historyPage : 0);
        return;
      }
      case 'notes.view_note': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const page = Number(data?.page ?? 0);
        const historyPage = Number(data?.historyPage ?? 0);
        await renderNoteDetails(ctx, noteId, {
          noteDate: data?.noteDate,
          page: Number.isFinite(page) ? page : 0,
          historyPage: Number.isFinite(historyPage) ? historyPage : 0
        });
        return;
      }
      case 'notes.delete_note': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const note = await getNoteById({ userId: user.id, id: noteId });
        if (note) {
          await markNoteArchiveDeleted(ctx, noteId);
          const archiveChatId = getNotesArchiveChatId();
          if (archiveChatId) {
            const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
            const marker = [
              labels.archive.deletedBy({ user: buildUserStatusLine(ctx) }),
              labels.archive.typeLine({ type: 'note' }),
              labels.archive.titleLine({ title }),
              labels.archive.timeLine({ time: note.note_date }),
              labels.archive.appUserLine({ id: note.id })
            ].join('\n');
            try {
              await ctx.api.sendMessage(archiveChatId, marker, { disable_notification: true });
            } catch (error) {
              console.error({ scope: 'notes', event: 'archive_delete_marker_failed', error, noteId, archiveChatId });
            }
          }
          await deleteNote({ userId: user.id, id: noteId });
          const page = Number(data?.page ?? 0);
          const historyPage = Number(data?.historyPage ?? 0);
          await renderNotesDate(ctx, note.note_date, Number.isFinite(page) ? page : 0, Number.isFinite(historyPage) ? historyPage : 0);
        } else {
          await renderNotesHistory(ctx);
        }
        return;
      }
      case 'notes.edit_menu': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const page = Number(data?.page ?? 0);
        const historyPage = Number(data?.historyPage ?? 0);
        await renderNoteEditMenu(ctx, noteId, {
          noteDate: data?.noteDate,
          page: Number.isFinite(page) ? page : 0,
          historyPage: Number.isFinite(historyPage) ? historyPage : 0
        });
        return;
      }
      case 'notes.edit_title': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const viewContext = {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        };
        setNotesFlow(stateKey, { mode: 'edit', noteId, step: 'title', viewContext });
        const skipBtn = await makeActionButton(ctx, {
          label: labels.notesButtons.skip(),
          action: 'notes.edit_title_skip',
          data: { noteId, ...viewContext }
        });
        const backBtn = await makeActionButton(ctx, { label: labels.notesButtons.back(), action: 'notes.edit_menu', data: { noteId, ...viewContext } });
        const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data).row().text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, { title: labels.notes.editMenuTitle(), bodyLines: [labels.notes.editTitlePrompt()], inlineKeyboard: kb });
        return;
      }
      case 'notes.edit_title_skip': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        await updateNote({ userId: user.id, id: noteId, title: null });
        clearNotesFlow(String(ctx.from.id));
        await renderNoteDetails(ctx, noteId, {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        });
        return;
      }
      case 'notes.edit_body': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const viewContext = {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        };
        setNotesFlow(stateKey, { mode: 'edit', noteId, step: 'body', viewContext });
        const backBtn = await makeActionButton(ctx, { label: labels.notesButtons.back(), action: 'notes.edit_menu', data: { noteId, ...viewContext } });
        const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, { title: labels.notes.editMenuTitle(), bodyLines: [labels.notes.editBodyPrompt()], inlineKeyboard: kb });
        return;
      }
      case 'notes.attach_done': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesToday(ctx);
          return;
        }
        await startNoteCaptionFlow(ctx, noteId, {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        });
        return;
      }
      case 'notes.attach_cancel': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        const stateKey = String(ctx.from.id);
        const session = userStates.get(stateKey)?.noteUploadSession;
        if (session?.timer) {
          clearTimeout(session.timer);
        }
        setNoteUploadSession(stateKey, undefined);
        clearNotesFlow(stateKey);
        if (!noteId) {
          await renderNotesToday(ctx);
          return;
        }
        await renderNoteDetails(ctx, noteId, {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        });
        return;
      }
      case 'notes.attachments_save': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        await startNoteCaptionFlow(ctx, data.noteId, {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        });
        return;
      }
      case 'notes.attachments_continue': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const session = userStates.get(stateKey)?.noteUploadSession;
        if (session?.timer) {
          clearTimeout(session.timer);
        }
        const timer = setTimeout(async () => {
          const latestState = userStates.get(stateKey);
          const currentSession = latestState?.noteUploadSession;
          if (!currentSession || currentSession.noteId !== data.noteId) return;
          const idleFor = Date.now() - currentSession.lastReceivedAt;
          if (idleFor < NOTE_UPLOAD_IDLE_MS) return;
          const saveBtn = await makeActionButton(ctx, {
            label: labels.notesButtons.saveNow(),
            action: 'notes.attachments_save',
            data: { noteId: data.noteId, noteDate: data.noteDate, page: data.page, historyPage: data.historyPage }
          });
          const continueBtn = await makeActionButton(ctx, {
            label: labels.notesButtons.continue(),
            action: 'notes.attachments_continue',
            data: { noteId: data.noteId, noteDate: data.noteDate, page: data.page, historyPage: data.historyPage }
          });
          const kb = new InlineKeyboard().text(saveBtn.text, saveBtn.callback_data).row().text(continueBtn.text, continueBtn.callback_data);
          setNoteUploadSession(stateKey, { ...currentSession, prompted: true });
          await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.attachmentsIdlePrompt()], inlineKeyboard: kb });
        }, NOTE_UPLOAD_IDLE_MS);
        setNoteUploadSession(stateKey, {
          ...(session ?? { noteId: data.noteId, pendingKinds: {}, lastReceivedAt: Date.now() }),
          noteId: data.noteId,
          viewContext: { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage },
          lastReceivedAt: Date.now(),
          prompted: false,
          timer
        });
        await renderNoteAttachmentPrompt(ctx, data.noteId, {
          noteDate: data.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        });
        return;
      }
      case 'notes.attach_more': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        const noteId = data?.noteId;
        if (!noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const viewContext = {
          noteDate: data?.noteDate,
          page: Number.isFinite(Number(data?.page)) ? Number(data?.page) : 0,
          historyPage: Number.isFinite(Number(data?.historyPage)) ? Number(data?.historyPage) : 0
        };
        setNotesFlow(String(ctx.from.id), { mode: 'create', step: 'attachments', noteId, viewContext });
        await renderNoteAttachmentPrompt(ctx, noteId, viewContext);
        return;
      }
      case 'notes.caption_all': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        setNotesFlow(String(ctx.from.id), {
          mode: 'create',
          step: 'caption_all',
          noteId: data.noteId,
          viewContext: { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage }
        });
        const skipBtn = await makeActionButton(ctx, {
          label: labels.notesButtons.skip(),
          action: 'notes.caption_skip',
          data: { noteId: data.noteId, noteDate: data.noteDate, page: data.page, historyPage: data.historyPage }
        });
        const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data);
        await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.captionAllPrompt()], inlineKeyboard: kb });
        return;
      }
      case 'notes.caption_by_category': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const pending = await listPendingNoteAttachments({ noteId: data.noteId });
        const categories = buildNoteCaptionCategories(pending);
        if (categories.length === 0) {
          await clearPendingNoteAttachments({ noteId: data.noteId });
          clearNotesFlow(String(ctx.from.id));
          await finalizeNoteArchive(ctx, data.noteId, { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
          return;
        }
        await promptNoteCaptionCategory(
          ctx,
          data.noteId,
          categories[0],
          { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage },
          categories
        );
        return;
      }
      case 'notes.caption_skip': {
        if (!ctx.from) break;
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        await clearPendingNoteAttachments({ noteId: data.noteId });
        clearNotesFlow(String(ctx.from.id));
        await finalizeNoteArchive(ctx, data.noteId, { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
        return;
      }
      case 'notes.caption_category_skip': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.notesFlow;
        if (!flow || flow.mode !== 'create' || flow.step !== 'caption_category' || !flow.currentCategory) {
          await renderNotesHistory(ctx);
          return;
        }
        const category = flow.currentCategory;
        const kinds =
          category === 'files'
            ? (['document', 'audio'] as NoteAttachmentKind[])
            : ([category] as NoteAttachmentKind[]);
        await clearPendingNoteAttachmentsByKinds({ noteId: flow.noteId, kinds });
        const remaining = (flow.captionCategories ?? []).filter((item) => item !== category);
        if (remaining.length === 0) {
          await clearPendingNoteAttachments({ noteId: flow.noteId });
          clearNotesFlow(stateKey);
          await finalizeNoteArchive(ctx, flow.noteId, flow.viewContext ?? {});
          return;
        }
        await promptNoteCaptionCategory(ctx, flow.noteId, remaining[0], flow.viewContext ?? {}, remaining);
        return;
      }
      case 'notes.attachments_kind': {
        const data = (payload as { data?: { noteId?: string; kind?: NoteAttachmentKind; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId || !data.kind) {
          await renderNotesHistory(ctx);
          return;
        }
        await sendNoteAttachmentsByKind(ctx, {
          noteId: data.noteId,
          kind: data.kind,
          noteDate: data.noteDate ?? '',
          page: Number(data.page ?? 0),
          historyPage: Number(data.historyPage ?? 0)
        });
        return;
      }
      case 'notes.attachment_open': {
        const data = (payload as { data?: { noteId?: string; attachmentId?: string; kind?: NoteAttachmentKind; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId || !data.attachmentId || !data.kind) {
          await renderNotesHistory(ctx);
          return;
        }
        const attachment = await getNoteAttachmentById({ noteId: data.noteId, attachmentId: data.attachmentId });
        if (!attachment) {
          await renderNoteAttachmentsList(ctx, {
            noteId: data.noteId,
            kind: data.kind,
            noteDate: data.noteDate ?? '',
            page: Number(data.page ?? 0),
            historyPage: Number(data.historyPage ?? 0)
          });
          return;
        }
        try {
          const targetId = ctx.chat?.id ?? ctx.from?.id;
          if (!targetId) {
            await renderScreen(ctx, { title: labels.notes.detailTitleLabel(), bodyLines: [labels.notes.attachmentSendFailed()] });
            return;
          }
          const category = resolveNoteCaptionCategory(attachment.kind);
          await sendNoteAttachmentsToUser(ctx, targetId, category, [attachment]);
        } catch (error) {
          console.error({ scope: 'notes', event: 'attachment_send_failed', error, attachmentId: attachment.id });
          await renderScreen(ctx, { title: labels.notes.detailTitleLabel(), bodyLines: [labels.notes.attachmentSendFailed()] });
        }
        const page = Number(data.page ?? 0);
        const historyPage = Number(data.historyPage ?? 0);
        await renderNoteAttachmentsList(ctx, {
          noteId: data.noteId,
          kind: data.kind,
          noteDate: data.noteDate ?? '',
          page: Number.isFinite(page) ? page : 0,
          historyPage: Number.isFinite(historyPage) ? historyPage : 0
        });
        return;
      }
      case 'notes.view_all': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const note = await getNoteById({ userId: user.id, id: data.noteId });
        if (!note) {
          await renderNotesHistory(ctx);
          return;
        }
        const local = formatInstantToLocal(note.created_at, user.timezone ?? config.defaultTimezone);
        const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
        await renderScreen(ctx, {
          title: labels.notes.detailTitleLabel(),
          bodyLines: [
            labels.notes.detailDate({ date: note.note_date }),
            labels.notes.detailTime({ time: local.time }),
            labels.notes.detailTitle({ title })
          ]
        });
        const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
        if (!targetId) {
          await renderNoteDetails(ctx, note.id, { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
          return;
        }
        await sendNoteEverything(ctx, { noteId: note.id, noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
        return;
      }
      case 'notes.body_view': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        const note = await getNoteById({ userId: user.id, id: data.noteId });
        if (!note) {
          await renderNotesHistory(ctx);
          return;
        }
        const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
        if (!targetId) {
          await renderNoteDetails(ctx, note.id, { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
          return;
        }
        if (note.content_group_key) {
          await copyArchiveGroupToUser(ctx, { userChatId: targetId, groupKey: note.content_group_key });
        } else if (note.description ?? note.body) {
          const fullText = note.description ?? note.body ?? '';
          for (const chunk of splitTextForTelegram(fullText)) {
            await ctx.api.sendMessage(targetId, chunk);
          }
        }
        await renderNoteDetails(ctx, note.id, { noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
        return;
      }
      case 'notes.send_all': {
        const data = (payload as { data?: { noteId?: string; noteDate?: string; page?: number; historyPage?: number } }).data;
        if (!data?.noteId) {
          await renderNotesHistory(ctx);
          return;
        }
        await sendNoteEverything(ctx, { noteId: data.noteId, noteDate: data.noteDate, page: data.page, historyPage: data.historyPage });
        return;
      }
      case 'settings.language': {
        const { settings, locale } = await ensureUserAndSettings(ctx);
        await renderLanguageSelection(ctx, {
          origin: 'settings',
          currentLocale: readStoredLanguageCode(settings.settings_json as Record<string, unknown>) ?? locale
        });
        return;
      }
      case 'settings.emoji_toggle': {
        const { user } = await ensureUserAndSettings(ctx);
        const current = await getUserBooleanSetting(user.id, EMOJI_SETTING_KEY, config.ui.emojiEnabled);
        const next = !current;
        await setUserSetting(user.id, EMOJI_SETTING_KEY, next ? 'true' : 'false');
        await runWithEmojiSetting(next, async () => {
          await renderSettingsRoot(ctx);
        });
        return;
      }
      case 'language.set': {
        const data = (payload as { data?: { language?: Locale; origin?: LanguageScreenOrigin } }).data;
        const language = data?.language;
        const origin = data?.origin ?? 'onboarding';
        if (!language) {
          await renderDashboard(ctx);
          return;
        }
        await applyLanguageSelection(ctx, language, origin);
        return;
      }
      case 'routines.root':
        await renderRoutinesRoot(ctx);
        return;
      case 'routines.add': {
        const telegramId = String(ctx.from?.id ?? '');
        clearRoutineFlow(telegramId);
        setRoutineFlow(telegramId, { mode: 'create', step: 'title', draft: { xpMode: 'none', routineType: 'boolean' } });
        await promptRoutineTitle(ctx, {});
        return;
      }
      case 'routines.view': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        await renderRoutineDetails(ctx, routineId);
        return;
      }
      case 'routines.tasks': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        await renderRoutineTasks(ctx, routineId);
        return;
      }
      case 'routines.toggle': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const routine = await getRoutineById(routineId);
        if (routine && routine.user_id === user.id) {
          await updateRoutine(routineId, { isActive: !routine.is_active });
          clearReportContextCache();
        }
        await renderRoutineDetails(ctx, routineId);
        return;
      }
      case 'reminders.edit_open': {
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        await renderReminderDetails(ctx, reminderId);
        return;
      }
      case 'reminders.desc_view': {
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const reminder = await getReminderById(reminderId);
        if (!reminder || reminder.user_id !== user.id) {
          await renderReminders(ctx);
          return;
        }
        const targetId = ctx.chat?.id ?? ctx.from?.id ?? Number(user.telegram_id);
        if (!targetId) {
          await renderReminderDetails(ctx, reminderId);
          return;
        }
        if (reminder.description) {
          for (const chunk of splitTextForTelegram(reminder.description)) {
            await ctx.api.sendMessage(targetId, chunk);
          }
        }
        await renderReminderDetails(ctx, reminderId);
        return;
      }
      case 'reminders.edit_title': {
        if (!ctx.from) break;
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        setReminderFlow(stateKey, { mode: 'edit', reminderId, step: 'title', draft: {} });
        await renderReminderTitlePrompt(ctx, 'edit', reminderId);
        return;
      }
      case 'reminders.edit_detail': {
        if (!ctx.from) break;
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        setReminderFlow(stateKey, { mode: 'edit', reminderId, step: 'description', draft: {} });
        await renderReminderDescriptionPrompt(ctx, 'edit', reminderId);
        return;
      }
      case 'reminders.edit_date': {
        if (!ctx.from) break;
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        setReminderFlow(stateKey, { mode: 'edit', reminderId, step: 'schedule_type', draft: {} });
        await renderReminderScheduleTypePrompt(ctx, 'edit', reminderId);
        return;
      }
      case 'reminders.edit_time': {
        if (!ctx.from) break;
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        setReminderFlow(stateKey, { mode: 'edit', reminderId, step: 'schedule_type', draft: {} });
        await renderReminderScheduleTypePrompt(ctx, 'edit', reminderId);
        return;
      }
      case 'reminders.edit_schedule': {
        if (!ctx.from) break;
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        setReminderFlow(stateKey, { mode: 'edit', reminderId, step: 'schedule_type', draft: {} });
        await renderReminderScheduleTypePrompt(ctx, 'edit', reminderId);
        return;
      }
      case 'reminders.attach': {
        if (!ctx.from) break;
        const reminderId = (payload as { data?: { reminderId?: string } }).data?.reminderId;
        const stateKey = String(ctx.from.id);
        const existing = userStates.get(stateKey)?.reminderFlow;
        const nextDraft = existing?.draft ?? {};
        if (reminderId) {
          setReminderFlow(stateKey, { mode: 'edit', reminderId, step: 'attachments', draft: nextDraft });
          await renderReminderAttachmentPrompt(ctx, { mode: 'edit', reminderId });
        } else {
          const existingReminderId = existing ? getReminderIdFromFlow(existing) : undefined;
          setReminderFlow(stateKey, { mode: 'create', reminderId: existingReminderId, step: 'attachments', draft: nextDraft });
          await renderReminderAttachmentPrompt(ctx, { mode: 'create', reminderId: existingReminderId });
        }
        return;
      }
      case 'reminders.attach_done': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        await startReminderCaptionFlow(ctx, flow);
        return;
      }
      case 'reminders.attachments_save': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        await startReminderCaptionFlow(ctx, flow);
        return;
      }
      case 'reminders.attachments_continue': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        const reminderId = flow ? getReminderIdFromFlow(flow) : undefined;
        if (!flow || !reminderId) {
          await renderReminders(ctx);
          return;
        }
        const session = userStates.get(stateKey)?.reminderUploadSession;
        if (session?.timer) {
          clearTimeout(session.timer);
        }
        const timer = setTimeout(async () => {
          const latestState = userStates.get(stateKey);
          const currentSession = latestState?.reminderUploadSession;
          if (!currentSession || currentSession.reminderId !== reminderId) return;
          const idleFor = Date.now() - currentSession.lastReceivedAt;
          if (idleFor < REMINDER_UPLOAD_IDLE_MS) return;
          const saveBtn = await makeActionButton(ctx, { label: labels.notesButtons.saveNow(), action: 'reminders.attachments_save', data: { reminderId } });
          const continueBtn = await makeActionButton(ctx, { label: labels.notesButtons.continue(), action: 'reminders.attachments_continue', data: { reminderId } });
          const kb = new InlineKeyboard().text(saveBtn.text, saveBtn.callback_data).row().text(continueBtn.text, continueBtn.callback_data);
          setReminderUploadSession(stateKey, { ...currentSession, prompted: true });
          await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.attachmentsIdlePrompt()], inlineKeyboard: kb });
        }, REMINDER_UPLOAD_IDLE_MS);
        setReminderUploadSession(stateKey, {
          ...(session ?? { reminderId, pendingKinds: {}, lastReceivedAt: Date.now() }),
          reminderId,
          lastReceivedAt: Date.now(),
          prompted: false,
          timer
        });
        await renderReminderAttachmentPrompt(ctx, { mode: flow.mode, reminderId });
        return;
      }
      case 'reminders.caption_all': {
        if (!ctx.from) break;
        const data = (payload as { data?: { reminderId?: string } }).data;
        if (!data?.reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        setReminderFlow(stateKey, { ...flow, step: 'caption_all' });
        const skipBtn = await makeActionButton(ctx, { label: labels.notesButtons.skip(), action: 'reminders.caption_skip', data: { reminderId: data.reminderId } });
        const kb = new InlineKeyboard().text(skipBtn.text, skipBtn.callback_data);
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.notes.captionAllPrompt()], inlineKeyboard: kb });
        return;
      }
      case 'reminders.caption_by_category': {
        if (!ctx.from) break;
        const data = (payload as { data?: { reminderId?: string } }).data;
        if (!data?.reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const categories = buildReminderCaptionCategories(flow.draft.attachments ?? []);
        if (categories.length === 0) {
          await finalizeReminderArchive(ctx, data.reminderId, flow);
          return;
        }
        await promptReminderCaptionCategory(ctx, data.reminderId, categories[0], categories);
        return;
      }
      case 'reminders.caption_skip': {
        if (!ctx.from) break;
        const data = (payload as { data?: { reminderId?: string } }).data;
        if (!data?.reminderId) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        await finalizeReminderArchive(ctx, data.reminderId, flow);
        return;
      }
      case 'reminders.caption_category_skip': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        const reminderId = flow ? getReminderIdFromFlow(flow) : undefined;
        if (!flow || !reminderId || flow.step !== 'caption_category' || !flow.currentCategory) {
          await renderReminders(ctx);
          return;
        }
        const category = flow.currentCategory;
        const kinds =
          category === 'files'
            ? (['document', 'audio'] as ReminderAttachmentKind[])
            : ([category] as ReminderAttachmentKind[]);
        const updatedAttachments = applyCaptionToReminderAttachments(flow.draft.attachments ?? [], kinds, null);
        const remaining = (flow.captionCategories ?? []).filter((item) => item !== category);
        const nextFlow: ReminderFlow = { ...flow, draft: { ...flow.draft, attachments: updatedAttachments } };
        if (remaining.length === 0) {
          setReminderFlow(stateKey, nextFlow);
          await finalizeReminderArchive(ctx, reminderId, nextFlow);
          return;
        }
        setReminderFlow(stateKey, { ...nextFlow, captionCategories: remaining });
        await promptReminderCaptionCategory(ctx, reminderId, remaining[0], remaining);
        return;
      }
      case 'reminders.skip_title': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow || flow.mode !== 'create') {
          await renderReminders(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const reminder = await createReminderDraft({ userId: user.id, title: null, timezone: user.timezone ?? config.defaultTimezone });
        setReminderFlow(stateKey, { mode: 'create', reminderId: reminder.id, step: 'description', draft: { ...flow.draft, title: null } });
        await renderReminderDescriptionPrompt(ctx, 'create', reminder.id);
        return;
      }
      case 'reminders.skip_description': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const reminderId = getReminderIdFromFlow(flow);
        if (reminderId) {
          await updateReminder(reminderId, { description: null });
        }
        setReminderFlow(stateKey, { ...flow, draft: { ...flow.draft, description: null } });
        await renderReminderScheduleTypePrompt(ctx, flow.mode, reminderId);
        return;
      }
      case 'reminders.description_done': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const reminderId = getReminderIdFromFlow(flow);
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const description = flow.draft.description ?? null;
        await updateReminder(reminderId, { description });
        await maybeArchiveReminderDescription(ctx, reminderId, description);
        setReminderFlow(stateKey, { ...flow, step: 'schedule_type', draft: { ...flow.draft, description } });
        await renderReminderScheduleTypePrompt(ctx, flow.mode, reminderId);
        return;
      }
      case 'reminders.schedule_type': {
        if (!ctx.from) break;
        const data = (payload as { data?: { scheduleType?: ReminderScheduleType; mode?: ReminderFlow['mode']; reminderId?: string } }).data;
        const scheduleType = data?.scheduleType;
        if (!scheduleType) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const nextDraft: ReminderDraft = { ...flow.draft, scheduleType };
        if (scheduleType === 'once') {
          setReminderFlow(stateKey, { ...flow, step: 'date_select', draft: nextDraft });
          await renderReminderDateSelect(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow) });
          return;
        }
        if (scheduleType === 'hourly') {
          setReminderFlow(stateKey, { ...flow, step: 'interval_minutes', draft: nextDraft });
          await renderReminderIntervalPrompt(ctx);
          return;
        }
        if (scheduleType === 'daily') {
          setReminderFlow(stateKey, { ...flow, step: 'daily_time', draft: nextDraft });
          await renderReminderDailyTimePrompt(ctx);
          return;
        }
        if (scheduleType === 'weekly') {
          setReminderFlow(stateKey, { ...flow, step: 'weekly_day', draft: nextDraft });
          await renderReminderWeeklyDayPrompt(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow) });
          return;
        }
        if (scheduleType === 'monthly') {
          setReminderFlow(stateKey, { ...flow, step: 'monthly_day', draft: nextDraft });
          await renderReminderMonthlyDayPrompt(ctx);
          return;
        }
        if (scheduleType === 'yearly') {
          setReminderFlow(stateKey, { ...flow, step: 'yearly_month', draft: nextDraft });
          await renderReminderYearlyMonthPrompt(ctx);
          return;
        }
        return;
      }
      case 'reminders.schedule_back': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        await renderReminderScheduleTypePrompt(ctx, flow.mode, getReminderIdFromFlow(flow));
        return;
      }
      case 'reminders.weekly_day_set': {
        if (!ctx.from) break;
        const data = (payload as { data?: { day?: number } }).data;
        const day = data?.day;
        if (day === undefined) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        setReminderFlow(stateKey, { ...flow, step: 'weekly_time', draft: { ...flow.draft, byWeekday: day } });
        await renderReminderDailyTimePrompt(ctx);
        return;
      }
      case 'reminders.yearly_month_set': {
        if (!ctx.from) break;
        const data = (payload as { data?: { month?: number } }).data;
        const month = data?.month;
        if (!month) {
          await renderReminders(ctx);
          return;
        }
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        setReminderFlow(stateKey, { ...flow, step: 'yearly_day', draft: { ...flow.draft, byMonth: month } });
        await renderReminderMonthlyDayPrompt(ctx);
        return;
      }
      case 'reminders.date_select': {
        if (!ctx.from) break;
        const data = (payload as { data?: { choice?: string; mode?: ReminderFlow['mode']; reminderId?: string } }).data;
        const choice = (data?.choice ?? 'today') as ReminderDraft['dateSource'];
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const mode = flow.mode;
        const reminderId = getReminderIdFromFlow(flow);
        const { user, settings } = await ensureUserAndSettings(ctx);
        const timezone = user.timezone ?? config.defaultTimezone;

        if (choice === 'custom') {
          const local = formatLocalTime(timezone);
          const draft: ReminderDraft = {
            ...buildCustomDateDraft(local.date, 'gregorian'),
            dateSource: 'custom',
            scheduleType: flow.draft.scheduleType ?? 'once'
          };
          if (mode === 'edit') {
            const editReminderId = reminderId as string;
            setReminderFlow(stateKey, { mode: 'edit', reminderId: editReminderId, step: 'custom_date', draft });
            await renderReminderCustomDatePicker(ctx, { mode: 'edit', reminderId: editReminderId, draft });
          } else {
            setReminderFlow(stateKey, { mode: 'create', step: 'custom_date', draft });
            await renderReminderCustomDatePicker(ctx, { mode: 'create', draft });
          }
          return;
        }

        let localDate = formatLocalTime(timezone).date;
        if (choice === 'tomorrow') {
          localDate = addDaysToLocalDate(localDate, 1, timezone);
        } else if (choice === 'weekend') {
          const weekendDay = getWeekendDay(settings.settings_json as Record<string, unknown>);
          localDate = getNextWeekendLocalDate(timezone, weekendDay);
        }

        const baseDraft: ReminderDraft = { localDate, dateSource: choice, scheduleType: flow.draft.scheduleType ?? 'once' };
        const draft = ensureReminderTimeDraft(baseDraft, timezone);
        if (mode === 'edit') {
          const editReminderId = reminderId as string;
          setReminderFlow(stateKey, { mode: 'edit', reminderId: editReminderId, step: 'time', draft });
          await renderReminderTimePicker(ctx, { mode: 'edit', reminderId: editReminderId, draft });
        } else {
          setReminderFlow(stateKey, { mode: 'create', step: 'time', draft });
          await renderReminderTimePicker(ctx, { mode: 'create', draft });
        }
        return;
      }
      case 'reminders.weekend_day': {
        const data = (payload as { data?: { mode?: ReminderFlow['mode']; reminderId?: string } }).data;
        const mode = data?.mode ?? 'create';
        const reminderId = data?.reminderId;
        const kb = new InlineKeyboard();
        for (let idx = 0; idx < WEEKDAY_KEYS.length; idx += 1) {
          const btn = await makeActionButton(ctx, {
            label: getWeekdayLabel(idx),
            action: 'reminders.weekend_day_set',
            data: { day: idx, mode, reminderId }
          });
          kb.text(btn.text, btn.callback_data);
          if ((idx + 1) % 3 === 0) kb.row();
        }
        const backBtn =
          mode === 'edit'
            ? await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: 'reminders.edit_date', data: { reminderId } })
            : await makeActionButton(ctx, { label: labels.remindersButtons.back(), action: 'reminders.new' });
        kb.row().text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, {
          title: labels.reminders.newTitle(),
          bodyLines: [labels.reminders.weekendDayPrompt()],
          inlineKeyboard: kb
        });
        return;
      }
      case 'reminders.weekend_day_set': {
        const data = (payload as { data?: { day?: number; mode?: ReminderFlow['mode']; reminderId?: string } }).data;
        const day = data?.day;
        if (typeof day === 'number') {
          const { user } = await ensureUserAndSettings(ctx);
          await updateUserSettingsJson(user.id, { weekend_day: day });
        }
        await renderReminderDateSelect(ctx, { mode: data?.mode ?? 'create', reminderId: data?.reminderId });
        return;
      }
      case 'reminders.date_adjust': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const data = (payload as { data?: { field?: 'year' | 'month' | 'day'; delta?: number } }).data;
        if (!data?.field || typeof data.delta !== 'number') {
          await renderReminders(ctx);
          return;
        }
        const draft = { ...(flow.draft ?? {}) };
        if (data.field === 'year') draft.year = (draft.year ?? 0) + data.delta;
        if (data.field === 'month') {
          const month = (draft.month ?? 1) + data.delta;
          draft.month = month < 1 ? 12 : month > 12 ? 1 : month;
        }
        if (data.field === 'day') draft.day = Math.max(1, (draft.day ?? 1) + data.delta);
        const nextDraft = clampCustomDateDraft(draft);
        setReminderFlow(stateKey, { ...flow, draft: nextDraft, step: 'custom_date' });
        await renderReminderCustomDatePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
        return;
      }
      case 'reminders.date_toggle': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const draft = flow.draft;
        const dateMode = draft.dateMode ?? 'gregorian';
        let nextDraft: ReminderDraft = draft;
        if (draft.year && draft.month && draft.day) {
          if (dateMode === 'gregorian') {
            const jalali = gregorianToJalali(draft.year, draft.month, draft.day);
            nextDraft = { ...draft, dateMode: 'jalali', year: jalali.year, month: jalali.month, day: jalali.day };
          } else {
            const greg = jalaliToGregorian(draft.year, draft.month, draft.day);
            nextDraft = { ...draft, dateMode: 'gregorian', year: greg.year, month: greg.month, day: greg.day };
          }
        }
        setReminderFlow(stateKey, { ...flow, draft: nextDraft, step: 'custom_date' });
        await renderReminderCustomDatePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
        return;
      }
      case 'reminders.date_manual': {
        await renderScreen(ctx, {
          title: labels.reminders.newTitle(),
          bodyLines: [labels.reminders.customDateManualPrompt()]
        });
        return;
      }
      case 'reminders.date_confirm': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const draft = flow.draft;
        if (!draft.year || !draft.month || !draft.day || !draft.dateMode) {
          await renderReminders(ctx);
          return;
        }
        let localDate = '';
        if (draft.dateMode === 'gregorian') {
          localDate = `${draft.year}-${String(draft.month).padStart(2, '0')}-${String(draft.day).padStart(2, '0')}`;
        } else {
          const greg = jalaliToGregorian(draft.year, draft.month, draft.day);
          localDate = `${greg.year}-${String(greg.month).padStart(2, '0')}-${String(greg.day).padStart(2, '0')}`;
        }
        const nextDraft = ensureReminderTimeDraft({ ...draft, localDate, dateSource: 'custom' }, (await ensureUserAndSettings(ctx)).user.timezone ?? config.defaultTimezone);
        setReminderFlow(stateKey, { ...flow, step: 'time', draft: nextDraft });
        await renderReminderTimePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
        return;
      }
      case 'reminders.time_adjust': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const delta = (payload as { data?: { delta?: number } }).data?.delta;
        if (typeof delta !== 'number') {
          await renderReminders(ctx);
          return;
        }
        const draft = ensureReminderTimeDraft(flow.draft, (await ensureUserAndSettings(ctx)).user.timezone ?? config.defaultTimezone);
        const nextMinutes = normalizeTimeMinutes((draft.timeMinutes ?? 0) + delta);
        const nextDraft = { ...draft, timeMinutes: nextMinutes, localTime: minutesToHhmm(nextMinutes) };
        setReminderFlow(stateKey, { ...flow, draft: nextDraft, step: 'time' });
        await renderReminderTimePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
        return;
      }
      case 'reminders.time_preset': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const preset = (payload as { data?: { preset?: string } }).data?.preset;
        const parsed = preset ? parseTimeHhmm(preset) : null;
        if (!parsed) {
          await renderReminders(ctx);
          return;
        }
        const nextDraft = { ...flow.draft, timeMinutes: parsed.minutes, localTime: parsed.hhmm };
        setReminderFlow(stateKey, { ...flow, draft: nextDraft, step: 'time' });
        await renderReminderTimePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
        return;
      }
      case 'reminders.time_manual': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        setReminderFlow(stateKey, { ...flow, step: 'time_manual' });
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.timeManualPrompt()] });
        return;
      }
      case 'reminders.time_confirm': {
        if (!ctx.from) break;
        const stateKey = String(ctx.from.id);
        const flow = userStates.get(stateKey)?.reminderFlow;
        if (!flow) {
          await renderReminders(ctx);
          return;
        }
        const draft = flow.draft;
        if (!draft.localDate || !draft.localTime) {
          await renderReminders(ctx);
          return;
        }
        await persistReminderSchedule(ctx, flow);
        return;
      }
      case 'reminders.toggle': {
        const data = (payload as { data?: { reminderId?: string } }).data;
        const reminderId = data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const reminder = await getReminderById(reminderId);
        if (!reminder || reminder.user_id !== user.id) {
          await renderReminders(ctx);
          return;
        }
        await toggleReminderEnabled(reminderId);
        await renderReminders(ctx);
        return;
      }
      case 'reminders.delete': {
        const data = (payload as { data?: { reminderId?: string } }).data;
        const reminderId = data?.reminderId;
        if (!reminderId) {
          await renderReminders(ctx);
          return;
        }

        const confirmBtn = await makeActionButton(ctx, {
          label: t('buttons.confirm_delete') ?? 'Confirm delete',
          action: 'reminders.delete_confirm',
          data: { reminderId }
        });
        const cancelBtn = await makeActionButton(ctx, {
          label: t('buttons.cancel') ?? 'Cancel',
          action: 'nav.reminders'
        });

        const kb = new InlineKeyboard().text(confirmBtn.text, confirmBtn.callback_data).text(cancelBtn.text, cancelBtn.callback_data);

        await renderScreen(ctx, {
          title: labels.reminders.title(),
          bodyLines: [labels.reminders.deleteConfirm()],
          inlineKeyboard: kb
        });
        return;
      }
      case 'reminders.delete_confirm': {
        const data = (payload as { data?: { reminderId?: string } }).data;
        const reminderId = data?.reminderId;
        if (reminderId) {
          const { user } = await ensureUserAndSettings(ctx);
          const reminder = await getReminderById(reminderId);
          if (reminder && reminder.user_id === user.id) {
            await markReminderArchiveDeleted(ctx, reminderId);
            await deleteReminder(reminderId);
          }
        }
        await renderReminders(ctx);
        return;
      }
      case 'routines.edit_title': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        setRoutineFlow(telegramId, { mode: 'edit', routineId, step: 'title', draft: {} });
        await promptRoutineTitle(ctx, { routineId });
        return;
      }
      case 'routines.edit_description': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        setRoutineFlow(telegramId, { mode: 'edit', routineId, step: 'description', draft: {} });
        await promptRoutineDescription(ctx, { routineId });
        return;
      }
      case 'routines.task_add': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearRoutineTaskFlow(telegramId);
        setRoutineTaskFlow(telegramId, { mode: 'create', routineId, step: 'title', draft: {} });
        await promptRoutineTaskTitle(ctx, { routineId });
        return;
      }
      case 'routines.task_edit': {
        const data = (payload as { data?: { routineId?: string; taskId?: string } }).data;
        if (!data?.routineId || !data.taskId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const routine = await getRoutineById(data.routineId);
        const task = await getRoutineTaskById(data.taskId);
        if (!routine || routine.user_id !== user.id || !task || task.routine_id !== routine.id) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearRoutineTaskFlow(telegramId);
        setRoutineTaskFlow(telegramId, {
          mode: 'edit',
          routineId: data.routineId,
          taskId: data.taskId,
          step: 'title',
          draft: {
            title: task.title,
            description: task.description,
            itemType: task.item_type,
            xpMode: task.xp_mode,
            xpValue: task.xp_value,
            xpMaxPerDay: task.xp_max_per_day ?? null,
            optionsJson: task.options_json ?? {}
          }
        });
        await promptRoutineTaskTitle(ctx, { routineId: data.routineId, taskId: data.taskId });
        return;
      }
      case 'routines.task_delete_confirm': {
        const data = (payload as { data?: { routineId?: string; taskId?: string } }).data;
        if (!data?.routineId || !data.taskId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        await renderRoutineTaskDeleteConfirm(ctx, { routineId: data.routineId, taskId: data.taskId });
        return;
      }
      case 'routines.task_delete': {
        const data = (payload as { data?: { routineId?: string; taskId?: string } }).data;
        if (!data?.routineId || !data.taskId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const routine = await getRoutineById(data.routineId);
        const task = await getRoutineTaskById(data.taskId);
        if (!routine || routine.user_id !== user.id || !task || task.routine_id !== routine.id) {
          await renderRoutinesRoot(ctx);
          return;
        }
        await deleteRoutineTask(data.taskId);
        clearRoutineTaskFlow(String(ctx.from?.id ?? ''));
        clearReportContextCache();
        await renderRoutineTasks(ctx, data.routineId, t('screens.routines.saved'));
        return;
      }
      case 'routines.task_select_type': {
        const data = (payload as { data?: { routineId?: string; taskId?: string; itemType?: RoutineTaskRow['item_type'] } }).data;
        if (!data?.routineId || !data.itemType) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.routineTaskFlow;
        if (flow && flow.routineId === data.routineId) {
          const draft = { ...flow.draft, itemType: data.itemType };
          setRoutineTaskFlow(telegramId, { ...flow, draft, step: 'xp_mode' });
          await promptRoutineTaskXpMode(ctx, { routineId: data.routineId, taskId: data.taskId ?? flow.taskId, itemType: data.itemType });
          return;
        }
        await renderRoutineTasks(ctx, data.routineId);
        return;
      }
      case 'routines.task_select_xp_mode': {
        const data = (payload as { data?: { routineId?: string; taskId?: string; xpMode?: RoutineTaskRow['xp_mode'] } }).data;
        if (!data?.routineId || !data.xpMode) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.routineTaskFlow;
        if (flow && flow.routineId === data.routineId) {
          const normalizedMode: RoutineTaskRow['xp_mode'] = (normalizeXpModeForItemType(flow.draft.itemType, data.xpMode) as RoutineTaskRow['xp_mode']) ?? 'none';
          if (normalizedMode === 'none') {
            const draft = { ...flow.draft, xpMode: normalizedMode, xpValue: null, xpMaxPerDay: null, optionsJson: {} };
            setRoutineTaskFlow(telegramId, { ...flow, draft, step: 'xp_value' });
            // save immediately
            const saved = flow.taskId
              ? await updateRoutineTask(flow.taskId, {
                  title: draft.title ?? '',
                  description: draft.description ?? null,
                  itemType: draft.itemType ?? 'boolean',
                  xpMode: normalizedMode,
                  xpValue: null,
                  xpMaxPerDay: null,
                  optionsJson: {}
                })
              : await createRoutineTask({
                  routineId: flow.routineId,
                  title: draft.title ?? t('screens.routine_tasks.default_title'),
                  description: draft.description ?? null,
                  itemType: draft.itemType ?? 'boolean',
                  xpMode: normalizedMode,
                  xpValue: null,
                  xpMaxPerDay: null,
                  optionsJson: {}
                });
            clearRoutineTaskFlow(telegramId);
            clearReportContextCache();
            await renderRoutineTasks(ctx, data.routineId, t('screens.routines.saved'));
            return;
          }
          const draft = { ...flow.draft, xpMode: normalizedMode, optionsJson: {} };
          setRoutineTaskFlow(telegramId, { ...flow, draft, step: 'xp_value' });
          await promptRoutineTaskXpValue(ctx, { routineId: data.routineId, taskId: flow.taskId, xpMode: normalizedMode, itemType: draft.itemType });
          return;
        }
        await renderRoutineTasks(ctx, data.routineId);
        return;
      }
      case 'routines.edit_type': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const kb = await buildRoutineTypeKeyboard(ctx, routineId);
        const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.view', data: { routineId } });
        kb.text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.choose_type')], inlineKeyboard: kb });
        return;
      }
      case 'routines.select_type': {
        const data = (payload as { data?: { routineId?: string; routineType?: 'boolean' | 'duration_minutes' } }).data;
        if (!data?.routineType) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.routineFlow;
        if (flow && flow.mode === 'create') {
          setRoutineFlow(telegramId, { ...flow, draft: { ...flow.draft, routineType: data.routineType }, step: 'xp_mode' });
          const kb = await buildRoutineXpModeKeyboard(ctx);
          const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.root' });
          kb.text(backBtn.text, backBtn.callback_data);
          await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.choose_xp_mode')], inlineKeyboard: kb });
          return;
        }
        if (data.routineId) {
          const { user } = await ensureUserAndSettings(ctx);
          const routine = await getRoutineById(data.routineId);
          if (routine && routine.user_id === user.id) {
            await updateRoutine(data.routineId, { routineType: data.routineType });
            clearReportContextCache();
          }
          await renderRoutineDetails(ctx, data.routineId);
          return;
        }
        await renderRoutinesRoot(ctx);
        return;
      }
      case 'routines.edit_xp_mode': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const kb = await buildRoutineXpModeKeyboard(ctx, routineId);
        const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.view', data: { routineId } });
        kb.text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.choose_xp_mode')], inlineKeyboard: kb });
        return;
      }
      case 'routines.select_xp_mode': {
        const data = (payload as { data?: { routineId?: string; xpMode?: 'fixed' | 'per_minute' | 'none' } }).data;
        if (!data?.xpMode) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.routineFlow;
        if (flow && flow.mode === 'create') {
          const draft = { ...flow.draft, xpMode: data.xpMode, xpValue: data.xpMode === 'none' ? null : flow.draft.xpValue };
          const nextFlow = { ...flow, draft };
          setRoutineFlow(telegramId, nextFlow);
          if (data.xpMode === 'none') {
            setRoutineFlow(telegramId, { ...nextFlow, step: 'confirm' });
            await promptRoutineConfirm(ctx, { ...nextFlow, step: 'confirm' });
            return;
          }
          setRoutineFlow(telegramId, { ...nextFlow, step: 'xp_value' });
          await promptRoutineXpValue(ctx, { xpMode: data.xpMode });
          return;
        }
        if (data.routineId) {
          const { user } = await ensureUserAndSettings(ctx);
          const routine = await getRoutineById(data.routineId);
          if (!routine || routine.user_id !== user.id) {
            await renderRoutinesRoot(ctx);
            return;
          }
          if (data.xpMode === 'none') {
            await updateRoutine(data.routineId, { xpMode: 'none', xpValue: null, xpMaxPerDay: null });
            clearReportContextCache();
            await renderRoutineDetails(ctx, data.routineId, t('screens.routines.saved'));
            return;
          }
          setRoutineFlow(telegramId, {
            mode: 'edit',
            routineId: data.routineId,
            step: 'xp_value',
            draft: { xpMode: data.xpMode }
          });
          await promptRoutineXpValue(ctx, { routineId: data.routineId, xpMode: data.xpMode });
          return;
        }
        await renderRoutinesRoot(ctx);
        return;
      }
      case 'routines.delete_confirm': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        await renderRoutineDeleteConfirm(ctx, routineId);
        return;
      }
      case 'routines.delete': {
        const routineId = (payload as { data?: { routineId?: string } }).data?.routineId;
        if (!routineId) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        const routine = await getRoutineById(routineId);
        if (!routine || routine.user_id !== user.id) {
          await renderRoutinesRoot(ctx);
          return;
        }
        await deleteRoutine(routineId);
        clearReportContextCache();
        await renderRoutinesRoot(ctx, t('screens.routines.deleted'));
        return;
      }
      case 'routines.save': {
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.routineFlow;
        if (!flow) {
          await renderRoutinesRoot(ctx);
          return;
        }
        const { user } = await ensureUserAndSettings(ctx);
        if (flow.mode === 'create') {
          await createRoutine({
            userId: user.id,
            title: flow.draft.title ?? t('screens.routines.default_title'),
            description: flow.draft.description ?? null,
            routineType: flow.draft.routineType ?? 'boolean',
            xpMode: flow.draft.xpMode ?? 'none',
            xpValue: flow.draft.xpValue ?? null,
            xpMaxPerDay: flow.draft.xpMaxPerDay ?? null,
            sortOrder: Date.now() % 100000
          });
          clearReportContextCache();
          clearRoutineFlow(telegramId);
          await renderRoutinesRoot(ctx, t('screens.routines.saved'));
          return;
        }
        if (flow.mode === 'edit' && flow.routineId) {
          await updateRoutine(flow.routineId, {
            title: flow.draft.title,
            description: flow.draft.description,
            xpMode: flow.draft.xpMode,
            xpValue: flow.draft.xpValue,
            xpMaxPerDay: flow.draft.xpMaxPerDay
          });
          clearReportContextCache();
          clearRoutineFlow(telegramId);
          await renderRoutineDetails(ctx, flow.routineId, t('screens.routines.saved'));
          return;
        }
        clearRoutineFlow(telegramId);
        await renderRoutinesRoot(ctx);
        return;
      }

      /* --- Reports --- */
      case 'reports.xp':
        await renderXpSummary(ctx);
        return;
      case 'reports.sleep':
      case 'reports.study':
      case 'reports.tasks':
      case 'reports.chart': {
        const kind = action.split('.')[1];
        await renderScreen(ctx, {
          titleKey: t('screens.reports.title'),
          bodyLines: [t('screens.coming_soon_generic', { feature: kind })],
          inlineKeyboard: await buildReportsMenuKeyboard(ctx)
        });
        return;
      }

      /* --- Rewards (Buy) --- */
      case 'rewards.buy':
        await renderRewardBuyList(ctx);
        return;

      case 'rewards.confirm': {
        const rewardId = (payload as { data?: { rewardId?: string } }).data?.rewardId;
        if (!rewardId) {
          await renderRewardBuyList(ctx);
          return;
        }

        const reward = await getRewardById(rewardId);
        if (!reward) {
          const kb = await buildRewardCenterKeyboard(ctx);
          await renderScreen(ctx, { titleKey: t('screens.rewards.title'), bodyLines: [t('errors.reward_not_found')], inlineKeyboard: kb });
          return;
        }

        await purchaseReward({ userId: user.id, reward });
        const balance = await getXpBalance(user.id);

        await logForUser({ userId: user.id, ctx, eventName: 'db_write', payload: { action: 'purchase_reward', reward_id: reward.id, cost: reward.xp_cost }, enabled });

        const kb = await buildRewardCenterKeyboard(ctx);
        await renderScreen(ctx, { titleKey: t('screens.rewards.title'), bodyLines: [t('screens.rewards.purchased', { title: reward.title, xp: reward.xp_cost }), t('screens.rewards.new_balance', { xp: balance })], inlineKeyboard: kb });
        return;
      }

      /* --- Rewards (Edit Store) --- */
      case 'rewards.edit_root':
        await renderRewardStoreEditorRoot(ctx);
        return;

      case 'rewards.add': {
        const telegramId = String(ctx.from?.id ?? '');
        userStates.set(telegramId, { ...(userStates.get(telegramId) || {}), rewardEdit: { mode: 'create', step: 'title', draft: {} } });

        const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'rewards.edit_root' });
        await renderScreen(ctx, { titleKey: t('screens.rewards.add_title'), bodyLines: [t('screens.rewards.ask_title')], inlineKeyboard: new InlineKeyboard().text(cancelBtn.text, cancelBtn.callback_data) });
        return;
      }

      case 'rewards.edit_open': {
        const rewardId = (payload as { data?: { rewardId?: string } }).data?.rewardId;
        if (!rewardId) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }
        const reward = await getRewardById(rewardId);
        if (!reward) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }
        await renderRewardEditMenu(ctx, reward);
        return;
      }

      case 'rewards.edit_title':
      case 'rewards.edit_description':
      case 'rewards.edit_xp': {
        const rewardId = (payload as { data?: { rewardId?: string } }).data?.rewardId;
        if (!rewardId) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }

        const step = action === 'rewards.edit_title' ? 'title' : action === 'rewards.edit_description' ? 'description' : 'xp';
        const telegramId = String(ctx.from?.id ?? '');
        userStates.set(telegramId, { ...(userStates.get(telegramId) || {}), rewardEdit: { mode: 'edit', rewardId, step, draft: {} } });

        const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'rewards.edit_open', data: { rewardId } });

        const prompt =
          step === 'title'
            ? t('screens.rewards.ask_new_title')
            : step === 'description'
              ? t('screens.rewards.ask_new_description')
              : t('screens.rewards.ask_new_xp');

        await renderScreen(ctx, { titleKey: t('screens.rewards.edit_title'), bodyLines: [prompt], inlineKeyboard: new InlineKeyboard().text(cancelBtn.text, cancelBtn.callback_data) });
        return;
      }

      case 'rewards.toggle_active': {
        const rewardId = (payload as { data?: { rewardId?: string } }).data?.rewardId;
        if (!rewardId) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }
        const reward = await getRewardById(rewardId);
        if (!reward) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }
        const updated = await updateReward({ rewardId, patch: { isActive: !reward.is_active } });
        await renderRewardEditMenu(ctx, updated);
        return;
      }

      case 'rewards.delete': {
        const rewardId = (payload as { data?: { rewardId?: string } }).data?.rewardId;
        if (!rewardId) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }
        const reward = await getRewardById(rewardId);
        if (!reward) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }

        const telegramId = String(ctx.from?.id ?? '');
        userStates.set(telegramId, { ...(userStates.get(telegramId) || {}), rewardEdit: { mode: 'edit', rewardId, step: 'confirm_delete', draft: {} } });

        const confirmBtn = await makeActionButton(ctx, { label: t('buttons.yes_delete'), action: 'rewards.delete_confirm', data: { rewardId } });
        const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'rewards.edit_open', data: { rewardId } });

        const kb = new InlineKeyboard().text(confirmBtn.text, confirmBtn.callback_data).row().text(cancelBtn.text, cancelBtn.callback_data);

        await renderScreen(ctx, { titleKey: t('screens.rewards.delete_title'), bodyLines: [t('screens.rewards.confirm_delete', { title: reward.title })], inlineKeyboard: kb });
        return;
      }

      case 'rewards.delete_confirm': {
        const rewardId = (payload as { data?: { rewardId?: string } }).data?.rewardId;
        if (!rewardId) {
          await renderRewardStoreEditorRoot(ctx);
          return;
        }
        await deleteReward(rewardId);

        const telegramId = String(ctx.from?.id ?? '');
        const st = { ...(userStates.get(telegramId) || {}) };
        delete st.rewardEdit;
        userStates.set(telegramId, st);

        await renderRewardStoreEditorRoot(ctx);
        return;
      }

      /* --- Settings --- */
      case 'settings.speed_test': {
        const startHandler = Date.now();
        const { user: u } = await ensureUserAndSettings(ctx);

        const supabaseStart = Date.now();
        await getOrCreateUserSettings(u.id);
        const supabaseMs = Date.now() - supabaseStart;

        const chatId = ctx.chat?.id;
        let telegramMs = 0;
        if (chatId) {
          const telegramStart = Date.now();
          await ctx.api.sendChatAction(chatId, 'typing');
          telegramMs = Date.now() - telegramStart;
        }

        const handlerMs = Date.now() - startHandler;

        const lines = [
          t('screens.settings.speed_title'),
          '',
          t('screens.settings.speed_supabase', { ms: supabaseMs }),
          t('screens.settings.speed_telegram', { ms: telegramMs }),
          t('screens.settings.speed_handler', { ms: handlerMs }),
          '',
          t('screens.settings.speed_note')
        ];

        const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.settings' });
        await renderScreen(ctx, { titleKey: t('screens.settings.speed_title'), bodyLines: lines, inlineKeyboard: new InlineKeyboard().text(backBtn.text, backBtn.callback_data) });
        return;
      }

      /* --- Daily Report --- */
      case 'dr.menu': {
        const reportDayId = (payload as { data?: { reportDayId?: string } }).data?.reportDayId;
        if (reportDayId) {
          // find cached date
          const cachedById = [...reportContextCache.entries()].find(([, v]) => v.reportDay.id === reportDayId);
          if (cachedById) {
            const date = cachedById[1].reportDay.local_date;
            await renderDailyReportRoot(ctx, date);
            return;
          }
        }
        await renderDailyReportRoot(ctx);
        return;
      }

      case 'dr.open_date': {
        const localDate = (payload as { data?: { localDate?: string } }).data?.localDate;
        if (!localDate) {
          await renderDailyReportRoot(ctx);
          return;
        }
        await renderDailyReportRoot(ctx, localDate);
        return;
      }

      case 'dr.status': {
        const data = (payload as { data?: { reportDayId?: string; filter?: 'all' | 'not_filled' | 'filled' } }).data;
        const reportDayId = data?.reportDayId;
        const filter = data?.filter ?? 'all';
        if (!reportDayId) {
          const { reportDay } = await ensureReportContext(ctx);
          await renderDailyStatusWithFilter(ctx, reportDay.id, filter);
          return;
        }
        await renderDailyStatusWithFilter(ctx, reportDayId, filter);
        return;
      }

      case 'dr.next': {
        const reportDayId = (payload as { data?: { reportDayId?: string } }).data?.reportDayId;

        const cached = reportDayId ? [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId) : null;
        let context = cached ?? null;
        if (!context) {
          const reportDayRow = reportDayId ? await getReportDayById(reportDayId) : null;
          if (reportDayRow) {
            context = await ensureSpecificReportContext(ctx, reportDayRow.local_date);
          } else {
            context = await ensureReportContext(ctx);
          }
        }
        const reportDay = context.reportDay;
        const items = context.items;

        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }

        const statuses = await listCompletionStatus(reportDay.id, filterRoutineDisplayItems(items));
        const next = statuses.find((s) => !s.filled && !s.skipped);
        if (!next) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: [t('screens.daily_report.all_done')], inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }
        await promptForItem(ctx, reportDay, next.item, { origin: 'next' });
        return;
      }

      case 'dr.item': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string; filter?: 'all' | 'not_filled' | 'filled'; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        if (!reportDayId || !itemId) {
          await ctx.answerCallbackQuery({ text: t('errors.item_not_found'), show_alert: true });
          return;
        }

        const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);
        let context = cached ?? null;
        if (!context) {
          const reportDayRow = await getReportDayById(reportDayId);
          if (reportDayRow) {
            context = await ensureSpecificReportContext(ctx, reportDayRow.local_date);
          } else {
            context = await ensureReportContext(ctx);
          }
        }
        const reportDay = context.reportDay;
        const items = context.items;

        const item = items.find((i) => i.id === itemId);
        if (!item) {
          await ctx.answerCallbackQuery({ text: t('errors.item_not_found'), show_alert: true });
          return;
        }
        const origin = data?.origin as 'next' | 'status' | undefined;
        const statusFilter = (data?.statusFilter as 'all' | 'not_filled' | 'filled' | undefined) ?? data?.filter;
        const opts: { origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } | undefined = origin
          ? { origin, statusFilter }
          : data?.filter
            ? { origin: 'status', statusFilter: data.filter }
            : undefined;
        await promptForItem(ctx, reportDay, item, opts);
        return;
      }

      case 'dr.time_set_hour':
      case 'dr.time_set_mtens':
      case 'dr.time_set_mones':
      case 'dr.time_set_ampm': {
        const data = (payload as { data?: any }).data as {
          reportDayId?: string;
          itemId?: string;
          hour12?: number;
          minuteTens?: number;
          minuteOnes?: number;
          ampm?: 'AM' | 'PM';
        };
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        if (!reportDayId || !itemId) return;

        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId);
        const draft = state?.timeDraft;

        if (!draft || draft.reportDayId !== reportDayId || draft.itemId !== itemId) {
          await renderDailyReportRoot(ctx);
          return;
        }

        const nextDraft = {
          ...draft,
          hour12: data.hour12 ?? draft.hour12,
          minuteTens: data.minuteTens ?? draft.minuteTens,
          minuteOnes: data.minuteOnes ?? draft.minuteOnes,
          ampm: (data.ampm ?? draft.ampm) as 'AM' | 'PM'
        };

        userStates.set(telegramId, { ...(state || {}), timeDraft: nextDraft });

        const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);
        let context = cached ?? null;
        if (!context) {
          const reportDayRow = await getReportDayById(reportDayId);
          if (reportDayRow) {
            context = await ensureSpecificReportContext(ctx, reportDayRow.local_date);
          } else {
            context = await ensureReportContext(ctx);
          }
        }
        const reportDay = context.reportDay;
        const items = context.items;
        const item = items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }
        const pickerOpts =
          nextDraft.mode === 'start_end'
            ? nextDraft.phase === 'start'
              ? {
                  title: t('screens.daily_report.duration_start_title'),
                  currentLabel: t('screens.daily_report.duration_start_current', { value: timeDraftToDisplay(nextDraft).label }),
                  hint: t('screens.daily_report.duration_start_hint', { label: item.label })
                }
              : {
                  title: t('screens.daily_report.duration_end_title'),
                  currentLabel: t('screens.daily_report.duration_end_current', { value: timeDraftToDisplay(nextDraft).label }),
                  hint: t('screens.daily_report.duration_end_hint', { start: nextDraft.startValue?.hhmm ?? '' })
                }
            : undefined;
        await renderTimePicker(ctx, reportDayId, item, nextDraft, pickerOpts);
        return;
      }

      case 'dr.time_save': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        if (!reportDayId || !itemId) return;

        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId);
        const draft = state?.timeDraft;

        if (!draft || draft.reportDayId !== reportDayId || draft.itemId !== itemId) {
          await renderDailyReportRoot(ctx);
          return;
        }

        const context = await ensureContextByReportDayId(ctx, reportDayId);
        const reportDay = context.reportDay;
        const item = context.items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }

        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items }) });
          return;
        }

        const { hhmm24 } = timeDraftToDisplay(draft);

        if (draft.mode === 'start_end') {
          const endMinutes = minutesFromHhmm(hhmm24) ?? 0;
          if (draft.phase === 'start' || !draft.startValue) {
            const nextDraft: TimeDraftState = { ...draft, phase: 'end', startValue: { hhmm: hhmm24, minutesTotal: endMinutes } };
            const pickerOpts = {
              title: t('screens.daily_report.duration_end_title'),
              currentLabel: t('screens.daily_report.duration_end_current', { value: timeDraftToDisplay(nextDraft).label }),
              hint: t('screens.daily_report.duration_end_hint', { start: nextDraft.startValue?.hhmm ?? hhmm24 })
            };
            userStates.set(telegramId, { ...(state || {}), timeDraft: nextDraft });
            await renderTimePicker(ctx, reportDayId, item, nextDraft, pickerOpts);
            return;
          }
          const duration = endMinutes - draft.startValue.minutesTotal;
          if (duration <= 0) {
            await renderScreen(ctx, {
              titleKey: t('screens.daily_report.title'),
              bodyLines: [t('screens.daily_report.duration_end_error')],
              inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items })
            });
            return;
          }
          await saveValue({ reportDayId, item, valueJson: { start: draft.startValue.hhmm, end: hhmm24, minutes: duration }, userId: reportDay.user_id });
          const userSettings = (await ensureUserAndSettings(ctx)).user.settings_json as Record<string, unknown>;
          await logForUser({
            userId: reportDay.user_id,
            ctx,
            eventName: 'db_write',
            payload: { action: 'save_value', item_id: item.id },
            enabled: telemetryEnabledForUser(userSettings)
          });
          const updated = { ...(userStates.get(telegramId) || {}) };
          delete updated.awaitingValue;
          delete updated.timeDraft;
          delete updated.numericDraft;
          userStates.set(telegramId, updated);
          await continueFlowAfterAction(ctx, reportDay, state?.awaitingValue?.origin, state?.awaitingValue?.statusFilter);
          return;
        }

        await handleSaveValue(ctx, hhmm24);

        const updated = { ...(userStates.get(telegramId) || {}) };
        delete updated.timeDraft;
        userStates.set(telegramId, updated);
        return;
      }

      case 'dr.num_delta': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string; delta?: number } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        const delta = data?.delta ?? 0;
        if (!reportDayId || !itemId) return;

        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId);
        if (!state?.numericDraft || state.numericDraft.reportDayId !== reportDayId || state.numericDraft.itemId !== itemId) {
          await renderDailyReportRoot(ctx);
          return;
        }

        const context = await ensureContextByReportDayId(ctx, reportDayId);
        const reportDay = context.reportDay;
        const item = context.items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }

        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items }) });
          return;
        }

        const current = state.numericDraft.value ?? 0;
        const next = Math.max(0, current + delta);

        userStates.set(telegramId, { ...state, numericDraft: { reportDayId, itemId, value: next, unit: state.numericDraft.unit } });

        await renderNumericInput(ctx, reportDayId, item, { reportDayId, itemId, value: next, unit: state.numericDraft.unit });
        return;
      }

      case 'dr.num_unit': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string; unit?: 'minutes' | 'seconds' } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        const unit = data?.unit;
        if (!reportDayId || !itemId || !unit) return;

        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId);
        const draft = state?.numericDraft;
        if (!draft || draft.reportDayId !== reportDayId || draft.itemId !== itemId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const context = await ensureContextByReportDayId(ctx, reportDayId);
        const reportDay = context.reportDay;
        const item = context.items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (item.item_type !== 'duration_minutes') {
          await renderNumericInput(ctx, reportDayId, item, draft);
          return;
        }
        const minutesValue = convertToMinutes(draft.value, draft.unit ?? 'minutes').minutes;
        const nextValue = unit === 'seconds' ? Math.max(0, Math.round(minutesValue * 60)) : minutesValue;
        const nextDraft: NumericDraftState = { ...draft, unit, value: nextValue };
        userStates.set(telegramId, { ...state, numericDraft: nextDraft });
        await renderNumericInput(ctx, reportDayId, item, nextDraft);
        return;
      }

      case 'dr.num_save': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        if (!reportDayId || !itemId) return;

        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId);
        const draft = state?.numericDraft;

        if (!draft || draft.reportDayId !== reportDayId || draft.itemId !== itemId) {
          await renderDailyReportRoot(ctx);
          return;
        }

        const context = await ensureContextByReportDayId(ctx, reportDayId);
        const reportDay = context.reportDay;
        const item = context.items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }

        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items }) });
          return;
        }

        const awaiting = state?.awaitingValue;
        let valueJson: Record<string, unknown> | null = null;
        if (item.item_type === 'duration_minutes') {
          const { minutes, seconds } = convertToMinutes(draft.value, draft.unit ?? 'minutes');
          valueJson = { value: minutes, minutes, ...(seconds != null ? { seconds } : {}) };
        } else if (item.item_type === 'number') {
          const n = Math.max(0, draft.value);
          const isPerMinute = ['per_minute', 'time'].includes(item.xp_mode ?? '');
          const isPerNumber = (item.xp_mode ?? '') === 'per_number';
          valueJson = { value: n, number: n, ...(isPerMinute ? { minutes: n } : {}), ...(isPerNumber ? { units: n } : {}) };
        } else {
          valueJson = { value: draft.value };
        }

        try {
          await saveValue({ reportDayId, item, valueJson, userId: reportDay.user_id });
          const userSettings = (await ensureUserAndSettings(ctx)).user.settings_json as Record<string, unknown>;
          await logForUser({
            userId: reportDay.user_id,
            ctx,
            eventName: 'db_write',
            payload: { action: 'save_value', item_id: item.id },
            enabled: telemetryEnabledForUser(userSettings)
          });
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'save_value_failed', error, reportDayId, itemId: item.id, valueJson });
          await renderScreen(ctx, {
            titleKey: t('screens.daily_report.title'),
            bodyLines: [t('screens.daily_report.save_failed')],
            inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items })
          });
          return;
        }

        const updated = { ...(userStates.get(telegramId) || {}) };
        delete updated.awaitingValue;
        delete updated.numericDraft;
        delete updated.timeDraft;
        userStates.set(telegramId, updated);
        await continueFlowAfterAction(ctx, reportDay, awaiting?.origin, awaiting?.statusFilter);
        return;
      }

      case 'dr.skip': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        if (!reportDayId || !itemId) {
          await renderDailyReportRoot(ctx);
          return;
        }

        const context = await ensureContextByReportDayId(ctx, reportDayId);
        const reportDay = context.reportDay;
        const item = context.items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }

        if (reportDay.locked) {
          await renderScreen(ctx, {
            titleKey: t('screens.daily_report.title'),
            bodyLines: isLockedMessageLines(reportDay),
            inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items })
          });
          return;
        }

        await saveValue({ reportDayId, item, valueJson: { skipped: true }, userId: reportDay.user_id });

        // Clear state for that field if it was awaiting this.
        const telegramId = String(ctx.from?.id ?? '');
        const st = { ...(userStates.get(telegramId) || {}) };
        const origin = st.awaitingValue?.origin;
        const statusFilter = st.awaitingValue?.statusFilter;
        if (st.awaitingValue?.itemId === itemId && st.awaitingValue?.reportDayId === reportDayId) delete st.awaitingValue;
        delete st.numericDraft;
        delete st.timeDraft;
        userStates.set(telegramId, st);

        await continueFlowAfterAction(ctx, reportDay, origin, statusFilter);
        return;
      }

      case 'dr.boolean': {
        const data = (payload as { data?: { reportDayId?: string; itemId?: string; value?: boolean } }).data;
        const reportDayId = data?.reportDayId;
        const itemId = data?.itemId;
        const value = data?.value;
        if (!reportDayId || !itemId || typeof value !== 'boolean') {
          await renderDailyReportRoot(ctx);
          return;
        }

        const context = await ensureContextByReportDayId(ctx, reportDayId);
        const reportDay = context.reportDay;
        const item = context.items.find((i) => i.id === itemId);
        if (!item) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (reportDay.locked) {
          await renderScreen(ctx, {
            titleKey: t('screens.daily_report.title'),
            bodyLines: isLockedMessageLines(reportDay),
            inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay, { items: context.items })
          });
          return;
        }

        const telegramId = String(ctx.from?.id ?? '');
        const awaiting = userStates.get(telegramId)?.awaitingValue;

        await saveValue({ reportDayId, item, valueJson: { value }, userId: reportDay.user_id });
        const userSettings = (await ensureUserAndSettings(ctx)).user.settings_json as Record<string, unknown>;
        await logForUser({
          userId: reportDay.user_id,
          ctx,
          eventName: 'db_write',
          payload: { action: 'save_value', item_id: item.id },
          enabled: telemetryEnabledForUser(userSettings)
        });

        const updated = { ...(userStates.get(telegramId) || {}) };
        delete updated.awaitingValue;
        delete updated.numericDraft;
        delete updated.timeDraft;
        userStates.set(telegramId, updated);

        await continueFlowAfterAction(ctx, reportDay, awaiting?.origin, awaiting?.statusFilter);
        return;
      }

      case 'dr.routine_open_tasks': {
        const data = (payload as { data?: { reportDayId?: string; routineId?: string; itemId?: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        if (!data?.reportDayId || !data.routineId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const reportDay = await getReportDayById(data.reportDayId);
        if (!reportDay) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const context = await ensureSpecificReportContext(ctx, reportDay.local_date);
        const routineItem =
          (data.itemId && context.items.find((i) => i.id === data.itemId)) ||
          context.items.find(
            (i) =>
              isRoutineParentItem(i) &&
              ((i.options_json ?? {}) as { routine_id?: string }).routine_id === data.routineId
          );
        if (!routineItem) {
          await renderDailyReportRoot(ctx, reportDay.local_date);
          return;
        }
        await renderRoutineDailyTasks(ctx, {
          reportDay: context.reportDay,
          routineItem,
          items: context.items,
          origin: data.origin,
          statusFilter: data.statusFilter
        });
        return;
      }

      case 'dr.routine_detail': {
        const data = (payload as { data?: { reportDayId?: string; routineId?: string; itemId?: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        if (!data?.reportDayId || !data.routineId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const reportDay = await getReportDayById(data.reportDayId);
        if (!reportDay) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const context = await ensureSpecificReportContext(ctx, reportDay.local_date);
        const routineItem =
          (data.itemId && context.items.find((i) => i.id === data.itemId)) ||
          context.items.find(
            (i) =>
              isRoutineParentItem(i) &&
              ((i.options_json ?? {}) as { routine_id?: string }).routine_id === data.routineId
          );
        if (!routineItem) {
          await renderDailyReportRoot(ctx, reportDay.local_date);
          return;
        }
        await renderRoutineDailyEntry(ctx, context.reportDay, routineItem, context.items, data.origin, data.statusFilter);
        return;
      }

      case 'dr.routine_done':
      case 'dr.routine_mark_done': {
        const data = (payload as { data?: { reportDayId?: string; routineId?: string; itemId?: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        if (!data?.reportDayId || !data.routineId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const awaiting = userStates.get(telegramId)?.awaitingValue;
        const resolvedOrigin = (data.origin as 'next' | 'status' | undefined) ?? awaiting?.origin;
        const resolvedStatusFilter = (data.statusFilter as 'all' | 'not_filled' | 'filled' | undefined) ?? awaiting?.statusFilter;
        const reportDay = await getReportDayById(data.reportDayId);
        if (!reportDay) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }
        const context = await ensureSpecificReportContext(ctx, reportDay.local_date);
        const routineItem =
          (data.itemId && context.items.find((i) => i.id === data.itemId)) ||
          context.items.find(
            (i) =>
              isRoutineParentItem(i) &&
              ((i.options_json ?? {}) as { routine_id?: string }).routine_id === data.routineId
          );
        if (!routineItem) {
          await renderDailyReportRoot(ctx, reportDay.local_date);
          return;
        }
        const routineId = ((routineItem.options_json ?? {}) as { routine_id?: string }).routine_id;
        await saveValue({
          reportDayId: reportDay.id,
          item: routineItem,
          valueJson: { value: true, completed_all: true, status: 'done' },
          userId: reportDay.user_id
        });
        if (routineId) {
          const taskItems = context.items.filter(
            (i) => isRoutineTaskItem(i) && ((i.options_json ?? {}) as { routine_id?: string }).routine_id === routineId
          );
          for (const task of taskItems) {
            await saveValue({
              reportDayId: reportDay.id,
              item: task,
              valueJson: { value: true, auto_completed: true },
              userId: reportDay.user_id,
              applyXp: false,
              resetXpApplied: true
            });
          }
        }
        const updatedState = { ...(userStates.get(telegramId) || {}) };
        if (updatedState.awaitingValue?.itemId === routineItem.id && updatedState.awaitingValue?.reportDayId === reportDay.id) delete updatedState.awaitingValue;
        delete updatedState.numericDraft;
        delete updatedState.timeDraft;
        userStates.set(telegramId, updatedState);
        await continueFlowAfterAction(ctx, reportDay, resolvedOrigin, resolvedStatusFilter);
        return;
      }

      case 'dr.routine_partial': {
        const data = (payload as { data?: { reportDayId?: string; routineId?: string; itemId?: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        if (!data?.reportDayId || !data.routineId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const awaiting = userStates.get(telegramId)?.awaitingValue;
        const resolvedOrigin = (data.origin as 'next' | 'status' | undefined) ?? awaiting?.origin;
        const resolvedStatusFilter = (data.statusFilter as 'all' | 'not_filled' | 'filled' | undefined) ?? awaiting?.statusFilter;
        const reportDay = await getReportDayById(data.reportDayId);
        if (!reportDay) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }
        const context = await ensureSpecificReportContext(ctx, reportDay.local_date);
        const routineItem =
          (data.itemId && context.items.find((i) => i.id === data.itemId)) ||
          context.items.find(
            (i) =>
              isRoutineParentItem(i) &&
              ((i.options_json ?? {}) as { routine_id?: string }).routine_id === data.routineId
          );
        if (!routineItem) {
          await renderDailyReportRoot(ctx, reportDay.local_date);
          return;
        }
        const updatedState = { ...(userStates.get(telegramId) || {}) };
        if (updatedState.awaitingValue?.itemId === routineItem.id && updatedState.awaitingValue?.reportDayId === reportDay.id) delete updatedState.awaitingValue;
        delete updatedState.numericDraft;
        delete updatedState.timeDraft;
        userStates.set(telegramId, updatedState);
        await saveValue({
          reportDayId: reportDay.id,
          item: routineItem,
          valueJson: { status: 'partial', completed_all: false, value: false },
          userId: reportDay.user_id,
          applyXp: false,
          resetXpApplied: true
        });
        await renderRoutineDailyTasks(ctx, { reportDay, routineItem, items: context.items, origin: resolvedOrigin, statusFilter: resolvedStatusFilter });
        return;
      }

      case 'dr.routine_skip': {
        const data = (payload as { data?: { reportDayId?: string; routineId?: string; itemId?: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        if (!data?.reportDayId || !data.routineId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const awaiting = userStates.get(telegramId)?.awaitingValue;
        const resolvedOrigin = (data.origin as 'next' | 'status' | undefined) ?? awaiting?.origin;
        const resolvedStatusFilter = (data.statusFilter as 'all' | 'not_filled' | 'filled' | undefined) ?? awaiting?.statusFilter;
        const reportDay = await getReportDayById(data.reportDayId);
        if (!reportDay) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }
        const context = await ensureSpecificReportContext(ctx, reportDay.local_date);
        const routineItem =
          (data.itemId && context.items.find((i) => i.id === data.itemId)) ||
          context.items.find(
            (i) =>
              isRoutineParentItem(i) &&
              ((i.options_json ?? {}) as { routine_id?: string }).routine_id === data.routineId
          );
        if (!routineItem) {
          await renderDailyReportRoot(ctx, reportDay.local_date);
          return;
        }
        const routineId = ((routineItem.options_json ?? {}) as { routine_id?: string }).routine_id;
        await saveValue({
          reportDayId: reportDay.id,
          item: routineItem,
          valueJson: { skipped: true, status: 'skipped' },
          userId: reportDay.user_id,
          applyXp: false,
          resetXpApplied: true
        });
        if (routineId) {
          const taskItems = context.items.filter(
            (i) => isRoutineTaskItem(i) && ((i.options_json ?? {}) as { routine_id?: string }).routine_id === routineId
          );
          for (const task of taskItems) {
            await saveValue({
              reportDayId: reportDay.id,
              item: task,
              valueJson: { skipped: true },
              userId: reportDay.user_id,
              applyXp: false,
              resetXpApplied: true
            });
          }
        }
        const updatedState = { ...(userStates.get(telegramId) || {}) };
        if (updatedState.awaitingValue?.itemId === routineItem.id && updatedState.awaitingValue?.reportDayId === reportDay.id) delete updatedState.awaitingValue;
        delete updatedState.numericDraft;
        delete updatedState.timeDraft;
        userStates.set(telegramId, updatedState);
        await continueFlowAfterAction(ctx, reportDay, resolvedOrigin, resolvedStatusFilter);
        return;
      }

      case 'dr.routine_undo': {
        const data = (payload as { data?: { reportDayId?: string; routineId?: string; itemId?: string; origin?: 'next' | 'status'; statusFilter?: 'all' | 'not_filled' | 'filled' } }).data;
        if (!data?.reportDayId || !data.routineId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const awaiting = userStates.get(telegramId)?.awaitingValue;
        const resolvedOrigin = (data.origin as 'next' | 'status' | undefined) ?? awaiting?.origin;
        const resolvedStatusFilter = (data.statusFilter as 'all' | 'not_filled' | 'filled' | undefined) ?? awaiting?.statusFilter;
        const reportDay = await getReportDayById(data.reportDayId);
        if (!reportDay) {
          await renderDailyReportRoot(ctx);
          return;
        }
        if (reportDay.locked) {
          await renderScreen(ctx, { titleKey: t('screens.daily_report.title'), bodyLines: isLockedMessageLines(reportDay), inlineKeyboard: await buildDailyReportKeyboard(ctx, reportDay) });
          return;
        }
        const context = await ensureSpecificReportContext(ctx, reportDay.local_date);
        const routineItem =
          (data.itemId && context.items.find((i) => i.id === data.itemId)) ||
          context.items.find(
            (i) =>
              isRoutineParentItem(i) &&
              ((i.options_json ?? {}) as { routine_id?: string }).routine_id === data.routineId
          );
        if (!routineItem) {
          await renderDailyReportRoot(ctx, reportDay.local_date);
          return;
        }
        const [statusRow] = await listCompletionStatus(reportDay.id, [routineItem]);
        const xpApplied = computeRoutineParentXp(routineItem, statusRow?.value?.value_json ?? null);
        const meta = routineMetaFromItem(routineItem);
        if (xpApplied > 0) {
          await addXpDelta({
            userId: reportDay.user_id,
            delta: -xpApplied,
            reason: `routine_undo:${reportDay.id}:${routineItem.id}`,
            refType: 'daily_report',
            refId: routineItem.id,
            metadata: {
              source_type: 'routine',
              routine_id: meta.routineId ?? null,
              routine_task_id: null,
              report_day_id: reportDay.id
            }
          });
        }
        await saveValue({
          reportDayId: reportDay.id,
          item: routineItem,
          valueJson: null,
          userId: reportDay.user_id,
          applyXp: false,
          resetXpApplied: true
        });
        const routineId = ((routineItem.options_json ?? {}) as { routine_id?: string }).routine_id;
        if (routineId) {
          const taskItems = context.items.filter(
            (i) => isRoutineTaskItem(i) && ((i.options_json ?? {}) as { routine_id?: string }).routine_id === routineId
          );
          for (const task of taskItems) {
            await saveValue({
              reportDayId: reportDay.id,
              item: task,
              valueJson: null,
              userId: reportDay.user_id,
              applyXp: false,
              resetXpApplied: true
            });
          }
        }
        const updatedState = { ...(userStates.get(telegramId) || {}) };
        if (updatedState.awaitingValue?.itemId === routineItem.id && updatedState.awaitingValue?.reportDayId === reportDay.id) delete updatedState.awaitingValue;
        delete updatedState.numericDraft;
        delete updatedState.timeDraft;
        userStates.set(telegramId, updatedState);
        await continueFlowAfterAction(ctx, reportDay, resolvedOrigin, resolvedStatusFilter);
        return;
      }

      case 'dr.lock': {
        const reportDayId = (payload as { data?: { reportDayId?: string } }).data?.reportDayId;
        if (!reportDayId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);
        const reportDay = cached?.reportDay ?? (await ensureReportContext(ctx)).reportDay;

        await lockReportDay({ reportDayId: reportDay.id });

        // Invalidate cache for that day so next render reflects lock.
        reportContextCache.forEach((v, k) => {
          if (v.reportDay.id === reportDay.id) reportContextCache.delete(k);
        });

        await renderDailyReportRoot(ctx, reportDay.local_date);
        return;
      }

      case 'dr.unlock': {
        const reportDayId = (payload as { data?: { reportDayId?: string } }).data?.reportDayId;
        if (!reportDayId) {
          await renderDailyReportRoot(ctx);
          return;
        }
        const cached = [...reportContextCache.values()].find((v) => v.reportDay.id === reportDayId);
        const reportDay = cached?.reportDay ?? (await ensureReportContext(ctx)).reportDay;

        await unlockReportDay({ reportDayId: reportDay.id, userId: reportDay.user_id });

        reportContextCache.forEach((v, k) => {
          if (v.reportDay.id === reportDay.id) reportContextCache.delete(k);
        });

        await renderDailyReportRoot(ctx, reportDay.local_date);
        return;
      }

      case 'dr.templates': {
        await renderTemplatesScreen(ctx);
        return;
      }

      case 'dr.template_details':
      case 'dr.template_actions': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await renderTemplateActions(ctx, templateId);
        return;
      }
      case 'dr.template_rename_prompt': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const tpl = await getTemplateById(templateId);
        if (!tpl || tpl.user_id !== u.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        setTemplateRenameFlow(telegramId, { templateId });
        const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.template_actions', data: { templateId } });
        const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, {
          titleKey: t('screens.daily_report.templates_rename_title'),
          bodyLines: [t('screens.daily_report.templates_rename_prompt')],
          inlineKeyboard: kb
        });
        return;
      }

      case 'dr.template_edit': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await renderTemplateEdit(ctx, templateId);
        return;
      }

      case 'dr.template_set_active': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        try {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(templateId);
          if (!tpl || tpl.user_id !== u.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          await setActiveTemplate({ userId: u.id, templateId });
          clearReportContextCache();
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'template_set_active_failed', error, templateId });
        }
        await renderTemplatesScreen(ctx);
        return;
      }

      case 'dr.template_duplicate': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        try {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(templateId);
          if (!tpl || tpl.user_id !== u.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          const newTitle = `Copy of ${tpl.title ?? t('screens.templates.default_title')}`;
          const duplicated = await duplicateTemplate({ userId: u.id, sourceTemplateId: templateId, newTitle });
          clearReportContextCache();
          await renderTemplateEdit(ctx, duplicated.id, t('screens.daily_report.templates_duplicate_success'));
          return;
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'template_duplicate_failed', error, templateId });
        }
        await renderTemplatesScreen(ctx);
        return;
      }

      case 'dr.template_delete_confirm': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
          await renderTemplateDeleteConfirm(ctx, templateId);
        return;
      }

      case 'dr.template_delete': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        try {
          const { user: u, settings } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(templateId);
          if (!tpl || tpl.user_id !== u.id) {
            await renderTemplatesScreen(ctx);
            return;
          }

          const settingsJson = (settings.settings_json ?? {}) as { active_template_id?: string | null };
          const activeTemplateId = settingsJson.active_template_id ?? null;

          await deleteTemplate({ userId: u.id, templateId });
          clearReportContextCache();

          if (activeTemplateId === templateId) {
            const remaining = await listUserTemplates(u.id);
            if (remaining.length > 0) {
              await setActiveTemplate({ userId: u.id, templateId: remaining[0].id });
            } else {
              const fallback = await ensureDefaultTemplate(u.id);
              await setActiveTemplate({ userId: u.id, templateId: fallback.id });
            }
          }

          await renderTemplatesScreen(ctx, t('screens.daily_report.templates_delete_success'));
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'template_delete_failed', error, templateId });
          await renderTemplatesScreen(ctx);
        }
        return;
      }

      case 'dr.template_new': {
        const telegramId = String(ctx.from?.id ?? '');
        setTemplateCreateFlow(telegramId, { step: 'title' });
        const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.templates' });
        const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, {
          titleKey: t('screens.templates.new_title'),
          bodyLines: [t('screens.templates.new_prompt')],
          inlineKeyboard: kb
        });
        return;
      }

      case 'dr.template_item_menu': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await renderTemplateItemMenu(ctx, data.templateId, data.itemId);
        return;
      }
      case 'dr.template_help': {
        const data = (payload as { data?: { templateId?: string; topic?: 'type' | 'category' | 'xp_mode'; itemId?: string; backToItem?: boolean } }).data;
        if (!data?.templateId || !data.topic) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await renderTemplateHelp(ctx, {
          templateId: data.templateId,
          topic: data.topic,
          itemId: data.itemId,
          backToItem: data.backToItem
        });
        return;
      }

      case 'builder.back': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await handleBuilderBackNavigation(ctx, templateId);
        return;
      }

      case 'builder.summary_continue': {
        const data = (payload as { data?: { templateId?: string; stage?: 'category' | 'xp'; backToItem?: boolean } }).data;
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.templateItemFlow;
        if (!data?.templateId || !data.stage || !flow || flow.mode !== 'create' || flow.templateId !== data.templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        if (data.stage === 'category') {
          setTemplateItemFlow(telegramId, { ...flow, step: 'xp_mode' });
          await promptXpModeSelection(ctx, { templateId: data.templateId, itemType: flow.draft.itemType });
          return;
        }
        await finalizeNewTemplateItem(ctx, telegramId, flow);
        return;
      }

      case 'builder.summary_edit': {
        const data = (payload as { data?: { templateId?: string; stage?: 'category' | 'xp'; backToItem?: boolean } }).data;
        const telegramId = String(ctx.from?.id ?? '');
        const flow = userStates.get(telegramId)?.templateItemFlow;
        if (!data?.templateId || !data.stage || !flow || flow.templateId !== data.templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        if (data.stage === 'category') {
          setTemplateItemFlow(telegramId, { ...flow, step: 'category' });
          await promptCategorySelection(ctx, { templateId: data.templateId, backToItem: data.backToItem, itemId: flow.itemId });
          return;
        }
        setTemplateItemFlow(telegramId, { ...flow, step: 'xp_mode' });
        await promptXpModeSelection(ctx, { templateId: data.templateId, itemId: flow.itemId, backToItem: data.backToItem, itemType: flow.draft.itemType });
        return;
      }

      case 'dr.template_item_add': {
        const templateId = (payload as { data?: { templateId?: string } }).data?.templateId;
        if (!templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const tpl = await getTemplateById(templateId);
        if (!tpl || tpl.user_id !== u.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearTemplateItemFlow(telegramId);
        setTemplateItemFlow(telegramId, { mode: 'create', templateId, step: 'label', draft: {} });
        updateBuilderStep(telegramId, templateId, 'builder.enterLabel');
        await promptLabelInput(ctx, { templateId });
        return;
      }

      case 'dr.template_item_edit_label': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const item = await getItemById(data.itemId);
        const tpl = await getTemplateById(data.templateId);
        if (!item || !tpl || tpl.user_id !== u.id || item.template_id !== tpl.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearTemplateItemFlow(telegramId);
        setTemplateItemFlow(telegramId, { mode: 'edit', templateId: data.templateId, itemId: data.itemId, step: 'label', draft: { label: item.label } });
        await promptLabelInput(ctx, { templateId: data.templateId, backToItemId: data.itemId });
        return;
      }

      case 'dr.template_item_edit_key': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const item = await getItemById(data.itemId);
        const tpl = await getTemplateById(data.templateId);
        if (!item || !tpl || tpl.user_id !== u.id || item.template_id !== tpl.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearTemplateItemFlow(telegramId);
        setTemplateItemFlow(telegramId, { mode: 'edit', templateId: data.templateId, itemId: data.itemId, step: 'key', draft: { itemKey: item.item_key } });
        await promptKeyInput(ctx, { templateId: data.templateId, itemId: data.itemId });
        return;
      }

      case 'dr.template_item_edit_type': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const item = await getItemById(data.itemId);
        const tpl = await getTemplateById(data.templateId);
        if (!item || !tpl || tpl.user_id !== u.id || item.template_id !== tpl.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearTemplateItemFlow(telegramId);
        setTemplateItemFlow(telegramId, {
          mode: 'edit',
          templateId: data.templateId,
          itemId: data.itemId,
          step: 'type',
          draft: { itemType: item.item_type }
        });
        await promptTypeSelection(ctx, { templateId: data.templateId, itemId: data.itemId, backToItem: true });
        return;
      }

      case 'dr.template_item_select_type': {
        const data = (payload as { data?: { templateId?: string; itemId?: string; itemType?: string } }).data;
        if (!data?.templateId || !data.itemType) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId)?.templateItemFlow;
        if (state && state.mode === 'create' && state.templateId === data.templateId && state.step === 'type') {
          setTemplateItemFlow(telegramId, { ...state, draft: { ...state.draft, itemType: data.itemType }, step: 'category' });
          await promptCategorySelection(ctx, { templateId: data.templateId });
          return;
        }
        if (data.itemId) {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(data.templateId);
          const item = await getItemById(data.itemId);
          if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          await updateItem(data.itemId, { item_type: data.itemType });
          clearTemplateItemFlow(telegramId);
          clearReportContextCache();
          await renderTemplateItemMenu(ctx, data.templateId, data.itemId, t('screens.daily_report.item_saved'));
          return;
        }
        await renderTemplateEdit(ctx, data.templateId);
        return;
      }

      case 'dr.template_item_edit_category': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const tpl = await getTemplateById(data.templateId);
        const item = await getItemById(data.itemId);
        if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await promptCategorySelection(ctx, { templateId: data.templateId, itemId: data.itemId, backToItem: true });
        return;
      }

      case 'dr.template_item_select_category': {
        const data = (payload as { data?: { templateId?: string; itemId?: string; category?: string } }).data;
        if (!data?.templateId || !data.category) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId)?.templateItemFlow;
        if (state && state.mode === 'create' && state.templateId === data.templateId) {
          const category = data.category === 'none' ? null : data.category;
          const nextFlow = { ...state, draft: { ...state.draft, category }, step: 'category' } as TemplateItemFlow;
          setTemplateItemFlow(telegramId, nextFlow);
          await renderStepSummary(ctx, { templateId: data.templateId, stage: 'category', flow: nextFlow });
          return;
        }
        if (data.itemId) {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(data.templateId);
          const item = await getItemById(data.itemId);
          if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          const category = data.category === 'none' ? null : data.category;
          await updateItem(data.itemId, { category });
          clearTemplateItemFlow(telegramId);
          clearReportContextCache();
          await renderTemplateItemMenu(ctx, data.templateId, data.itemId, t('screens.daily_report.item_saved'));
          return;
        }
        await renderTemplateEdit(ctx, data.templateId);
        return;
      }

      case 'dr.template_item_custom_category': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const currentFlow = userStates.get(telegramId)?.templateItemFlow;
        if (currentFlow && currentFlow.mode === 'create' && currentFlow.templateId === data.templateId) {
          setTemplateItemFlow(telegramId, { ...currentFlow, step: 'category_custom' });
          await promptCustomCategoryInput(ctx, { templateId: data.templateId, itemId: data.itemId });
          return;
        }
        if (data.itemId) {
          const { user: u } = await ensureUserAndSettings(ctx);
          const item = await getItemById(data.itemId);
          const tpl = await getTemplateById(data.templateId);
          if (!tpl || !item || tpl.user_id !== u.id || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          setTemplateItemFlow(telegramId, {
            mode: 'edit',
            templateId: data.templateId,
            itemId: data.itemId,
            step: 'category_custom',
            draft: { ...itemToDraft(item) }
          });
          await promptCustomCategoryInput(ctx, { templateId: data.templateId, itemId: data.itemId, backAction: 'dr.template_item_menu' });
          return;
        }
        await renderTemplateEdit(ctx, data.templateId);
        return;
      }

      case 'dr.template_item_edit_xp': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const { user: u } = await ensureUserAndSettings(ctx);
        const tpl = await getTemplateById(data.templateId);
        const item = await getItemById(data.itemId);
        if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        clearTemplateItemFlow(telegramId);
        setTemplateItemFlow(telegramId, {
          mode: 'edit',
          templateId: data.templateId,
          itemId: data.itemId,
          step: 'xp_mode',
          draft: {
            xpMode: ((item.xp_mode as TemplateItemFlow['draft']['xpMode']) ?? 'none') as TemplateItemFlow['draft']['xpMode'],
            xpValue: item.xp_value ?? 0,
            xpMaxPerDay: (item as ReportItemRow & { xp_max_per_day?: number | null }).xp_max_per_day ?? null,
            optionsJson: item.options_json ?? {},
            itemType: item.item_type as TemplateItemFlow['draft']['itemType']
          }
        });
        await promptXpModeSelection(ctx, { templateId: data.templateId, itemId: data.itemId, backToItem: true, itemType: item.item_type });
        return;
      }

      case 'dr.template_item_select_xp_mode': {
        const data = (payload as { data?: { templateId?: string; itemId?: string; xpMode?: string } }).data;
        if (!data?.templateId || !data.xpMode) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const telegramId = String(ctx.from?.id ?? '');
        const state = userStates.get(telegramId)?.templateItemFlow;
        const itemType = state?.draft.itemType;
        const chosenMode = data.xpMode === 'time' ? 'per_minute' : data.xpMode;
        const normalizedXpMode = normalizeXpModeForItemType(itemType, chosenMode) ?? 'none';
        const nextDraft: TemplateItemFlow['draft'] = {
          ...(state?.draft ?? {}),
          xpMode: chosenMode as TemplateItemFlow['draft']['xpMode'],
          optionsJson:
            chosenMode === 'per_minute'
              ? { ...(state?.draft.optionsJson ?? {}), per: 'minute' }
              : chosenMode === 'per_number'
                ? { ...(state?.draft.optionsJson ?? {}), perNumber: 1, xpPerUnit: state?.draft.xpValue ?? 1 }
                : {}
        };

        if (state && state.mode === 'create' && state.templateId === data.templateId) {
          if (normalizedXpMode === 'none') {
            const updatedFlow = { ...state, draft: { ...nextDraft, xpMode: 'none', xpValue: null, xpMaxPerDay: null }, step: 'xp_mode' } as TemplateItemFlow;
            await renderStepSummary(ctx, { templateId: data.templateId, stage: 'xp', flow: updatedFlow });
            return;
          }
          setTemplateItemFlow(telegramId, { ...state, draft: { ...nextDraft, xpMode: normalizedXpMode }, step: 'xp_value' });
          await promptXpValueInput(ctx, { templateId: data.templateId });
          return;
        }

        if (data.itemId) {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(data.templateId);
          const item = await getItemById(data.itemId);
          if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }

          const normalizedForExisting = normalizeXpModeForItemType(item.item_type, normalizedXpMode);

          if (normalizedForExisting === 'none') {
            await updateItem(data.itemId, { xp_mode: null, xp_value: null, xp_max_per_day: null, options_json: {} });
            clearTemplateItemFlow(telegramId);
            clearReportContextCache();
            await renderTemplateItemMenu(ctx, data.templateId, data.itemId, t('screens.daily_report.item_saved'));
            return;
          }

          setTemplateItemFlow(telegramId, {
            mode: 'edit',
            templateId: data.templateId,
            itemId: data.itemId,
            step: 'xp_value',
            draft: { ...nextDraft, xpMode: normalizedForExisting }
          });
          await promptXpValueInput(ctx, { templateId: data.templateId, itemId: data.itemId, backToItem: true });
          return;
        }
        await renderTemplateEdit(ctx, data.templateId);
        return;
      }

      case 'dr.template_item_toggle_enabled': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        try {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(data.templateId);
          const item = await getItemById(data.itemId);
          if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          await setItemEnabled(item.id, !item.enabled);
          clearReportContextCache();
          await renderTemplateItemMenu(ctx, data.templateId, data.itemId, t('screens.daily_report.item_saved'));
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'template_item_toggle_failed', error, data });
          await renderTemplateEdit(ctx, data.templateId);
        }
        return;
      }

      case 'dr.template_item_move_up':
      case 'dr.template_item_move_down': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        const direction = action === 'dr.template_item_move_up' ? 'up' : 'down';
        try {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(data.templateId);
          const item = await getItemById(data.itemId);
          if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          await moveItem(data.templateId, data.itemId, direction);
          clearReportContextCache();
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'template_item_move_failed', error, data, direction });
        }
        await renderTemplateEdit(ctx, data.templateId);
        return;
      }

      case 'dr.template_item_delete_confirm': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        await renderTemplateItemDeleteConfirm(ctx, data.templateId, data.itemId);
        return;
      }

      case 'dr.template_item_delete': {
        const data = (payload as { data?: { templateId?: string; itemId?: string } }).data;
        if (!data?.templateId || !data.itemId) {
          await renderTemplatesScreen(ctx);
          return;
        }
        try {
          const { user: u } = await ensureUserAndSettings(ctx);
          const tpl = await getTemplateById(data.templateId);
          const item = await getItemById(data.itemId);
          if (!tpl || tpl.user_id !== u.id || !item || item.template_id !== tpl.id) {
            await renderTemplatesScreen(ctx);
            return;
          }
          await deleteItem(data.itemId);
          clearReportContextCache();
          await renderTemplateEdit(ctx, data.templateId, t('screens.daily_report.item_deleted'));
        } catch (error) {
          console.error({ scope: 'daily_report', event: 'template_item_delete_failed', error, data });
          await renderTemplateEdit(ctx, data.templateId);
        }
        return;
      }

      case 'dr.history': {
        await renderHistory(ctx, '7d');
        return;
      }

      case 'dr.history_7d': {
        await renderHistory(ctx, '7d');
        return;
      }

      case 'dr.history_30d': {
        await renderHistory(ctx, '30d');
        return;
      }

      case 'dr.history_open_day': {
        const reportDayId = (payload as { data?: { reportDayId?: string } }).data?.reportDayId;
        if (!reportDayId) {
          await renderHistory(ctx);
          return;
        }
        await renderHistoryDay(ctx, reportDayId);
        return;
      }

      case 'hist.open': {
        const localDate = (payload as { data?: { localDate?: string } }).data?.localDate;
        if (!localDate) {
          await renderHistory(ctx);
          return;
        }
        const { reportDay } = await ensureSpecificReportContext(ctx, localDate);
        await renderHistoryDay(ctx, reportDay.id);
        return;
      }

      /* --- Error report --- */
      case 'error.send_report': {
        const errorCode =
          (payload as { errorCode?: string; data?: { errorCode?: string } }).errorCode ?? (payload as { data?: { errorCode?: string } }).data?.errorCode;

        if (!errorCode) {
          await ctx.answerCallbackQuery({ text: t('errors.report_not_found'), show_alert: true });
          return;
        }

        const report = await getErrorReportByCode(errorCode);
        if (!report) {
          await ctx.answerCallbackQuery({ text: t('errors.report_not_found'), show_alert: true });
          return;
        }

        const targetId = config.telegram.adminId ? Number(config.telegram.adminId) : ctx.from?.id;
        if (targetId) {
          const events =
            Array.isArray(report.recent_events) && report.recent_events.length > 0
              ? report.recent_events
                  .slice(0, 5)
                  .map((ev: any) => `• ${ev.event_name ?? 'event'}${ev.screen ? ` @ ${ev.screen}` : ''}`)
                  .join('\n')
              : 'No events captured.';

          const rawText = ['*Error report*', `Code: ${report.error_code}`, `Trace: ${report.trace_id}`, `Created: ${report.created_at}`, `User: ${report.user_id}`, '', 'Recent events:', events].join('\n');
          const truncatedText = rawText.length > 3500 ? `${rawText.slice(0, 3500)}... (truncated)` : rawText;
          const safeText = escapeMarkdown(truncatedText);
          try {
            await ctx.api.sendMessage(targetId, safeText, { parse_mode: 'Markdown' });
          } catch (error) {
            console.error({ scope: 'error_report', event: 'send_failure', error, text: rawText });
            await ctx.api.sendMessage(targetId, truncatedText);
          }
        }

        await ctx.answerCallbackQuery({ text: t('screens.error_report.sent'), show_alert: true });
        await logTelemetryEvent({ userId: report.user_id, traceId, eventName: 'error_report_sent', payload: { error_code: errorCode, target: config.telegram.adminId ?? ctx.from?.id }, enabled });
        return;
      }

      default:
        await ctx.answerCallbackQuery({ text: t('errors.action_expired'), show_alert: true });
        return;
    }
  } catch (error) {
    console.error({ scope: 'callback_tokens', event: 'consume_failure', error });
    await ctx.answerCallbackQuery({ text: t('errors.unexpected_try_again'), show_alert: true });
  }
});

/**
 * Text message handler: only active during explicit flows.
 */
bot.on('message:text', async (ctx: Context) => {
  if (!ctx.from || !ctx.message || typeof ctx.message.text !== 'string') return;

  const rawText = ctx.message.text;
  const text = rawText.trim();
  const stateKey = String(ctx.from.id);
  const state = userStates.get(stateKey) ?? {};

  if (state.notesFlow) {
    const flow = state.notesFlow;
    const { user } = await ensureUserAndSettings(ctx);

    if (flow.mode === 'create') {
      if (flow.step === 'title') {
        const title = text.length > 0 ? text : null;
        setNotesFlow(stateKey, { mode: 'create', step: 'body', draft: { ...flow.draft, title } });
        await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.askBody()] });
        return;
      }
      if (flow.step === 'body') {
        if (!text) {
          await renderScreen(ctx, { title: labels.notes.title(), bodyLines: [labels.notes.askBody()] });
          return;
        }
        const note = await createNote({
          userId: user.id,
          noteDate: flow.draft.noteDate,
          title: flow.draft.title ?? null,
          body: rawText
        });
        setNotesFlow(stateKey, { mode: 'create', step: 'attachments', noteId: note.id, viewContext: { noteDate: note.note_date } });
        const title = note.title && note.title.trim().length > 0 ? note.title : labels.notes.untitled();
        const preview = buildPreviewText(note.body, 120);
        if (rawText.length > ARCHIVE_DESCRIPTION_LIMIT) {
          const archiveChatId = getNotesArchiveChatId();
          if (archiveChatId) {
            const telegramMeta = resolveTelegramUserMeta(ctx, user);
            const archiveResult = await sendArchiveItemToChannel(ctx.api, {
              archiveChatId,
              user: {
                firstName: telegramMeta.firstName,
                lastName: telegramMeta.lastName,
                username: telegramMeta.username,
                telegramId: telegramMeta.telegramId,
                appUserId: user.id
              },
              timeLabel: buildArchiveTimeLabel(note.created_at, user.timezone ?? config.defaultTimezone),
              kindLabel: labels.archive.kindNote(),
              title: note.title ?? null,
              description: rawText,
              attachments: []
            });
            const updatedItem = await upsertArchiveItem({
              existing: null,
              ownerUserId: user.id,
              kind: 'note',
              entityId: note.id,
              channelId: archiveChatId,
              title: note.title ?? null,
              description: rawText,
              summary: emptyArchiveSummary(),
              messageIds: archiveResult.messageIds,
              messageMeta: archiveResult.messageMeta,
              meta: {
                username: telegramMeta.username ?? null,
                first_name: telegramMeta.firstName ?? null,
                last_name: telegramMeta.lastName ?? null,
                telegram_id: telegramMeta.telegramId ?? null,
                created_at: note.created_at,
                summary_line: buildCaptionSummaryLine(emptyArchiveSummary())
              }
            });
            await updateNote({ userId: user.id, id: note.id, archiveItemId: updatedItem.id });
          }
        }
        const doneBtn = await makeActionButton(ctx, {
          label: labels.notesButtons.attachDone(),
          action: 'notes.attach_done',
          data: { noteId: note.id, noteDate: note.note_date }
        });
        const cancelBtn = await makeActionButton(ctx, {
          label: labels.notesButtons.attachCancel(),
          action: 'notes.attach_cancel',
          data: { noteId: note.id, noteDate: note.note_date }
        });
        const kb = new InlineKeyboard().text(doneBtn.text, doneBtn.callback_data).row().text(cancelBtn.text, cancelBtn.callback_data);
        await renderScreen(ctx, {
          title: labels.notes.title(),
          bodyLines: [
            labels.notes.saved(),
            '',
            labels.notes.previewDate({ date: note.note_date }),
            labels.notes.previewTitle({ title }),
            labels.notes.previewBody({ preview }),
            '',
            labels.notes.attachmentsPrompt()
          ],
          inlineKeyboard: kb
        });
        return;
      }
      if (flow.step === 'attachments') {
        await renderNoteAttachmentPrompt(ctx, flow.noteId, flow.viewContext ?? {});
        return;
      }
      if (flow.step === 'caption_choice') {
        const pending = await listPendingNoteAttachments({ noteId: flow.noteId });
        const summaryLine = buildCaptionSummaryLine(buildSummaryFromNoteAttachments(pending));
        await renderNoteCaptionChoice(ctx, flow.noteId, flow.viewContext ?? {}, flow.captionCategories ?? [], summaryLine);
        return;
      }
      if (flow.step === 'caption_all') {
        const caption = text.length > 0 ? rawText : null;
        const pending = await listPendingNoteAttachments({ noteId: flow.noteId });
        const categories = buildNoteCaptionCategories(pending);
        if (categories.length > 0) {
          const captionPatch: {
            notePhotoCaption?: string | null;
            noteVideoCaption?: string | null;
            noteVoiceCaption?: string | null;
            noteVideoNoteCaption?: string | null;
            noteFileCaption?: string | null;
          } = {};
          for (const category of categories) {
            Object.assign(captionPatch, buildNoteCaptionPatch(category, caption));
          }
          await updateNote({ userId: user.id, id: flow.noteId, ...captionPatch });
        }
        await clearPendingNoteAttachments({ noteId: flow.noteId });
        clearNotesFlow(stateKey);
        await finalizeNoteArchive(ctx, flow.noteId, flow.viewContext ?? {});
        return;
      }
      if (flow.step === 'caption_category' && flow.currentCategory) {
        const caption = text.length > 0 ? rawText : null;
        const category = flow.currentCategory;
        const kinds =
          category === 'files'
            ? (['document', 'audio'] as NoteAttachmentKind[])
            : ([category] as NoteAttachmentKind[]);
        await updateNote({ userId: user.id, id: flow.noteId, ...buildNoteCaptionPatch(category, caption) });
        await clearPendingNoteAttachmentsByKinds({ noteId: flow.noteId, kinds });
        const remaining = (flow.captionCategories ?? []).filter((item) => item !== category);
        if (remaining.length === 0) {
          await clearPendingNoteAttachments({ noteId: flow.noteId });
          clearNotesFlow(stateKey);
          await finalizeNoteArchive(ctx, flow.noteId, flow.viewContext ?? {});
          return;
        }
        await promptNoteCaptionCategory(ctx, flow.noteId, remaining[0], flow.viewContext ?? {}, remaining);
        return;
      }
    }
    if (flow.mode === 'edit') {
      if (flow.step === 'title') {
        await updateNote({ userId: user.id, id: flow.noteId, title: text.length > 0 ? rawText : null });
        clearNotesFlow(stateKey);
        await renderNoteDetails(ctx, flow.noteId, flow.viewContext ?? {});
        return;
      }
      if (flow.step === 'body') {
        if (!text) {
          await renderScreen(ctx, { title: labels.notes.editMenuTitle(), bodyLines: [labels.notes.editBodyPrompt()] });
          return;
        }
        await updateNote({ userId: user.id, id: flow.noteId, body: rawText });
        if (rawText.length > ARCHIVE_DESCRIPTION_LIMIT) {
          const archiveChatId = getNotesArchiveChatId();
          if (archiveChatId) {
            const note = await getNoteById({ userId: user.id, id: flow.noteId });
            const noteTitle = note?.title ?? null;
            const createdAt = note?.created_at ?? new Date().toISOString();
            const telegramMeta = resolveTelegramUserMeta(ctx, user);
            const archiveResult = await sendArchiveItemToChannel(ctx.api, {
              archiveChatId,
              user: {
                firstName: telegramMeta.firstName,
                lastName: telegramMeta.lastName,
                username: telegramMeta.username,
                telegramId: telegramMeta.telegramId,
                appUserId: user.id
              },
              timeLabel: buildArchiveTimeLabel(createdAt, user.timezone ?? config.defaultTimezone),
              kindLabel: labels.archive.kindNote(),
              title: noteTitle,
              description: rawText,
              attachments: []
            });
            const existingItem = await getArchiveItemByEntity({ kind: 'note', entityId: flow.noteId });
            const updatedItem = await upsertArchiveItem({
              existing: existingItem,
              ownerUserId: user.id,
              kind: 'note',
              entityId: flow.noteId,
              channelId: archiveChatId,
              title: noteTitle,
              description: rawText,
              summary: emptyArchiveSummary(),
              messageIds: archiveResult.messageIds,
              messageMeta: archiveResult.messageMeta,
              meta: {
                username: telegramMeta.username ?? null,
                first_name: telegramMeta.firstName ?? null,
                last_name: telegramMeta.lastName ?? null,
                telegram_id: telegramMeta.telegramId ?? null,
                created_at: createdAt,
                summary_line: buildCaptionSummaryLine(emptyArchiveSummary())
              }
            });
            await updateNote({ userId: user.id, id: flow.noteId, archiveItemId: updatedItem.id });
          }
        }
        clearNotesFlow(stateKey);
        await renderNoteDetails(ctx, flow.noteId, flow.viewContext ?? {});
        return;
      }
    }
  }

  // 1) Daily Report free text input (when explicitly requested)
  if (state.awaitingValue) {
    await handleSaveValue(ctx, text);
    return;
  }

  // 2) Settings routine steps (kept for backward-compat)
  if (state.settingsRoutine?.step === 'label') {
    userStates.set(stateKey, { ...state, settingsRoutine: { step: 'xp', label: text } });

    const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.settings' });
    await renderScreen(ctx, { titleKey: t('screens.settings.title'), bodyLines: [t('screens.settings.enter_xp_for_routine')], inlineKeyboard: new InlineKeyboard().text(backBtn.text, backBtn.callback_data) });
    return;
  }

  if (state.settingsRoutine?.step === 'xp') {
    const xp = Number(text);
    if (!Number.isInteger(xp)) {
      const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'nav.settings' });
      await renderScreen(ctx, { titleKey: t('screens.settings.title'), bodyLines: [t('screens.settings.invalid_xp')], inlineKeyboard: new InlineKeyboard().text(backBtn.text, backBtn.callback_data) });
      return;
    }

    const label = state.settingsRoutine.label ?? t('screens.settings.default_routine_label');
    const { user } = await ensureUserAndSettings(ctx);
    const template = await ensureDefaultTemplate(user.id);
    await ensureDefaultItems(user.id);

    await upsertItem({
      templateId: template.id,
      label,
      itemKey: `routine_${Date.now()}`,
      itemType: 'boolean',
      category: 'routine',
      xpMode: 'fixed',
      xpValue: xp,
      optionsJson: {},
      sortOrder: Date.now() % 100000
    });

    userStates.set(stateKey, { ...state, settingsRoutine: undefined });
    await renderSettingsRoot(ctx);
    return;
  }

  if (state.reminderFlow) {
    const flow = state.reminderFlow;
    const raw = text.trim();
    const { user } = await ensureUserAndSettings(ctx);
    const timezone = user.timezone ?? config.defaultTimezone;

    if (flow.step === 'custom_date') {
      const jalaliMatch = raw.match(/^J(\d{4})-(\d{2})-(\d{2})$/i);
      if (jalaliMatch) {
        const year = Number(jalaliMatch[1]);
        const month = Number(jalaliMatch[2]);
        const day = Number(jalaliMatch[3]);
        if (!isValidJalaliDate(year, month, day)) {
          await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.newInvalidDate(), labels.reminders.customDateHint()] });
          return;
        }
        const greg = jalaliToGregorian(year, month, day);
        const localDate = `${greg.year}-${String(greg.month).padStart(2, '0')}-${String(greg.day).padStart(2, '0')}`;
        const nextDraft = ensureReminderTimeDraft({ ...flow.draft, localDate, dateSource: 'custom', scheduleType: flow.draft.scheduleType ?? 'once' }, timezone);
        setReminderFlow(stateKey, { ...flow, step: 'time', draft: nextDraft });
        await renderReminderTimePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
        return;
      }

      if (!isValidLocalDate(raw)) {
        await renderScreen(ctx, {
          title: labels.reminders.newTitle(),
          bodyLines: [labels.reminders.newInvalidDate(), labels.reminders.customDateHint()]
        });
        return;
      }
      const nextDraft = ensureReminderTimeDraft({ ...flow.draft, localDate: raw, dateSource: 'custom', scheduleType: flow.draft.scheduleType ?? 'once' }, timezone);
      setReminderFlow(stateKey, { ...flow, step: 'time', draft: nextDraft });
      await renderReminderTimePicker(ctx, { mode: flow.mode, reminderId: getReminderIdFromFlow(flow), draft: nextDraft });
      return;
    }

    if (flow.step === 'time_manual') {
      const parsed = parseTimeHhmm(raw);
      if (!parsed) {
        await renderScreen(ctx, {
          title: labels.reminders.newTitle(),
          bodyLines: [labels.reminders.newInvalidTime(), labels.reminders.timeManualPrompt()]
        });
        return;
      }
      const nextDraft = { ...flow.draft, localTime: parsed.hhmm, timeMinutes: parsed.minutes };
      setReminderFlow(stateKey, { ...flow, draft: nextDraft });
      await persistReminderSchedule(ctx, { ...flow, draft: nextDraft });
      return;
    }

    if (flow.step === 'title') {
      const title = raw;
      if (!title) {
        await renderReminderTitlePrompt(ctx, flow.mode, getReminderIdFromFlow(flow));
        return;
      }

      if (flow.mode === 'edit') {
        await updateReminder(flow.reminderId, { title });
        clearReminderFlow(stateKey);
        await renderReminderDetails(ctx, flow.reminderId, labels.reminders.editSaved());
        return;
      }
      const timezone = user.timezone ?? config.defaultTimezone;
      const draft = flow.draft;
      const reminder = await createReminderDraft({ userId: user.id, title, timezone });
      if (title && reminder.title !== title) {
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.titleSaveFailed()] });
        return;
      }
      setReminderFlow(stateKey, { ...flow, reminderId: reminder.id, step: 'description', draft: { ...draft, title } });
      await renderReminderDescriptionPrompt(ctx, flow.mode, reminder.id);
      return;
    }

    if (flow.step === 'description') {
      const description = raw === '-' || raw.length === 0 ? null : raw;
      if (flow.mode === 'edit') {
        await updateReminder(flow.reminderId, { description });
        await maybeArchiveReminderDescription(ctx, flow.reminderId, description);
        clearReminderFlow(stateKey);
        await renderReminderDetails(ctx, flow.reminderId, labels.reminders.editSaved());
        return;
      }
      const reminderId = getReminderIdFromFlow(flow);
      if (reminderId) {
        await updateReminder(reminderId, { description });
        await maybeArchiveReminderDescription(ctx, reminderId, description);
      }
      setReminderFlow(stateKey, { ...flow, step: 'schedule_type', draft: { ...flow.draft, description } });
      await renderReminderScheduleTypePrompt(ctx, flow.mode, reminderId);
      return;
    }

    if (flow.step === 'caption_all') {
      const caption = text.length > 0 ? rawText : null;
      const updatedAttachments = applyCaptionToReminderAttachments(flow.draft.attachments ?? [], [...NOTE_ATTACHMENT_KINDS], caption);
      const nextFlow: ReminderFlow = { ...flow, draft: { ...flow.draft, attachments: updatedAttachments } };
      setReminderFlow(stateKey, nextFlow);
      const reminderId = getReminderIdFromFlow(flow);
      if (reminderId) {
        await finalizeReminderArchive(ctx, reminderId, nextFlow);
      } else {
        await renderReminders(ctx);
      }
      return;
    }

    if (flow.step === 'caption_category' && flow.currentCategory) {
      const category = flow.currentCategory;
      const kinds =
        category === 'files'
          ? (['document', 'audio'] as ReminderAttachmentKind[])
          : ([category] as ReminderAttachmentKind[]);
      const updatedAttachments = applyCaptionToReminderAttachments(flow.draft.attachments ?? [], kinds, text.length > 0 ? rawText : null);
      const remaining = (flow.captionCategories ?? []).filter((item) => item !== category);
      const nextFlow: ReminderFlow = { ...flow, draft: { ...flow.draft, attachments: updatedAttachments } };
      if (remaining.length === 0) {
        setReminderFlow(stateKey, nextFlow);
        const reminderId = getReminderIdFromFlow(flow);
        if (reminderId) {
          await finalizeReminderArchive(ctx, reminderId, nextFlow);
        } else {
          await renderReminders(ctx);
        }
        return;
      }
      setReminderFlow(stateKey, { ...nextFlow, captionCategories: remaining });
      const reminderId = getReminderIdFromFlow(flow);
      if (reminderId) {
        await promptReminderCaptionCategory(ctx, reminderId, remaining[0], remaining);
      } else {
        await renderReminders(ctx);
      }
      return;
    }

    if (flow.step === 'interval_minutes') {
      const interval = Number(raw);
      if (!Number.isInteger(interval) || interval <= 0) {
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.intervalInvalid()] });
        return;
      }
      const nextDraft = { ...flow.draft, intervalMinutes: interval };
      setReminderFlow(stateKey, { ...flow, draft: nextDraft });
      await persistReminderSchedule(ctx, { ...flow, draft: nextDraft });
      return;
    }

    if (flow.step === 'daily_time' || flow.step === 'weekly_time' || flow.step === 'monthly_time' || flow.step === 'yearly_time') {
      const parsed = parseTimeHhmm(raw);
      if (!parsed) {
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.newInvalidTime()] });
        return;
      }
      const nextDraft = { ...flow.draft, atTime: parsed.hhmm };
      setReminderFlow(stateKey, { ...flow, draft: nextDraft });
      await persistReminderSchedule(ctx, { ...flow, draft: nextDraft });
      return;
    }

    if (flow.step === 'monthly_day') {
      const day = Number(raw);
      if (!Number.isInteger(day) || day < 1 || day > 31) {
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.monthlyDayInvalid()] });
        return;
      }
      const nextDraft = { ...flow.draft, byMonthday: day };
      setReminderFlow(stateKey, { ...flow, step: 'monthly_time', draft: nextDraft });
      await renderReminderDailyTimePrompt(ctx);
      return;
    }

    if (flow.step === 'yearly_day') {
      const day = Number(raw);
      if (!Number.isInteger(day) || day < 1 || day > 31) {
        await renderScreen(ctx, { title: labels.reminders.newTitle(), bodyLines: [labels.reminders.monthlyDayInvalid()] });
        return;
      }
      const nextDraft = { ...flow.draft, byMonthday: day };
      setReminderFlow(stateKey, { ...flow, step: 'yearly_time', draft: nextDraft });
      await renderReminderDailyTimePrompt(ctx);
      return;
    }
  }

  if (state.templateCreate) {
    const trimmed = text.trim();
    const telegramId = stateKey;
    if (!trimmed) {
      const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.templates' });
      const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
      await renderScreen(ctx, {
        titleKey: t('screens.templates.new_title'),
        bodyLines: [t('screens.templates.new_prompt')],
        inlineKeyboard: kb
      });
      return;
    }
    try {
      const { user: u } = await ensureUserAndSettings(ctx);
      const newTemplate = await createUserTemplate({ userId: u.id, title: trimmed });
      clearTemplateCreateFlow(telegramId);
      clearReportContextCache();
      await renderTemplateEdit(ctx, newTemplate.id);
      return;
    } catch (error) {
      console.error({ scope: 'daily_report', event: 'template_new_failed', error, title: trimmed });
      clearTemplateCreateFlow(telegramId);
      await renderTemplatesScreen(ctx);
      return;
    }
  }

  if (state.templateRename) {
    const { templateId } = state.templateRename;
    const trimmed = text.trim();
    if (!trimmed) {
      const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'dr.template_actions', data: { templateId } });
      const kb = new InlineKeyboard().text(backBtn.text, backBtn.callback_data);
      await renderScreen(ctx, {
        titleKey: t('screens.daily_report.templates_rename_title'),
        bodyLines: [t('screens.daily_report.templates_rename_prompt')],
        inlineKeyboard: kb
      });
      return;
    }
    const { user: u } = await ensureUserAndSettings(ctx);
    await updateTemplateTitle({ templateId, userId: u.id, title: trimmed });
    clearTemplateRenameFlow(stateKey);
    clearReportContextCache();
    await renderTemplateActions(ctx, templateId);
    return;
  }

  const routineFlow = state.routineFlow;
  if (routineFlow) {
    const telegramId = stateKey;
    if (routineFlow.step === 'title') {
      if (!text) {
        await promptRoutineTitle(ctx, { routineId: routineFlow.routineId });
        return;
      }
      if (routineFlow.mode === 'create') {
        const draft = { ...routineFlow.draft, title: text };
        setRoutineFlow(telegramId, { ...routineFlow, draft, step: 'description' });
        await promptRoutineDescription(ctx, {});
        return;
      }
      if (routineFlow.mode === 'edit' && routineFlow.routineId) {
        await updateRoutine(routineFlow.routineId, { title: text });
        clearRoutineFlow(telegramId);
        clearReportContextCache();
        await renderRoutineDetails(ctx, routineFlow.routineId, t('screens.routines.saved'));
        return;
      }
    }

    if (routineFlow.step === 'description') {
      const description = text === '-' || text.toLowerCase() === '/skip' ? null : text;
      if (routineFlow.mode === 'create') {
        const draft = { ...routineFlow.draft, description };
        setRoutineFlow(telegramId, { ...routineFlow, draft, step: 'type' });
        const kb = await buildRoutineTypeKeyboard(ctx);
        const backBtn = await makeActionButton(ctx, { label: t('buttons.back'), action: 'routines.root' });
        kb.text(backBtn.text, backBtn.callback_data);
        await renderScreen(ctx, { titleKey: t('screens.routines.title'), bodyLines: [t('screens.routines.choose_type')], inlineKeyboard: kb });
        return;
      }
      if (routineFlow.mode === 'edit' && routineFlow.routineId) {
        await updateRoutine(routineFlow.routineId, { description });
        clearRoutineFlow(telegramId);
        clearReportContextCache();
        await renderRoutineDetails(ctx, routineFlow.routineId, t('screens.routines.saved'));
        return;
      }
    }

    if (routineFlow.step === 'xp_value') {
      const xpVal = Number(text);
      if (!Number.isInteger(xpVal)) {
        await ctx.reply(t('screens.daily_report.invalid_number'));
        return;
      }
      if (routineFlow.mode === 'create') {
        const draft = { ...routineFlow.draft, xpValue: xpVal };
        if (draft.xpMode === 'per_minute' || draft.xpMode === 'per_number') {
          setRoutineFlow(telegramId, { ...routineFlow, draft, step: 'xp_max' });
          await promptRoutineXpMax(ctx, {});
          return;
        }
        setRoutineFlow(telegramId, { ...routineFlow, draft, step: 'confirm' });
        await promptRoutineConfirm(ctx, { ...routineFlow, draft, step: 'confirm' });
        return;
      }
      if (routineFlow.mode === 'edit' && routineFlow.routineId) {
        const xpMode = routineFlow.draft.xpMode ?? 'none';
        if (xpMode === 'per_minute' || xpMode === 'per_number') {
          const draft = { ...routineFlow.draft, xpValue: xpVal };
          setRoutineFlow(telegramId, { ...routineFlow, draft, step: 'xp_max' });
          await promptRoutineXpMax(ctx, { routineId: routineFlow.routineId });
          return;
        }
        await updateRoutine(routineFlow.routineId, { xpMode, xpValue: xpVal, xpMaxPerDay: xpMode === 'none' ? null : routineFlow.draft.xpMaxPerDay ?? null });
        clearRoutineFlow(telegramId);
        clearReportContextCache();
        await renderRoutineDetails(ctx, routineFlow.routineId, t('screens.routines.saved'));
        return;
      }
    }

    if (routineFlow.step === 'xp_max') {
      const xpMax = Number(text);
      const parsedMax = Number.isInteger(xpMax) && xpMax > 0 ? xpMax : null;
      if (routineFlow.mode === 'create') {
        const draft = { ...routineFlow.draft, xpMaxPerDay: parsedMax };
        setRoutineFlow(telegramId, { ...routineFlow, draft, step: 'confirm' });
        await promptRoutineConfirm(ctx, { ...routineFlow, draft, step: 'confirm' });
        return;
      }
      if (routineFlow.mode === 'edit' && routineFlow.routineId) {
        await updateRoutine(routineFlow.routineId, { xpMode: routineFlow.draft.xpMode, xpValue: routineFlow.draft.xpValue, xpMaxPerDay: parsedMax ?? null });
        clearRoutineFlow(telegramId);
        clearReportContextCache();
        await renderRoutineDetails(ctx, routineFlow.routineId, t('screens.routines.saved'));
        return;
      }
    }
  }

  const routineTaskFlow = state.routineTaskFlow;
  if (routineTaskFlow) {
    const telegramId = stateKey;
    const saveTask = async (draft: RoutineTaskFlow['draft']): Promise<void> => {
      const xpMode = normalizeXpModeForItemType(draft.itemType, draft.xpMode ?? null) ?? 'none';
      const ratioOpts = draft.optionsJson ?? {};
      const ratioPerRaw = Number((ratioOpts as { per?: unknown; perNumber?: unknown }).per ?? (ratioOpts as { perNumber?: unknown }).perNumber);
      const ratioXpRaw = Number((ratioOpts as { xp?: unknown; xpPerUnit?: unknown }).xp ?? (ratioOpts as { xpPerUnit?: unknown }).xpPerUnit);
      const normalizedPer = Number.isFinite(ratioPerRaw) && ratioPerRaw > 0 ? ratioPerRaw : 1;
      const normalizedXp = Number.isFinite(ratioXpRaw) && ratioXpRaw >= 0 ? ratioXpRaw : draft.xpValue ?? 0;
      const payload = {
        title: draft.title ?? t('screens.routine_tasks.default_title'),
        description: draft.description ?? null,
        itemType: draft.itemType ?? 'boolean',
        xpMode,
        xpValue: xpMode === 'none' ? null : normalizedXp,
        xpMaxPerDay: xpMode === 'per_minute' || xpMode === 'per_number' ? draft.xpMaxPerDay ?? null : null,
        optionsJson: xpMode === 'none' ? {} : { per: normalizedPer, xp: normalizedXp }
      };
      if (routineTaskFlow.mode === 'edit' && routineTaskFlow.taskId) {
        await updateRoutineTask(routineTaskFlow.taskId, payload);
      } else {
        await createRoutineTask({ routineId: routineTaskFlow.routineId, ...payload });
      }
      clearRoutineTaskFlow(telegramId);
      clearReportContextCache();
      await renderRoutineTasks(ctx, routineTaskFlow.routineId, t('screens.routines.saved'));
    };

    if (routineTaskFlow.step === 'title') {
      if (!text) {
        await promptRoutineTaskTitle(ctx, { routineId: routineTaskFlow.routineId, taskId: routineTaskFlow.taskId });
        return;
      }
      const draft = { ...routineTaskFlow.draft, title: text };
      setRoutineTaskFlow(telegramId, { ...routineTaskFlow, draft, step: 'description' });
      await promptRoutineTaskDescription(ctx, { routineId: routineTaskFlow.routineId, taskId: routineTaskFlow.taskId });
      return;
    }

    if (routineTaskFlow.step === 'description') {
      const description = text === '-' || text.toLowerCase() === '/skip' ? null : text;
      const draft = { ...routineTaskFlow.draft, description };
      setRoutineTaskFlow(telegramId, { ...routineTaskFlow, draft, step: 'type' });
      await promptRoutineTaskType(ctx, { routineId: routineTaskFlow.routineId, taskId: routineTaskFlow.taskId });
      return;
    }

    if (routineTaskFlow.step === 'xp_value') {
      const xpMode = routineTaskFlow.draft.xpMode ?? 'none';
      if (xpMode === 'fixed') {
        const xpVal = parseNonNegativeNumber(text);
        if (xpVal === null) {
          await ctx.reply(t('screens.daily_report.invalid_number'));
          return;
        }
        if (routineTaskFlow.draft.itemType === 'duration_minutes') {
          const draft = { ...routineTaskFlow.draft, xpValue: xpVal, optionsJson: { per: 1, xp: xpVal } };
          setRoutineTaskFlow(telegramId, { ...routineTaskFlow, draft, step: 'xp_max' });
          await promptRoutineTaskXpMax(ctx, { routineId: routineTaskFlow.routineId, taskId: routineTaskFlow.taskId });
          return;
        }
        await saveTask({ ...routineTaskFlow.draft, xpValue: xpVal, optionsJson: { per: 1, xp: xpVal } });
        return;
      }

      const ratio = parseXpRatio(text);
      if (!ratio) {
        await ctx.reply(t('screens.daily_report.invalid_number'));
        return;
      }
      if (xpMode === 'per_minute') {
        const draft = { ...routineTaskFlow.draft, xpValue: ratio.xp, optionsJson: { per: ratio.per, xp: ratio.xp } };
        setRoutineTaskFlow(telegramId, { ...routineTaskFlow, draft, step: 'xp_max' });
        await promptRoutineTaskXpMax(ctx, { routineId: routineTaskFlow.routineId, taskId: routineTaskFlow.taskId });
        return;
      }
      if (xpMode === 'per_number') {
        const draft = { ...routineTaskFlow.draft, xpValue: ratio.xp, optionsJson: { per: ratio.per, xp: ratio.xp } };
        setRoutineTaskFlow(telegramId, { ...routineTaskFlow, draft, step: 'xp_max' });
        await promptRoutineTaskXpMax(ctx, { routineId: routineTaskFlow.routineId, taskId: routineTaskFlow.taskId });
        return;
      }
      await saveTask({ ...routineTaskFlow.draft, xpValue: ratio.xp, optionsJson: { per: ratio.per, xp: ratio.xp } });
      return;
    }

    if (routineTaskFlow.step === 'xp_max') {
      const maxVal = Number(text);
      const xpMax = Number.isInteger(maxVal) && maxVal > 0 ? maxVal : null;
      await saveTask({ ...routineTaskFlow.draft, xpMaxPerDay: xpMax });
      return;
    }
  }

  const templateFlow = state.templateItemFlow;
  if (templateFlow) {
    const telegramId = stateKey;
    try {
      if (templateFlow.step === 'label') {
        if (!text) {
          await promptLabelInput(ctx, { templateId: templateFlow.templateId, backToItemId: templateFlow.itemId });
          return;
        }
        if (templateFlow.mode === 'create') {
          const itemKey = await generateUniqueItemKey(templateFlow.templateId, text);
          setTemplateItemFlow(telegramId, { ...templateFlow, draft: { ...templateFlow.draft, label: text, itemKey }, step: 'type' });
          await promptTypeSelection(ctx, { templateId: templateFlow.templateId });
          return;
        }
        if (!templateFlow.itemId) {
          clearTemplateItemFlow(telegramId);
          await renderTemplatesScreen(ctx);
          return;
        }
        await updateItem(templateFlow.itemId, { label: text });
        clearTemplateItemFlow(telegramId);
        clearReportContextCache();
        await renderTemplateItemMenu(ctx, templateFlow.templateId, templateFlow.itemId, t('screens.daily_report.item_saved'));
        return;
      }

      if (templateFlow.step === 'category_custom') {
        const customName = text.trim();
        if (!customName) {
          await promptCustomCategoryInput(ctx, { templateId: templateFlow.templateId, itemId: templateFlow.itemId });
          return;
        }
        if (templateFlow.mode === 'create') {
          const draft = { ...templateFlow.draft, category: customName };
          const nextFlow = { ...templateFlow, draft, step: 'category' } as TemplateItemFlow;
          await renderStepSummary(ctx, { templateId: templateFlow.templateId, stage: 'category', flow: nextFlow });
          return;
        }
        if (!templateFlow.itemId) {
          clearTemplateItemFlow(telegramId);
          await renderTemplateEdit(ctx, templateFlow.templateId);
          return;
        }
        await updateItem(templateFlow.itemId, { category: customName });
        clearTemplateItemFlow(telegramId);
        clearReportContextCache();
        await renderTemplateItemMenu(ctx, templateFlow.templateId, templateFlow.itemId, t('screens.daily_report.item_saved'));
        return;
      }

      if (templateFlow.step === 'key') {
        const cleanedKey = slugifyItemKey(text);
        if (!cleanedKey) {
          await promptKeyInput(ctx, { templateId: templateFlow.templateId, itemId: templateFlow.itemId as string });
          return;
        }
        const items = await listAllItems(templateFlow.templateId);
        const duplicate = items.some((i) => i.item_key === cleanedKey && i.id !== templateFlow.itemId);
        if (duplicate) {
          await ctx.reply(t('screens.daily_report.duplicate_key'));
          return;
        }
        if (!templateFlow.itemId) {
          clearTemplateItemFlow(telegramId);
          await renderTemplateEdit(ctx, templateFlow.templateId);
          return;
        }
        await updateItem(templateFlow.itemId, { item_key: cleanedKey });
        clearTemplateItemFlow(telegramId);
        clearReportContextCache();
        await renderTemplateItemMenu(ctx, templateFlow.templateId, templateFlow.itemId, t('screens.daily_report.item_saved'));
        return;
      }

      if (templateFlow.step === 'xp_value') {
        const xpMode = templateFlow.draft.xpMode ?? 'none';
        if (xpMode === 'per_number') {
          const ratioMatch = text.trim().match(/^(\d+)\s*:\s*(\d+)$/);
          if (!ratioMatch) {
            await ctx.reply(t('screens.templates.invalid_ratio'));
            return;
          }
          const perNumber = Number(ratioMatch[1]);
          const xpPerUnit = Number(ratioMatch[2]);
          if (!Number.isInteger(perNumber) || perNumber <= 0 || !Number.isInteger(xpPerUnit) || xpPerUnit <= 0) {
            await ctx.reply(t('screens.templates.invalid_ratio'));
            return;
          }
          const draft = {
            ...templateFlow.draft,
            xpValue: xpPerUnit,
            optionsJson: { ...(templateFlow.draft.optionsJson ?? {}), perNumber, xpPerUnit }
          };
          setTemplateItemFlow(telegramId, { ...templateFlow, draft, step: 'xp_max' });
          await promptXpMaxInput(ctx, { templateId: templateFlow.templateId, itemId: templateFlow.itemId });
          return;
        }
        const xpVal = Number(text);
        if (!Number.isInteger(xpVal)) {
          await ctx.reply(t('screens.daily_report.invalid_number'));
          return;
        }
        if (xpMode === 'per_minute') {
          const draft = { ...templateFlow.draft, xpValue: xpVal };
          setTemplateItemFlow(telegramId, { ...templateFlow, draft, step: 'xp_max' });
          await promptXpMaxInput(ctx, { templateId: templateFlow.templateId, itemId: templateFlow.itemId });
          return;
        }
        if (templateFlow.mode === 'create') {
          const draft = { ...templateFlow.draft, xpValue: xpVal };
          const nextFlow = { ...templateFlow, draft, step: 'xp_value' } as TemplateItemFlow;
          await renderStepSummary(ctx, { templateId: templateFlow.templateId, stage: 'xp', flow: nextFlow });
          return;
        }
        if (!templateFlow.itemId) {
          clearTemplateItemFlow(telegramId);
          await renderTemplateEdit(ctx, templateFlow.templateId);
          return;
        }
        const xpPayload = deriveXpStorage({ ...templateFlow.draft, xpValue: xpVal });
        await updateItem(templateFlow.itemId, {
          xp_mode: xpPayload.xpMode,
          xp_value: xpPayload.xpValue,
          xp_max_per_day: xpPayload.xpMax,
          options_json: xpPayload.optionsJson
        });
        clearTemplateItemFlow(telegramId);
        clearReportContextCache();
        await renderTemplateItemMenu(ctx, templateFlow.templateId, templateFlow.itemId, t('screens.daily_report.item_saved'));
        return;
      }

      if (templateFlow.step === 'xp_max') {
        const maxVal = Number(text);
        const xpMax = Number.isInteger(maxVal) && maxVal > 0 ? maxVal : null;
        if (templateFlow.mode === 'create') {
          const draft = { ...templateFlow.draft, xpMaxPerDay: xpMax };
          const nextFlow = { ...templateFlow, draft, step: 'xp_max' } as TemplateItemFlow;
          await renderStepSummary(ctx, { templateId: templateFlow.templateId, stage: 'xp', flow: nextFlow });
          return;
        }
        if (!templateFlow.itemId) {
          clearTemplateItemFlow(telegramId);
          await renderTemplateEdit(ctx, templateFlow.templateId);
          return;
        }
        const xpPayload = deriveXpStorage({ ...templateFlow.draft, xpMaxPerDay: xpMax });
        await updateItem(templateFlow.itemId, {
          xp_mode: xpPayload.xpMode,
          xp_value: xpPayload.xpValue,
          xp_max_per_day: xpPayload.xpMax,
          options_json: xpPayload.optionsJson
        });
        clearTemplateItemFlow(telegramId);
        clearReportContextCache();
        await renderTemplateItemMenu(ctx, templateFlow.templateId, templateFlow.itemId, t('screens.daily_report.item_saved'));
        return;
      }
    } catch (error) {
      console.error({ scope: 'daily_report', event: 'template_flow_error', error, templateFlow });
      clearTemplateItemFlow(telegramId);
      await ctx.reply(t('errors.unexpected'));
      return;
    }
  }

  // 3) Reward Store edit flow
  if (state.rewardEdit) {
    const flow = state.rewardEdit;

    // Handle confirm_delete step (text input ignored)
    if (flow.step === 'confirm_delete') {
      await renderRewardStoreEditorRoot(ctx);
      return;
    }

    if (flow.step === 'title') {
      if (flow.mode === 'create') {
        userStates.set(stateKey, { ...state, rewardEdit: { ...flow, step: 'description', draft: { ...flow.draft, title: text } } });
        const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'rewards.edit_root' });
        await renderScreen(ctx, { titleKey: t('screens.rewards.add_title'), bodyLines: [t('screens.rewards.ask_description')], inlineKeyboard: new InlineKeyboard().text(cancelBtn.text, cancelBtn.callback_data) });
        return;
      }

      if (!flow.rewardId) {
        userStates.set(stateKey, { ...state, rewardEdit: undefined });
        await renderRewardStoreEditorRoot(ctx);
        return;
      }

      const updated = await updateReward({ rewardId: flow.rewardId, patch: { title: text } });
      userStates.set(stateKey, { ...state, rewardEdit: undefined });
      await renderRewardEditMenu(ctx, updated);
      return;
    }

    if (flow.step === 'description') {
      const desc = text === '-' ? null : text;

      if (flow.mode === 'create') {
        userStates.set(stateKey, { ...state, rewardEdit: { ...flow, step: 'xp', draft: { ...flow.draft, description: desc } } });
        const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: 'rewards.edit_root' });
        await renderScreen(ctx, { titleKey: t('screens.rewards.add_title'), bodyLines: [t('screens.rewards.ask_xp_cost')], inlineKeyboard: new InlineKeyboard().text(cancelBtn.text, cancelBtn.callback_data) });
        return;
      }

      if (!flow.rewardId) {
        userStates.set(stateKey, { ...state, rewardEdit: undefined });
        await renderRewardStoreEditorRoot(ctx);
        return;
      }

      const updated = await updateReward({ rewardId: flow.rewardId, patch: { description: desc } });
      userStates.set(stateKey, { ...state, rewardEdit: undefined });
      await renderRewardEditMenu(ctx, updated);
      return;
    }

    if (flow.step === 'xp') {
      const xp = Number(text);
      if (!Number.isInteger(xp) || xp <= 0) {
        const cancelAction = flow.mode === 'create' ? 'rewards.edit_root' : 'rewards.edit_open';
        const cancelData = flow.mode === 'create' ? {} : { rewardId: flow.rewardId };
        const cancelBtn = await makeActionButton(ctx, { label: t('buttons.cancel'), action: cancelAction, data: cancelData });
        await renderScreen(ctx, { titleKey: t('screens.rewards.add_title'), bodyLines: [t('screens.rewards.invalid_xp_cost')], inlineKeyboard: new InlineKeyboard().text(cancelBtn.text, cancelBtn.callback_data) });
        return;
      }

      if (flow.mode === 'create') {
        const { user } = await ensureUserAndSettings(ctx);
        await createReward({ userId: user.id, title: flow.draft.title ?? 'Reward', description: flow.draft.description ?? null, xpCost: xp });
        userStates.set(stateKey, { ...state, rewardEdit: undefined });
        await renderRewardStoreEditorRoot(ctx);
        return;
      }

      if (!flow.rewardId) {
        userStates.set(stateKey, { ...state, rewardEdit: undefined });
        await renderRewardStoreEditorRoot(ctx);
        return;
      }

      const updated = await updateReward({ rewardId: flow.rewardId, patch: { xpCost: xp } });
      userStates.set(stateKey, { ...state, rewardEdit: undefined });
      await renderRewardEditMenu(ctx, updated);
      return;
    }

    userStates.set(stateKey, { ...state, rewardEdit: undefined });
    await renderRewardStoreEditorRoot(ctx);
  }
});

bot.on('message:photo', async (ctx: Context) => {
  const photos = ctx.message?.photo;
  if (!photos || photos.length === 0) return;
  const photo = photos[photos.length - 1];
  await handleNoteAttachmentMessage(ctx, {
    kind: 'photo',
    fileId: photo.file_id,
    fileUniqueId: photo.file_unique_id,
    caption: ctx.message?.caption ?? null
  });
  await handleReminderAttachmentMessage(ctx, {
    kind: 'photo',
    fileId: photo.file_id,
    fileUniqueId: photo.file_unique_id,
    caption: ctx.message?.caption ?? null
  });
});

bot.on('message:video', async (ctx: Context) => {
  const video = ctx.message?.video;
  if (!video) return;
  await handleNoteAttachmentMessage(ctx, {
    kind: 'video',
    fileId: video.file_id,
    fileUniqueId: video.file_unique_id,
    caption: ctx.message?.caption ?? null
  });
  await handleReminderAttachmentMessage(ctx, {
    kind: 'video',
    fileId: video.file_id,
    fileUniqueId: video.file_unique_id,
    caption: ctx.message?.caption ?? null,
    mimeType: video.mime_type ?? null
  });
});

bot.on('message:voice', async (ctx: Context) => {
  const voice = ctx.message?.voice;
  if (!voice) return;
  await handleNoteAttachmentMessage(ctx, {
    kind: 'voice',
    fileId: voice.file_id,
    fileUniqueId: voice.file_unique_id
  });
  await handleReminderAttachmentMessage(ctx, {
    kind: 'voice',
    fileId: voice.file_id,
    fileUniqueId: voice.file_unique_id,
    mimeType: voice.mime_type ?? null
  });
});

bot.on('message:document', async (ctx: Context) => {
  const document = ctx.message?.document;
  if (!document) return;
  await handleNoteAttachmentMessage(ctx, {
    kind: 'document',
    fileId: document.file_id,
    fileUniqueId: document.file_unique_id,
    caption: ctx.message?.caption ?? null
  });
  await handleReminderAttachmentMessage(ctx, {
    kind: 'document',
    fileId: document.file_id,
    fileUniqueId: document.file_unique_id,
    caption: ctx.message?.caption ?? null,
    mimeType: document.mime_type ?? null
  });
});

bot.on('message:audio', async (ctx: Context) => {
  const audio = ctx.message?.audio;
  if (!audio) return;
  await handleNoteAttachmentMessage(ctx, {
    kind: 'audio',
    fileId: audio.file_id,
    fileUniqueId: audio.file_unique_id,
    caption: ctx.message?.caption ?? null
  });
  await handleReminderAttachmentMessage(ctx, {
    kind: 'audio',
    fileId: audio.file_id,
    fileUniqueId: audio.file_unique_id,
    caption: ctx.message?.caption ?? null,
    mimeType: audio.mime_type ?? null
  });
});

bot.on('message:video_note', async (ctx: Context) => {
  const videoNote = ctx.message?.video_note;
  if (!videoNote) return;
  await handleNoteAttachmentMessage(ctx, {
    kind: 'video_note',
    fileId: videoNote.file_id,
    fileUniqueId: videoNote.file_unique_id
  });
  await handleReminderAttachmentMessage(ctx, {
    kind: 'video_note',
    fileId: videoNote.file_id,
    fileUniqueId: videoNote.file_unique_id
  });
});

bot.catch((err: BotError<Context>) => {
  const { ctx, error } = err;
  const errorMessage = error instanceof Error ? error.message : String(error);
  const updateType = ctx.update ? Object.keys(ctx.update)[0] : undefined;
  const chatId = typeof ctx.chat?.id === 'number' ? ctx.chat.id : undefined;
  const updatePayload = ctx.update ? { updateId: ctx.update.update_id, updateType } : undefined;
  logError('Telegram bot error', { error: errorMessage, update: updatePayload });
  void logReporter.report('error', 'Telegram bot error', {
    stack: error instanceof Error ? error.stack : undefined,
    context: {
      updateId: ctx.update?.update_id,
      updateType,
      chatId,
      handler: error instanceof GrammyError ? error.method : undefined
    }
  });
});
