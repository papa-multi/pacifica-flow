"use strict";

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function buildMainKeyboard(project) {
  const menu = project && project.menus && Array.isArray(project.menus.main) ? project.menus.main : [];
  const rows = [];
  if (menu.length) {
    rows.push(menu.slice(0, 2).map((label) => ({ text: label })));
    rows.push(menu.slice(2, 4).map((label) => ({ text: label })));
    rows.push(menu.slice(4, 6).map((label) => ({ text: label })));
    rows.push(menu.slice(6, 8).map((label) => ({ text: label })));
    rows.push(menu.slice(8, 10).map((label) => ({ text: label })));
  }
  return {
    keyboard: rows.filter((row) => row.length > 0),
    resize_keyboard: true,
    is_persistent: true,
  };
}

function buildInlineMenu(items) {
  return {
    inline_keyboard: items
      .map((row) =>
        row
          .map((item) => ({
            text: item.label,
            callback_data: item.data,
          }))
          .filter(Boolean)
      )
      .filter((row) => row.length > 0),
  };
}

function buildWorkflowKeyboard() {
  return buildInlineMenu([
    [
      { label: "Overview", data: "menu:overview" },
      { label: "Plan", data: "menu:plan" },
      { label: "Draft", data: "menu:draft" },
    ],
    [
      { label: "Rewrite", data: "menu:rewrite" },
      { label: "Summary", data: "menu:summary" },
      { label: "Context", data: "menu:context" },
    ],
    [
      { label: "Next step", data: "menu:next_steps" },
      { label: "Continue", data: "menu:continue" },
      { label: "Menu", data: "menu:home" },
    ],
  ]);
}

async function telegramApi(botToken, method, payload = null, timeoutMs = 60000) {
  if (!botToken) throw new Error("missing_bot_token");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: payload ? JSON.stringify(payload) : "{}",
      signal: controller.signal,
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || !body.ok) {
      throw new Error(body.description || `${method} failed ${response.status}`);
    }
    return body.result;
  } finally {
    clearTimeout(timer);
  }
}

async function sendMessage(botToken, chatId, text, opts = {}) {
  return telegramApi(
    botToken,
    "sendMessage",
    {
      chat_id: chatId,
      text: String(text || ""),
      disable_web_page_preview: true,
      reply_markup: opts.replyMarkup || undefined,
    },
    opts.timeoutMs || 60000
  );
}

async function editMessage(botToken, chatId, messageId, text, opts = {}) {
  return telegramApi(
    botToken,
    "editMessageText",
    {
      chat_id: chatId,
      message_id: messageId,
      text: String(text || ""),
      disable_web_page_preview: true,
      reply_markup: opts.replyMarkup || undefined,
    },
    opts.timeoutMs || 60000
  );
}

async function answerCallback(botToken, callbackQueryId, text = "") {
  return telegramApi(botToken, "answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text: String(text || ""),
    show_alert: false,
  });
}

async function setMyCommands(botToken, commands) {
  return telegramApi(botToken, "setMyCommands", {
    commands,
    scope: { type: "default" },
  });
}

module.exports = {
  answerCallback,
  buildInlineMenu,
  buildMainKeyboard,
  buildWorkflowKeyboard,
  editMessage,
  escapeHtml,
  sendMessage,
  setMyCommands,
  telegramApi,
};
