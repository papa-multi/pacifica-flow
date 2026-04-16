"use strict";

const fs = require("fs");
const path = require("path");
const { ensureParent, readJson, writeJsonAtomic } = require("../nextgen/core/json-file");

function defaultState(projectKey) {
  return {
    schemaVersion: 1,
    projectKey: projectKey || "pacifica-flow",
    offset: 0,
    startedAt: Date.now(),
    updatedAt: Date.now(),
    projects: {},
    chats: {},
  };
}

function readState(stateFile, projectKey) {
  const fallback = defaultState(projectKey);
  try {
    const payload = readJson(stateFile, fallback);
    if (!payload || typeof payload !== "object") return fallback;
    payload.schemaVersion = Number(payload.schemaVersion || 1);
    payload.projectKey = String(payload.projectKey || projectKey || "pacifica-flow");
    payload.offset = Number(payload.offset || 0);
    payload.projects = payload.projects && typeof payload.projects === "object" ? payload.projects : {};
    payload.chats = payload.chats && typeof payload.chats === "object" ? payload.chats : {};
    return payload;
  } catch (_) {
    return fallback;
  }
}

function writeState(stateFile, value) {
  ensureParent(stateFile);
  writeJsonAtomic(stateFile, value);
}

function ensureProjectProjectState(state, projectKey) {
  if (!state.projects || typeof state.projects !== "object") state.projects = {};
  if (!state.projects[projectKey] || typeof state.projects[projectKey] !== "object") {
    state.projects[projectKey] = {
      brief: "",
      notes: [],
      updatedAt: Date.now(),
    };
  }
  const projectState = state.projects[projectKey];
  projectState.brief = String(projectState.brief || "");
  projectState.notes = Array.isArray(projectState.notes) ? projectState.notes : [];
  projectState.updatedAt = Number(projectState.updatedAt || Date.now());
  return projectState;
}

function ensureChatState(state, chatId, projectKey) {
  const chatKey = String(chatId);
  if (!state.chats || typeof state.chats !== "object") state.chats = {};
  if (!state.chats[chatKey] || typeof state.chats[chatKey] !== "object") {
    state.chats[chatKey] = {
      chatId: chatKey,
      projectKey,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      mode: "idle",
      summary: "",
      decisions: [],
      preferences: {},
      recentMessages: [],
      watchedWallets: [],
      lastAction: "",
      lastPrompt: "",
      activeWorkflow: "",
      lastAlertAt: 0,
      pendingWallet: "",
    };
  }
  const chat = state.chats[chatKey];
  chat.chatId = chatKey;
  chat.projectKey = projectKey;
  chat.mode = String(chat.mode || "idle");
  chat.summary = String(chat.summary || "");
  chat.decisions = Array.isArray(chat.decisions) ? chat.decisions : [];
  chat.preferences = chat.preferences && typeof chat.preferences === "object" ? chat.preferences : {};
  chat.recentMessages = Array.isArray(chat.recentMessages) ? chat.recentMessages : [];
  chat.watchedWallets = Array.isArray(chat.watchedWallets) ? chat.watchedWallets : [];
  chat.lastAlertAt = Number(chat.lastAlertAt || 0);
  chat.pendingWallet = String(chat.pendingWallet || "");
  chat.updatedAt = Number(chat.updatedAt || Date.now());
  return chat;
}

function trimArray(items, limit) {
  const cap = Math.max(0, Number(limit || 0));
  if (!cap) return [];
  return items.slice(-cap);
}

function appendRecentMessage(chat, entry, limit) {
  if (!Array.isArray(chat.recentMessages)) chat.recentMessages = [];
  chat.recentMessages.push({
    role: entry.role,
    text: String(entry.text || ""),
    intent: String(entry.intent || ""),
    ts: Number(entry.ts || Date.now()),
  });
  chat.recentMessages = trimArray(chat.recentMessages, limit);
}

function mergeCopilotUpdate(chat, update, limits) {
  const limit = limits && typeof limits === "object" ? limits : {};
  const recentLimit = Math.max(4, Number(limit.recentMessagesLimit || 16));
  const decisionsLimit = Math.max(8, Number(limit.decisionsLimit || 60));
  if (update && typeof update === "object") {
    if (update.summaryUpdate) {
      const nextSummary = String(update.summaryUpdate).trim();
      if (nextSummary) {
        chat.summary = nextSummary;
      }
    }
    if (Array.isArray(update.decisions) && update.decisions.length) {
      const seen = new Set(chat.decisions.map((line) => String(line).trim()).filter(Boolean));
      for (const decision of update.decisions) {
        const text = String(decision || "").trim();
        if (!text || seen.has(text)) continue;
        chat.decisions.push(text);
        seen.add(text);
      }
      chat.decisions = trimArray(chat.decisions, decisionsLimit);
    }
    if (update.nextStep) {
      chat.lastAction = String(update.nextStep).trim();
    }
    if (update.intent) {
      chat.activeWorkflow = String(update.intent).trim();
    }
  }
  if (Array.isArray(chat.recentMessages)) {
    chat.recentMessages = trimArray(chat.recentMessages, recentLimit);
  }
  chat.updatedAt = Date.now();
}

function setProjectBrief(state, projectKey, briefText) {
  const projectState = ensureProjectProjectState(state, projectKey);
  projectState.brief = String(briefText || "").trim();
  projectState.updatedAt = Date.now();
  return projectState;
}

function appendSessionDecision(chat, decisionText, limits) {
  const limit = limits && typeof limits === "object" ? limits : {};
  const decisionsLimit = Math.max(8, Number(limit.decisionsLimit || 60));
  const text = String(decisionText || "").trim();
  if (!text) return chat;
  if (!Array.isArray(chat.decisions)) chat.decisions = [];
  const next = text;
  if (!chat.decisions.includes(next)) {
    chat.decisions.push(next);
  }
  chat.decisions = trimArray(chat.decisions, decisionsLimit);
  chat.updatedAt = Date.now();
  return chat;
}

function normalizeWatchedWalletEntry(entry) {
  const safe = entry && typeof entry === "object" ? entry : {};
  const wallet = String(safe.wallet || "").trim();
  if (!wallet) return null;
  return {
    wallet,
    label: String(safe.label || "").trim(),
    createdAt: Number(safe.createdAt || Date.now()),
    updatedAt: Number(safe.updatedAt || Date.now()),
    baselineSetAt: Number(safe.baselineSetAt || 0),
    lastCheckAt: Number(safe.lastCheckAt || 0),
    lastStateChangeAt: Number(safe.lastStateChangeAt || 0),
    lastSnapshotAt: Number(safe.lastSnapshotAt || 0),
    lastSeenKeys: Array.isArray(safe.lastSeenKeys) ? safe.lastSeenKeys.map((value) => String(value || "").trim()).filter(Boolean) : [],
    lastSeenPositions: Array.isArray(safe.lastSeenPositions)
      ? safe.lastSeenPositions.filter(Boolean)
      : [],
    baselineKeys: Array.isArray(safe.baselineKeys) ? safe.baselineKeys.map((value) => String(value || "").trim()).filter(Boolean) : [],
    lastOpenCount: Number(safe.lastOpenCount || 0),
    firstTradeAt: Number(safe.firstTradeAt || 0),
    lastTradeAt: Number(safe.lastTradeAt || 0),
    recentTradeCount: Number(safe.recentTradeCount || 0),
    recentTradeVolumeUsd: Number(safe.recentTradeVolumeUsd || 0),
    alertPreferences: {
      enabled: Boolean(safe.alertPreferences && safe.alertPreferences.enabled),
      types: Array.isArray(safe.alertPreferences && safe.alertPreferences.types)
        ? Array.from(
            new Set(
              safe.alertPreferences.types
                .map((value) => String(value || "").trim().toLowerCase())
                .filter(Boolean)
            )
          )
        : [],
    },
    lastError: String(safe.lastError || "").trim(),
    muted: Boolean(safe.muted),
    lastEventAt: Number(safe.lastEventAt || 0),
  };
}

function ensureWatchedWallets(chat) {
  if (!chat || typeof chat !== "object") return [];
  if (!Array.isArray(chat.watchedWallets)) chat.watchedWallets = [];
  chat.watchedWallets = chat.watchedWallets.map(normalizeWatchedWalletEntry).filter(Boolean);
  return chat.watchedWallets;
}

function upsertWatchedWallet(chat, wallet, label) {
  const watchList = ensureWatchedWallets(chat);
  const normalizedWallet = String(wallet || "").trim();
  if (!normalizedWallet) return null;
  const existing = watchList.find((item) => item.wallet === normalizedWallet) || null;
  if (existing) {
    if (label !== undefined) existing.label = String(label || "").trim();
    existing.updatedAt = Date.now();
    return existing;
  }
  const created = normalizeWatchedWalletEntry({
    wallet: normalizedWallet,
    label: String(label || "").trim(),
    createdAt: Date.now(),
    updatedAt: Date.now(),
    baselineSetAt: 0,
    lastCheckAt: 0,
    lastStateChangeAt: 0,
    lastSnapshotAt: 0,
    lastSeenKeys: [],
    lastSeenPositions: [],
    baselineKeys: [],
    lastOpenCount: 0,
    firstTradeAt: 0,
    lastTradeAt: 0,
    recentTradeCount: 0,
    recentTradeVolumeUsd: 0,
    alertPreferences: {
      enabled: false,
      types: [],
    },
    lastError: "",
    muted: false,
    lastEventAt: 0,
  });
  watchList.push(created);
  return created;
}

function removeWatchedWallet(chat, wallet) {
  const normalizedWallet = String(wallet || "").trim();
  if (!normalizedWallet || !chat || typeof chat !== "object") return false;
  ensureWatchedWallets(chat);
  const before = chat.watchedWallets.length;
  chat.watchedWallets = chat.watchedWallets.filter((entry) => entry.wallet !== normalizedWallet);
  return chat.watchedWallets.length !== before;
}

function findWatchedWallet(chat, wallet) {
  const normalizedWallet = String(wallet || "").trim();
  if (!normalizedWallet || !chat || typeof chat !== "object") return null;
  ensureWatchedWallets(chat);
  return chat.watchedWallets.find((entry) => entry.wallet === normalizedWallet) || null;
}

function updateProjectNotes(state, projectKey, noteText, limits) {
  const projectState = ensureProjectProjectState(state, projectKey);
  const text = String(noteText || "").trim();
  if (!text) return projectState;
  const notesLimit = Math.max(4, Number(limits && limits.notesLimit ? limits.notesLimit : 40));
  projectState.notes.push({
    id: `note_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    text,
    createdAt: Date.now(),
  });
  projectState.notes = trimArray(projectState.notes, notesLimit);
  projectState.updatedAt = Date.now();
  return projectState;
}

module.exports = {
  appendSessionDecision,
  appendRecentMessage,
  defaultState,
  ensureChatState,
  ensureWatchedWallets,
  ensureProjectProjectState,
  findWatchedWallet,
  mergeCopilotUpdate,
  readState,
  setProjectBrief,
  removeWatchedWallet,
  updateProjectNotes,
  upsertWatchedWallet,
  writeState,
};
