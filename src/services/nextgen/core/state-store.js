"use strict";

const path = require("path");
const config = require("../common/config");
const { readJson, writeJsonAtomic } = require("./json-file");

const STATE_PATHS = {
  wallets: path.join(config.stateDir, "wallet-current-state.json"),
  positions: path.join(config.stateDir, "wallet-positions.json"),
  orders: path.join(config.stateDir, "wallet-orders.json"),
  trades: path.join(config.stateDir, "wallet-trade-summary.json"),
  live: path.join(config.stateDir, "wallet-live-status.json"),
  markets: path.join(config.stateDir, "market-state.json"),
  meta: path.join(config.stateDir, "meta.json"),
};

function emptyState() {
  return {
    wallets: {},
    positions: {},
    orders: {},
    trades: {},
    live: {},
    markets: {},
    meta: {
      generatedAt: 0,
      eventCount: 0,
      lastSequenceByEntity: {},
    },
  };
}

function loadState() {
  const state = emptyState();
  for (const [key, filePath] of Object.entries(STATE_PATHS)) {
    state[key] = readJson(filePath, state[key]);
  }
  return state;
}

function saveState(state) {
  const safe = state || emptyState();
  for (const [key, filePath] of Object.entries(STATE_PATHS)) {
    writeJsonAtomic(filePath, safe[key]);
  }
}

module.exports = {
  STATE_PATHS,
  emptyState,
  loadState,
  saveState,
};
