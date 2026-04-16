"use strict";

const path = require("path");

function readInt(name, fallback) {
  const raw = process.env[name];
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

module.exports = {
  rootDir: path.resolve(__dirname, "..", "..", "..", ".."),
  dataDir: path.resolve(__dirname, "..", "..", "..", "..", "data", "nextgen"),
  eventsDir: path.resolve(__dirname, "..", "..", "..", "..", "data", "nextgen", "events"),
  stateDir: path.resolve(__dirname, "..", "..", "..", "..", "data", "nextgen", "state"),
  fixturesDir: path.resolve(__dirname, "..", "..", "..", "..", "data", "nextgen", "fixtures"),
  apiBase: process.env.PACIFICA_API_BASE || "https://api.pacifica.fi/api/v1",
  wsUrl: process.env.PACIFICA_WS_URL || "wss://ws.pacifica.fi/ws",
  redisUrl: process.env.REDIS_URL || "redis://localhost:6379/0",
  postgresUrl:
    process.env.POSTGRES_URL || "postgres://postgres:postgres@localhost:5432/pacifica_nextgen",
  kafkaBrokers: process.env.KAFKA_BROKERS || "localhost:9092",
  walletShardCount: readInt("WALLET_SHARD_COUNT", 64),
  marketGatewayConnections: readInt("MARKET_GATEWAY_CONNECTIONS", 4),
  walletGatewayConnections: readInt("WALLET_GATEWAY_CONNECTIONS", 16),
  liveHeadMaxAgeMs: readInt("LIVE_HEAD_MAX_AGE_MS", 60_000),
  reconcileIntervalMs: readInt("RECONCILE_INTERVAL_MS", 300_000),
};
