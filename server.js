const fs = require("fs");
const http = require("http");
const path = require("path");
const zlib = require("zlib");
const crypto = require("crypto");
const { URL } = require("url");
const speakeasy = require("speakeasy");
const QRCode = require("qrcode");
let nodemailer = null;
try {
  nodemailer = require("nodemailer");
} catch (_error) {
  nodemailer = null;
}

let createGeneralDataComponent = null;
let createWalletTrackingComponent = null;
let createWalletTrackingNextgenComponent = null;
let createCreatorStudioComponent = null;
let createLiveTradesHostComponent = null;

const { createClockSync } = require("./src/services/transport/clock_sync");
const { createRateLimitGuard } = require("./src/services/transport/rate_limit_guard");
const { createRestClient } = require("./src/services/transport/rest_client");
const { createRetryPolicy } = require("./src/services/transport/retry_policy");
const { createSigner } = require("./src/services/transport/signer");
const { createWsClient } = require("./src/services/transport/ws_client");
const { createSolanaRpcClient } = require("./src/services/indexer/solana_rpc_client");
const { createShardedSolanaRpcClient } = require("./src/services/indexer/sharded_solana_rpc_client");
const { assignShardByKey } = require("./src/services/indexer/sharding");
const { createPacificaSignedRequest } = require("./src/services/pacifica/agent_signing");
const { createCopyTradingExecutionWorker } = require("./src/services/copy_trading/execution_worker");
const {
  DEFAULT_START_TIME_MS: ONCHAIN_DEFAULT_START_TIME_MS,
  OnChainWalletDiscovery,
} = require("./src/services/indexer/onchain_wallet_discovery");

const { PacificaPipelineService } = require("./src/services/pipeline");
const {
  ensureDir,
  normalizeAddress,
  readJson,
  writeJsonAtomic,
} = require("./src/services/pipeline/utils");
const { CollectionStore } = require("./src/services/creator/collection_store");
const { WalletExplorerStore } = require("./src/services/analytics/wallet_explorer_store");
const { buildWalletRecordFromState } = require("./src/services/analytics/wallet_stats");
const { ExchangeWalletIndexer } = require("./src/services/indexer/exchange_wallet_indexer");
const { SolanaLogsTriggerMonitor, deriveRpcWsUrl } = require("./src/services/indexer/solana_logs_trigger_monitor");
const { createWalletSource } = require("./src/services/indexer/wallet_source");
const {
  buildMinimalUiDataset,
  getMinimalUiSourceMtimeMs,
  loadMinimalUiDataset,
  writeMinimalUiDataset,
} = require("./src/services/read_model/minimal_ui_dataset");
const { createWalletTrackingReadModel } = require("./src/services/read_model/wallet_tracking_read_model");
const {
  loadWalletExplorerV3Snapshot,
} = require("./src/services/read_model/wallet_storage_v3");
const { LiveWalletTriggerStore } = require("./src/services/analytics/live_wallet_trigger_store");
const { WalletMetricsHistoryStore } = require("./src/services/analytics/wallet_metrics_history_store");
const { PositionLifecycleStore } = require("./src/services/analytics/position_lifecycle_store");
const {
  RuntimeStatusStore,
} = require("./src/services/runtime/runtime_status_store");
const { createNextgenRuntime } = require("./src/services/nextgen/runtime");

let copyTradingExecutionWorker = null;

const PORT = Number(process.env.PORT || 3200);
const HOST = process.env.HOST || "0.0.0.0";
const SERVICE_RAW = String(process.env.PACIFICA_SERVICE || "all").toLowerCase();
const VALID_SERVICES = new Set(["all", "api", "ui", "live"]);
const SERVICE = VALID_SERVICES.has(SERVICE_RAW) ? SERVICE_RAW : "all";

const ENABLE_API = SERVICE !== "live";
const ENABLE_UI = SERVICE === "all" || SERVICE === "ui";
const ENABLE_LIVE = SERVICE === "all" || SERVICE === "api" || SERVICE === "live";
const WALLET_PERFORMANCE_UPSTREAM_PORT = Math.max(
  1,
  Number(process.env.PACIFICA_WALLET_PERFORMANCE_UPSTREAM_PORT || 3332)
);
const LIVE_TRADES_UPSTREAM_BASE = String(process.env.PACIFICA_LIVE_TRADES_UPSTREAM_BASE || "")
  .trim()
  .replace(/\/+$/, "");
const WALLET_PERFORMANCE_UPSTREAM_BASE = String(
  process.env.PACIFICA_WALLET_PERFORMANCE_UPSTREAM_BASE ||
    LIVE_TRADES_UPSTREAM_BASE ||
    `http://127.0.0.1:${WALLET_PERFORMANCE_UPSTREAM_PORT}`
)
  .trim()
  .replace(/\/+$/, "");
const MINIMAL_UI_MODE =
  String(process.env.PACIFICA_MINIMAL_UI_MODE || "true").trim().toLowerCase() === "true";
const STARTUP_COMPONENT_WARMUP_ENABLED =
  String(process.env.PACIFICA_STARTUP_COMPONENT_WARMUP_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";
const STARTUP_ROUTE_WARMUP_ENABLED =
  String(process.env.PACIFICA_STARTUP_ROUTE_WARMUP_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";
const NEXTGEN_WALLET_API_SHADOW_ENABLED = /^(1|true|yes|on)$/i.test(
  String(process.env.PACIFICA_NEXTGEN_WALLET_API_SHADOW_ENABLED || "").trim()
);
const STARTUP_ANALYTICS_WARMUP_ENABLED =
  String(process.env.PACIFICA_STARTUP_ANALYTICS_WARMUP_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";
const STARTUP_BLOCKING_WARMUP_ENABLED =
  String(process.env.PACIFICA_STARTUP_BLOCKING_WARMUP_ENABLED || "false")
    .trim()
    .toLowerCase() === "true";

function buildWalletPerformanceUpstreamUrl(requestPath = "/api/wallet-performance") {
  const normalizedPath = String(requestPath || "/api/wallet-performance").trim();
  if (/^https?:\/\//i.test(normalizedPath)) {
    return normalizedPath;
  }
  const safePath = normalizedPath.startsWith("/") ? normalizedPath : `/${normalizedPath}`;
  return `${WALLET_PERFORMANCE_UPSTREAM_BASE}${safePath}`;
}

function pickUnrealizedPnlUsdFromRecord(record = null) {
  const safeRecord =
    record && typeof record === "object" ? record : {};
  const safeAll =
    safeRecord.all && typeof safeRecord.all === "object" ? safeRecord.all : {};
  const candidates = [
    safeRecord.unrealizedPnlUsd,
    safeRecord.positionPnlUsd,
    safeRecord.unrealizedPnl,
    safeRecord.unrealized,
    safeRecord.unrealizedUsd,
    safeRecord.unrealized_usd,
    safeRecord.unrealized_pnl_usd,
    safeRecord.unrealized_pnl,
    safeAll.unrealizedPnlUsd,
    safeAll.positionPnlUsd,
    safeAll.unrealizedPnl,
    safeAll.unrealized,
    safeAll.unrealizedUsd,
    safeAll.unrealized_usd,
    safeAll.unrealized_pnl_usd,
    safeAll.unrealized_pnl,
  ];
  for (const candidate of candidates) {
    const value = Number(candidate);
    if (Number.isFinite(value)) {
      return value;
    }
  }
  return NaN;
}

function pickUnrealizedPnlUsd(primary = null, fallback = null) {
  const primaryValue = pickUnrealizedPnlUsdFromRecord(primary);
  if (Number.isFinite(primaryValue)) {
    return primaryValue;
  }
  return pickUnrealizedPnlUsdFromRecord(fallback);
}

function ensureUiComponentFactories() {
  if (!createGeneralDataComponent) {
    ({ createGeneralDataComponent } = require("./src/components/general-data"));
  }
  if (!createWalletTrackingComponent) {
    ({ createWalletTrackingComponent } = require("./src/components/wallet-tracking"));
  }
  if (!createWalletTrackingNextgenComponent) {
    ({ createWalletTrackingNextgenComponent } = require("./src/components/wallet-tracking-nextgen"));
  }
  if (!createCreatorStudioComponent) {
    ({ createCreatorStudioComponent } = require("./src/components/creator-studio"));
  }
  if (!createLiveTradesHostComponent) {
    ({ createLiveTradesHostComponent } = require("./src/components/live-trades-host"));
  }
}

const API_BASE = process.env.PACIFICA_API_BASE || "https://api.pacifica.fi/api/v1";
const WS_URL = process.env.PACIFICA_WS_URL || "wss://ws.pacifica.fi/ws";
const ACCOUNT_ENV_PRESENT = Object.prototype.hasOwnProperty.call(process.env, "PACIFICA_ACCOUNT");
const INITIAL_ACCOUNT = normalizeAddress(process.env.PACIFICA_ACCOUNT || "") || null;
const WS_ENABLED = String(process.env.PACIFICA_WS_ENABLED || "true").toLowerCase() !== "false";
const INDEXER_ENABLED = String(process.env.PACIFICA_INDEXER_ENABLED || "true").toLowerCase() !== "false";
const INDEXER_RUNNER_ENABLED =
  String(process.env.PACIFICA_INDEXER_RUNNER_ENABLED || "true").toLowerCase() !== "false";
const ONCHAIN_ENABLED =
  String(process.env.PACIFICA_ONCHAIN_ENABLED || "true").toLowerCase() !== "false";
const ONCHAIN_RUNNER_ENABLED =
  String(process.env.PACIFICA_ONCHAIN_RUNNER_ENABLED || "false").toLowerCase() !== "false";
const GLOBAL_KPI_ENABLED =
  String(process.env.PACIFICA_GLOBAL_KPI_ENABLED || "true").toLowerCase() !== "false";
const MINIMAL_UI_SERVER =
  MINIMAL_UI_MODE &&
  ENABLE_UI &&
  !INDEXER_ENABLED &&
  !INDEXER_RUNNER_ENABLED &&
  !ONCHAIN_ENABLED &&
  !ONCHAIN_RUNNER_ENABLED &&
  !GLOBAL_KPI_ENABLED;
const PIPELINE_PERSIST_EVERY_MS = Math.max(
  1000,
  Number(
    process.env.PACIFICA_PIPELINE_PERSIST_EVERY_MS ||
      (!INDEXER_ENABLED && !ONCHAIN_ENABLED ? 15000 : 5000)
  )
);
const PIPELINE_SKIP_REPLAY =
  Object.prototype.hasOwnProperty.call(process.env, "PACIFICA_PIPELINE_SKIP_REPLAY")
    ? String(process.env.PACIFICA_PIPELINE_SKIP_REPLAY || "false").toLowerCase() === "true"
    : Boolean(
        !ENABLE_UI ||
          (!INDEXER_RUNNER_ENABLED && !ONCHAIN_RUNNER_ENABLED && !GLOBAL_KPI_ENABLED)
      );
const API_CONFIG_KEY = String(process.env.PACIFICA_API_CONFIG_KEY || "").trim();
const RATE_LIMIT_WINDOW_MS = Math.max(
  1000,
  Number(process.env.PACIFICA_RATE_LIMIT_WINDOW_MS || 60000)
);
const API_RPM_CAP = Math.max(
  1,
  Number(process.env.PACIFICA_API_RPM_CAP || process.env.PACIFICA_RATE_LIMIT_CAPACITY || 250)
);
const RATE_LIMIT_SAFETY_RATIO = Math.min(
  0.99,
  Math.max(0.5, Number(process.env.PACIFICA_RATE_LIMIT_SAFETY_RATIO || 0.9))
);
const RATE_LIMIT_CAPACITY = Math.max(
  0.1,
  Number(((API_RPM_CAP * RATE_LIMIT_WINDOW_MS) / 60000).toFixed(3))
);
const API_USAGE_LOG_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_API_USAGE_LOG_INTERVAL_MS || 15000)
);
const REST_RETRY_MAX_ATTEMPTS = Math.max(
  1,
  Number(process.env.PACIFICA_REST_RETRY_MAX_ATTEMPTS || 2)
);
const REST_RETRY_BASE_DELAY_MS = Math.max(
  100,
  Number(process.env.PACIFICA_REST_RETRY_BASE_DELAY_MS || 500)
);
const REST_RETRY_MAX_DELAY_MS = Math.max(
  REST_RETRY_BASE_DELAY_MS,
  Number(process.env.PACIFICA_REST_RETRY_MAX_DELAY_MS || 5000)
);
const MULTI_EGRESS_PROXY_FILE = process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE
  ? path.resolve(process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE)
  : path.join(__dirname, "data", "indexer", "working_proxies.txt");
const MULTI_EGRESS_CONFIG_ENABLED = Object.prototype.hasOwnProperty.call(
  process.env,
  "PACIFICA_MULTI_EGRESS_ENABLED"
)
  ? String(process.env.PACIFICA_MULTI_EGRESS_ENABLED || "false").toLowerCase() === "true"
  : fs.existsSync(MULTI_EGRESS_PROXY_FILE);
const MULTI_EGRESS_MAX_PROXIES = Math.max(
  0,
  Number(process.env.PACIFICA_MULTI_EGRESS_MAX_PROXIES || 0)
);
const MULTI_EGRESS_RPM_CAP_PER_IP = Math.max(
  1,
  Number(process.env.PACIFICA_MULTI_EGRESS_RPM_CAP_PER_IP || 250)
);
const MULTI_EGRESS_INCLUDE_DIRECT =
  String(process.env.PACIFICA_MULTI_EGRESS_INCLUDE_DIRECT || "true").toLowerCase() !== "false";
const MULTI_EGRESS_TRANSPORT =
  String(process.env.PACIFICA_MULTI_EGRESS_TRANSPORT || "fetch").toLowerCase() === "fetch"
    ? "fetch"
    : "curl";
const MULTI_EGRESS_MATCH_WORKERS =
  String(process.env.PACIFICA_MULTI_EGRESS_MATCH_WORKERS || "true").toLowerCase() !== "false";
const LIVE_FOCUS_SYMBOL_MAX_RAW = Number(process.env.PACIFICA_LIVE_FOCUS_SYMBOL_MAX || 0);
const LIVE_FOCUS_SYMBOL_MAX =
  Number.isFinite(LIVE_FOCUS_SYMBOL_MAX_RAW) && LIVE_FOCUS_SYMBOL_MAX_RAW > 0
    ? Math.max(1, Math.floor(LIVE_FOCUS_SYMBOL_MAX_RAW))
    : Infinity;
const LIVE_BOOTSTRAP_SYMBOL_MAX = Math.max(
  1,
  Number(process.env.PACIFICA_LIVE_BOOTSTRAP_SYMBOL_MAX || 6)
);
const SOLANA_RPC_URL =
  String(process.env.SOLANA_RPC_URL || "").trim() || "https://api.mainnet-beta.solana.com";
const HELIUS_API_KEYS_FILE = process.env.PACIFICA_HELIUS_API_KEYS_FILE
  ? path.resolve(process.env.PACIFICA_HELIUS_API_KEYS_FILE)
  : path.join(__dirname, "data", "indexer", "helius_keys.txt");
const HELIUS_API_KEYS = uniqStrings([
  ...String(process.env.PACIFICA_HELIUS_API_KEYS || "")
    .split(",")
    .map((item) => String(item || "").trim()),
  ...(fs.existsSync(HELIUS_API_KEYS_FILE)
    ? fs
        .readFileSync(HELIUS_API_KEYS_FILE, "utf8")
        .split(/\r?\n/)
        .map((item) => String(item || "").trim())
    : []),
]);
const HELIUS_RPC_URL_TEMPLATE = String(
  process.env.PACIFICA_HELIUS_RPC_URL_TEMPLATE || "https://mainnet.helius-rpc.com/?api-key={key}"
).trim();
const SOLANA_RPC_RATE_LIMIT_CAPACITY = Math.max(
  1,
  Number(process.env.SOLANA_RPC_RATE_LIMIT_CAPACITY || 3)
);
const SOLANA_RPC_RATE_LIMIT_SAFETY_RATIO = Math.min(
  0.99,
  Math.max(0.5, Number(process.env.SOLANA_RPC_RATE_LIMIT_SAFETY_RATIO || 0.9))
);
const SOLANA_RPC_RATE_LIMIT_WINDOW_MS = Math.max(
  1000,
  Number(process.env.SOLANA_RPC_RATE_LIMIT_WINDOW_MS || 1000)
);
const SOLANA_RPC_GLOBAL_BUCKET_CAPACITY = Math.max(
  0.1,
  Number(process.env.SOLANA_RPC_GLOBAL_BUCKET_CAPACITY || 1.5)
);
const SOLANA_RPC_GLOBAL_REFILL_PER_SEC = Math.max(
  0.01,
  Number(process.env.SOLANA_RPC_GLOBAL_REFILL_PER_SEC || 0.8)
);
const SOLANA_RPC_SIG_REFILL_PER_SEC = Math.max(
  0.01,
  Number(process.env.SOLANA_RPC_SIG_REFILL_PER_SEC || 0.35)
);
const SOLANA_RPC_TX_REFILL_PER_SEC = Math.max(
  0.01,
  Number(process.env.SOLANA_RPC_TX_REFILL_PER_SEC || 0.5)
);
const SOLANA_RPC_SIG_MAX_CONCURRENCY = Math.max(
  1,
  Number(process.env.SOLANA_RPC_SIG_MAX_CONCURRENCY || 1)
);
const SOLANA_RPC_TX_MAX_CONCURRENCY = Math.max(
  1,
  Number(process.env.SOLANA_RPC_TX_MAX_CONCURRENCY || 2)
);
const SOLANA_RPC_SIG_INITIAL_CONCURRENCY = Math.max(
  1,
  Number(process.env.SOLANA_RPC_SIG_INITIAL_CONCURRENCY || 1)
);
const SOLANA_RPC_TX_INITIAL_CONCURRENCY = Math.max(
  1,
  Number(process.env.SOLANA_RPC_TX_INITIAL_CONCURRENCY || 1)
);
const SOLANA_RPC_429_BASE_DELAY_MS = Math.max(
  500,
  Number(process.env.SOLANA_RPC_429_BASE_DELAY_MS || 2500)
);
const SOLANA_RPC_429_MAX_DELAY_MS = Math.max(
  SOLANA_RPC_429_BASE_DELAY_MS,
  Number(process.env.SOLANA_RPC_429_MAX_DELAY_MS || 120000)
);
const SOLANA_RPC_RAMP_QUIET_MS = Math.max(
  10000,
  Number(process.env.SOLANA_RPC_RAMP_QUIET_MS || 180000)
);
const SOLANA_RPC_RAMP_STEP_MS = Math.max(
  5000,
  Number(process.env.SOLANA_RPC_RAMP_STEP_MS || 60000)
);
const ONCHAIN_MODE = String(process.env.PACIFICA_ONCHAIN_MODE || "backfill").toLowerCase();
const ONCHAIN_START_RAW = String(process.env.PACIFICA_ONCHAIN_START_TIME_MS || "").trim();
const ONCHAIN_START_TIME_MS =
  ONCHAIN_START_RAW && Number.isFinite(Number(ONCHAIN_START_RAW))
    ? Number(ONCHAIN_START_RAW)
    : ONCHAIN_DEFAULT_START_TIME_MS;
const ONCHAIN_END_RAW = String(process.env.PACIFICA_ONCHAIN_END_TIME_MS || "").trim();
const ONCHAIN_END_TIME_MS =
  ONCHAIN_END_RAW && Number.isFinite(Number(ONCHAIN_END_RAW))
    ? Number(ONCHAIN_END_RAW)
    : null;
const ONCHAIN_SCAN_PAGES_PER_CYCLE = Math.max(
  1,
  Number(process.env.PACIFICA_ONCHAIN_SCAN_PAGES_PER_CYCLE || 1)
);
const ONCHAIN_SIGNATURE_PAGE_LIMIT = Math.max(
  1,
  Math.min(1000, Number(process.env.PACIFICA_ONCHAIN_SIGNATURE_PAGE_LIMIT || 20))
);
const ONCHAIN_TX_CONCURRENCY = Math.max(
  1,
  Math.min(16, Number(process.env.PACIFICA_ONCHAIN_TX_CONCURRENCY || 1))
);
const ONCHAIN_VALIDATE_BATCH = Math.max(
  0,
  Number(process.env.PACIFICA_ONCHAIN_VALIDATE_BATCH || 20)
);
const ONCHAIN_DISCOVERY_TYPE = String(
  process.env.PACIFICA_ONCHAIN_DISCOVERY_TYPE || "deposit_only"
).toLowerCase();
const ONCHAIN_DEPOSIT_VAULTS = (() => {
  const raw = String(
    process.env.PACIFICA_DEPOSIT_VAULTS || "72R843XwZxqWhsJceARQQTTbYtWy6Zw9et2YV4FpRHTa"
  )
    .split(",")
    .map((item) => normalizeAddress(item))
    .filter(Boolean);
  return Array.from(new Set(raw));
})();
const ONCHAIN_MAX_TX_PER_CYCLE = Math.max(
  1,
  Number(process.env.PACIFICA_ONCHAIN_MAX_TX_PER_CYCLE || 12)
);
const ONCHAIN_SCAN_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_ONCHAIN_SCAN_INTERVAL_MS || 15000)
);
const ONCHAIN_CHECKPOINT_INTERVAL_MS = Math.max(
  500,
  Number(process.env.PACIFICA_ONCHAIN_CHECKPOINT_INTERVAL_MS || 3000)
);
const ONCHAIN_LOG_INTERVAL_MS = Math.max(
  3000,
  Number(process.env.PACIFICA_ONCHAIN_LOG_INTERVAL_MS || 30000)
);
const ONCHAIN_LOG_FORMAT =
  String(process.env.PACIFICA_ONCHAIN_LOG_FORMAT || "kv").toLowerCase() === "json"
    ? "json"
    : "kv";
const ONCHAIN_PENDING_MAX_ATTEMPTS = Math.max(
  1,
  Number(process.env.PACIFICA_ONCHAIN_PENDING_MAX_ATTEMPTS || 12)
);
const ONCHAIN_PROGRAM_IDS = (() => {
  const raw = String(
    process.env.PACIFICA_PROGRAM_IDS ||
      "PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH"
  )
    .split(",")
    .map((item) => normalizeAddress(item))
    .filter(Boolean);
  return Array.from(new Set(raw));
})();
const TRIGGER_MENTION_ADDRESSES = Array.from(
  new Set([...ONCHAIN_PROGRAM_IDS, ...ONCHAIN_DEPOSIT_VAULTS].filter(Boolean))
);
const ONCHAIN_EXCLUDE_ADDRESSES = String(process.env.PACIFICA_ONCHAIN_EXCLUDE_ADDRESSES || "")
  .split(",")
  .map((item) => normalizeAddress(item))
  .filter(Boolean);

const WALLET_SOURCE_FILE = process.env.PACIFICA_WALLET_SOURCE_FILE
  ? path.resolve(process.env.PACIFICA_WALLET_SOURCE_FILE)
  : path.join(__dirname, "config", "wallet-seeds.txt");
const DEPOSIT_WALLETS_PATH = process.env.PACIFICA_DEPOSIT_WALLETS_PATH
  ? path.resolve(process.env.PACIFICA_DEPOSIT_WALLETS_PATH)
  : path.join(__dirname, "data", "indexer", "deposit_wallets.json");
const WALLET_SOURCE_URL = String(process.env.PACIFICA_WALLET_SOURCE_URL || "").trim();
const INDEXER_SCAN_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_INDEXER_SCAN_INTERVAL_MS || 30000)
);
const INDEXER_DISCOVERY_INTERVAL_MS = Math.max(
  15000,
  Number(process.env.PACIFICA_INDEXER_DISCOVERY_INTERVAL_MS || 15000)
);
const INDEXER_BATCH_SIZE = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_BATCH_SIZE || 40)
);
const INDEXER_MAX_PAGES_PER_WALLET = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_MAX_PAGES_PER_WALLET || 3)
);
const INDEXER_FULL_HISTORY_PER_WALLET =
  String(process.env.PACIFICA_INDEXER_FULL_HISTORY_PER_WALLET || "true").toLowerCase() !==
  "false";
const INDEXER_TRADES_PAGE_LIMIT = Math.max(
  20,
  Math.min(4000, Number(process.env.PACIFICA_INDEXER_TRADES_PAGE_LIMIT || 400))
);
const INDEXER_FUNDING_PAGE_LIMIT = Math.max(
  20,
  Math.min(4000, Number(process.env.PACIFICA_INDEXER_FUNDING_PAGE_LIMIT || 400))
);
const INDEXER_WALLET_SCAN_CONCURRENCY = Math.max(
  1,
  Math.min(2048, Number(process.env.PACIFICA_INDEXER_WALLET_SCAN_CONCURRENCY || 4))
);
const INDEXER_REST_SHARD_PARALLELISM_MAX = Math.max(
  1,
  Math.min(256, Number(process.env.PACIFICA_INDEXER_REST_SHARD_PARALLELISM_MAX || 1))
);
const INDEXER_LIVE_WALLETS_PER_SCAN = Math.max(
  0,
  Math.min(2048, Number(process.env.PACIFICA_INDEXER_LIVE_WALLETS_PER_SCAN || 10))
);
const INDEXER_LIVE_WALLETS_PER_SCAN_MIN = Math.max(
  1,
  Math.min(
    512,
    Number(
      process.env.PACIFICA_INDEXER_LIVE_WALLETS_PER_SCAN_MIN ||
        Math.max(4, Math.ceil(INDEXER_WALLET_SCAN_CONCURRENCY * 0.1))
    )
  )
);
const INDEXER_LIVE_WALLETS_PER_SCAN_MAX = Math.max(
  INDEXER_LIVE_WALLETS_PER_SCAN_MIN,
  Math.min(2048, Number(process.env.PACIFICA_INDEXER_LIVE_WALLETS_PER_SCAN_MAX || 256))
);
const INDEXER_LIVE_REFRESH_TARGET_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_INDEXER_LIVE_REFRESH_TARGET_MS || 90000)
);
const INDEXER_LIVE_REFRESH_CONCURRENCY = Math.max(
  1,
  Math.min(
    2048,
    Number(
      process.env.PACIFICA_INDEXER_LIVE_REFRESH_CONCURRENCY ||
        Math.max(
          INDEXER_LIVE_WALLETS_PER_SCAN_MIN,
          Math.min(512, INDEXER_LIVE_WALLETS_PER_SCAN_MAX)
        )
    )
  )
);
const INDEXER_LIVE_TRIGGER_POLL_MS = Math.max(
  500,
  Number(process.env.PACIFICA_INDEXER_LIVE_TRIGGER_POLL_MS || 2000)
);
const INDEXER_LIVE_TRIGGER_READ_LIMIT = Math.max(
  100,
  Number(process.env.PACIFICA_INDEXER_LIVE_TRIGGER_READ_LIMIT || 5000)
);
const INDEXER_LIVE_TRIGGER_TTL_MS = Math.max(
  10000,
  Number(process.env.PACIFICA_INDEXER_LIVE_TRIGGER_TTL_MS || 600000)
);
const INDEXER_LIVE_MAX_PAGES_PER_WALLET = Math.max(
  1,
  Math.min(50, Number(process.env.PACIFICA_INDEXER_LIVE_MAX_PAGES_PER_WALLET || 1))
);
const INDEXER_FULL_HISTORY_PAGES_PER_SCAN = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_FULL_HISTORY_PAGES_PER_SCAN || 12)
);
const INDEXER_DEEP_HISTORY_PAGES_PER_SCAN = Math.max(
  1,
  Math.min(
    INDEXER_FULL_HISTORY_PAGES_PER_SCAN,
    Number(process.env.PACIFICA_INDEXER_DEEP_HISTORY_PAGES_PER_SCAN || 3)
  )
);
const INDEXER_ACTIVATION_TRADES_PAGE_LIMIT = Math.max(
  20,
  Math.min(
    INDEXER_TRADES_PAGE_LIMIT,
    Number(process.env.PACIFICA_INDEXER_ACTIVATION_TRADES_PAGE_LIMIT || 64)
  )
);
const INDEXER_ACTIVATION_FUNDING_PAGE_LIMIT = Math.max(
  20,
  Math.min(
    INDEXER_FUNDING_PAGE_LIMIT,
    Number(process.env.PACIFICA_INDEXER_ACTIVATION_FUNDING_PAGE_LIMIT || 64)
  )
);
const INDEXER_DEEP_HISTORY_TRADES_PAGE_LIMIT = Math.max(
  20,
  Math.min(
    INDEXER_TRADES_PAGE_LIMIT,
    Number(process.env.PACIFICA_INDEXER_DEEP_HISTORY_TRADES_PAGE_LIMIT || 160)
  )
);
const INDEXER_DEEP_HISTORY_FUNDING_PAGE_LIMIT = Math.max(
  20,
  Math.min(
    INDEXER_FUNDING_PAGE_LIMIT,
    Number(process.env.PACIFICA_INDEXER_DEEP_HISTORY_FUNDING_PAGE_LIMIT || 160)
  )
);
const INDEXER_HISTORY_AUDIT_REPAIR_BATCH_SIZE = Math.max(
  0,
  Number(process.env.PACIFICA_INDEXER_HISTORY_AUDIT_REPAIR_BATCH_SIZE || 16)
);
const INDEXER_HISTORY_AUDIT_REPAIR_HOT_WALLET_LIMIT = Math.max(
  INDEXER_HISTORY_AUDIT_REPAIR_BATCH_SIZE,
  Number(process.env.PACIFICA_INDEXER_HISTORY_AUDIT_REPAIR_HOT_WALLET_LIMIT || 1024)
);
const INDEXER_TOP_CORRECTION_COHORT_SIZE = Math.max(
  10,
  Number(process.env.PACIFICA_INDEXER_TOP_CORRECTION_COHORT_SIZE || 100)
);
const INDEXER_HISTORY_REQUEST_MAX_ATTEMPTS = Math.max(
  1,
  Math.min(6, Number(process.env.PACIFICA_INDEXER_HISTORY_REQUEST_MAX_ATTEMPTS || 3))
);
const INDEXER_ACTIVATION_RESERVE_MIN = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_ACTIVATION_RESERVE_MIN || 40)
);
const INDEXER_ACTIVATION_RESERVE_MAX = Math.max(
  INDEXER_ACTIVATION_RESERVE_MIN,
  Number(process.env.PACIFICA_INDEXER_ACTIVATION_RESERVE_MAX || 72)
);
const INDEXER_CONTINUATION_RESERVE_MIN = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_CONTINUATION_RESERVE_MIN || 48)
);
const INDEXER_CONTINUATION_RESERVE_MAX = Math.max(
  INDEXER_CONTINUATION_RESERVE_MIN,
  Number(process.env.PACIFICA_INDEXER_CONTINUATION_RESERVE_MAX || 108)
);
const INDEXER_DEEP_BACKFILL_MIN_PER_SCAN = Math.max(
  0,
  Number(process.env.PACIFICA_INDEXER_DEEP_BACKFILL_MIN_PER_SCAN || 12)
);
const INDEXER_BACKFILL_PROXY_STRICT_CONTINUATION_THRESHOLD = Math.max(
  INDEXER_BATCH_SIZE,
  Number(
    process.env.PACIFICA_INDEXER_BACKFILL_PROXY_STRICT_CONTINUATION_THRESHOLD ||
      INDEXER_BATCH_SIZE * 8
  )
);
const INDEXER_BACKFILL_PROXY_STRICT_MIN_WEIGHT = Math.max(
  0.2,
  Math.min(
    4,
    Number(process.env.PACIFICA_INDEXER_BACKFILL_PROXY_STRICT_MIN_WEIGHT || 0.95)
  )
);
const INDEXER_BACKFILL_PROXY_STRICT_MAX_LATENCY_MS = Math.max(
  2000,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_PROXY_STRICT_MAX_LATENCY_MS || 14000)
);
const INDEXER_BACKFILL_PROXY_STRICT_MAX_FAILURE_RATIO = Math.max(
  0.05,
  Math.min(
    0.95,
    Number(process.env.PACIFICA_INDEXER_BACKFILL_PROXY_STRICT_MAX_FAILURE_RATIO || 0.45)
  )
);
const INDEXER_BACKFILL_PROXY_STRICT_MAX_TIMEOUT_RATIO = Math.max(
  0.05,
  Math.min(
    0.95,
    Number(process.env.PACIFICA_INDEXER_BACKFILL_PROXY_STRICT_MAX_TIMEOUT_RATIO || 0.35)
  )
);
const INDEXER_BACKFILL_PROXY_STRICT_MAX_NETWORK_RATIO = Math.max(
  0.05,
  Math.min(
    0.95,
    Number(process.env.PACIFICA_INDEXER_BACKFILL_PROXY_STRICT_MAX_NETWORK_RATIO || 0.22)
  )
);
const INDEXER_BACKFILL_QUALITY_WINDOW_MS = Math.max(
  10000,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_QUALITY_WINDOW_MS || 30000)
);
const INDEXER_BACKFILL_QUALITY_WINDOW_COUNT = Math.max(
  3,
  Math.min(24, Number(process.env.PACIFICA_INDEXER_BACKFILL_QUALITY_WINDOW_COUNT || 6))
);
const INDEXER_BACKFILL_WINDOW_HIGH_LATENCY_COUNT_THRESHOLD = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_WINDOW_HIGH_LATENCY_COUNT_THRESHOLD || 2)
);
const INDEXER_BACKFILL_WINDOW_BAD_COUNT_THRESHOLD = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_WINDOW_BAD_COUNT_THRESHOLD || 2)
);
const INDEXER_BACKFILL_WINDOW_COOLDOWN_MS = Math.max(
  30000,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_WINDOW_COOLDOWN_MS || 120000)
);
const INDEXER_DEEP_BACKFILL_MAX_SHARE_PER_PROXY = Math.max(
  0.02,
  Math.min(0.5, Number(process.env.PACIFICA_INDEXER_DEEP_BACKFILL_MAX_SHARE_PER_PROXY || 0.14))
);
const INDEXER_DEEP_BACKFILL_MIN_TASKS_PER_PROXY = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_DEEP_BACKFILL_MIN_TASKS_PER_PROXY || 4)
);
const INDEXER_DEEP_BACKFILL_MAX_TASKS_PER_PROXY = Math.max(
  INDEXER_DEEP_BACKFILL_MIN_TASKS_PER_PROXY,
  Number(process.env.PACIFICA_INDEXER_DEEP_BACKFILL_MAX_TASKS_PER_PROXY || 16)
);
const INDEXER_BACKFILL_PAGE_BUDGET_WHEN_LIVE_PRESSURE = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_PAGE_BUDGET_WHEN_LIVE_PRESSURE || 1)
);
const INDEXER_ACTIVATE_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_INDEXER_ACTIVATE_TIMEOUT_MS || 20000)
);
const INDEXER_RECENT_TIMEOUT_MS = Math.max(
  INDEXER_ACTIVATE_TIMEOUT_MS,
  Number(process.env.PACIFICA_INDEXER_RECENT_TIMEOUT_MS || 30000)
);
const INDEXER_BACKFILL_TIMEOUT_MS = Math.max(
  INDEXER_RECENT_TIMEOUT_MS,
  Number(process.env.PACIFICA_INDEXER_BACKFILL_TIMEOUT_MS || 45000)
);
const INDEXER_LIVE_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_INDEXER_LIVE_TIMEOUT_MS || 20000)
);
const INDEXER_ACTIVATE_REQUEST_TIMEOUT_MS = Math.max(
  3000,
  Number(
    process.env.PACIFICA_INDEXER_ACTIVATE_REQUEST_TIMEOUT_MS ||
      Math.min(INDEXER_ACTIVATE_TIMEOUT_MS, 10000)
  )
);
const INDEXER_RECENT_REQUEST_TIMEOUT_MS = Math.max(
  3000,
  Number(
    process.env.PACIFICA_INDEXER_RECENT_REQUEST_TIMEOUT_MS ||
      Math.min(INDEXER_RECENT_TIMEOUT_MS, 10000)
  )
);
const INDEXER_BACKFILL_REQUEST_TIMEOUT_MS = Math.max(
  3000,
  Number(
    process.env.PACIFICA_INDEXER_BACKFILL_REQUEST_TIMEOUT_MS ||
      Math.min(INDEXER_BACKFILL_TIMEOUT_MS, 12000)
  )
);
const INDEXER_LIVE_REQUEST_TIMEOUT_MS = Math.max(
  3000,
  Number(
    process.env.PACIFICA_INDEXER_LIVE_REQUEST_TIMEOUT_MS ||
      Math.min(INDEXER_LIVE_TIMEOUT_MS, 9000)
  )
);
const INDEXER_STATE_SAVE_MIN_INTERVAL_MS = Math.max(
  500,
  Number(process.env.PACIFICA_INDEXER_STATE_SAVE_MIN_INTERVAL_MS || 15000)
);
const INDEXER_CACHE_ENTRIES_PER_ENDPOINT = Math.max(
  20,
  Number(process.env.PACIFICA_INDEXER_CACHE_ENTRIES_PER_ENDPOINT || 500)
);
const INDEXER_BACKLOG_MODE_ENABLED =
  String(process.env.PACIFICA_INDEXER_BACKLOG_MODE_ENABLED || "true").toLowerCase() !== "false";
const INDEXER_BACKLOG_WALLETS_THRESHOLD = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_BACKLOG_WALLETS_THRESHOLD || 2000)
);
const INDEXER_BACKLOG_AVG_WAIT_MS_THRESHOLD = Math.max(
  1000,
  Number(process.env.PACIFICA_INDEXER_BACKLOG_AVG_WAIT_MS_THRESHOLD || 2700000)
);
const INDEXER_BACKLOG_DISCOVER_EVERY_CYCLES = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_BACKLOG_DISCOVER_EVERY_CYCLES || 8)
);
const INDEXER_BACKLOG_REFILL_BATCH = Math.max(
  INDEXER_BATCH_SIZE,
  Number(process.env.PACIFICA_INDEXER_BACKLOG_REFILL_BATCH || INDEXER_BATCH_SIZE * 8)
);
const INDEXER_SCAN_RAMP_QUIET_MS = Math.max(
  10000,
  Number(process.env.PACIFICA_INDEXER_SCAN_RAMP_QUIET_MS || 180000)
);
const INDEXER_SCAN_RAMP_STEP_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_INDEXER_SCAN_RAMP_STEP_MS || 60000)
);
const INDEXER_CLIENT_429_COOLDOWN_BASE_MS = Math.max(
  1000,
  Number(process.env.PACIFICA_INDEXER_CLIENT_429_COOLDOWN_BASE_MS || 2000)
);
const INDEXER_CLIENT_429_COOLDOWN_MAX_MS = Math.max(
  INDEXER_CLIENT_429_COOLDOWN_BASE_MS,
  Number(process.env.PACIFICA_INDEXER_CLIENT_429_COOLDOWN_MAX_MS || 120000)
);
const INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_BASE_MS = Math.max(
  250,
  Number(process.env.PACIFICA_INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_BASE_MS || 1000)
);
const INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_MAX_MS = Math.max(
  INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_BASE_MS,
  Number(process.env.PACIFICA_INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_MAX_MS || 30000)
);
const INDEXER_CLIENT_TIMEOUT_COOLDOWN_BASE_MS = Math.max(
  250,
  Number(process.env.PACIFICA_INDEXER_CLIENT_TIMEOUT_COOLDOWN_BASE_MS || 1500)
);
const INDEXER_CLIENT_TIMEOUT_COOLDOWN_MAX_MS = Math.max(
  INDEXER_CLIENT_TIMEOUT_COOLDOWN_BASE_MS,
  Number(process.env.PACIFICA_INDEXER_CLIENT_TIMEOUT_COOLDOWN_MAX_MS || 45000)
);
const INDEXER_CLIENT_DEFAULT_COOLDOWN_MS = Math.max(
  100,
  Number(process.env.PACIFICA_INDEXER_CLIENT_DEFAULT_COOLDOWN_MS || 1000)
);
const INDEXER_DISCOVERY_ONLY = (() => {
  if (Object.prototype.hasOwnProperty.call(process.env, "PACIFICA_INDEXER_DISCOVERY_ONLY")) {
    return String(process.env.PACIFICA_INDEXER_DISCOVERY_ONLY || "").toLowerCase() === "true";
  }
  // Default to discover+scan so newly discovered wallets are indexed continuously.
  return false;
})();
const ONCHAIN_SCAN_PAGES_MAX = Math.max(
  ONCHAIN_SCAN_PAGES_PER_CYCLE,
  Number(process.env.PACIFICA_ONCHAIN_SCAN_PAGES_MAX || ONCHAIN_SCAN_PAGES_PER_CYCLE * 4)
);
const ONCHAIN_RUNNER_INTERVAL_MS = Math.max(
  1000,
  Number(process.env.PACIFICA_ONCHAIN_RUNNER_INTERVAL_MS || INDEXER_DISCOVERY_INTERVAL_MS)
);

function parseWalletSeedList(raw) {
  return String(raw || "")
    .split(",")
    .map((entry) => normalizeAddress(entry))
    .filter(Boolean);
}

function uniqWalletSeeds(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((entry) => normalizeAddress(entry))
        .filter(Boolean)
    )
  );
}

function uniqStrings(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    )
  );
}

function parseProxyRows(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return [];

  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return parsed
        .map((row) => {
          if (typeof row === "string") return row.trim();
          if (row && typeof row === "object" && row.proxy) return String(row.proxy).trim();
          return "";
        })
        .filter(Boolean);
    }

    if (parsed && typeof parsed === "object") {
      if (Array.isArray(parsed.results)) {
        return parsed.results
          .filter((row) => row && typeof row === "object")
          .filter((row) => row.ok === true || Number(row.statusCode || 0) === 200)
          .map((row) => String(row.proxy || "").trim())
          .filter(Boolean);
      }
      const candidateKeys = ["proxies", "rows", "items"];
      for (const key of candidateKeys) {
        if (Array.isArray(parsed[key])) {
          return parsed[key]
            .map((row) => {
              if (typeof row === "string") return row.trim();
              if (row && typeof row === "object" && row.proxy) return String(row.proxy).trim();
              return "";
            })
            .filter(Boolean);
        }
      }
    }
  } catch (_error) {
    // fall through to line-based parser
  }

  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !line.startsWith("#"));
}

function loadProxyList(filePath) {
  if (!filePath) return [];
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, "utf8");
  return uniqStrings(parseProxyRows(raw));
}

function createRoundRobinRestClient(clients = []) {
  const rows = (Array.isArray(clients) ? clients : []).filter(
    (client) => client && typeof client.get === "function"
  );
  if (!rows.length) return null;
  let cursor = 0;
  return {
    get(pathname, options = {}) {
      const client = rows[cursor % rows.length];
      cursor += 1;
      return client.get(pathname, options);
    },
    post(pathname, options = {}) {
      const client = rows[cursor % rows.length];
      cursor += 1;
      return client.post(pathname, options);
    },
    request(method, pathname, options = {}) {
      const client = rows[cursor % rows.length];
      cursor += 1;
      return client.request(method, pathname, options);
    },
  };
}

function createShardRoutedRestClient(entries = [], options = {}) {
  const rows = (Array.isArray(entries) ? entries : [])
    .map((entry, idx) => {
      const client =
        entry && entry.client && typeof entry.client.get === "function"
          ? entry.client
          : entry && typeof entry.get === "function"
          ? entry
          : null;
      if (!client) return null;
      return {
        id: String((entry && entry.id) || `client_${idx + 1}`),
        client,
      };
    })
    .filter(Boolean);
  const fallbackClient =
    options && options.fallbackClient && typeof options.fallbackClient.get === "function"
      ? options.fallbackClient
      : null;

  if (!rows.length) return fallbackClient;

  let cursor = 0;
  const selectClient = (routeOptions = {}) => {
    const shardKey = String((routeOptions && routeOptions.shardKey) || "").trim();
    if (shardKey) {
      const assigned = assignShardByKey(shardKey, rows);
      if (assigned && assigned.item && assigned.item.client) {
        return assigned.item.client;
      }
    }
    const client = rows[cursor % rows.length];
    cursor += 1;
    return client && client.client ? client.client : fallbackClient;
  };

  const stripRouteOptions = (routeOptions = {}) => {
    const next = { ...(routeOptions || {}) };
    delete next.shardKey;
    return next;
  };

  return {
    get(pathname, routeOptions = {}) {
      const client = selectClient(routeOptions);
      return client.get(pathname, stripRouteOptions(routeOptions));
    },
    post(pathname, routeOptions = {}) {
      const client = selectClient(routeOptions);
      return client.post(pathname, stripRouteOptions(routeOptions));
    },
    request(method, pathname, routeOptions = {}) {
      const client = selectClient(routeOptions);
      return client.request(method, pathname, stripRouteOptions(routeOptions));
    },
  };
}

const INLINE_WALLET_SEEDS = parseWalletSeedList(process.env.PACIFICA_WALLET_SEEDS || "");

const DATA_ROOT = process.env.PACIFICA_DATA_DIR
  ? path.resolve(process.env.PACIFICA_DATA_DIR)
  : path.join(__dirname, "data");
const UI_MINIMAL_DATASET_PATH = path.join(DATA_ROOT, "ui", "minimal_dashboard.json");
const COPY_TRADING_STORE_PATH = path.join(DATA_ROOT, "copy_trading", "state.json");
const PIPELINE_DATA_DIR = path.join(DATA_ROOT, "pipeline");
const CREATOR_DATA_DIR = path.join(DATA_ROOT, "creator");
const INDEXER_DATA_DIR = process.env.PACIFICA_INDEXER_DATA_DIR
  ? path.resolve(process.env.PACIFICA_INDEXER_DATA_DIR)
  : path.join(DATA_ROOT, "indexer");
const INDEXER_EXTERNAL_SHARDS_DIR = process.env.PACIFICA_INDEXER_EXTERNAL_SHARDS_DIR
  ? path.resolve(process.env.PACIFICA_INDEXER_EXTERNAL_SHARDS_DIR)
  : path.join(path.join(DATA_ROOT, "indexer"), "shards");
const walletTrackingReadModel = createWalletTrackingReadModel({
  dataRoot: DATA_ROOT,
});
const INDEXER_WORKER_SHARD_COUNT = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_WORKER_SHARD_COUNT || 1)
);
const INDEXER_MULTI_WORKER_SHARDS = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_MULTI_WORKER_SHARDS || INDEXER_WORKER_SHARD_COUNT || 1)
);
const INDEXER_WORKER_SHARD_INDEX = Math.max(
  0,
  Math.min(
    INDEXER_WORKER_SHARD_COUNT - 1,
    Number(process.env.PACIFICA_INDEXER_WORKER_SHARD_INDEX || 0)
  )
);
const INDEXER_EXTERNAL_SHARDS_ENABLED = (() => {
  if (Object.prototype.hasOwnProperty.call(process.env, "PACIFICA_INDEXER_EXTERNAL_SHARDS_ENABLED")) {
    return String(process.env.PACIFICA_INDEXER_EXTERNAL_SHARDS_ENABLED || "")
      .trim()
      .toLowerCase() === "true";
  }
  return !INDEXER_ENABLED && INDEXER_MULTI_WORKER_SHARDS > 1;
})();
const LIVE_POSITIONS_DATA_DIR = path.join(DATA_ROOT, "live_positions");
const LIVE_WALLET_TRIGGER_FILE = String(
  process.env.PACIFICA_LIVE_WALLET_TRIGGER_FILE ||
    path.join(LIVE_POSITIONS_DATA_DIR, "wallet_activity_triggers.ndjson")
).trim();
const RUNTIME_STATUS_ENABLED =
  String(process.env.PACIFICA_RUNTIME_STATUS_ENABLED || "true").toLowerCase() !== "false";
const POSITION_LIFECYCLE_LOOP_ENABLED =
  String(process.env.PACIFICA_POSITION_LIFECYCLE_LOOP_ENABLED || "true").toLowerCase() !==
  "false";
const WALLET_METRICS_HISTORY_LOOP_ENABLED =
  String(process.env.PACIFICA_WALLET_METRICS_HISTORY_LOOP_ENABLED || "true").toLowerCase() !==
  "false";
const RUNTIME_STATUS_DIR = process.env.PACIFICA_RUNTIME_STATUS_DIR
  ? path.resolve(process.env.PACIFICA_RUNTIME_STATUS_DIR)
  : path.join(DATA_ROOT, "runtime", "status");
const RUNTIME_STATUS_PUBLISH_MS = Math.max(
  2000,
  Number(process.env.PACIFICA_RUNTIME_STATUS_PUBLISH_MS || 5000)
);
const RUNTIME_STATUS_STALE_MS = Math.max(
  15000,
  Number(process.env.PACIFICA_RUNTIME_STATUS_STALE_MS || 10 * 60 * 1000)
);
const LOCAL_RUNTIME_STATUS_PUBLISH_ENABLED =
  RUNTIME_STATUS_ENABLED &&
  (INDEXER_RUNNER_ENABLED ||
    ONCHAIN_RUNNER_ENABLED ||
    GLOBAL_KPI_ENABLED ||
    (ENABLE_LIVE && WS_ENABLED) ||
    POSITION_LIFECYCLE_LOOP_ENABLED ||
    WALLET_METRICS_HISTORY_LOOP_ENABLED);

const SNAPSHOT_REFRESH_MS = Math.max(
  10000,
  Number(process.env.PACIFICA_SNAPSHOT_REFRESH_MS || 45000)
);
const SNAPSHOT_ENABLED =
  String(process.env.PACIFICA_SNAPSHOT_ENABLED || "true").toLowerCase() !== "false";
const GLOBAL_KPI_VOLUME_METHOD = (() => {
  const raw = String(process.env.PACIFICA_GLOBAL_KPI_VOLUME_METHOD || "defillama_compat")
    .toLowerCase()
    .trim();
  if (raw === "prices_rolling_24h") return "prices_rolling_24h";
  return "defillama_compat";
})();
// Historical tracking start date currently used for Pacifica volume reconstruction.
// Can be overridden with PACIFICA_GLOBAL_KPI_DEFILLAMA_START_DATE.
const GLOBAL_KPI_DEFILLAMA_START_DATE = String(
  process.env.PACIFICA_GLOBAL_KPI_DEFILLAMA_START_DATE || "2025-06-09"
)
  .trim()
  .slice(0, 10);
const GLOBAL_KPI_BACKFILL_DAYS_PER_RUN = Math.max(
  1,
  Math.min(120, Number(process.env.PACIFICA_GLOBAL_KPI_BACKFILL_DAYS_PER_RUN || 5))
);
const GLOBAL_KPI_STRICT_DAILY =
  String(process.env.PACIFICA_GLOBAL_KPI_STRICT_DAILY || "false").toLowerCase() !== "false";
const GLOBAL_KPI_SYMBOL_CONCURRENCY = Math.max(
  1,
  Math.min(32, Number(process.env.PACIFICA_GLOBAL_KPI_SYMBOL_CONCURRENCY || 4))
);
const GLOBAL_KPI_RECENT_REPAIR_DAYS = Math.max(
  0,
  Math.min(30, Number(process.env.PACIFICA_GLOBAL_KPI_RECENT_REPAIR_DAYS || 7))
);
const GLOBAL_KPI_USE_MULTI_EGRESS =
  String(process.env.PACIFICA_GLOBAL_KPI_USE_MULTI_EGRESS || "false").toLowerCase() === "true";
const MULTI_EGRESS_ENABLED =
  MULTI_EGRESS_CONFIG_ENABLED && (INDEXER_ENABLED || GLOBAL_KPI_USE_MULTI_EGRESS);
const GLOBAL_KPI_EXTERNAL_SHARDS_ONLY =
  String(process.env.PACIFICA_GLOBAL_KPI_EXTERNAL_SHARDS_ONLY || "true").toLowerCase() !==
  "false";
const GLOBAL_KPI_DEFAULT_REFRESH_MS =
  GLOBAL_KPI_VOLUME_METHOD === "defillama_compat" ? 300000 : 15000;
const GLOBAL_KPI_REFRESH_MS = Math.max(
  GLOBAL_KPI_VOLUME_METHOD === "defillama_compat" ? 60000 : 5000,
  Number(process.env.PACIFICA_GLOBAL_KPI_REFRESH_MS || GLOBAL_KPI_DEFAULT_REFRESH_MS)
);
const GLOBAL_KPI_PATH = process.env.PACIFICA_GLOBAL_KPI_PATH
  ? path.resolve(process.env.PACIFICA_GLOBAL_KPI_PATH)
  : path.join(PIPELINE_DATA_DIR, "global_kpi.json");
const GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION = 8;
const WALLET_DATASET_COMPACT_SOURCE_PATH = path.join(
  DATA_ROOT,
  "ui",
  ".build",
  "wallet_dataset_compact_source.json"
);

const PUBLIC_DIR = path.join(__dirname, "public");
const minimalUiDatasetCache = {
  mtimeMs: 0,
  sourceMtimeMs: 0,
  value: null,
  inflight: null,
};
const textFileCache = new Map();
const binaryFileCache = new Map();
const htmlRenderCache = new Map();
const encodedResponseCache = new Map();
const runtimeAssetVersionCache = {
  signature: "",
  value: "dev",
};
const exchangeBootstrapCache = {
  value: null,
  inflight: null,
  fetchedAt: 0,
};

function getRuntimeAssetVersion() {
  const assetPaths = [
    path.join(PUBLIC_DIR, "app.js"),
    path.join(PUBLIC_DIR, "auth-ui.js"),
    path.join(PUBLIC_DIR, "styles.css"),
    path.join(PUBLIC_DIR, "home.js"),
    path.join(PUBLIC_DIR, "theme.css"),
    path.join(PUBLIC_DIR, "wallet-performance", "app.js"),
    path.join(PUBLIC_DIR, "wallet-performance", "styles.css"),
    path.join(PUBLIC_DIR, "social-trade", "app.js"),
    path.join(PUBLIC_DIR, "social-trade", "styles.css"),
    path.join(PUBLIC_DIR, "live-trade", "app.js"),
    path.join(PUBLIC_DIR, "live-trade", "styles.css"),
    path.join(PUBLIC_DIR, "copy-trading", "app.js"),
    path.join(PUBLIC_DIR, "copy-trading", "styles.css"),
    path.join(PUBLIC_DIR, "settings", "app.js"),
    path.join(PUBLIC_DIR, "settings", "styles.css"),
  ];
  const signature = assetPaths
    .filter((filePath) => fs.existsSync(filePath))
    .map((filePath) => {
      const stat = fs.statSync(filePath);
      return `${path.relative(PUBLIC_DIR, filePath)}:${Number(stat.mtimeMs || 0)}:${Number(stat.size || 0)}`;
    })
    .join("|");
  if (signature && runtimeAssetVersionCache.signature === signature && runtimeAssetVersionCache.value) {
    return runtimeAssetVersionCache.value;
  }
  const version = signature
    ? crypto.createHash("sha1").update(signature).digest("hex").slice(0, 12)
    : "dev";
  runtimeAssetVersionCache.signature = signature;
  runtimeAssetVersionCache.value = version;
  return version;
}

function applyRuntimeAssetVersion(html, version) {
  if (typeof html !== "string" || !html) return html;
  const safeVersion = String(version || "dev").trim() || "dev";
  let nextHtml = html
    .replace(/\/styles\.css\?v=[^"'\s>]+/g, `/styles.css?v=${safeVersion}`)
    .replace(/\/app\.js\?v=[^"'\s>]+/g, `/app.js?v=${safeVersion}`)
    .replace(/\/auth-ui\.js\?v=[^"'\s>]+/g, `/auth-ui.js?v=${safeVersion}`)
    .replace(/\/home\.js\?v=[^"'\s>]+/g, `/home.js?v=${safeVersion}`)
    .replace(/\/theme\.css\?v=[^"'\s>]+/g, `/theme.css?v=${safeVersion}`)
    .replace(
      /\/wallet-performance\/styles\.css\?v=[^"'\s>]+/g,
      `/wallet-performance/styles.css?v=${safeVersion}`
    )
    .replace(
      /\/wallet-performance\/app(?:\.src)?\.js\?v=[^"'\s>]+/g,
      `/wallet-performance/app.js?v=${safeVersion}`
    )
    .replace(/\/social-trade\/styles\.css\?v=[^"'\s>]+/g, `/social-trade/styles.css?v=${safeVersion}`)
    .replace(/\/social-trade\/app\.js\?v=[^"'\s>]+/g, `/social-trade/app.js?v=${safeVersion}`)
    .replace(/\/live-trade\/styles\.css\?v=[^"'\s>]+/g, `/live-trade/styles.css?v=${safeVersion}`)
    .replace(/\/live-trade\/app(?:\.src)?\.js\?v=[^"'\s>]+/g, `/live-trade/app.js?v=${safeVersion}`)
    .replace(/\/copy-trading\/styles\.css\?v=[^"'\s>]+/g, `/copy-trading/styles.css?v=${safeVersion}`)
    .replace(/\/copy-trading\/app(?:\.src)?\.js\?v=[^"'\s>]+/g, `/copy-trading/app.js?v=${safeVersion}`);
    nextHtml = nextHtml
      .replace(/\/settings\/styles\.css\?v=[^"'\s>]+/g, `/settings/styles.css?v=${safeVersion}`)
      .replace(/\/settings\/app(?:\.src)?\.js\?v=[^"'\s>]+/g, `/settings/app.js?v=${safeVersion}`);
  if (nextHtml.includes("window.__PF_ASSET_VERSION__")) {
    nextHtml = nextHtml.replace(
      /window\.__PF_ASSET_VERSION__\s*=\s*["'][^"']*["']/,
      `window.__PF_ASSET_VERSION__="${safeVersion}"`
    );
  } else if (nextHtml.includes('<script id="pf-app-entry"')) {
    nextHtml = nextHtml.replace(
      /<script\s+id="pf-app-entry"/,
      `<script>window.__PF_ASSET_VERSION__="${safeVersion}";</script><script id="pf-app-entry"`
    );
  }
  return nextHtml;
}

function readUtf8FileCached(filePath, stat = null) {
  const safeStat = stat && typeof stat === "object" ? stat : fs.statSync(filePath);
  const cacheKey = `${filePath}:${Number(safeStat.mtimeMs || 0)}:${Number(safeStat.size || 0)}`;
  const cached = textFileCache.get(cacheKey);
  if (cached && typeof cached === "string") return cached;
  const value = fs.readFileSync(filePath, "utf8");
  textFileCache.clear();
  textFileCache.set(cacheKey, value);
  return value;
}

function readFileBufferCached(filePath, stat = null) {
  const safeStat = stat && typeof stat === "object" ? stat : fs.statSync(filePath);
  const cacheKey = `${filePath}:${Number(safeStat.mtimeMs || 0)}:${Number(safeStat.size || 0)}`;
  const cached = binaryFileCache.get(cacheKey);
  if (Buffer.isBuffer(cached)) return cached;
  const value = fs.readFileSync(filePath);
  binaryFileCache.clear();
  binaryFileCache.set(cacheKey, value);
  return value;
}

function getCachedRenderedHtml(cacheKey) {
  const cached = htmlRenderCache.get(cacheKey);
  return cached && typeof cached === "string" ? cached : null;
}

function setCachedRenderedHtml(cacheKey, value) {
  if (typeof value !== "string") return value;
  if (htmlRenderCache.size > 12) {
    htmlRenderCache.clear();
  }
  htmlRenderCache.set(cacheKey, value);
  return value;
}

function selectResponseEncoding(req) {
  const acceptEncoding =
    req && req.headers && typeof req.headers["accept-encoding"] === "string"
      ? req.headers["accept-encoding"]
      : "";
  if (/\bbr\b/i.test(acceptEncoding)) return "br";
  if (/\bgzip\b/i.test(acceptEncoding)) return "gzip";
  return "";
}

function isCompressibleContentType(contentType = "") {
  return /(?:^text\/|javascript|json|xml|svg)/i.test(String(contentType || ""));
}

function encodeBufferCached(cacheKey, encoding, buffer) {
  const fullKey = `${cacheKey}:${encoding}`;
  const cached = encodedResponseCache.get(fullKey);
  if (Buffer.isBuffer(cached)) return cached;
  let encoded = buffer;
  if (encoding === "br") {
    encoded = zlib.brotliCompressSync(buffer, {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: 4,
      },
    });
  } else if (encoding === "gzip") {
    encoded = zlib.gzipSync(buffer, { level: 6 });
  }
  if (encodedResponseCache.size > 24) {
    encodedResponseCache.clear();
  }
  encodedResponseCache.set(fullKey, encoded);
  return encoded;
}

function writeResponseBody(req, res, statusCode, headers, body, cacheKey = null) {
  const baseHeaders = { ...(headers || {}) };
  const sourceBuffer = Buffer.isBuffer(body) ? body : Buffer.from(String(body || ""), "utf8");
  const contentType = String(baseHeaders["Content-Type"] || baseHeaders["content-type"] || "");
  const encoding =
    sourceBuffer.length >= 1024 && isCompressibleContentType(contentType)
      ? selectResponseEncoding(req)
      : "";
  const finalBuffer =
    encoding && cacheKey
      ? encodeBufferCached(cacheKey, encoding, sourceBuffer)
      : encoding === "br"
      ? zlib.brotliCompressSync(sourceBuffer, {
          params: {
            [zlib.constants.BROTLI_PARAM_QUALITY]: 4,
          },
        })
      : encoding === "gzip"
      ? zlib.gzipSync(sourceBuffer, { level: 6 })
      : sourceBuffer;
  if (encoding) {
    baseHeaders["Content-Encoding"] = encoding;
    baseHeaders.Vary = "Accept-Encoding";
  }
  baseHeaders["Content-Length"] = finalBuffer.length;
  res.writeHead(statusCode, baseHeaders);
  if (req && req.method === "HEAD") {
    res.end();
    return;
  }
  res.end(finalBuffer);
}

const upstreamProxyResponseCache = new Map();

async function proxyUpstreamContent(
  req,
  res,
  sourceUrl,
  { timeoutMs = 10000, cacheKey = null, retries = 0, retryDelayMs = 0, allowStaleOnErrorMs = 0 } = {}
) {
  const maxRetries = Math.max(0, Number(retries || 0));
  const safeRetryDelayMs = Math.max(0, Number(retryDelayMs || 0));
  let lastError = null;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const signal =
        typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
          ? AbortSignal.timeout(timeoutMs)
          : undefined;
      const upstreamRes = await fetch(sourceUrl, {
        method: req && req.method ? req.method : "GET",
        headers: {
          Accept: req?.headers?.accept || "*/*",
          "Accept-Encoding": req?.headers?.["accept-encoding"] || "identity",
          "User-Agent": req?.headers?.["user-agent"] || "PacificaFlow/1.0",
        },
        signal,
      });

      const headers = {};
      upstreamRes.headers.forEach((value, key) => {
        const lower = String(key || "").toLowerCase();
        if (lower === "content-length" || lower === "transfer-encoding" || lower === "connection") return;
        headers[key] = value;
      });

      const body = Buffer.from(await upstreamRes.arrayBuffer());
      if (cacheKey && upstreamRes.status < 500 && body.length > 0) {
        upstreamProxyResponseCache.set(cacheKey, {
          fetchedAt: Date.now(),
          status: upstreamRes.status,
          headers,
          body,
        });
      }
      writeResponseBody(req, res, upstreamRes.status, headers, body, cacheKey);
      return true;
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries && safeRetryDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, safeRetryDelayMs));
      }
    }
  }

  if (cacheKey && allowStaleOnErrorMs > 0) {
    const cached = upstreamProxyResponseCache.get(cacheKey);
    const staleAgeMs = cached ? Math.max(0, Date.now() - Number(cached.fetchedAt || 0)) : null;
    if (cached && cached.body && staleAgeMs !== null && staleAgeMs <= allowStaleOnErrorMs) {
      const headers = {
        ...(cached.headers || {}),
        "x-pacifica-stale": "1",
        "x-pacifica-stale-age-ms": String(staleAgeMs),
      };
      writeResponseBody(req, res, Number(cached.status || 200), headers, cached.body, cacheKey);
      return true;
    }
  }

  throw lastError || new Error("upstream_fetch_failed");
}

function toFiniteNumberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function isWalletPerformancePositionRowOpen(row) {
  const safeRow = row && typeof row === "object" ? row : {};
  const statusTokens = [
    safeRow.status,
    safeRow.positionStatus,
    safeRow.lifecycle,
    safeRow.lifecycleStage,
    safeRow.state,
    safeRow.freshness,
  ]
    .map((value) => String(value || "").trim().toLowerCase())
    .filter(Boolean);
  const isClosed = statusTokens.some((token) =>
    token === "closed" ||
    token === "inactive" ||
    token === "stale" ||
    token === "expired" ||
    token === "settled" ||
    token === "liquidated" ||
    token === "provisional"
  );
  if (isClosed) return false;

  const side = String(safeRow.side || safeRow.sideLabel || "").trim().toLowerCase();
  const sizeAbs = Math.abs(
    Number(
      safeRow.size !== undefined && safeRow.size !== null
        ? safeRow.size
        : safeRow.amount !== undefined && safeRow.amount !== null
        ? safeRow.amount
        : safeRow.positionSize !== undefined && safeRow.positionSize !== null
        ? safeRow.positionSize
        : 0
    )
  );
  const valueAbs = Math.abs(
    Number(
      safeRow.positionValueUsd !== undefined && safeRow.positionValueUsd !== null
        ? safeRow.positionValueUsd
        : safeRow.positionValue !== undefined && safeRow.positionValue !== null
        ? safeRow.positionValue
        : safeRow.notionalUsd !== undefined && safeRow.notionalUsd !== null
        ? safeRow.notionalUsd
        : 0
    )
  );
  const marginAbs = Math.abs(
    Number(
      safeRow.marginUsd !== undefined && safeRow.marginUsd !== null
        ? safeRow.marginUsd
        : safeRow.margin !== undefined && safeRow.margin !== null
        ? safeRow.margin
        : 0
    )
  );
  if (Number.isFinite(sizeAbs) && sizeAbs > 0) return true;
  if (Number.isFinite(valueAbs) && valueAbs > 0) return true;
  if ((side === "long" || side === "short") && Number.isFinite(marginAbs) && marginAbs > 0) return true;
  return false;
}

function normalizeWalletPerformanceSummaryPayload(payload, options = {}) {
  const safePayload = payload && typeof payload === "object" ? payload : {};
  const verifiedOpenPositionsByWallet =
    options && options.verifiedOpenPositionsByWallet && typeof options.verifiedOpenPositionsByWallet === "object"
      ? options.verifiedOpenPositionsByWallet
      : {};
  if (!Array.isArray(safePayload.rows)) return safePayload;
  safePayload.rows = safePayload.rows.map((row) => {
    if (!row || typeof row !== "object") return row;
    const nextRow = { ...row };
    const walletKey = String(nextRow.wallet || "").trim();
    const verifiedOpenCount =
      walletKey && verifiedOpenPositionsByWallet[walletKey] !== undefined
        ? toFiniteNumberOrNull(verifiedOpenPositionsByWallet[walletKey])
        : null;
    const openCandidates = [
      nextRow.displayOpenPositions,
      nextRow.openPositions,
      nextRow.liveOpenPositions,
      nextRow.open_positions,
    ]
      .map((value) => toFiniteNumberOrNull(value))
      .filter((value) => value !== null);
    const maxOpen = openCandidates.length ? Math.max.apply(null, openCandidates) : 0;
    const normalizedOpen =
      verifiedOpenCount !== null
        ? Math.max(0, Math.round(verifiedOpenCount))
        : Math.max(0, Math.round(maxOpen));
    nextRow.displayOpenPositions = normalizedOpen;
    nextRow.openPositions = normalizedOpen;
    nextRow.liveOpenPositions = normalizedOpen;
    return nextRow;
  });
  return safePayload;
}

function normalizeWalletPerformanceDetailPayload(payload) {
  const safePayload = payload && typeof payload === "object" ? payload : {};
  const rawPositions = Array.isArray(safePayload.positions) ? safePayload.positions : [];
  const positions = rawPositions.filter((row) => isWalletPerformancePositionRowOpen(row));
  safePayload.positions = positions;
  const openCount = positions.length;
  if (!safePayload.account || typeof safePayload.account !== "object") {
    safePayload.account = {};
  }
  safePayload.account.openPositions = openCount;
  if (openCount <= 0) {
    safePayload.account.oiCurrent = 0;
    safePayload.account.oi_current = 0;
  }
  if (safePayload.summary && typeof safePayload.summary === "object") {
    safePayload.summary.openPositions = openCount;
    safePayload.summary.displayOpenPositions = openCount;
    safePayload.summary.liveOpenPositions = openCount;
  }
  return safePayload;
}

function extractWalletPerformanceExactQueryWallet(requestPath = "") {
  try {
    const parsed = new URL(String(requestPath || ""), "http://local");
    const needle = String(parsed.searchParams.get("q") || "").trim();
    if (!needle) return null;
    if (/\s/.test(needle)) return null;
    if (needle.length < 24) return null;
    return needle;
  } catch (_error) {
    return null;
  }
}

async function fetchWalletPerformanceVerifiedOpenCount(wallet, { timeoutMs = 20000, retries = 1, retryDelayMs = 200 } = {}) {
  const safeWallet = String(wallet || "").trim();
  if (!safeWallet) return null;
  const detailUrl = buildWalletPerformanceUpstreamUrl(
    `/api/wallet-performance/wallet/${encodeURIComponent(safeWallet)}?force=1`
  );
  const maxRetries = Math.max(0, Number(retries || 0));
  const safeRetryDelayMs = Math.max(0, Number(retryDelayMs || 0));
  let lastError = null;
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const signal =
        typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
          ? AbortSignal.timeout(timeoutMs)
          : undefined;
      const response = await fetch(detailUrl, {
        method: "GET",
        headers: {
          Accept: "application/json",
          "Cache-Control": "no-cache",
          Pragma: "no-cache",
          "User-Agent": "PacificaFlow/1.0",
        },
        signal,
      });
      if (!response.ok) {
        throw new Error(`wallet_performance_verify_http_${response.status}`);
      }
      const payload = await response.json();
      const positions = Array.isArray(payload && payload.positions) ? payload.positions : [];
      return positions.filter((row) => isWalletPerformancePositionRowOpen(row)).length;
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries && safeRetryDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, safeRetryDelayMs));
      }
    }
  }
  throw lastError || new Error("wallet_performance_verify_failed");
}

async function proxyUpstreamWalletPerformanceJson(
  req,
  res,
  sourceUrl,
  {
    timeoutMs = 65000,
    retries = 2,
    retryDelayMs = 250,
    isWalletDetailRoute = false,
    exactWalletToVerify = null,
  } = {}
) {
  const maxRetries = Math.max(0, Number(retries || 0));
  const safeRetryDelayMs = Math.max(0, Number(retryDelayMs || 0));
  let lastError = null;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const signal =
        typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
          ? AbortSignal.timeout(timeoutMs)
          : undefined;
      const upstreamRes = await fetch(sourceUrl, {
        method: req && req.method ? req.method : "GET",
        headers: {
          Accept: "application/json",
          "Cache-Control": "no-cache",
          Pragma: "no-cache",
          "User-Agent": req?.headers?.["user-agent"] || "PacificaFlow/1.0",
        },
        signal,
      });
      const text = await upstreamRes.text();
      let payload = null;
      try {
        payload = text ? JSON.parse(text) : null;
      } catch (error) {
        throw new Error(`wallet_performance_upstream_invalid_json:${error.message || error}`);
      }
      let normalizedPayload = null;
      if (isWalletDetailRoute) {
        normalizedPayload = normalizeWalletPerformanceDetailPayload(payload);
      } else {
        const verifiedOpenPositionsByWallet = {};
        if (exactWalletToVerify) {
          try {
            const verifiedOpenCount = await fetchWalletPerformanceVerifiedOpenCount(exactWalletToVerify, {
              timeoutMs: Math.min(25000, timeoutMs),
              retries: 1,
              retryDelayMs: 250,
            });
            if (verifiedOpenCount !== null) {
              verifiedOpenPositionsByWallet[String(exactWalletToVerify)] = verifiedOpenCount;
            }
          } catch (_verifyError) {
            // Keep upstream summary if exact-wallet verification fails.
          }
        }
        normalizedPayload = normalizeWalletPerformanceSummaryPayload(payload, {
          verifiedOpenPositionsByWallet,
        });
      }
      sendJson(res, upstreamRes.status, normalizedPayload);
      return true;
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries && safeRetryDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, safeRetryDelayMs));
      }
    }
  }

  throw lastError || new Error("wallet_performance_proxy_failed");
}

const localJsonFetchCache = new Map();

function fetchLocalJson(pathname, timeoutMs = 8000) {
  const cacheKey = `${pathname}:${Math.max(250, Number(timeoutMs || 0))}`;
  const cached = localJsonFetchCache.get(cacheKey);
  const now = Date.now();
  if (cached && cached.value && now - Number(cached.fetchedAt || 0) <= 1500) {
    return Promise.resolve(JSON.parse(JSON.stringify(cached.value)));
  }
  if (cached && cached.inflight) {
    return cached.inflight.then((value) => JSON.parse(JSON.stringify(value)));
  }
  const entry = { fetchedAt: 0, value: null, inflight: null };
  const task = new Promise((resolve, reject) => {
    const localReq = http.request(
      {
        hostname: "127.0.0.1",
        port: PORT,
        path: pathname,
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      },
      (localRes) => {
        let body = "";
        localRes.setEncoding("utf8");
        localRes.on("data", (chunk) => {
          body += chunk;
        });
        localRes.on("end", () => {
          if (localRes.statusCode && localRes.statusCode >= 400) {
            reject(new Error(`local_request_failed_${localRes.statusCode}`));
            return;
          }
          try {
            resolve(JSON.parse(body || "null"));
          } catch (error) {
            reject(error);
          }
        });
      }
    );
    localReq.on("error", reject);
    localReq.setTimeout(timeoutMs, () => {
      localReq.destroy(new Error("local_request_timeout"));
    });
    localReq.end();
  });
  entry.inflight = task;
  localJsonFetchCache.set(cacheKey, entry);
  return task
    .then((value) => {
      entry.value = value;
      entry.fetchedAt = Date.now();
      return value;
    })
    .finally(() => {
      entry.inflight = null;
      while (localJsonFetchCache.size > 32) {
        const oldestKey = localJsonFetchCache.keys().next().value;
        localJsonFetchCache.delete(oldestKey);
      }
    });
}

async function getExchangeBootstrapPayload() {
  const now = Date.now();
  if (
    exchangeBootstrapCache.value &&
    now - Number(exchangeBootstrapCache.fetchedAt || 0) < 15000
  ) {
    return exchangeBootstrapCache.value;
  }
  if (exchangeBootstrapCache.inflight) {
    return exchangeBootstrapCache.inflight;
  }
  exchangeBootstrapCache.inflight = Promise.all([
    fetchLocalJson("/api/exchange/overview?timeframe=all", 30000).catch(() => null),
    fetchLocalJson("/api/exchange/overview?timeframe=24h", 30000).catch(() => null),
    fetchLocalJson("/api/exchange/overview?timeframe=30d", 30000).catch(() => null),
    fetchLocalJson("/api/exchange/metric-series", 30000).catch(() => null),
  ])
    .then(([overview, overview24h, overview30d, series]) => {
      const payload = {
        overview: overview && typeof overview === "object" ? overview : null,
        overview24h: overview24h && typeof overview24h === "object" ? overview24h : null,
        overview30d: overview30d && typeof overview30d === "object" ? overview30d : null,
        series: series && typeof series === "object" ? series : null,
      };
      exchangeBootstrapCache.value = payload;
      exchangeBootstrapCache.fetchedAt = Date.now();
      return payload;
    })
    .catch(() => {
      return exchangeBootstrapCache.value || { overview: null, series: null };
    })
    .finally(() => {
      exchangeBootstrapCache.inflight = null;
    });
  return exchangeBootstrapCache.inflight;
}

function sendJson(res, statusCode, payload, extraHeaders = null) {
  const data = JSON.stringify(payload);
  writeResponseBody(res.__pfRequest || null, res, statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    ...(extraHeaders || {}),
  }, data);
}

function parseCookies(cookieHeader = "") {
  const cookies = {};
  String(cookieHeader || "")
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean)
    .forEach((part) => {
      const index = part.indexOf("=");
      if (index <= 0) return;
      const key = decodeURIComponent(part.slice(0, index).trim());
      const value = decodeURIComponent(part.slice(index + 1).trim());
      if (key) cookies[key] = value;
    });
  return cookies;
}

function buildCookie(name, value, options = {}) {
  const parts = [`${encodeURIComponent(name)}=${encodeURIComponent(value || "")}`];
  if (options.maxAge !== undefined) parts.push(`Max-Age=${Math.max(0, Math.floor(Number(options.maxAge || 0) / 1000))}`);
  if (options.expires instanceof Date) parts.push(`Expires=${options.expires.toUTCString()}`);
  parts.push(`Path=${options.path || "/"}`);
  parts.push("SameSite=Lax");
  if (options.httpOnly !== false) parts.push("HttpOnly");
  if (options.secure) parts.push("Secure");
  return parts.join("; ");
}

function normalizeEmailAddress(value) {
  const email = String(value || "").trim().toLowerCase();
  if (!email) return "";
  return email;
}

function normalizeAuthDisplayName(value, fallbackEmail = "") {
  const text = String(value || "").trim();
  if (text) return text.slice(0, 48);
  const email = String(fallbackEmail || "").trim();
  if (!email) return "Trader";
  const local = email.split("@")[0] || email;
  return local
    .split(/[._-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
    .slice(0, 48) || "Trader";
}

function createPasswordHash(password, salt = crypto.randomBytes(16).toString("hex")) {
  const safePassword = String(password || "");
  const hash = crypto.scryptSync(safePassword, salt, 64).toString("hex");
  return { salt, hash };
}

function verifyPassword(password, salt, expectedHash) {
  try {
    const { hash } = createPasswordHash(password, salt);
    return crypto.timingSafeEqual(Buffer.from(hash, "hex"), Buffer.from(String(expectedHash || ""), "hex"));
  } catch (_error) {
    return false;
  }
}

function createVerificationCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

function createVerificationCodeHash(code, salt = crypto.randomBytes(16).toString("hex")) {
  const safeCode = String(code || "");
  const hash = crypto.scryptSync(safeCode, salt, 64).toString("hex");
  return { salt, hash };
}

function verifyVerificationCode(code, salt, expectedHash) {
  try {
    const { hash } = createVerificationCodeHash(code, salt);
    return crypto.timingSafeEqual(Buffer.from(hash, "hex"), Buffer.from(String(expectedHash || ""), "hex"));
  } catch (_error) {
    return false;
  }
}

const TOTP_STEP_SECONDS = 30;
const TOTP_DIGITS = 6;
const TOTP_WINDOW_STEPS = 1;
const BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

function base32Encode(buffer) {
  const bytes = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || []);
  let bits = 0;
  let value = 0;
  let output = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      output += BASE32_ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) {
    output += BASE32_ALPHABET[(value << (5 - bits)) & 31];
  }
  while (output.length % 8 !== 0) {
    output += "=";
  }
  return output;
}

function base32Decode(secret) {
  const normalized = String(secret || "")
    .toUpperCase()
    .replace(/=+$/g, "")
    .replace(/[^A-Z2-7]/g, "");
  let bits = 0;
  let value = 0;
  const output = [];
  for (const char of normalized) {
    const idx = BASE32_ALPHABET.indexOf(char);
    if (idx < 0) continue;
    value = (value << 5) | idx;
    bits += 5;
    if (bits >= 8) {
      output.push((value >>> (bits - 8)) & 255);
      bits -= 8;
    }
  }
  return Buffer.from(output);
}

function generateTotpCode(secret, timeMs = Date.now()) {
  const key = base32Decode(secret);
  if (!key.length) return "";
  const counter = Math.floor(Number(timeMs || Date.now()) / 1000 / TOTP_STEP_SECONDS);
  const buffer = Buffer.alloc(8);
  buffer.writeBigUInt64BE(BigInt(counter));
  const hmac = crypto.createHmac("sha1", key).update(buffer).digest();
  const offset = hmac[hmac.length - 1] & 0x0f;
  const binary =
    ((hmac[offset] & 0x7f) << 24) |
    ((hmac[offset + 1] & 0xff) << 16) |
    ((hmac[offset + 2] & 0xff) << 8) |
    (hmac[offset + 3] & 0xff);
  const token = String(binary % 10 ** TOTP_DIGITS).padStart(TOTP_DIGITS, "0");
  return token;
}

function createMfaSecret(email, displayName) {
  const issuer = "PacificaFlow";
  const accountName = normalizeAuthDisplayName(displayName, email) || email;
  const generated = speakeasy.generateSecret({
    name: `${issuer}:${accountName}`,
    issuer,
    length: 20,
  });
  const secret = String(generated.base32 || "").replace(/=+$/g, "");
  const otpauth = String(generated.otpauth_url || buildMfaOtpAuthUrl(issuer, accountName, secret));
  return { issuer, accountName, secret, otpauth };
}

function buildMfaOtpAuthUrl(issuer, accountName, secret) {
  return `otpauth://totp/${encodeURIComponent(String(issuer || "PacificaFlow"))}:${encodeURIComponent(
    String(accountName || "")
  )}?secret=${encodeURIComponent(String(secret || ""))}&issuer=${encodeURIComponent(
    String(issuer || "PacificaFlow")
  )}&algorithm=SHA1&digits=${TOTP_DIGITS}&period=${TOTP_STEP_SECONDS}`;
}

function verifyTotpCode(secret, code) {
  const expected = String(code || "").trim();
  if (!secret || !/^\d{6}$/.test(expected)) return false;
  return speakeasy.totp.verify({
    secret: String(secret || ""),
    encoding: "base32",
    token: expected,
    window: TOTP_WINDOW_STEPS,
    step: TOTP_STEP_SECONDS,
  });
}

function normalizeBackupCode(code) {
  return String(code || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function formatBackupCode(code) {
  const normalized = normalizeBackupCode(code);
  if (!normalized) return "";
  const parts = normalized.match(/.{1,4}/g) || [normalized];
  return parts.join("-");
}

function hashBackupCode(code) {
  return crypto.createHash("sha256").update(normalizeBackupCode(code)).digest("hex");
}

function getBackupCodeStats(user) {
  const codes = Array.isArray(user && user.mfaBackupCodes) ? user.mfaBackupCodes : [];
  const remaining = codes.reduce((count, item) => count + (item && !item.usedAt ? 1 : 0), 0);
  const generatedAt = Number(user && user.mfaBackupCodesGeneratedAt ? user.mfaBackupCodesGeneratedAt : 0) || null;
  const usedAt = Number(user && user.mfaBackupCodesLastUsedAt ? user.mfaBackupCodesLastUsedAt : 0) || null;
  return {
    configured: codes.length > 0,
    remaining,
    generatedAt,
    lastUsedAt: usedAt,
  };
}

function verifyBackupCode(user, rawCode) {
  const codes = Array.isArray(user && user.mfaBackupCodes) ? user.mfaBackupCodes : [];
  const normalized = normalizeBackupCode(rawCode);
  if (!normalized || !codes.length) return false;
  const hash = hashBackupCode(normalized);
  const matched = codes.find((item) => item && !item.usedAt && String(item.hash || "") === hash);
  if (!matched) return false;
  matched.usedAt = Date.now();
  user.mfaBackupCodesLastUsedAt = matched.usedAt;
  user.updatedAt = Date.now();
  return true;
}

function createMfaBackupCodes(user, count = 10) {
  const total = Math.max(6, Math.min(12, Number(count) || 10));
  const generatedAt = Date.now();
  const codes = [];
  const hashes = [];
  const seen = new Set();
  while (codes.length < total) {
    const raw = crypto.randomBytes(6).toString("hex").toUpperCase();
    const normalized = normalizeBackupCode(raw);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    codes.push(formatBackupCode(normalized));
    hashes.push({
      hash: hashBackupCode(normalized),
      createdAt: generatedAt,
      usedAt: null,
    });
  }
  user.mfaBackupCodes = hashes;
  user.mfaBackupCodesGeneratedAt = generatedAt;
  user.mfaBackupCodesLastUsedAt = null;
  user.updatedAt = generatedAt;
  return codes;
}

function verifyMfaCodeOrBackup(user, code) {
  return verifyTotpCode(user && user.mfaSecret, code) || verifyBackupCode(user, code);
}

const AUTH_SMTP_HOST = String(process.env.PACIFICA_AUTH_SMTP_HOST || process.env.SMTP_HOST || "127.0.0.1").trim();
const AUTH_SMTP_PORT = Math.max(1, Number(process.env.PACIFICA_AUTH_SMTP_PORT || process.env.SMTP_PORT || 25));
const AUTH_SMTP_SECURE = /^(1|true|yes|on)$/i.test(
  String(process.env.PACIFICA_AUTH_SMTP_SECURE || process.env.SMTP_SECURE || "").trim()
);
const AUTH_SMTP_USER = String(process.env.PACIFICA_AUTH_SMTP_USER || process.env.SMTP_USER || "").trim();
const AUTH_SMTP_PASS = String(process.env.PACIFICA_AUTH_SMTP_PASS || process.env.SMTP_PASS || "").trim();
const AUTH_MAIL_FROM = String(
  process.env.PACIFICA_AUTH_MAIL_FROM || process.env.SMTP_FROM || "PacificaFlow <no-reply@pacificaflow.xyz>"
).trim();
const AUTH_ALLOWED_IPS = new Set(
  String(process.env.PACIFICA_AUTH_ALLOWED_IPS || "172.30.34.253")
    .split(/[,\s]+/)
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .map((item) => item.replace(/^::ffff:/i, ""))
);
const TWITTER_CLIENT_ID = String(
  process.env.PACIFICA_AUTH_TWITTER_CLIENT_ID || ""
).trim();
const TWITTER_CLIENT_SECRET = String(
  process.env.PACIFICA_AUTH_TWITTER_CLIENT_SECRET || ""
).trim();
const TWITTER_REDIRECT_URI = String(
  process.env.PACIFICA_AUTH_TWITTER_REDIRECT_URI || ""
).trim();
let authMailTransport = null;

function getAuthMailTransport() {
  if (authMailTransport) return authMailTransport;
  if (!nodemailer) return null;
  const transportOptions = {
    host: AUTH_SMTP_HOST,
    port: AUTH_SMTP_PORT,
    secure: AUTH_SMTP_SECURE,
  };
  if (AUTH_SMTP_USER && AUTH_SMTP_PASS) {
    transportOptions.auth = {
      user: AUTH_SMTP_USER,
      pass: AUTH_SMTP_PASS,
    };
  }
  authMailTransport = nodemailer.createTransport(transportOptions);
  return authMailTransport;
}

async function sendAuthVerificationEmail({ email, code, displayName }) {
  const transport = getAuthMailTransport();
  if (!transport) {
    throw new Error("email_transport_not_configured");
  }
  const subject = "Your PacificaFlow verification code";
  const text =
    `Hi ${displayName || "Trader"},\n\n` +
    `Your PacificaFlow verification code is: ${code}\n\n` +
    `This code expires in 10 minutes.\n` +
    `If you did not request this sign-up, you can ignore this email.\n`;
  const html = `
    <div style="font-family:Arial,Helvetica,sans-serif;background:#f4f8fc;padding:24px">
      <div style="max-width:560px;margin:0 auto;background:#ffffff;border:1px solid #d6e6f2;border-radius:18px;padding:24px;color:#0a1826">
        <div style="font-size:12px;letter-spacing:0.16em;text-transform:uppercase;color:#5d7891">PacificaFlow</div>
        <h2 style="margin:12px 0 8px;font-size:28px;line-height:1.2">Verify your email</h2>
        <p style="margin:0 0 20px;font-size:15px;line-height:1.6;color:#294257">Use the code below to finish creating your PacificaFlow account.</p>
        <div style="font-size:32px;font-weight:700;letter-spacing:0.18em;background:#eaf5fd;border:1px solid #cfe3f2;border-radius:16px;padding:18px 20px;display:inline-block">${code}</div>
        <p style="margin:20px 0 0;font-size:13px;line-height:1.55;color:#567087">This code expires in 10 minutes. If you did not request this, you can ignore this email.</p>
      </div>
    </div>
  `;
  await transport.sendMail({
    from: AUTH_MAIL_FROM,
    to: email,
    subject,
    text,
    html,
  });
}

function isTwitterOAuthConfigured() {
  return Boolean(TWITTER_CLIENT_ID && TWITTER_CLIENT_SECRET && TWITTER_REDIRECT_URI);
}

function sanitizeRelativePath(value, fallback = "/") {
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  if (/^https?:\/\//i.test(raw) || raw.startsWith("//")) return fallback;
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function buildTwitterReturnPath(rawReturnTo) {
  const returnTo = sanitizeRelativePath(rawReturnTo, "/");
  return returnTo;
}

function normalizeRequestIp(raw) {
  const value = String(raw || "").trim();
  if (!value) return "";
  return value.replace(/^::ffff:/i, "").replace(/:\d+$/, "");
}

function getRequestClientIps(req) {
  const headers = req && req.headers ? req.headers : {};
  const forwardedFor = String(headers["x-forwarded-for"] || headers["X-Forwarded-For"] || "")
    .split(",")
    .map((item) => normalizeRequestIp(item))
    .filter(Boolean);
  const realIp = normalizeRequestIp(headers["x-real-ip"] || headers["X-Real-IP"] || "");
  const connectingIp = normalizeRequestIp(headers["cf-connecting-ip"] || headers["CF-Connecting-IP"] || "");
  const socketIp = normalizeRequestIp(
    req && req.socket
      ? req.socket.remoteAddress
      : req && req.connection
        ? req.connection.remoteAddress
        : ""
  );
  return [...new Set([...forwardedFor, realIp, connectingIp, socketIp].filter(Boolean))];
}

function isAuthRequestIpAllowed(req) {
  if (!AUTH_ALLOWED_IPS.size) return true;
  return getRequestClientIps(req).some((ip) => AUTH_ALLOWED_IPS.has(ip));
}

function buildTwitterAuthorizeUrl(stateToken, codeChallenge) {
  const url = new URL("https://twitter.com/i/oauth2/authorize");
  url.searchParams.set("client_id", TWITTER_CLIENT_ID);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("redirect_uri", TWITTER_REDIRECT_URI);
  url.searchParams.set("scope", "users.read tweet.read offline.access");
  url.searchParams.set("state", stateToken);
  url.searchParams.set("code_challenge", codeChallenge);
  url.searchParams.set("code_challenge_method", "S256");
  return url.toString();
}

function createTwitterCodeVerifier() {
  return crypto.randomBytes(32).toString("base64url");
}

function createTwitterCodeChallenge(verifier) {
  return crypto.createHash("sha256").update(String(verifier || ""), "utf8").digest("base64url");
}

async function exchangeTwitterCodeForToken(code, codeVerifier) {
  const response = await fetch("https://api.twitter.com/2/oauth2/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body: new URLSearchParams({
      client_id: TWITTER_CLIENT_ID,
      client_secret: TWITTER_CLIENT_SECRET,
      grant_type: "authorization_code",
      code: String(code || ""),
      redirect_uri: TWITTER_REDIRECT_URI,
      code_verifier: String(codeVerifier || ""),
    }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload && payload.error_description ? String(payload.error_description) : "twitter_token_exchange_failed";
    throw new Error(message);
  }
  return payload;
}

async function fetchTwitterIdentity(accessToken) {
  const response = await fetch("https://api.twitter.com/2/users/me?user.fields=profile_image_url,username,name,verified", {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
    },
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload && payload.title ? String(payload.title) : "twitter_profile_fetch_failed";
    throw new Error(message);
  }
  return payload && payload.data ? payload.data : {};
}

function buildTwitterLoginDisplayName(profile = {}) {
  const name = String(profile.name || "").trim();
  const username = String(profile.username || "").trim();
  return name || username || "Twitter User";
}

function buildTwitterUsername(profile = {}) {
  return String(profile.username || "").trim();
}

function buildTwitterAvatar(profile = {}) {
  return String(profile.profile_image_url || "").trim();
}

function getUserByEmail(email) {
  const store = loadCopyTradingStore();
  const key = String(email || "").trim().toLowerCase();
  if (!key || !store.usersByEmail) return null;
  return store.usersByEmail[key] || null;
}

function getUserByTwitterId(twitterId) {
  const store = loadCopyTradingStore();
  const key = String(twitterId || "").trim();
  if (!key || !store.usersByTwitterId) return null;
  return store.usersByTwitterId[key] || null;
}

function indexUserRecord(user) {
  const store = loadCopyTradingStore();
  if (!user || !user.id) return null;
  if (user.email && !String(user.email).endsWith("@twitter.local")) {
    store.usersByEmail[String(user.email).trim().toLowerCase()] = user;
  }
  if (user.twitterId) {
    store.usersByTwitterId[String(user.twitterId).trim()] = user;
  }
  return user;
}

function findUserById(userId) {
  const store = loadCopyTradingStore();
  if (!userId) return null;
  const collections = [store.usersByEmail, store.usersByTwitterId];
  for (const collection of collections) {
    if (!collection || typeof collection !== "object") continue;
    const match = Object.values(collection).find((row) => row && row.id === userId);
    if (match) return match;
  }
  return null;
}

function findOrCreateTwitterUser(profile = {}) {
  const twitterId = String(profile.id || "").trim();
  if (!twitterId) {
    throw new Error("twitter_profile_missing_id");
  }
  const displayName = buildTwitterLoginDisplayName(profile);
  const avatarUrl = buildTwitterAvatar(profile);
  const username = buildTwitterUsername(profile);
  const existingByTwitterId = getUserByTwitterId(twitterId);
  if (existingByTwitterId) {
    existingByTwitterId.twitterUsername = username || existingByTwitterId.twitterUsername || "";
    existingByTwitterId.twitterName = String(profile.name || existingByTwitterId.twitterName || "").trim();
    existingByTwitterId.twitterAvatar = avatarUrl || existingByTwitterId.twitterAvatar || "";
    if (!existingByTwitterId.displayName && displayName) {
      existingByTwitterId.displayName = displayName;
    }
    existingByTwitterId.updatedAt = Date.now();
    indexUserRecord(existingByTwitterId);
    return existingByTwitterId;
  }
  const syntheticEmail = `twitter:${twitterId}@twitter.local`;
  const user = {
    id: crypto.randomUUID(),
    email: syntheticEmail,
    displayName,
    authProvider: "twitter",
    twitterId,
    twitterUsername: username,
    twitterName: String(profile.name || "").trim(),
    twitterAvatar: avatarUrl,
    emailVerifiedAt: null,
    mfaEnabled: false,
    mfaEnabledAt: null,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  indexUserRecord(user);
  return user;
}

function createTwitterStateRecord({ returnTo = "/" } = {}) {
  const stateToken = crypto.randomUUID().replace(/-/g, "");
  const store = loadCopyTradingStore();
  if (!store.pendingTwitterStatesByToken || typeof store.pendingTwitterStatesByToken !== "object") {
    store.pendingTwitterStatesByToken = {};
  }
  const codeVerifier = createTwitterCodeVerifier();
  store.pendingTwitterStatesByToken[stateToken] = {
    token: stateToken,
    codeVerifier,
    returnTo: buildTwitterReturnPath(returnTo),
    createdAt: Date.now(),
    expiresAt: Date.now() + 1000 * 60 * 10,
  };
  saveCopyTradingStore();
  return stateToken;
}

function getTwitterStateRecord(stateToken) {
  const store = loadCopyTradingStore();
  if (!stateToken || !store.pendingTwitterStatesByToken) return null;
  const record = store.pendingTwitterStatesByToken[String(stateToken).trim()] || null;
  if (!record) return null;
  if (Number(record.expiresAt || 0) <= Date.now()) {
    delete store.pendingTwitterStatesByToken[String(stateToken).trim()];
    saveCopyTradingStore();
    return null;
  }
  return record;
}

async function buildMfaQrDataUrl(otpauthUrl) {
  return QRCode.toDataURL(String(otpauthUrl || ""), {
    errorCorrectionLevel: "M",
    margin: 1,
    scale: 7,
  });
}

function createCopyTradingStore() {
  return {
    version: 2,
    usersByEmail: {},
    usersByTwitterId: {},
    sessionsByToken: {},
    pendingSignupsByEmail: {},
    pendingSigninChallengesByToken: {},
    pendingTwitterStatesByToken: {},
    pendingMfaSetupsByUserId: {},
    leadersByUserId: {},
    apiKeysByUserId: {},
    copyProfilesByUserId: {},
    copyRiskRulesByUserId: {},
    leaderEventsByUserId: {},
    copyIntentsByUserId: {},
    executionReceiptsByUserId: {},
    copiedPositionsByUserId: {},
    manualOverridesByUserId: {},
    copyActivityLogByUserId: {},
    copyAlertsByUserId: {},
    copyRuntimeStatusByUserId: {},
    executionByUserId: {},
    executionEventsByUserId: {},
    executionStateByUserId: {},
    updatedAt: Date.now(),
  };
}

let copyTradingStoreCache = null;

function loadCopyTradingStore() {
  if (copyTradingStoreCache && typeof copyTradingStoreCache === "object") {
    return copyTradingStoreCache;
  }
  const store = readJson(COPY_TRADING_STORE_PATH, null);
  copyTradingStoreCache =
    store && typeof store === "object" && Number(store.version || 0) >= 1 ? store : createCopyTradingStore();
  if (!copyTradingStoreCache.pendingSignupsByEmail || typeof copyTradingStoreCache.pendingSignupsByEmail !== "object") {
    copyTradingStoreCache.pendingSignupsByEmail = {};
  }
  if (!copyTradingStoreCache.usersByTwitterId || typeof copyTradingStoreCache.usersByTwitterId !== "object") {
    copyTradingStoreCache.usersByTwitterId = {};
  }
  if (
    !copyTradingStoreCache.pendingSigninChallengesByToken ||
    typeof copyTradingStoreCache.pendingSigninChallengesByToken !== "object"
  ) {
    copyTradingStoreCache.pendingSigninChallengesByToken = {};
  }
  if (
    !copyTradingStoreCache.pendingTwitterStatesByToken ||
    typeof copyTradingStoreCache.pendingTwitterStatesByToken !== "object"
  ) {
    copyTradingStoreCache.pendingTwitterStatesByToken = {};
  }
  if (!copyTradingStoreCache.pendingMfaSetupsByUserId || typeof copyTradingStoreCache.pendingMfaSetupsByUserId !== "object") {
    copyTradingStoreCache.pendingMfaSetupsByUserId = {};
  }
  if (!copyTradingStoreCache.executionByUserId || typeof copyTradingStoreCache.executionByUserId !== "object") {
    copyTradingStoreCache.executionByUserId = {};
  }
  if (!copyTradingStoreCache.executionEventsByUserId || typeof copyTradingStoreCache.executionEventsByUserId !== "object") {
    copyTradingStoreCache.executionEventsByUserId = {};
  }
  if (!copyTradingStoreCache.executionStateByUserId || typeof copyTradingStoreCache.executionStateByUserId !== "object") {
    copyTradingStoreCache.executionStateByUserId = {};
  }
  if (!copyTradingStoreCache.copyProfilesByUserId || typeof copyTradingStoreCache.copyProfilesByUserId !== "object") {
    copyTradingStoreCache.copyProfilesByUserId = {};
  }
  if (!copyTradingStoreCache.copyRiskRulesByUserId || typeof copyTradingStoreCache.copyRiskRulesByUserId !== "object") {
    copyTradingStoreCache.copyRiskRulesByUserId = {};
  }
  if (!copyTradingStoreCache.leaderEventsByUserId || typeof copyTradingStoreCache.leaderEventsByUserId !== "object") {
    copyTradingStoreCache.leaderEventsByUserId = {};
  }
  if (!copyTradingStoreCache.copyIntentsByUserId || typeof copyTradingStoreCache.copyIntentsByUserId !== "object") {
    copyTradingStoreCache.copyIntentsByUserId = {};
  }
  if (!copyTradingStoreCache.executionReceiptsByUserId || typeof copyTradingStoreCache.executionReceiptsByUserId !== "object") {
    copyTradingStoreCache.executionReceiptsByUserId = {};
  }
  if (!copyTradingStoreCache.copiedPositionsByUserId || typeof copyTradingStoreCache.copiedPositionsByUserId !== "object") {
    copyTradingStoreCache.copiedPositionsByUserId = {};
  }
  if (!copyTradingStoreCache.manualOverridesByUserId || typeof copyTradingStoreCache.manualOverridesByUserId !== "object") {
    copyTradingStoreCache.manualOverridesByUserId = {};
  }
  if (!copyTradingStoreCache.copyActivityLogByUserId || typeof copyTradingStoreCache.copyActivityLogByUserId !== "object") {
    copyTradingStoreCache.copyActivityLogByUserId = {};
  }
  if (!copyTradingStoreCache.copyAlertsByUserId || typeof copyTradingStoreCache.copyAlertsByUserId !== "object") {
    copyTradingStoreCache.copyAlertsByUserId = {};
  }
  if (!copyTradingStoreCache.copyRuntimeStatusByUserId || typeof copyTradingStoreCache.copyRuntimeStatusByUserId !== "object") {
    copyTradingStoreCache.copyRuntimeStatusByUserId = {};
  }
  return copyTradingStoreCache;
}

function saveCopyTradingStore() {
  const store = loadCopyTradingStore();
  store.updatedAt = Date.now();
  ensureDir(path.dirname(COPY_TRADING_STORE_PATH));
  writeJsonAtomic(COPY_TRADING_STORE_PATH, store);
  return store;
}

function getAuthSessionFromRequest(req) {
  const cookies = parseCookies(req && req.headers ? req.headers.cookie || "" : "");
  const token = String(cookies.pf_session || "").trim();
  if (!token) return null;
  const store = loadCopyTradingStore();
  const session = store.sessionsByToken && store.sessionsByToken[token];
  if (!session) return null;
  if (Number(session.expiresAt || 0) <= Date.now()) {
    delete store.sessionsByToken[token];
    saveCopyTradingStore();
    return null;
  }
  const user = session.userId ? findUserById(session.userId) : null;
  if (!user) return null;
  return { token, session, user };
}

function getLeaderWalletsForUser(userId) {
  const store = loadCopyTradingStore();
  const rows = Array.isArray(store.leadersByUserId && store.leadersByUserId[userId])
    ? store.leadersByUserId[userId]
    : [];
  return rows.slice().sort((left, right) => Number(right.addedAt || 0) - Number(left.addedAt || 0));
}

function getUserById(userId) {
  return findUserById(userId);
}

function normalizeApiKeyLabel(value) {
  const label = String(value || "").trim();
  return label.replace(/\s+/g, " ").slice(0, 80);
}

function normalizeApiKeyValue(value) {
  return String(value || "").trim();
}

function toNum(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function maskApiKeyValue(value) {
  const key = normalizeApiKeyValue(value);
  if (!key) return "";
  if (key.length <= 8) return `${key.slice(0, 4)}…`;
  return `${key.slice(0, 4)}…${key.slice(-4)}`;
}

function getApiKeysForUser(userId) {
  const store = loadCopyTradingStore();
  const rows = Array.isArray(store.apiKeysByUserId && store.apiKeysByUserId[userId])
    ? store.apiKeysByUserId[userId]
    : [];
  return rows
    .slice()
    .sort((left, right) => Number(right.createdAt || 0) - Number(left.createdAt || 0));
}

function serializeApiKeyRecord(row) {
  if (!row) return null;
  const accountWallet = normalizeApiKeyValue(row.accountWallet || row.account || "");
  const agentWallet = normalizeApiKeyValue(row.agentWallet || row.apiKey || "");
  const agentPrivateKey = normalizeApiKeyValue(row.agentPrivateKey || row.apiSecret || "");
  return {
    id: row.id,
    label: row.label || "Pacifica access",
    accountWallet,
    accountWalletPreview: maskApiKeyValue(accountWallet),
    agentWallet,
    agentWalletPreview: maskApiKeyValue(agentWallet),
    hasSecret: Boolean(agentPrivateKey),
    agentPrivateKeyPreview: maskApiKeyValue(agentPrivateKey),
    createdAt: row.createdAt || null,
    updatedAt: row.updatedAt || null,
  };
}

function serializeBalanceSnapshot(row) {
  if (!row || typeof row !== "object") return null;
  const balanceValue = Number(row.balance);
  const pendingBalanceValue = Number(row.pendingBalance);
  const amountValue = Number(row.amount);
  return {
    balance: Number.isFinite(balanceValue) ? balanceValue : null,
    pendingBalance: Number.isFinite(pendingBalanceValue) ? pendingBalanceValue : null,
    amount: Number.isFinite(amountValue) ? amountValue : null,
    eventType: row.eventType || null,
    createdAt: row.createdAt || null,
  };
}

function serializeCopyProfile(row) {
  const next = normalizeCopyProfile(row);
  return {
    copyMode: next.copyMode,
    copyOnlyNewTrades: next.copyOnlyNewTrades,
    copyExistingTrades: next.copyExistingTrades,
    allowReopenAfterManualClose: next.allowReopenAfterManualClose,
    pauseNewOpensOnly: next.pauseNewOpensOnly,
    stopMode: next.stopMode,
    updatedAt: next.updatedAt,
  };
}

function serializeCopyRiskRules(row) {
  const next = normalizeCopyRiskRules(row);
  return next;
}

function serializeCopyRuntimeStatus(row) {
  return normalizeCopyRuntimeStatus(row);
}

function serializeCopyPosition(row) {
  if (!row || typeof row !== "object") return null;
  return {
    token: String(row.token || "-").trim().toUpperCase() || "-",
    side: String(row.side || "").trim().toLowerCase(),
    size: Number.isFinite(Number(row.size)) ? Number(row.size) : null,
    positionSizeUsd: Number.isFinite(Number(row.positionSizeUsd)) ? Number(row.positionSizeUsd) : null,
    entry: Number.isFinite(Number(row.entry)) ? Number(row.entry) : null,
    markPrice: Number.isFinite(Number(row.markPrice)) ? Number(row.markPrice) : null,
    pnlUsd: Number.isFinite(Number(row.pnlUsd)) ? Number(row.pnlUsd) : null,
    roiPct: Number.isFinite(Number(row.roiPct)) ? Number(row.roiPct) : null,
    liquidationPrice: Number.isFinite(Number(row.liquidationPrice)) ? Number(row.liquidationPrice) : null,
    margin: Number.isFinite(Number(row.margin)) ? Number(row.margin) : null,
    leverage: Number.isFinite(Number(row.leverage)) ? Number(row.leverage) : null,
    openedAt: Number.isFinite(Number(row.openedAt)) ? Number(row.openedAt) : null,
    updatedAt: Number.isFinite(Number(row.updatedAt)) ? Number(row.updatedAt) : null,
    source: row.source || "pacifica",
  };
}

function serializeCopyActivityRow(row) {
  if (!row || typeof row !== "object") return null;
  return {
    id: row.id || null,
    createdAt: Number.isFinite(Number(row.createdAt)) ? Number(row.createdAt) : null,
    userId: row.userId || null,
    leaderWallet: row.leaderWallet || null,
    symbol: row.symbol || row.token || null,
    token: row.token || null,
    side: row.side || null,
    action: row.action || row.kind || null,
    reason: row.reason || row.message || null,
    status: row.status || null,
    scope: row.scope || null,
    source: row.source || "engine",
    amount: Number.isFinite(Number(row.amount)) ? Number(row.amount) : null,
    leader: row.leader || null,
    payload: row.payload || null,
  };
}

function handleCopyTradingWorkerEvent(userId, event, store) {
  const payload = event && typeof event === "object" ? event : {};
  appendCopyActivityLog(userId, {
    userId,
    leaderWallet: payload.leaderWallet || null,
    symbol: payload.token || null,
    token: payload.token || null,
    side: payload.side || null,
    action: payload.action || payload.kind || null,
    reason: payload.message || payload.reason || null,
    status: payload.status || null,
    scope: payload.scope || null,
    source: "worker",
    amount: Number.isFinite(Number(payload.amount)) ? Number(payload.amount) : null,
    payload,
  });
  if (String(payload.kind || "").toLowerCase() === "error") {
    appendCopyAlert(userId, {
      severity: "bad",
      leaderWallet: payload.leaderWallet || null,
      symbol: payload.token || null,
      message: payload.message || "Execution error",
      source: "worker",
    });
  }
  if (String(payload.kind || "").toLowerCase() === "leader_update") {
    const storeRow = {
      id: payload.id || crypto.randomUUID(),
      createdAt: Number(payload.createdAt || Date.now()),
      leaderWallet: payload.leaderWallet || null,
      action: "leader_update",
      message: payload.message || "Leader activity changed.",
      source: "worker",
      payload,
    };
    const localStore = loadCopyTradingStore();
    if (!localStore.leaderEventsByUserId || typeof localStore.leaderEventsByUserId !== "object") {
      localStore.leaderEventsByUserId = {};
    }
    if (!Array.isArray(localStore.leaderEventsByUserId[userId])) {
      localStore.leaderEventsByUserId[userId] = [];
    }
    localStore.leaderEventsByUserId[userId].unshift(storeRow);
    localStore.leaderEventsByUserId[userId] = localStore.leaderEventsByUserId[userId].slice(0, 200);
    saveCopyTradingStore();
  }
  if (String(payload.scope || "").toLowerCase() === "execution" && String(payload.kind || "").toLowerCase() === "order_submitted") {
    const localStore = loadCopyTradingStore();
    if (!localStore.executionReceiptsByUserId || typeof localStore.executionReceiptsByUserId !== "object") {
      localStore.executionReceiptsByUserId = {};
    }
    if (!Array.isArray(localStore.executionReceiptsByUserId[userId])) {
      localStore.executionReceiptsByUserId[userId] = [];
    }
    localStore.executionReceiptsByUserId[userId].unshift({
      id: payload.id || crypto.randomUUID(),
      createdAt: Number(payload.createdAt || Date.now()),
      status: payload.status || "submitted",
      message: payload.message || null,
      payload,
    });
    localStore.executionReceiptsByUserId[userId] = localStore.executionReceiptsByUserId[userId].slice(0, 200);
    saveCopyTradingStore();
  }
  if (String(payload.kind || "").toLowerCase() === "cycle") {
    const currentRuntime = getCopyRuntimeStatusForUser(userId);
    saveCopyRuntimeStatusForUser(userId, {
      state:
        currentRuntime.state === "paused" ||
        currentRuntime.state === "stopped_keep" ||
        currentRuntime.state === "close_all" ||
        currentRuntime.state === "blocked"
          ? currentRuntime.state
          : "running",
      health: "healthy",
      lastHealthyAt: Number(payload.createdAt || Date.now()),
    });
  }
  broadcastCopyTradingEvent(userId, {
    type: "copy-event",
    event: serializeCopyActivityRow({
      ...payload,
      userId,
    }),
  });
}

function buildCopyDashboardStatusSnapshot(userId) {
  const store = loadCopyTradingStore();
  const user = getUserById(userId);
  const apiKeys = getApiKeysForUser(userId).map(serializeApiKeyRecord).filter(Boolean);
  const leaders = getLeaderWalletsForUser(userId).map(serializeCopyLeaderWallet).filter(Boolean);
  const profile = serializeCopyProfile(store.copyProfilesByUserId && store.copyProfilesByUserId[userId]);
  const risk = serializeCopyRiskRules(store.copyRiskRulesByUserId && store.copyRiskRulesByUserId[userId]);
  const runtime = serializeCopyRuntimeStatus(store.copyRuntimeStatusByUserId && store.copyRuntimeStatusByUserId[userId]);
  const execution =
    copyTradingExecutionWorker && typeof copyTradingExecutionWorker.getUserExecutionView === "function"
      ? copyTradingExecutionWorker.getUserExecutionView(userId)
      : {
          ok: true,
          config: {
            enabled: false,
            dryRun: true,
            copyRatio: 1,
            maxAllocationPct: 100,
            minNotionalUsd: 1,
            fixedNotionalUsd: 50,
            slippagePercent: 0.5,
            retryPolicy: "balanced",
            syncBehavior: "baseline_only",
            leveragePolicy: "cap",
            accessKeyId: "",
          },
          status: { events: [] },
          apiKeys,
        };
  const authenticated = Boolean(user);
  const twoFactorEnabled = Boolean(user && user.mfaEnabled && user.mfaSecret);
  const pacificaReady = apiKeys.some((row) => row && row.accountWallet && row.agentWallet && row.hasSecret);
  const executionEnabled = Boolean(execution && execution.config && execution.config.enabled);
  const executionMode = execution && execution.config && execution.config.dryRun === false ? "live" : "dry_run";
  const readiness = {
    authenticated,
    twoFactorEnabled,
    pacificaReady,
    leaderCount: leaders.length,
    executionEnabled,
    executionMode,
    runtimeState: runtime.state,
    syncHealth: runtime.health,
    active: authenticated && twoFactorEnabled && pacificaReady && leaders.length > 0 && executionEnabled,
  };
  return {
    ok: true,
    user: serializeAuthUser(user),
    authenticated,
    twoFactorEnabled,
    pacificaReady,
    readiness,
    profile,
    risk,
    runtime,
    leaders,
    apiKeys,
    execution,
  };
}

function normalizeCopyMode(value) {
  const mode = String(value || "").trim().toLowerCase();
  if (["proportional", "fixed_amount", "fixed_allocation", "smart_sync"].includes(mode)) return mode;
  if (["fixed", "fixed_per_trade", "per_trade"].includes(mode)) return "fixed_amount";
  if (["allocation", "fixed_per_leader", "leader_allocation"].includes(mode)) return "fixed_allocation";
  return "proportional";
}

function normalizeCsvSymbols(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => String(item || "").trim().toUpperCase())
      .filter(Boolean);
  }
  return String(value || "")
    .split(/[,\s]+/g)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function normalizeCopyProfile(row) {
  const next = row && typeof row === "object" ? row : {};
  return {
    copyMode: normalizeCopyMode(next.copyMode || next.mode),
    copyOnlyNewTrades: true,
    copyExistingTrades: false,
    allowReopenAfterManualClose:
      next.allowReopenAfterManualClose !== undefined ? Boolean(next.allowReopenAfterManualClose) : false,
    pauseNewOpensOnly: next.pauseNewOpensOnly !== undefined ? Boolean(next.pauseNewOpensOnly) : false,
    stopMode: ["keep", "close_all"].includes(String(next.stopMode || "").trim())
      ? String(next.stopMode).trim()
      : "keep",
    updatedAt: Number(next.updatedAt || 0) || null,
  };
}

function normalizeLeaderRiskRow(row) {
  const next = row && typeof row === "object" ? row : {};
  return {
    maxAllocationPct: Number.isFinite(Number(next.maxAllocationPct)) ? Math.max(0, Number(next.maxAllocationPct)) : null,
    maxDrawdownPct: Number.isFinite(Number(next.maxDrawdownPct)) ? Math.max(0, Number(next.maxDrawdownPct)) : null,
    dailyLossCapUsd: Number.isFinite(Number(next.dailyLossCapUsd)) ? Math.max(0, Number(next.dailyLossCapUsd)) : null,
    leverageCap: Number.isFinite(Number(next.leverageCap)) ? Math.max(0, Number(next.leverageCap)) : null,
    maxOpenPositions: Number.isFinite(Number(next.maxOpenPositions)) ? Math.max(0, Number(next.maxOpenPositions)) : null,
    allowedSymbols: normalizeCsvSymbols(next.allowedSymbols || []),
    blockedSymbols: normalizeCsvSymbols(next.blockedSymbols || []),
    updatedAt: Number(next.updatedAt || 0) || null,
  };
}

function normalizeCopyRiskRules(row) {
  const next = row && typeof row === "object" ? row : {};
  return {
    global: {
      totalCopiedExposureCapUsd: Number.isFinite(Number(next.global && next.global.totalCopiedExposureCapUsd))
        ? Math.max(0, Number(next.global.totalCopiedExposureCapUsd))
        : Number.isFinite(Number(next.totalCopiedExposureCapUsd))
        ? Math.max(0, Number(next.totalCopiedExposureCapUsd))
        : null,
      totalDailyLossCapUsd: Number.isFinite(Number(next.global && next.global.totalDailyLossCapUsd))
        ? Math.max(0, Number(next.global.totalDailyLossCapUsd))
        : Number.isFinite(Number(next.totalDailyLossCapUsd))
        ? Math.max(0, Number(next.totalDailyLossCapUsd))
        : null,
      leverageCap: Number.isFinite(Number(next.global && next.global.leverageCap))
        ? Math.max(0, Number(next.global.leverageCap))
        : Number.isFinite(Number(next.leverageCap))
        ? Math.max(0, Number(next.leverageCap))
        : null,
      maxOpenPositions: Number.isFinite(Number(next.global && next.global.maxOpenPositions))
        ? Math.max(0, Number(next.global.maxOpenPositions))
        : Number.isFinite(Number(next.maxOpenPositions))
        ? Math.max(0, Number(next.maxOpenPositions))
        : null,
      maxAllocationPct: Number.isFinite(Number(next.global && next.global.maxAllocationPct))
        ? Math.max(0, Number(next.global.maxAllocationPct))
        : Number.isFinite(Number(next.maxAllocationPct))
        ? Math.max(0, Number(next.maxAllocationPct))
        : null,
      autoPauseAfterFailures: Number.isFinite(Number(next.global && next.global.autoPauseAfterFailures))
        ? Math.max(0, Number(next.global.autoPauseAfterFailures))
        : Number.isFinite(Number(next.autoPauseAfterFailures))
        ? Math.max(0, Number(next.autoPauseAfterFailures))
        : 3,
      blockedSymbols: normalizeCsvSymbols(next.global && next.global.blockedSymbols ? next.global.blockedSymbols : next.blockedSymbols || []),
      allowedSymbols: normalizeCsvSymbols(next.global && next.global.allowedSymbols ? next.global.allowedSymbols : next.allowedSymbols || []),
    },
    leaders: (() => {
      const input = next.leaders && typeof next.leaders === "object" ? next.leaders : {};
      const nextLeaders = {};
      Object.keys(input).forEach((key) => {
        nextLeaders[String(key || "").trim().toUpperCase()] = normalizeLeaderRiskRow(input[key]);
      });
      return nextLeaders;
    })(),
    updatedAt: Number(next.updatedAt || 0) || null,
  };
}

function normalizeCopyRuntimeStatus(row) {
  const next = row && typeof row === "object" ? row : {};
  const state = String(next.state || next.mode || "running").trim();
  return {
    state: ["running", "paused", "stopped_keep", "close_all", "blocked"].includes(state) ? state : "running",
    health: ["healthy", "degraded", "paused", "blocked"].includes(String(next.health || "").trim())
      ? String(next.health).trim()
      : "healthy",
    lastStateChangeAt: Number(next.lastStateChangeAt || 0) || null,
    lastHealthyAt: Number(next.lastHealthyAt || 0) || null,
    lastBlockedAt: Number(next.lastBlockedAt || 0) || null,
    pausedReason: next.pausedReason ? String(next.pausedReason) : null,
    blockedReason: next.blockedReason ? String(next.blockedReason) : null,
    openOnly: next.openOnly !== undefined ? Boolean(next.openOnly) : false,
    closeAllRequested: next.closeAllRequested !== undefined ? Boolean(next.closeAllRequested) : false,
    updatedAt: Number(next.updatedAt || 0) || null,
  };
}

function getCopyProfileForUser(userId) {
  const store = loadCopyTradingStore();
  const row = store.copyProfilesByUserId && store.copyProfilesByUserId[userId];
  return normalizeCopyProfile(row);
}

function saveCopyProfileForUser(userId, patch = {}) {
  const store = loadCopyTradingStore();
  if (!store.copyProfilesByUserId || typeof store.copyProfilesByUserId !== "object") {
    store.copyProfilesByUserId = {};
  }
  const current = normalizeCopyProfile(store.copyProfilesByUserId[userId]);
  const next = normalizeCopyProfile({ ...current, ...patch, updatedAt: Date.now() });
  store.copyProfilesByUserId[userId] = next;
  saveCopyTradingStore();
  return next;
}

function getCopyRiskRulesForUser(userId) {
  const store = loadCopyTradingStore();
  const row = store.copyRiskRulesByUserId && store.copyRiskRulesByUserId[userId];
  return normalizeCopyRiskRules(row);
}

function saveCopyRiskRulesForUser(userId, patch = {}) {
  const store = loadCopyTradingStore();
  if (!store.copyRiskRulesByUserId || typeof store.copyRiskRulesByUserId !== "object") {
    store.copyRiskRulesByUserId = {};
  }
  const current = normalizeCopyRiskRules(store.copyRiskRulesByUserId[userId]);
  const merged = {
    ...current,
    ...patch,
    global: {
      ...(current.global || {}),
      ...(patch.global || {}),
    },
    leaders: {
      ...(current.leaders || {}),
      ...(patch.leaders || {}),
    },
    updatedAt: Date.now(),
  };
  const next = normalizeCopyRiskRules(merged);
  store.copyRiskRulesByUserId[userId] = next;
  saveCopyTradingStore();
  return next;
}

function getCopyRuntimeStatusForUser(userId) {
  const store = loadCopyTradingStore();
  const row = store.copyRuntimeStatusByUserId && store.copyRuntimeStatusByUserId[userId];
  return normalizeCopyRuntimeStatus(row);
}

function saveCopyRuntimeStatusForUser(userId, patch = {}) {
  const store = loadCopyTradingStore();
  if (!store.copyRuntimeStatusByUserId || typeof store.copyRuntimeStatusByUserId !== "object") {
    store.copyRuntimeStatusByUserId = {};
  }
  const current = normalizeCopyRuntimeStatus(store.copyRuntimeStatusByUserId[userId]);
  const next = normalizeCopyRuntimeStatus({ ...current, ...patch, updatedAt: Date.now() });
  store.copyRuntimeStatusByUserId[userId] = next;
  saveCopyTradingStore();
  return next;
}

function getCopyActivityLogForUser(userId) {
  const store = loadCopyTradingStore();
  const rows = Array.isArray(store.copyActivityLogByUserId && store.copyActivityLogByUserId[userId])
    ? store.copyActivityLogByUserId[userId]
    : [];
  return rows.slice().sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
}

function appendCopyActivityLog(userId, event) {
  const store = loadCopyTradingStore();
  if (!store.copyActivityLogByUserId || typeof store.copyActivityLogByUserId !== "object") {
    store.copyActivityLogByUserId = {};
  }
  if (!Array.isArray(store.copyActivityLogByUserId[userId])) {
    store.copyActivityLogByUserId[userId] = [];
  }
  const payload = {
    id: crypto.randomUUID(),
    createdAt: Date.now(),
    ...event,
  };
  store.copyActivityLogByUserId[userId].unshift(payload);
  store.copyActivityLogByUserId[userId] = store.copyActivityLogByUserId[userId].slice(0, 500);
  saveCopyTradingStore();
  return payload;
}

function getCopyAlertsForUser(userId) {
  const store = loadCopyTradingStore();
  const rows = Array.isArray(store.copyAlertsByUserId && store.copyAlertsByUserId[userId])
    ? store.copyAlertsByUserId[userId]
    : [];
  return rows.slice().sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
}

function appendCopyAlert(userId, event) {
  const store = loadCopyTradingStore();
  if (!store.copyAlertsByUserId || typeof store.copyAlertsByUserId !== "object") {
    store.copyAlertsByUserId = {};
  }
  if (!Array.isArray(store.copyAlertsByUserId[userId])) {
    store.copyAlertsByUserId[userId] = [];
  }
  const payload = {
    id: crypto.randomUUID(),
    createdAt: Date.now(),
    severity: event && event.severity ? String(event.severity) : "warn",
    ...event,
  };
  store.copyAlertsByUserId[userId].unshift(payload);
  store.copyAlertsByUserId[userId] = store.copyAlertsByUserId[userId].slice(0, 100);
  saveCopyTradingStore();
  return payload;
}

function getCopyCopiedPositionsForUser(userId) {
  const store = loadCopyTradingStore();
  const rows = Array.isArray(store.copiedPositionsByUserId && store.copiedPositionsByUserId[userId])
    ? store.copiedPositionsByUserId[userId]
    : [];
  return rows.slice().sort((a, b) => Number(b.updatedAt || b.openedAt || 0) - Number(a.updatedAt || a.openedAt || 0));
}

function setCopyCopiedPositionsForUser(userId, rows) {
  const store = loadCopyTradingStore();
  if (!store.copiedPositionsByUserId || typeof store.copiedPositionsByUserId !== "object") {
    store.copiedPositionsByUserId = {};
  }
  store.copiedPositionsByUserId[userId] = Array.isArray(rows) ? rows.slice() : [];
  saveCopyTradingStore();
  return store.copiedPositionsByUserId[userId];
}

function getCopyTradingClientSet(userId) {
  const key = String(userId || "").trim();
  if (!key) return null;
  if (!copyTradingSseClients.has(key)) {
    copyTradingSseClients.set(key, new Set());
  }
  return copyTradingSseClients.get(key);
}

function broadcastCopyTradingEvent(userId, event) {
  const clients = getCopyTradingClientSet(userId);
  if (!clients || !clients.size) return;
  const data = `data: ${JSON.stringify(event)}\n\n`;
  for (const res of clients) {
    try {
      res.write(data);
    } catch (_error) {}
  }
}

function registerCopyTradingSseClient(userId, req, res) {
  const clients = getCopyTradingClientSet(userId);
  if (!clients) return false;
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.write(`event: ready\ndata: ${JSON.stringify({ ok: true, userId, createdAt: Date.now() })}\n\n`);
  const heartbeat = setInterval(() => {
    try {
      res.write(`event: ping\ndata: ${JSON.stringify({ ok: true, ts: Date.now() })}\n\n`);
    } catch (_error) {}
  }, 25000);
  const cleanup = () => {
    clearInterval(heartbeat);
    clients.delete(res);
    try {
      res.end();
    } catch (_error) {}
  };
  clients.add(res);
  req.on("close", cleanup);
  req.on("error", cleanup);
  return true;
}

function serializeCopyLeaderWallet(row) {
  if (!row) return null;
  return {
    id: row.id || row.wallet,
    wallet: String(row.wallet || "").trim(),
    walletPreview: maskApiKeyValue(row.wallet || ""),
    label: row.label || row.name || "",
    note: row.note || "",
    allocationPct: Number.isFinite(Number(row.allocationPct)) ? Number(row.allocationPct) : null,
    allocationUsd: Number.isFinite(Number(row.allocationUsd)) ? Number(row.allocationUsd) : null,
    paused: Boolean(row.paused),
    stopMode: row.stopMode || "keep",
    copiedPositionsCount: Number.isFinite(Number(row.copiedPositionsCount)) ? Number(row.copiedPositionsCount) : null,
    currentCopiedExposureUsd: Number.isFinite(Number(row.currentCopiedExposureUsd)) ? Number(row.currentCopiedExposureUsd) : null,
    pnlUsd: Number.isFinite(Number(row.pnlUsd)) ? Number(row.pnlUsd) : null,
    lastSyncAt: Number(row.lastSyncAt || 0) || null,
    syncState: row.syncState || "unknown",
    createdAt: Number(row.addedAt || row.createdAt || 0) || null,
    updatedAt: Number(row.updatedAt || 0) || null,
  };
}

function serializeAuthUser(user) {
  if (!user) return null;
  const backupStats = getBackupCodeStats(user);
  return {
    id: user.id,
    email: user.email && !String(user.email).endsWith("@twitter.local") ? user.email : null,
    displayName: user.displayName || normalizeAuthDisplayName("", user.email),
    authProvider: user.authProvider || (user.twitterId ? "twitter" : "password"),
    twitterId: user.twitterId || null,
    twitterUsername: user.twitterUsername || null,
    twitterName: user.twitterName || null,
    twitterAvatar: user.twitterAvatar || null,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
    emailVerifiedAt: user.emailVerifiedAt || null,
    mfaEnabled: Boolean(user.mfaEnabled),
    mfaEnabledAt: user.mfaEnabledAt || null,
    mfaBackupCodesConfigured: backupStats.configured,
    mfaBackupCodesRemaining: backupStats.remaining,
    mfaBackupCodesGeneratedAt: backupStats.generatedAt,
    mfaBackupCodesLastUsedAt: backupStats.lastUsedAt,
    apiKeyCount: getApiKeysForUser(user.id).length,
  };
}

function validateLeaderWallet(rawWallet) {
  const wallet = String(rawWallet || "").trim();
  if (!wallet) return "";
  if (wallet.length < 20 || wallet.length > 64) return "";
  if (!/^[A-Za-z0-9]+$/.test(wallet)) return "";
  return wallet;
}

async function parseJsonRequestBody(req, limitBytes = 1_000_000) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on("data", (chunk) => {
      total += chunk.length;
      if (total > limitBytes) {
        reject(new Error("request_body_too_large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (!chunks.length) {
        resolve({});
        return;
      }
      try {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve(text ? JSON.parse(text) : {});
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

async function fetchLeaderActivityPayload(wallet, options = {}) {
  const normalizedWallet = validateLeaderWallet(wallet);
  if (!normalizedWallet) {
    throw new Error("invalid_leader_wallet");
  }
  const basePath = `/api/wallet-performance/wallet/${encodeURIComponent(normalizedWallet)}?force=1`;
  const liveTradePath = `/api/live-trades/wallet/${encodeURIComponent(normalizedWallet)}`;
  const [performance, liveTrade] = await Promise.all([
    fetchLocalJson(basePath, Math.min(25000, Number(options.timeoutMs || 20000))).catch((error) => ({
      error: error && error.message ? String(error.message) : "wallet_performance_fetch_failed",
    })),
    fetchLocalJson(liveTradePath, Math.min(15000, Number(options.timeoutMs || 20000))).catch((error) => ({
      error: error && error.message ? String(error.message) : "live_trade_fetch_failed",
    })),
  ]);
  const performanceSummary =
    performance && performance.summary && typeof performance.summary === "object" ? performance.summary : {};
  const liveSummary =
    liveTrade && liveTrade.summary && typeof liveTrade.summary === "object" ? liveTrade.summary : {};
  const positions =
    (performance && Array.isArray(performance.positions) && performance.positions) ||
    (liveTrade && Array.isArray(liveTrade.positions) && liveTrade.positions) ||
    [];
  const fundingHistory =
    (performance && Array.isArray(performance.fundingHistory) && performance.fundingHistory) ||
    (performance && Array.isArray(performance.funding) && performance.funding) ||
    [];
  const openOrders =
    (performance && Array.isArray(performance.openOrders) && performance.openOrders) ||
    (performance && Array.isArray(performance.orders) && performance.orders) ||
    [];
  const recentEvents =
    (liveTrade && Array.isArray(liveTrade.positions) && liveTrade.positions.slice(0, 50)) ||
    (liveTrade && Array.isArray(liveTrade.events) && liveTrade.events) ||
    [];
  return {
    wallet: normalizedWallet,
    fetchedAt: Date.now(),
    performance,
    liveTrade,
    summary: {
      wallet: normalizedWallet,
      totalPnlUsd: toNum(
        performanceSummary.totalPnlUsd ?? performanceSummary.pnlUsd ?? performanceSummary.all?.totalPnlUsd,
        NaN
      ),
      realizedPnlUsd: toNum(
        performanceSummary.pnlUsd ?? performanceSummary.all?.pnlUsd ?? performanceSummary.all?.realizedPnlUsd,
        NaN
      ),
      unrealizedPnlUsd: toNum(
        performanceSummary.unrealizedPnlUsd ?? performanceSummary.positionPnlUsd ?? performanceSummary.unrealizedPnl,
        NaN
      ),
      accountEquityUsd: toNum(
        performanceSummary.accountEquityUsd ?? performanceSummary.walletEquityUsd ?? performanceSummary.accountEquity,
        NaN
      ),
      returnPct: toNum(performanceSummary.returnPct ?? performanceSummary.all?.returnPct, NaN),
      drawdownPct: toNum(performanceSummary.drawdownPct ?? performanceSummary.all?.drawdownPct, NaN),
      openPositions: Math.max(
        0,
        Number(
          performanceSummary.openPositions ??
            performanceSummary.all?.openPositions ??
            liveSummary.openPositions ??
            positions.length
        ) || 0
      ),
      lastTrade:
        performanceSummary.lastTrade ??
        performanceSummary.lastActivityAt ??
        liveSummary.lastTrade ??
        liveSummary.lastEventAt ??
        performanceSummary.updatedAt ??
        null,
      positionCount: positions.length,
      fundingHistoryCount: fundingHistory.length,
      openOrdersCount: openOrders.length,
      liveEventCount: recentEvents.length,
    },
    positions,
    fundingHistory,
    openOrders,
    recentEvents,
  };
}

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fmtServerNumber(value, decimals = 0) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function fmtServerMoney(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "Blocked";
  return `$${fmtServerNumber(num, 2)}`;
}

function fmtServerDuration(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return "-";
  if (num < 1000) return `${Math.round(num)}ms`;
  if (num < 60000) return `${Math.round(num / 1000)}s`;
  if (num < 3600000) return `${Math.round(num / 60000)}m`;
  if (num < 86400000) return `${(num / 3600000).toFixed(1)}h`;
  return `${(num / 86400000).toFixed(1)}d`;
}

function fmtServerDate(value) {
  if (!value) return "-";
  const ts = Number(value);
  const date = Number.isFinite(ts) ? new Date(ts) : new Date(value);
  if (!Number.isFinite(date.getTime())) return "-";
  return date.toISOString().slice(0, 10);
}

function fmtServerTime(value) {
  if (!value) return "-";
  const ts = Number(value);
  const date = Number.isFinite(ts) ? new Date(ts) : new Date(value);
  if (!Number.isFinite(date.getTime())) return "-";
  return date.toISOString().replace(/\.\d{3}Z$/, "Z").replace("T", " ");
}

function shortWalletLabel(wallet) {
  const raw = String(wallet || "");
  if (raw.length <= 14) return raw;
  return `${raw.slice(0, 6)}...${raw.slice(-4)}`;
}

function renderWalletRowsHtml(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const wallet = String(row && row.wallet ? row.wallet : "");
      const firstTrade = row && row.firstTrade;
      const lastTrade = row && (row.lastTrade || row.lastActivity || row.updatedAt);
      const pnlUsd = Number(row && row.pnlUsd);
      return [
        `<tr data-wallet="${escapeHtml(wallet)}" data-wallet-row="${escapeHtml(wallet)}">`,
        `<td><div class="wallet-cell"><strong class="wallet-cell-value">${escapeHtml(shortWalletLabel(wallet))}</strong></div></td>`,
        `<td>${escapeHtml(fmtServerNumber(row && row.trades, 0))}</td>`,
        `<td>${escapeHtml(Number.isFinite(Number(row && row.volumeUsd)) ? fmtServerMoney(row && row.volumeUsd) : "-")}</td>`,
        `<td>${escapeHtml(fmtServerNumber(row && row.totalWins, 0))}</td>`,
        `<td>${escapeHtml(fmtServerNumber(row && row.totalLosses, 0))}</td>`,
        `<td class="${pnlUsd >= 0 ? "good" : "bad"}">${escapeHtml(Number.isFinite(pnlUsd) ? fmtServerMoney(pnlUsd) : "-")}</td>`,
        `<td>${escapeHtml(fmtServerNumber(row && row.openPositions, 0))}</td>`,
        `<td>${escapeHtml(Number.isFinite(Number(row && row.winRate)) ? `${fmtServerNumber(row && row.winRate, 2)}%` : "-")}</td>`,
        `<td>${escapeHtml(fmtServerDate(firstTrade))}</td>`,
        `<td>${escapeHtml(fmtServerDate(lastTrade))}</td>`,
        `<td class="wallet-action-cell"><div class="wallet-action-group"><button class="btn-ghost inspect-btn" data-wallet="${escapeHtml(wallet)}">Inspect</button></div></td>`,
        "</tr>",
      ].join("");
    })
    .join("");
}

function renderOpsSummaryHtml(items = []) {
  return (Array.isArray(items) ? items : [])
    .map((item) => {
      const label = escapeHtml(String((item && item.label) || ""));
      const value = escapeHtml(String((item && item.value) || "-"));
      return `<div class="ops-summary-item"><span class="ops-summary-label">${label}</span><strong class="ops-summary-value">${value}</strong></div>`;
    })
    .join("");
}

function renderWalletsIndexHtml(baseHtml, snapshotPayload) {
  if (!baseHtml || typeof baseHtml !== "string") return baseHtml;
  const payload =
    snapshotPayload && typeof snapshotPayload === "object"
      ? JSON.parse(JSON.stringify(snapshotPayload))
      : {};
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  const counts = payload.counts && typeof payload.counts === "object" ? payload.counts : {};
  const sorting = payload.sorting && typeof payload.sorting === "object" ? payload.sorting : {};
  const generatedAt = Number(payload.generatedAt || Date.now());
  const progress = payload.progress && typeof payload.progress === "object" ? payload.progress : {};
  const system = payload.system && typeof payload.system === "object" ? payload.system : {};
  const freshness = payload.freshness && typeof payload.freshness === "object" ? payload.freshness : {};
  const exchangeOverview = payload.exchangeOverview && typeof payload.exchangeOverview === "object" ? payload.exchangeOverview : {};
  const walletVolumeProgress =
    progress.walletVolumeProgress && typeof progress.walletVolumeProgress === "object"
      ? progress.walletVolumeProgress
      : {};
  const discoveryStatus =
    progress.discoveryStatus && typeof progress.discoveryStatus === "object"
      ? progress.discoveryStatus
      : {};
  const discoveryState = String(
    discoveryStatus.status ||
      (progress.discoveryWindow && progress.discoveryWindow.mode) ||
      "unknown"
  );
  const discoverySnapshotAt = Number(discoveryStatus.updatedAt || 0) || null;
  const discoveryScanAt =
    Number(discoveryStatus.lastScanAt || 0) ||
    Number(freshness.lastSuccessfulWalletDiscoveryAt || 0) ||
    null;
  const discoveryStaleAfterMs = Number(discoveryStatus.staleAfterMs || 0) || null;
  const discoveryHealthPct =
    discoveryScanAt && discoveryStaleAfterMs && discoveryStaleAfterMs > 0
      ? Math.max(0, Math.min(100, (1 - Math.max(0, Date.now() - discoveryScanAt) / discoveryStaleAfterMs) * 100))
      : Math.max(
          0,
          Math.min(100, Number((progress.discoveryWindow && progress.discoveryWindow.percentComplete) || 0))
        );
  const discoveryNew1h = Number(discoveryStatus.newWallets && discoveryStatus.newWallets.last1h);
  const discoveryNew6h = Number(discoveryStatus.newWallets && discoveryStatus.newWallets.last6h);
  const discoveryNew24h = Number(discoveryStatus.newWallets && discoveryStatus.newWallets.last24h);
  const discoveryLatestNewAt = Number(discoveryStatus.latestNewWalletAt || 0) || null;
  const discoveryWalletCount = Math.max(
    Number(discoveryStatus.walletCount || 0),
    Number(counts.discoveredWallets || counts.totalWallets || payload.total || rows.length)
  );
  const discoveryConfirmedWallets = Math.max(
    Number(discoveryStatus.confirmedWallets || 0),
    Number(counts.validatedWallets || 0)
  );
  const metaText = `Showing ${fmtServerNumber(rows.length, 0)} of ${fmtServerNumber(
    Number(counts.filteredRows || payload.total || rows.length),
    0
  )} wallets · updated ${new Date(generatedAt).toISOString()}`;
  const walletProgressSummary = renderOpsSummaryHtml([
    {
      label: "Wallets",
      value: fmtServerNumber(Number(counts.totalWallets || payload.total || rows.length), 0),
    },
    {
      label: "History Done",
      value: fmtServerNumber(Number(counts.tradeHistoryCompleteWallets || 0), 0),
    },
    {
      label: "Metrics Done",
      value: fmtServerNumber(Number(counts.metricsCompleteWallets || 0), 0),
    },
    {
      label: "Rankable",
      value: fmtServerNumber(Number(counts.leaderboardEligibleWallets || 0), 0),
    },
  ]);
  const walletDiscoverySummary = renderOpsSummaryHtml([
    {
      label: "Snapshot",
      value: fmtServerTime(discoverySnapshotAt),
    },
    {
      label: "Latest Scan",
      value: fmtServerTime(discoveryScanAt),
    },
    {
      label: "Discovered",
      value: fmtServerNumber(discoveryWalletCount, 0),
    },
    {
      label: "Validated",
      value: fmtServerNumber(discoveryConfirmedWallets, 0),
    },
    {
      label: "New 1H",
      value: Number.isFinite(discoveryNew1h) ? fmtServerNumber(discoveryNew1h, 0) : "-",
    },
    {
      label: "New 6H",
      value: Number.isFinite(discoveryNew6h) ? fmtServerNumber(discoveryNew6h, 0) : "-",
    },
    {
      label: "New 24H",
      value: Number.isFinite(discoveryNew24h) ? fmtServerNumber(discoveryNew24h, 0) : "-",
    },
    {
      label: "Latest New",
      value: fmtServerTime(discoveryLatestNewAt),
    },
  ]);
  const walletSystemSummary = renderOpsSummaryHtml([
    {
      label: "Proxy Health",
      value: `${fmtServerNumber(Number(system.proxyHealth && system.proxyHealth.active || 0), 0)} / ${fmtServerNumber(
        Number(system.proxyHealth && system.proxyHealth.total || 0),
        0
      )}`,
    },
    {
      label: "REST Pace",
      value:
        system.restPace && Number.isFinite(Number(system.restPace.rpmCap))
          ? `${fmtServerNumber(system.restPace.used1m, 1)} / ${fmtServerNumber(system.restPace.rpmCap, 1)}`
          : "-",
    },
    {
      label: "WS Wallets",
      value: fmtServerNumber(Number(system.wsHealth && system.wsHealth.activeWallets || 0), 0),
    },
    {
      label: "WS Backlog",
      value: fmtServerNumber(Number(system.wsHealth && system.wsHealth.promotionBacklog || 0), 0),
    },
  ]);
  const walletFreshnessSummary = renderOpsSummaryHtml([
    {
      label: "Wallet Sync",
      value: freshness.lastSuccessfulSyncAt ? fmtServerDate(freshness.lastSuccessfulSyncAt) : "-",
    },
    {
      label: "Discovery",
      value: freshness.lastSuccessfulWalletDiscoveryAt
        ? fmtServerDate(freshness.lastSuccessfulWalletDiscoveryAt)
        : "-",
    },
    {
      label: "Exchange Rollup",
      value: freshness.lastSuccessfulExchangeRollupAt
        ? fmtServerDate(freshness.lastSuccessfulExchangeRollupAt)
        : "-",
    },
    {
      label: "Lag",
      value: Number.isFinite(Number(freshness.lagBehindNowMs))
        ? `${fmtServerNumber(Number(freshness.lagBehindNowMs) / 1000, 0)}s`
        : "-",
    },
  ]);
  const walletVolumeSummary = renderOpsSummaryHtml([
    {
      label: "Raw Volume",
      value: fmtServerMoney(
        Number(walletVolumeProgress.currentRawVolumeUsd ?? exchangeOverview.walletSummedVolumeUsd ?? 0)
      ),
    },
    {
      label: "Verified",
      value: fmtServerMoney(
        Number(walletVolumeProgress.currentVerifiedVolumeUsd ?? exchangeOverview.walletVerifiedVolumeUsd ?? 0)
      ),
    },
    {
      label: "Raw Delta",
      value: Number.isFinite(Number(walletVolumeProgress.deltaRawVolumeUsd))
        ? `${Number(walletVolumeProgress.deltaRawVolumeUsd) > 0 ? "+" : ""}${fmtServerMoney(
            Number(walletVolumeProgress.deltaRawVolumeUsd)
          )}`
        : "-",
    },
    {
      label: "Wallets Done",
      value: fmtServerNumber(Number(walletVolumeProgress.currentCompletedWallets || counts.trackedFullWallets || 0), 0),
    },
    {
      label: "Wallets / Hour",
      value: Number.isFinite(Number(walletVolumeProgress.completedWalletsPerHour))
        ? fmtServerNumber(Number(walletVolumeProgress.completedWalletsPerHour), 1)
        : "-",
    },
    {
      label: "Coverage",
      value: Number.isFinite(Number(walletVolumeProgress.walletCoveragePct))
        ? `${fmtServerNumber(Number(walletVolumeProgress.walletCoveragePct), 2)}%`
        : "-",
    },
    {
      label: "ETA",
      value: Number.isFinite(Number(walletVolumeProgress.etaWalletCompletionMs))
        ? fmtServerDuration(Number(walletVolumeProgress.etaWalletCompletionMs))
        : Number.isFinite(Number(walletVolumeProgress.etaVolumeGapMs))
        ? fmtServerDuration(Number(walletVolumeProgress.etaVolumeGapMs))
        : "-",
    },
  ]);
  const walletProgressMeta = `History complete ${fmtServerNumber(
    Number(counts.tradeHistoryCompleteWallets || 0),
    0
  )} | metrics complete ${fmtServerNumber(Number(counts.metricsCompleteWallets || 0), 0)} | rankable ${fmtServerNumber(
    Number(counts.leaderboardEligibleWallets || 0),
    0
  )}`;
  const walletDiscoveryMeta = `${String(discoveryState || "unknown")} | latest snapshot ${fmtServerTime(
    discoverySnapshotAt
  )} | latest scan ${fmtServerTime(discoveryScanAt)} | discovered ${fmtServerNumber(
    discoveryWalletCount,
    0
  )} | new ${Number.isFinite(discoveryNew1h) ? fmtServerNumber(discoveryNew1h, 0) : "-"} in 1H, ${
    Number.isFinite(discoveryNew6h) ? fmtServerNumber(discoveryNew6h, 0) : "-"
  } in 6H, ${Number.isFinite(discoveryNew24h) ? fmtServerNumber(discoveryNew24h, 0) : "-"} in 24H`;
  const walletSystemMeta = `Mode ${String(system.mode || "unknown")} | proxies ${fmtServerNumber(
    Number(system.proxyHealth && system.proxyHealth.active || 0),
    0
  )} / ${fmtServerNumber(Number(system.proxyHealth && system.proxyHealth.total || 0), 0)}`;
  const walletFreshnessMeta = freshness.lastSuccessfulSyncAt
    ? `Last wallet sync ${fmtServerDate(freshness.lastSuccessfulSyncAt)}`
    : "Last wallet sync -";
  const walletVolumeMeta = `Raw ${fmtServerMoney(
    Number(walletVolumeProgress.currentRawVolumeUsd ?? exchangeOverview.walletSummedVolumeUsd ?? 0)
  )} | verified ${fmtServerMoney(
    Number(walletVolumeProgress.currentVerifiedVolumeUsd ?? exchangeOverview.walletVerifiedVolumeUsd ?? 0)
  )}${
    Number.isFinite(Number(walletVolumeProgress.deltaRawVolumeUsd))
      ? ` | last snapshot ${Number(walletVolumeProgress.deltaRawVolumeUsd) >= 0 ? "+" : ""}${fmtServerMoney(
          Number(walletVolumeProgress.deltaRawVolumeUsd)
        )}`
      : ""
  }${
    Number.isFinite(Number(walletVolumeProgress.snapshotIntervalMs))
      ? ` | snapshot interval ${fmtServerDuration(Number(walletVolumeProgress.snapshotIntervalMs))}`
      : ""
  }${
    Number.isFinite(Number(walletVolumeProgress.completedWalletsPerHour))
      ? ` | ${fmtServerNumber(Number(walletVolumeProgress.completedWalletsPerHour), 1)} wallets/hour`
      : ""
  }`;
  const walletProgressPct = Math.max(
    0,
    Math.min(100, Number(progress.walletHistoryTimeline && progress.walletHistoryTimeline.percentComplete || 0))
  );
  const walletDiscoveryPct = discoveryHealthPct;
  const walletVolumePct = Math.max(
    0,
    Math.min(
      100,
      Number(
        walletVolumeProgress.walletCoveragePct ??
          (((Number(exchangeOverview.volumeRatio || 0) || 0) * 100) || 0)
      )
    )
  );
  const bootstrapJson = JSON.stringify(payload).replace(/</g, "\\u003c");
  const bootstrapRenderScript = [
    "<script>",
    "(function(){",
    "function n(v,d){const num=Number(v);if(!Number.isFinite(num))return '-';return num.toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});}",
    "function money(v){const num=Number(v);if(!Number.isFinite(num))return '-';return '$'+n(num,2);}",
    "function duration(v){const num=Number(v);if(!Number.isFinite(num)||num<0)return '-';if(num<1000)return Math.round(num)+'ms';if(num<60000)return Math.round(num/1000)+'s';if(num<3600000)return Math.round(num/60000)+'m';if(num<86400000)return (num/3600000).toFixed(1)+'h';return (num/86400000).toFixed(1)+'d';}",
    "function shortWallet(v){const raw=String(v||'');if(raw.length<=14)return raw;return raw.slice(0,6)+'...'+raw.slice(-4);}",
    "function dateOnly(v){if(!v)return '-';const ts=Number(v);const d=Number.isFinite(ts)?new Date(ts):new Date(v);if(!Number.isFinite(d.getTime()))return '-';return d.toISOString().slice(0,10);}",
    "function timeOnly(v){if(!v)return '-';const ts=Number(v);const d=Number.isFinite(ts)?new Date(ts):new Date(v);if(!Number.isFinite(d.getTime()))return '-';return d.toISOString().replace(/\\.\\d{3}Z$/,'Z').replace('T',' ');}",
    "function title(v){return String(v||'').replace(/[_-]+/g,' ').replace(/\\b\\w/g,function(ch){return ch.toUpperCase();});}",
    "function setText(id,val){const node=document.getElementById(id);if(node)node.textContent=String(val==null?'':val);}",
    "function renderSummary(id,items){const host=document.getElementById(id);if(!host)return;host.innerHTML=(Array.isArray(items)?items:[]).map(function(item){return '<div class=\"ops-summary-item\"><span class=\"ops-summary-label\">'+String(item.label||'')+'</span><strong class=\"ops-summary-value\">'+String(item.value||'-')+'</strong></div>';}).join('');}",
    "function setProgress(id,pct){const node=document.getElementById(id);if(!node)return;const value=Number(pct);node.style.width=(Number.isFinite(value)?Math.max(0,Math.min(100,value)):0)+'%';}",
    "function render(){",
    "var payload=window.__PF_BOOTSTRAP_WALLETS__||null;if(!payload)return;",
    "var rows=Array.isArray(payload.rows)?payload.rows:[];",
    "var counts=payload.counts&&typeof payload.counts==='object'?payload.counts:{};",
    "var progress=payload.progress&&typeof payload.progress==='object'?payload.progress:{};",
    "var system=payload.system&&typeof payload.system==='object'?payload.system:{};",
    "var freshness=payload.freshness&&typeof payload.freshness==='object'?payload.freshness:{};",
    "var exchangeOverview=payload.exchangeOverview&&typeof payload.exchangeOverview==='object'?payload.exchangeOverview:{};",
    "var discoveryStatus=progress.discoveryStatus&&typeof progress.discoveryStatus==='object'?progress.discoveryStatus:{};",
    "var walletVolumeProgress=progress.walletVolumeProgress&&typeof progress.walletVolumeProgress==='object'?progress.walletVolumeProgress:{};",
    "var sorting=payload.sorting&&typeof payload.sorting==='object'?payload.sorting:{};",
    "var discoverySnapshotAt=Number(discoveryStatus.updatedAt||0)||null;",
    "var discoveryScanAt=Number(discoveryStatus.lastScanAt||0)||Number(freshness.lastSuccessfulWalletDiscoveryAt||0)||null;",
    "var discoveryStaleAfterMs=Number(discoveryStatus.staleAfterMs||0)||null;",
    "var discoveryHealthPct=discoveryScanAt&&discoveryStaleAfterMs&&discoveryStaleAfterMs>0?Math.max(0,Math.min(100,(1-Math.max(0,Date.now()-discoveryScanAt)/discoveryStaleAfterMs)*100)):Math.max(0,Math.min(100,Number(progress.discoveryWindow&&progress.discoveryWindow.percentComplete||0)));",
    "var discoveryWalletCount=Math.max(Number(discoveryStatus.walletCount||0),Number(counts.discoveredWallets||counts.totalWallets||payload.total||rows.length));",
    "var discoveryConfirmed=Math.max(Number(discoveryStatus.confirmedWallets||0),Number(counts.validatedWallets||0));",
    "var discoveryNew1h=Number(discoveryStatus.newWallets&&discoveryStatus.newWallets.last1h);",
    "var discoveryNew6h=Number(discoveryStatus.newWallets&&discoveryStatus.newWallets.last6h);",
    "var discoveryNew24h=Number(discoveryStatus.newWallets&&discoveryStatus.newWallets.last24h);",
    "var discoveryLatestNewAt=Number(discoveryStatus.latestNewWalletAt||0)||null;",
    "var discoveryState=String(discoveryStatus.status||progress.discoveryWindow&&progress.discoveryWindow.mode||'unknown');",
    "var tbody=document.getElementById('wallet-table-body');",
    "if(tbody){tbody.innerHTML=rows.length?rows.map(function(row){var wallet=String(row&&row.wallet||'');var safeWallet=wallet.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\\\"/g,'&quot;');var pnl=Number(row&&row.pnlUsd);var lastTrade=row&&(row.lastTrade||row.lastActivity||row.updatedAt);return '<tr data-wallet=\"'+safeWallet+'\" data-wallet-row=\"'+safeWallet+'\">'+'<td><div class=\"wallet-cell\"><strong class=\"wallet-cell-value\">'+shortWallet(wallet)+'</strong></div></td>'+'<td>'+n(row&&row.trades,0)+'</td>'+'<td>'+(Number.isFinite(Number(row&&row.volumeUsd))?money(row&&row.volumeUsd):'-')+'</td>'+'<td>'+n(row&&row.totalWins,0)+'</td>'+'<td>'+n(row&&row.totalLosses,0)+'</td>'+'<td class=\"'+(pnl>=0?'good':'bad')+'\">'+(Number.isFinite(pnl)?money(pnl):'-')+'</td>'+'<td>'+n(row&&row.openPositions,0)+'</td>'+'<td>'+(Number.isFinite(Number(row&&row.winRate))?n(row&&row.winRate,2)+'%':'-')+'</td>'+'<td>'+dateOnly(row&&row.firstTrade)+'</td>'+'<td>'+dateOnly(lastTrade)+'</td>'+'<td class=\"wallet-action-cell\"><div class=\"wallet-action-group\"><button class=\"btn-ghost inspect-btn\" data-wallet=\"'+safeWallet+'\">Inspect</button></div></td>'+'</tr>';}).join(''):'<tr><td colspan=\"11\" class=\"muted\">No wallet rows in snapshot.</td></tr>';}",
    "setText('wallet-total-label', n(Number(counts.totalWallets||payload.total||rows.length),0)+' wallets');",
    "setText('wallet-table-meta', 'Showing '+n(rows.length,0)+' of '+n(Number(counts.filteredRows||payload.total||rows.length),0)+' wallets · updated '+new Date(Number(payload.generatedAt||Date.now())).toISOString());",
    "setText('wallet-page-label', 'Page '+Number(payload.page||1)+' / '+Number(payload.pages||1));",
    "setText('wallet-system-mode', String(system.mode||'unknown'));",
    "setText('wallet-discovery-label', title(discoveryState||'unknown'));",
    "setText('wallet-shard-label', n(Number(system.activeShardCount||0),0)+' shards');",
    "setText('wallet-freshness-label', freshness.lastSuccessfulWalletDiscoveryAt ? ('discover '+dateOnly(freshness.lastSuccessfulWalletDiscoveryAt)) : 'stale');",
    "setText('wallet-volume-label', String(walletVolumeProgress.progressState||'stalled'));",
    "renderSummary('wallet-progress-summary',[{label:'Wallets',value:n(Number(counts.totalWallets||payload.total||rows.length),0)},{label:'History Done',value:n(Number(counts.tradeHistoryCompleteWallets||0),0)},{label:'Metrics Done',value:n(Number(counts.metricsCompleteWallets||0),0)},{label:'Rankable',value:n(Number(counts.leaderboardEligibleWallets||0),0)}]);",
    "renderSummary('wallet-discovery-summary',[{label:'Snapshot',value:timeOnly(discoverySnapshotAt)},{label:'Latest Scan',value:timeOnly(discoveryScanAt)},{label:'Discovered',value:n(discoveryWalletCount,0)},{label:'Validated',value:n(discoveryConfirmed,0)},{label:'New 1H',value:Number.isFinite(discoveryNew1h)?n(discoveryNew1h,0):'-'},{label:'New 6H',value:Number.isFinite(discoveryNew6h)?n(discoveryNew6h,0):'-'},{label:'New 24H',value:Number.isFinite(discoveryNew24h)?n(discoveryNew24h,0):'-'},{label:'Latest New',value:timeOnly(discoveryLatestNewAt)}]);",
    "renderSummary('wallet-system-summary',[{label:'Proxy Health',value:n(Number(system.proxyHealth&&system.proxyHealth.active||0),0)+' / '+n(Number(system.proxyHealth&&system.proxyHealth.total||0),0)},{label:'REST Pace',value:system.restPace&&Number.isFinite(Number(system.restPace.rpmCap))?(n(system.restPace.used1m,1)+' / '+n(system.restPace.rpmCap,1)):'-'},{label:'WS Wallets',value:n(Number(system.wsHealth&&system.wsHealth.activeWallets||0),0)},{label:'WS Backlog',value:n(Number(system.wsHealth&&system.wsHealth.promotionBacklog||0),0)}]);",
    "renderSummary('wallet-freshness-summary',[{label:'Wallet Sync',value:freshness.lastSuccessfulSyncAt?dateOnly(freshness.lastSuccessfulSyncAt):'-'},{label:'Discovery',value:freshness.lastSuccessfulWalletDiscoveryAt?dateOnly(freshness.lastSuccessfulWalletDiscoveryAt):'-'},{label:'Exchange Rollup',value:freshness.lastSuccessfulExchangeRollupAt?dateOnly(freshness.lastSuccessfulExchangeRollupAt):'-'},{label:'Lag',value:Number.isFinite(Number(freshness.lagBehindNowMs))?n(Number(freshness.lagBehindNowMs)/1000,0)+'s':'-'}]);",
    "renderSummary('wallet-volume-summary',[{label:'Raw Volume',value:money(Number(walletVolumeProgress.currentRawVolumeUsd??exchangeOverview.walletSummedVolumeUsd??0))},{label:'Verified',value:money(Number(walletVolumeProgress.currentVerifiedVolumeUsd??exchangeOverview.walletVerifiedVolumeUsd??0))},{label:'Raw Delta',value:Number.isFinite(Number(walletVolumeProgress.deltaRawVolumeUsd))?((Number(walletVolumeProgress.deltaRawVolumeUsd)>0?'+':'')+money(Number(walletVolumeProgress.deltaRawVolumeUsd))):'-'},{label:'Wallets Done',value:n(Number(walletVolumeProgress.currentCompletedWallets||counts.trackedFullWallets||0),0)},{label:'Wallets / Hour',value:Number.isFinite(Number(walletVolumeProgress.completedWalletsPerHour))?n(Number(walletVolumeProgress.completedWalletsPerHour),1):'-'},{label:'Coverage',value:Number.isFinite(Number(walletVolumeProgress.walletCoveragePct))?(n(Number(walletVolumeProgress.walletCoveragePct),2)+'%'):'-'},{label:'ETA',value:Number.isFinite(Number(walletVolumeProgress.etaWalletCompletionMs))?duration(Number(walletVolumeProgress.etaWalletCompletionMs)):(Number.isFinite(Number(walletVolumeProgress.etaVolumeGapMs))?duration(Number(walletVolumeProgress.etaVolumeGapMs)):'-')}]);",
    "setText('wallet-progress-meta','History complete '+n(Number(counts.tradeHistoryCompleteWallets||0),0)+' | metrics complete '+n(Number(counts.metricsCompleteWallets||0),0)+' | rankable '+n(Number(counts.leaderboardEligibleWallets||0),0));",
    "setText('wallet-discovery-meta',String(discoveryState||'unknown')+' | latest snapshot '+timeOnly(discoverySnapshotAt)+' | latest scan '+timeOnly(discoveryScanAt)+' | discovered '+n(discoveryWalletCount,0)+' | new '+(Number.isFinite(discoveryNew1h)?n(discoveryNew1h,0):'-')+' in 1H, '+(Number.isFinite(discoveryNew6h)?n(discoveryNew6h,0):'-')+' in 6H, '+(Number.isFinite(discoveryNew24h)?n(discoveryNew24h,0):'-')+' in 24H');",
    "setText('wallet-system-meta','Mode '+String(system.mode||'unknown')+' | proxies '+n(Number(system.proxyHealth&&system.proxyHealth.active||0),0)+' / '+n(Number(system.proxyHealth&&system.proxyHealth.total||0),0));",
    "setText('wallet-freshness-meta', freshness.lastSuccessfulSyncAt ? ('Last wallet sync '+dateOnly(freshness.lastSuccessfulSyncAt)) : 'Last wallet sync -');",
    "setText('wallet-volume-meta','Raw '+money(Number(walletVolumeProgress.currentRawVolumeUsd??exchangeOverview.walletSummedVolumeUsd??0))+' | verified '+money(Number(walletVolumeProgress.currentVerifiedVolumeUsd??exchangeOverview.walletVerifiedVolumeUsd??0))+(Number.isFinite(Number(walletVolumeProgress.deltaRawVolumeUsd))?(' | last snapshot '+(Number(walletVolumeProgress.deltaRawVolumeUsd)>=0?'+':'')+money(Number(walletVolumeProgress.deltaRawVolumeUsd))):'')+(Number.isFinite(Number(walletVolumeProgress.snapshotIntervalMs))?(' | snapshot interval '+duration(Number(walletVolumeProgress.snapshotIntervalMs))):'')+(Number.isFinite(Number(walletVolumeProgress.completedWalletsPerHour))?(' | '+n(Number(walletVolumeProgress.completedWalletsPerHour),1)+' wallets/hour'):'') );",
    "setProgress('wallet-progress-bar', Number(progress.walletHistoryTimeline&&progress.walletHistoryTimeline.percentComplete||0));",
    "setProgress('wallet-discovery-bar', discoveryHealthPct);",
    "setProgress('wallet-volume-bar', Number(walletVolumeProgress.walletCoveragePct||Number(exchangeOverview.volumeRatio||0)*100||0));",
    "}",
    "if(document.readyState==='loading'){document.addEventListener('DOMContentLoaded',render,{once:true});}else{render();}",
    "})();",
    "</script>",
  ].join("");
  let html = baseHtml;
  html = html.replace(
    '<section id="exchange-view" class="view active">',
    '<section id="exchange-view" class="view" hidden aria-hidden="true">'
  );
  html = html.replace(
    '<section id="wallets-view" class="view" hidden aria-hidden="true">',
    '<section id="wallets-view" class="view active" aria-hidden="false">'
  );
  html = html.replace(
    /(<span id="wallet-total-label"[^>]*>)([^<]*)(<\/span>)/,
    (_match, open, _inner, close) =>
      `${open}${escapeHtml(`${fmtServerNumber(Number(counts.totalWallets || payload.total || rows.length), 0)} wallets`)}${close}`
  );
  html = html.replace(
    /(<p id="wallet-table-meta"[^>]*>)([\s\S]*?)(<\/p>)/,
    (_match, open, _inner, close) => `${open}${escapeHtml(metaText)}${close}`
  );
  html = html.replace(
    /(<span id="wallet-page-label">)([\s\S]*?)(<\/span>)/,
    (_match, open, _inner, close) =>
      `${open}${escapeHtml(`Page ${Number(payload.page || 1)} / ${Number(payload.pages || 1)}`)}${close}`
  );
  html = html.replace(
    /(<div id="wallet-progress-summary" class="ops-summary-grid">)([\s\S]*?)(<\/div>)/,
    (_match, open, _inner, close) => `${open}${walletProgressSummary}${close}`
  );
  html = html.replace(
    /(<div id="wallet-discovery-summary" class="ops-summary-grid">)([\s\S]*?)(<\/div>)/,
    (_match, open, _inner, close) => `${open}${walletDiscoverySummary}${close}`
  );
  html = html.replace(
    /(<div id="wallet-system-summary" class="ops-summary-grid">)([\s\S]*?)(<\/div>)/,
    (_match, open, _inner, close) => `${open}${walletSystemSummary}${close}`
  );
  html = html.replace(
    /(<div id="wallet-freshness-summary" class="ops-summary-grid">)([\s\S]*?)(<\/div>)/,
    (_match, open, _inner, close) => `${open}${walletFreshnessSummary}${close}`
  );
  html = html.replace(
    /(<div id="wallet-volume-summary" class="ops-summary-grid">)([\s\S]*?)(<\/div>)/,
    (_match, open, _inner, close) => `${open}${walletVolumeSummary}${close}`
  );
  html = html.replace(
    /(<p id="wallet-progress-meta" class="ops-meta">)([\s\S]*?)(<\/p>)/,
    (_match, open, _inner, close) => `${open}${escapeHtml(walletProgressMeta)}${close}`
  );
  html = html.replace(
    /(<p id="wallet-discovery-meta" class="ops-meta">)([\s\S]*?)(<\/p>)/,
    (_match, open, _inner, close) => `${open}${escapeHtml(walletDiscoveryMeta)}${close}`
  );
  html = html.replace(
    /(<p id="wallet-system-meta" class="ops-meta">)([\s\S]*?)(<\/p>)/,
    (_match, open, _inner, close) => `${open}${escapeHtml(walletSystemMeta)}${close}`
  );
  html = html.replace(
    /(<p id="wallet-freshness-meta" class="ops-meta">)([\s\S]*?)(<\/p>)/,
    (_match, open, _inner, close) => `${open}${escapeHtml(walletFreshnessMeta)}${close}`
  );
  html = html.replace(
    /(<p id="wallet-volume-meta" class="ops-meta">)([\s\S]*?)(<\/p>)/,
    (_match, open, _inner, close) => `${open}${escapeHtml(walletVolumeMeta)}${close}`
  );
  html = html.replace(
    /(<div id="wallet-progress-bar" class="ops-progress-bar"[^>]*)(><\/div>)/,
    (_match, open, close) => `${open} style="width:${walletProgressPct}%;"${close}`
  );
  html = html.replace(
    /(<div id="wallet-discovery-bar" class="ops-progress-bar"[^>]*)(><\/div>)/,
    (_match, open, close) => `${open} style="width:${walletDiscoveryPct}%;"${close}`
  );
  html = html.replace(
    /(<div id="wallet-volume-bar" class="ops-progress-bar"[^>]*)(><\/div>)/,
    (_match, open, close) => `${open} style="width:${walletVolumePct}%;"${close}`
  );
  html = html.replace(
    /(<span id="wallet-discovery-label" class="muted-pill">)([\s\S]*?)(<\/span>)/,
    (_match, open, _inner, close) =>
      `${open}${escapeHtml(
        String(discoveryState || "unknown")
          .replace(/[_-]+/g, " ")
          .replace(/\b\w/g, (ch) => ch.toUpperCase())
      )}${close}`
  );
  html = html.replace(
    /(<span id="wallet-volume-label" class="muted-pill">)([\s\S]*?)(<\/span>)/,
    (_match, open, _inner, close) =>
      `${open}${escapeHtml(String(walletVolumeProgress.progressState || "stalled"))}${close}`
  );
  html = html.replace(
    /(<tbody id="wallet-table-body">)([\s\S]*?)(<\/tbody>)/,
    (_match, open, _inner, close) =>
      `${open}${renderWalletRowsHtml(rows) || '<tr><td colspan="11" class="muted">No wallet rows in snapshot.</td></tr>'}${close}`
  );
  html = html.replace(
    "</body>",
    () =>
      `<script>window.__PF_BOOTSTRAP_VIEW__="wallets";window.__PF_BOOTSTRAP_WALLETS__=${bootstrapJson};</script>${bootstrapRenderScript}</body>`
  );
  return html;
}

function renderWalletsShellHtml(baseHtml, snapshotPayload = null) {
  let html = baseHtml;
  html = html.replace(
    '<section id="exchange-view" class="view active">',
    '<section id="exchange-view" class="view" hidden aria-hidden="true">'
  );
  html = html.replace(
    '<section id="wallets-view" class="view" hidden aria-hidden="true">',
    '<section id="wallets-view" class="view active" aria-hidden="false">'
  );
  const bootstrapScript = `<script>window.__PF_BOOTSTRAP_VIEW__="wallets";${
    snapshotPayload && typeof snapshotPayload === "object"
      ? `window.__PF_BOOTSTRAP_WALLETS__=${JSON.stringify(snapshotPayload).replace(/</g, "\\u003c")};`
      : ""
  }</script>`;
  html = html.replace("</body>", `${bootstrapScript}</body>`);
  return html;
}

function isUiReadModelOnlyEnabled(walletStore = null) {
  return (
    !INDEXER_ENABLED &&
    !ONCHAIN_ENABLED &&
    !GLOBAL_KPI_ENABLED &&
    !SNAPSHOT_ENABLED &&
    walletStore &&
    walletStore.externalShardsEnabled
  );
}

function isDefaultWalletsApiQuery(url) {
  if (!url || url.pathname !== "/api/wallets") return false;
  return walletTrackingReadModel.isDefaultWalletSearchParams(url.searchParams);
}

function getMinimalUiDatasetFileMtimeMs() {
  try {
    return Number(fs.statSync(UI_MINIMAL_DATASET_PATH).mtimeMs || 0) || 0;
  } catch (_error) {
    return 0;
  }
}

function refreshMinimalUiDataset(force = false) {
  if (minimalUiDatasetCache.inflight) return minimalUiDatasetCache.inflight;
  minimalUiDatasetCache.inflight = Promise.resolve()
    .then(() => {
      const sourceMtimeMs = getMinimalUiSourceMtimeMs(
        INDEXER_EXTERNAL_SHARDS_DIR,
        GLOBAL_KPI_PATH,
        INDEXER_MULTI_WORKER_SHARDS
      );
      const diskPayload = loadMinimalUiDataset(UI_MINIMAL_DATASET_PATH);
      const diskMtimeMs = getMinimalUiDatasetFileMtimeMs();
      const diskFreshEnough =
        !force &&
        diskPayload &&
        sourceMtimeMs > 0 &&
        sourceMtimeMs <= Math.max(diskMtimeMs, Number(diskPayload.generatedAt || 0) || 0);
      if (diskFreshEnough) {
        minimalUiDatasetCache.value = diskPayload;
        minimalUiDatasetCache.mtimeMs = diskMtimeMs;
        minimalUiDatasetCache.sourceMtimeMs = sourceMtimeMs;
        return diskPayload;
      }
      const dataset = buildMinimalUiDataset({
        shardsDir: INDEXER_EXTERNAL_SHARDS_DIR,
        globalKpiPath: GLOBAL_KPI_PATH,
        shardCount: INDEXER_MULTI_WORKER_SHARDS,
      });
      writeMinimalUiDataset(UI_MINIMAL_DATASET_PATH, dataset);
      minimalUiDatasetCache.value = dataset;
      minimalUiDatasetCache.mtimeMs = getMinimalUiDatasetFileMtimeMs();
      minimalUiDatasetCache.sourceMtimeMs = sourceMtimeMs;
      return dataset;
    })
    .finally(() => {
      minimalUiDatasetCache.inflight = null;
    });
  return minimalUiDatasetCache.inflight;
}

function getMinimalUiDataset(options = {}) {
  const force = Boolean(options.force);
  const sourceMtimeMs = getMinimalUiSourceMtimeMs(
    INDEXER_EXTERNAL_SHARDS_DIR,
    GLOBAL_KPI_PATH,
    INDEXER_MULTI_WORKER_SHARDS
  );
  if (
    !force &&
    minimalUiDatasetCache.value &&
    minimalUiDatasetCache.sourceMtimeMs >= sourceMtimeMs &&
    minimalUiDatasetCache.mtimeMs > 0
  ) {
    return minimalUiDatasetCache.value;
  }
  const diskPayload = loadMinimalUiDataset(UI_MINIMAL_DATASET_PATH);
  if (diskPayload && !force) {
    minimalUiDatasetCache.value = diskPayload;
    minimalUiDatasetCache.mtimeMs = getMinimalUiDatasetFileMtimeMs();
    minimalUiDatasetCache.sourceMtimeMs = sourceMtimeMs;
    if (sourceMtimeMs > minimalUiDatasetCache.mtimeMs) {
      refreshMinimalUiDataset(false).catch(() => null);
    }
    return diskPayload;
  }
  return null;
}

function buildMinimalFreshness(snapshot) {
  const generatedAt = Number((snapshot && snapshot.generatedAt) || Date.now());
  const exchangeUpdatedAt = Number(
    (snapshot && snapshot.exchangeOverview && snapshot.exchangeOverview.updatedAt) || generatedAt
  );
  return {
    lastSuccessfulSyncAt: generatedAt,
    lastSuccessfulWalletDiscoveryAt: generatedAt,
    lastSuccessfulExchangeRollupAt: exchangeUpdatedAt,
    lagBehindNowMs: Math.max(0, Date.now() - exchangeUpdatedAt),
  };
}

function buildMinimalSystemSummary() {
  return {
    mode: "minimal",
    activeShardCount: Number(process.env.PACIFICA_INDEXER_MULTI_WORKER_SHARDS || 0),
    proxyHealth: {
      active: 0,
      total: 0,
      cooling: 0,
    },
    heliusHealth: {
      active: 0,
      total: 0,
      rate429PerMin: 0,
    },
    restPace: {
      used1m: 0,
      rpmCap: Number(API_RPM_CAP || 0),
      headroomPct: 100,
    },
    wsHealth: {
      enabled: false,
      activeWallets: 0,
      openConnections: 0,
      promotionBacklog: 0,
    },
  };
}

function buildMinimalExchangeApiPayload(snapshot) {
  const safeSnapshot = snapshot && typeof snapshot === "object" ? snapshot : {};
  const overview =
    safeSnapshot.exchangeOverview && typeof safeSnapshot.exchangeOverview === "object"
      ? safeSnapshot.exchangeOverview
      : {};
  const freshness = buildMinimalFreshness(safeSnapshot);
  const totalVolumeUsd = Number(overview.totalHistoricalVolumeUsd || 0) || 0;
  const openInterestUsd = Number(overview.openInterestUsd || 0) || 0;
  const walletCount = Number(overview.walletCount || (safeSnapshot.wallets && safeSnapshot.wallets.total) || 0) || 0;
  return {
    generatedAt: Number(safeSnapshot.generatedAt || Date.now()),
    minimal: true,
    kpis: {
      activeAccounts: walletCount,
      totalTrades: null,
      totalVolumeUsd,
      totalFeesUsd: null,
      openInterestAtEnd: openInterestUsd,
    },
    kpiPeriodChanges: {},
    metricSemantics: {
      activeAccounts: { state: "complete", method: "tracked_wallet_addresses" },
      totalTrades: { state: "unavailable", method: "omitted_in_minimal_mode" },
      totalVolumeUsd: { state: "complete", method: "exchange_overview_snapshot" },
      totalFeesUsd: { state: "unavailable", method: "omitted_in_minimal_mode" },
      openInterestAtEnd: { state: "complete", method: "exchange_overview_snapshot" },
    },
    progress: {
      currentDate: freshness.lastSuccessfulExchangeRollupAt
        ? new Date(freshness.lastSuccessfulExchangeRollupAt).toISOString().slice(0, 10)
        : null,
      percentComplete: 100,
      daysProcessed: null,
      daysRemaining: 0,
      symbolRollupCompleteDays: null,
      symbolRollupPendingDays: null,
      totalTradesStatus: "unavailable",
      totalFeesStatus: "unavailable",
    },
    system: buildMinimalSystemSummary(),
    freshness,
    source: {
      datasetMode: "minimal",
      totalVolumeMethod: "minimal_dashboard_snapshot",
      volumeRankSource: "disabled_in_minimal_mode",
      prices: [],
    },
    volumeRank: [],
    exchangeOverview: overview,
    validation: safeSnapshot.validation || null,
  };
}

function buildMinimalWalletsApiPayload(snapshot, url) {
  const safeSnapshot = snapshot && typeof snapshot === "object" ? snapshot : {};
  const addresses =
    safeSnapshot.wallets && Array.isArray(safeSnapshot.wallets.addresses)
      ? safeSnapshot.wallets.addresses
      : [];
  const params = url && url.searchParams ? url.searchParams : new URLSearchParams();
  const page = Math.max(1, Number(params.get("page") || 1) || 1);
  const pageSize = 20;
  const query = String(params.get("q") || "").trim().toLowerCase();
  const filtered = query
    ? addresses.filter((wallet) => String(wallet || "").toLowerCase().includes(query))
    : addresses;
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const start = (Math.min(page, pages) - 1) * pageSize;
  const freshness = buildMinimalFreshness(safeSnapshot);
  const system = buildMinimalSystemSummary();
  const totalWallets = addresses.length;
  return {
    generatedAt: Number(safeSnapshot.generatedAt || Date.now()),
    minimal: true,
    page: Math.min(page, pages),
    pageSize,
    total: filtered.length,
    pages,
    rows: filtered.slice(start, start + pageSize).map((wallet, index) => ({
      rank: start + index + 1,
      wallet,
    })),
    counts: {
      totalWallets,
      filteredRows: filtered.length,
      discoveredWallets: totalWallets,
      validatedWallets: totalWallets,
      trackedWallets: totalWallets,
      trackedLiteWallets: 0,
      trackedFullWallets: 0,
      tradeHistoryCompleteWallets: 0,
      metricsCompleteWallets: 0,
      leaderboardEligibleWallets: 0,
    },
    sorting: {
      key: "wallet",
      dir: "asc",
      label: "Wallet Address",
      shortLabel: "Wallet",
      description: "Lightweight address-only snapshot. Heavy wallet metrics are intentionally omitted from the request path.",
    },
    filters: {
      applied: [],
      availableSymbols: [],
    },
    progress: {
      discovered: totalWallets,
      validated: totalWallets,
      tradeHistoryComplete: 0,
      trackedLite: 0,
      trackedFull: 0,
      discoveryWindow: {
        percentComplete: 100,
        mode: "minimal",
      },
      walletHistoryTimeline: {
        percentComplete: 0,
        mode: "minimal",
      },
    },
    system,
    freshness,
    performance: {
      datasetBuiltAt: Number(safeSnapshot.generatedAt || Date.now()),
    },
    warmup: {
      walletExplorerReady: true,
      retryAfterMs: 0,
    },
    mode: "minimal_wallet_addresses",
  };
}

function renderMinimalWalletsHtml(snapshot, url) {
  const payload = buildMinimalWalletsApiPayload(snapshot, url);
  const overview =
    snapshot && snapshot.exchangeOverview && typeof snapshot.exchangeOverview === "object"
      ? snapshot.exchangeOverview
      : {};
  const validation =
    snapshot && snapshot.validation && typeof snapshot.validation === "object"
      ? snapshot.validation
      : {};
  const rowsHtml = (Array.isArray(payload.rows) ? payload.rows : [])
    .map(
      (row) =>
        `<tr><td>${escapeHtml(fmtServerNumber(row.rank, 0))}</td><td>${escapeHtml(
          row.wallet
        )}</td></tr>`
    )
    .join("");
  const autoRefreshSeconds = 30;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="${autoRefreshSeconds}">
  <title>Pacifica Minimal Dashboard</title>
  <link rel="icon" type="image/png" href="/favicon.png?v=20260325-brand1">
  <link rel="shortcut icon" type="image/png" href="/favicon.png?v=20260325-brand1">
  <link rel="apple-touch-icon" href="/apple-touch-icon.png?v=20260325-brand1">
  <style>
    body{font-family:ui-sans-serif,system-ui,sans-serif;margin:0;background:#f6f7f4;color:#111}
    .wrap{max-width:1200px;margin:0 auto;padding:24px}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:20px}
    .card{background:#fff;border:1px solid #d9ddd3;border-radius:14px;padding:16px}
    h1,h2{margin:0 0 10px}
    h1{font-size:28px}
    h2{font-size:18px}
    .muted{color:#5d6459}
    table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #d9ddd3;border-radius:14px;overflow:hidden}
    th,td{padding:10px 12px;border-bottom:1px solid #ecefe7;text-align:left;font-size:14px}
    th{background:#f0f3eb}
    .pager{display:flex;justify-content:space-between;align-items:center;margin:14px 0}
    a{color:#0b5c47;text-decoration:none}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Pacifica Minimal Dashboard</h1>
    <p class="muted">Addresses only + exchange overview. Auto-refresh every ${autoRefreshSeconds}s. Generated ${escapeHtml(
      new Date(Number(snapshot.generatedAt || Date.now())).toISOString()
    )}.</p>
    <div class="grid">
      <div class="card"><h2>Exchange Total Volume</h2><div>${escapeHtml(
        fmtServerMoney(overview.totalHistoricalVolumeUsd)
      )}</div></div>
      <div class="card"><h2>Wallet Summed Volume</h2><div>${escapeHtml(
        fmtServerMoney(overview.walletSummedVolumeUsd)
      )}</div></div>
      <div class="card"><h2>Gap</h2><div>${escapeHtml(
        fmtServerMoney(overview.volumeGapUsd)
      )}</div></div>
      <div class="card"><h2>Wallet Count</h2><div>${escapeHtml(
        fmtServerNumber((snapshot.wallets && snapshot.wallets.total) || 0, 0)
      )}</div></div>
      <div class="card"><h2>24h Volume</h2><div>${escapeHtml(
        fmtServerMoney(overview.dailyVolumeUsd)
      )}</div></div>
      <div class="card"><h2>Validation</h2><div>${escapeHtml(
        validation.walletVolumeMatchesExchange ? "Matched" : "Mismatch"
      )}</div></div>
    </div>
    <div class="pager">
      <div>Showing ${escapeHtml(fmtServerNumber(payload.rows.length, 0))} of ${escapeHtml(
        fmtServerNumber(payload.total, 0)
      )} wallet addresses</div>
      <div>Page ${escapeHtml(fmtServerNumber(payload.page, 0))} / ${escapeHtml(
        fmtServerNumber(payload.pages, 0)
      )}</div>
    </div>
    <table>
      <thead><tr><th>#</th><th>Wallet Address</th></tr></thead>
      <tbody>${rowsHtml || '<tr><td colspan="2">No wallet addresses available.</td></tr>'}</tbody>
    </table>
  </div>
</body>
</html>`;
}

async function hydrateWalletSearchRowsWithLiveDetail(rows = [], options = {}) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const localPort = Number(options && options.localPort) || PORT || 3200;
  const liveTradesUpstreamBase = String(process.env.PACIFICA_LIVE_TRADES_UPSTREAM_BASE || "")
    .trim()
    .replace(/\/+$/, "");
  const hasUpstreamBase = /^https?:\/\//i.test(liveTradesUpstreamBase);
  if (!sourceRows.length) {
    return sourceRows;
  }
  return Promise.all(
    sourceRows.map(async (row) => {
      const wallet = normalizeAddress(String((row && row.wallet) || "").trim());
      if (!wallet) return row;
      let timeout = null;
      try {
        let payload = null;
        const sources = [
          `http://127.0.0.1:${localPort}/api/live-trades/wallet/${encodeURIComponent(wallet)}`,
          ...(hasUpstreamBase
            ? [`${liveTradesUpstreamBase}/api/live-trades/wallet/${encodeURIComponent(wallet)}`]
            : []),
        ];
        for (const sourceUrl of sources) {
          const controller = new AbortController();
          timeout = setTimeout(() => controller.abort(), 8000);
          const upstreamRes = await fetch(sourceUrl, {
            method: "GET",
            signal: controller.signal,
          });
          clearTimeout(timeout);
          timeout = null;
          if (!upstreamRes.ok) {
            continue;
          }
          payload = await upstreamRes.json();
          if (payload && payload.summary && typeof payload.summary === "object") {
            break;
          }
        }
        if (!payload || !payload.summary || typeof payload.summary !== "object") return row;
        const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
        const positions = Array.isArray(payload && payload.positions) ? payload.positions : [];
        const canonicalOpenedAt =
          Math.max(
            Number(summary.lastOpenedAt || 0),
            Number((row && row.lastOpenedAt) || 0)
          ) || 0;
        const liveLastScanAt =
          Math.max(
            Number((row && row.liveLastScanAt) || 0),
            Number(summary.lastAccountAt || 0),
            Number(summary.lastPositionsAt || 0),
            Number(summary.lastMaterializedOpenAt || 0),
            Number(summary.lastHintOpenAt || 0),
            Number(summary.liveScannedAt || 0)
          ) || 0;
        const unrealizedPnlUsd = Number(
          Number(toFinite(pickUnrealizedPnlUsd(summary, row), 0)).toFixed(2)
        );
        const realizedPnlUsd = Number(Number((row && row.pnlUsd) || 0).toFixed(2));
        return {
          ...row,
          openPositions: Math.max(0, positions.length, Number(summary.openPositions || 0)),
          exposureUsd: Number(
            Number(summary.exposureUsd !== undefined ? summary.exposureUsd : row.exposureUsd || 0).toFixed(2)
          ),
          unrealizedPnlUsd,
          totalPnlUsd: Number((realizedPnlUsd + unrealizedPnlUsd).toFixed(2)),
          lastOpenedAt: canonicalOpenedAt || row.lastOpenedAt || null,
          lastActivity: canonicalOpenedAt || row.lastActivity || null,
          lastActiveAt: canonicalOpenedAt || row.lastActiveAt || null,
          liveLastScanAt: liveLastScanAt || row.liveLastScanAt || null,
          liveActiveRank: summary.liveActiveRank || row.liveActiveRank || null,
          liveActiveScore: Number(
            Number(summary.liveActiveScore !== undefined ? summary.liveActiveScore : row.liveActiveScore || 0).toFixed(2)
          ),
          recentEvents15m: Number(summary.recentEvents15m || row.recentEvents15m || 0),
          recentEvents1h: Number(summary.recentEvents1h || row.recentEvents1h || 0),
        };
      } catch (_error) {
        return row;
      } finally {
        if (timeout) clearTimeout(timeout);
      }
    })
  );
}

async function hydrateWalletProfileWithLiveDetail(profile = null, options = {}) {
  if (!profile || !profile.found) return profile;
  const wallet = normalizeAddress(String((profile && profile.wallet) || "").trim());
  if (!wallet) return profile;
  const localPort = Number(options && options.localPort) || PORT || 3200;
  const liveTradesUpstreamBase = String(process.env.PACIFICA_LIVE_TRADES_UPSTREAM_BASE || "")
    .trim()
    .replace(/\/+$/, "");
  const hasUpstreamBase = /^https?:\/\//i.test(liveTradesUpstreamBase);
  let timeout = null;
  try {
    let payload = null;
    const sources = [
      `http://127.0.0.1:${localPort}/api/live-trades/wallet/${encodeURIComponent(wallet)}`,
      ...(hasUpstreamBase
        ? [`${liveTradesUpstreamBase}/api/live-trades/wallet/${encodeURIComponent(wallet)}`]
        : []),
    ];
    for (const sourceUrl of sources) {
      const controller = new AbortController();
      timeout = setTimeout(() => controller.abort(), 8000);
      const res = await fetch(sourceUrl, {
        method: "GET",
        signal: controller.signal,
      });
      clearTimeout(timeout);
      timeout = null;
      if (!res.ok) continue;
      payload = await res.json();
      if (payload && payload.summary && typeof payload.summary === "object") break;
    }
    const summary = payload && payload.summary && typeof payload.summary === "object" ? payload.summary : null;
    if (!summary) return profile;
    const all = summary.all && typeof summary.all === "object" ? summary.all : {};
    const topSymbols = Array.isArray(summary.topSymbols)
      ? summary.topSymbols
      : Array.isArray(all.topSymbols)
      ? all.topSymbols
      : [];
    const mergedSummary = {
      ...(profile.summary && typeof profile.summary === "object" ? profile.summary : {}),
      volumeUsd:
        Number.isFinite(Number(all.volumeUsd)) ? Number(all.volumeUsd) : profile?.summary?.volumeUsd,
      pnlUsd:
        Number.isFinite(Number(all.pnlUsd)) ? Number(all.pnlUsd) : profile?.summary?.pnlUsd,
      trades:
        Number.isFinite(Number(all.trades)) ? Number(all.trades) : profile?.summary?.trades,
      feesPaidUsd:
        summary.feesPaidUsd !== undefined
          ? Number(summary.feesPaidUsd)
          : all.feesPaidUsd !== undefined
          ? Number(all.feesPaidUsd)
          : profile?.summary?.feesPaidUsd,
      feesUsd:
        all.feesUsd !== undefined ? Number(all.feesUsd) : profile?.summary?.feesUsd,
      netFeesUsd:
        all.netFeesUsd !== undefined ? Number(all.netFeesUsd) : profile?.summary?.netFeesUsd,
      firstTrade:
        all.firstTrade || profile?.summary?.firstTrade || null,
      lastTrade:
        Number.isFinite(Number(all.lastTrade))
          ? Number(all.lastTrade)
          : Number.isFinite(Number(profile?.summary?.lastTrade))
          ? Number(profile.summary.lastTrade)
          : null,
      winRate:
        Number.isFinite(Number(all.winRatePct)) ? Number(all.winRatePct) : profile?.summary?.winRate,
      totalWins:
        Number.isFinite(Number(all.wins)) ? Number(all.wins) : profile?.summary?.totalWins,
      totalLosses:
        Number.isFinite(Number(all.losses)) ? Number(all.losses) : profile?.summary?.totalLosses,
      openPositions:
        Number.isFinite(Number(summary.openPositions))
          ? Number(summary.openPositions)
          : profile?.summary?.openPositions,
      exposureUsd:
        Number.isFinite(Number(summary.exposureUsd)) ? Number(summary.exposureUsd) : profile?.summary?.exposureUsd,
      unrealizedPnlUsd: Number(
        toFinite(
          pickUnrealizedPnlUsd(summary, profile && profile.summary),
          profile?.summary?.unrealizedPnlUsd
        )
      ),
      totalPnlUsd:
        Number.isFinite(Number(summary.totalPnlUsd))
          ? Number(summary.totalPnlUsd)
          : profile?.summary?.totalPnlUsd,
      lastActivityAt:
        Number(summary.lastOpenedAt || summary.lastActivityAt || summary.lastActivity || 0) ||
        profile?.summary?.lastActivityAt ||
        null,
      lastOpenedAt:
        Number(summary.lastOpenedAt || 0) || profile?.summary?.lastOpenedAt || null,
      liveLastScanAt:
        Number(summary.lastPositionsAt || summary.lastAccountAt || 0) || profile?.summary?.liveLastScanAt || null,
    };
    const canonicalActivityAt =
      Number(mergedSummary.lastOpenedAt || 0) ||
      Number(profile?.summary?.lastOpenedAt || 0) ||
      null;
    if (canonicalActivityAt) {
      mergedSummary.lastOpenedAt = canonicalActivityAt;
      mergedSummary.lastActivityAt = canonicalActivityAt;
    }
    return {
      ...profile,
      generatedAt: Date.now(),
      lastOpenedAt: canonicalActivityAt || profile?.lastOpenedAt || null,
      lastActivityAt: canonicalActivityAt || profile?.lastActivityAt || null,
      lastActivity: canonicalActivityAt || profile?.lastActivity || null,
      summary: mergedSummary,
      symbolBreakdown: topSymbols.map((row) => ({
        symbol: String((row && row.symbol) || "").toUpperCase(),
        volumeUsd: Number(row && row.volumeUsd ? row.volumeUsd : 0),
        volumeCompact: null,
      })),
      live: {
        lastOpenedAt: mergedSummary.lastOpenedAt || null,
        lastActivityAt: mergedSummary.lastActivityAt || null,
        liveLastScanAt: mergedSummary.liveLastScanAt || null,
      },
    };
  } catch (_error) {
    return profile;
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function serveUiSnapshotFastPath(req, res, url, walletStore = null) {
  const slimUiMode = isUiReadModelOnlyEnabled(walletStore);
  if (req.method !== "GET" || (!slimUiMode && !MINIMAL_UI_MODE)) {
    return false;
  }

  if (MINIMAL_UI_MODE && url.pathname === "/api/exchange/overview") {
    const snapshot = getMinimalUiDataset({ force: false });
    if (snapshot && snapshot.exchangeOverview) {
      sendJson(res, 200, buildMinimalExchangeApiPayload(snapshot));
      return true;
    }
  }

  if (MINIMAL_UI_MODE && url.pathname === "/api/system/status") {
    const snapshot = getMinimalUiDataset({ force: false });
    if (snapshot) {
      sendJson(res, 200, {
        generatedAt: Number(snapshot.generatedAt || Date.now()),
        status: {
          generatedAt: Number(snapshot.generatedAt || Date.now()),
          source: "minimal_dashboard_snapshot",
          summary: {
            systemMode: "minimal",
            walletExplorerProgress: {
              discovered: Number((snapshot.wallets && snapshot.wallets.total) || 0),
              trackedAddresses: Number((snapshot.wallets && snapshot.wallets.total) || 0),
            },
            exchangeOverviewProgress: {
              totalVolumeUsd: Number(
                (snapshot.exchangeOverview && snapshot.exchangeOverview.totalHistoricalVolumeUsd) || 0
              ),
              walletSummedVolumeUsd: Number(
                (snapshot.exchangeOverview && snapshot.exchangeOverview.walletSummedVolumeUsd) || 0
              ),
              volumeGapUsd: Number(
                (snapshot.exchangeOverview && snapshot.exchangeOverview.volumeGapUsd) || 0
              ),
              volumeRatio: Number(
                (snapshot.exchangeOverview && snapshot.exchangeOverview.volumeRatio) || 0
              ),
              updatedAt: Number(
                (snapshot.exchangeOverview && snapshot.exchangeOverview.updatedAt) ||
                  snapshot.generatedAt ||
                  Date.now()
              ),
            },
            systemStatus: {
              mode: "minimal",
              configuredShardWorkers: Number(
                process.env.PACIFICA_INDEXER_MULTI_WORKER_SHARDS || 0
              ),
              datasetMode: "wallet_addresses_and_exchange_overview_only",
            },
            validation: snapshot.validation || null,
          },
        },
      });
      return true;
    }
  }

  if (MINIMAL_UI_MODE && url.pathname === "/api/volume-series") {
    sendJson(res, 200, {
      volume: {
        daily: [],
        weekly: [],
        monthly: [],
        yearly: [],
      },
      fees: {
        daily: [],
        weekly: [],
        monthly: [],
        yearly: [],
      },
      minimal: true,
    });
    return true;
  }

  if (!slimUiMode) {
    return false;
  }

  if (url.pathname === "/api/system/status") {
    const requestFull =
      String(url.searchParams.get("full") || "")
        .trim()
        .toLowerCase() === "1";
    const requestForce =
      String(url.searchParams.get("force") || url.searchParams.get("refresh") || "")
        .trim()
        .toLowerCase() === "1";
    if (requestFull || requestForce) {
      return false;
    }
    const snapshot = walletTrackingReadModel.getStatusSummary(false);
    if (snapshot && snapshot.status && typeof snapshot.status === "object") {
      sendJson(res, 200, {
        generatedAt: Number(snapshot.generatedAt || Date.now()),
        status: snapshot.status,
        warmup: {
          persistedSummary: true,
        },
      });
      return true;
    }
    return false;
  }

  return false;
}

function buildEgressUsageSnapshot(entries = []) {
  const rows = (Array.isArray(entries) ? entries : []).map((entry) => {
    const st =
      entry &&
      entry.rateGuard &&
      typeof entry.rateGuard.getState === "function"
        ? entry.rateGuard.getState()
        : null;
    return {
      id: entry && entry.id ? entry.id : "unknown",
      used1m: st ? Number(st.used1m || 0) : 0,
      rpmCap: st ? Number(st.rpmCap || 0) : 0,
      tokens: st ? Number(st.tokens || 0) : 0,
      pauseRemainingMs: st ? Number(st.pauseRemainingMs || 0) : 0,
      proxy: entry && entry.proxyUrl ? entry.proxyUrl : null,
    };
  });

  const usedTotal = rows.reduce((sum, row) => sum + Number(row.used1m || 0), 0);
  const capTotal = rows.reduce((sum, row) => sum + Number(row.rpmCap || 0), 0);
  const active = rows.filter((row) => Number(row.used1m || 0) > 0).length;
  const top = rows
    .slice()
    .sort((a, b) => Number(b.used1m || 0) - Number(a.used1m || 0))
    .slice(0, 5)
    .map((row) => `${row.id}:${Number(row.used1m || 0).toFixed(1)}/${Number(row.rpmCap || 0).toFixed(1)}`);

  return {
    clients: rows.length,
    active,
    rpmUsedTotal: Number(usedTotal.toFixed(2)),
    rpmCapTotal: Number(capTotal.toFixed(2)),
    top,
    rows,
  };
}

function createManagedLoop({
  name,
  logger = console,
  runTask,
  getDelayMs,
}) {
  const state = {
    running: false,
    inFlight: false,
    timer: null,
    nextRunAt: null,
    nextDelayMs: null,
    lastStartedAt: null,
    lastFinishedAt: null,
    lastDurationMs: null,
    lastSuccessAt: null,
    lastError: null,
  };

  const schedule = (delayMs = 0) => {
    if (!state.running) return;
    if (state.timer) {
      clearTimeout(state.timer);
      state.timer = null;
    }
    const nextDelay = Math.max(0, Number(delayMs || 0));
    state.nextDelayMs = nextDelay;
    state.nextRunAt = Date.now() + nextDelay;
    state.timer = setTimeout(async () => {
      state.timer = null;
      state.inFlight = true;
      state.lastStartedAt = Date.now();
      let result = null;
      let error = null;
      try {
        result = await runTask();
        state.lastSuccessAt = Date.now();
        state.lastError = null;
      } catch (taskError) {
        error = taskError;
        state.lastError = taskError && taskError.message ? taskError.message : `${taskError}`;
        logger.warn(`[${name}] ${state.lastError}`);
      } finally {
        state.inFlight = false;
        state.lastFinishedAt = Date.now();
        state.lastDurationMs = state.lastFinishedAt - Number(state.lastStartedAt || state.lastFinishedAt);
        const computedDelay =
          typeof getDelayMs === "function" ? getDelayMs({ result, error, state }) : 0;
        schedule(computedDelay);
      }
    }, nextDelay);
  };

  return {
    start(initialDelayMs = 0) {
      if (state.running) return;
      state.running = true;
      schedule(initialDelayMs);
    },
    stop() {
      state.running = false;
      state.nextRunAt = null;
      state.nextDelayMs = null;
      if (state.timer) {
        clearTimeout(state.timer);
        state.timer = null;
      }
    },
    getState() {
      return {
        running: state.running,
        inFlight: state.inFlight,
        nextRunAt: state.nextRunAt,
        nextDelayMs: state.nextDelayMs,
        lastStartedAt: state.lastStartedAt,
        lastFinishedAt: state.lastFinishedAt,
        lastDurationMs: state.lastDurationMs,
        lastSuccessAt: state.lastSuccessAt,
        lastError: state.lastError,
      };
    },
  };
}

function describeFileDataset(filePath) {
  if (!filePath) return null;
  let exists = false;
  let size = null;
  let updatedAt = null;
  try {
    if (fs.existsSync(filePath)) {
      exists = true;
      const stats = fs.statSync(filePath);
      size = Number(stats.size || 0);
      updatedAt = Number(stats.mtimeMs || 0) || null;
    }
  } catch (_error) {
    exists = false;
  }
  return {
    path: filePath,
    exists,
    size,
    updatedAt,
  };
}

function computeDefiLlamaV2FromPrices(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce(
    (acc, item) => {
      const reportedVolume24h = Number(
        item && item.volume_24h !== undefined ? item.volume_24h : 0
      );
      const volume24h = Number.isFinite(reportedVolume24h) ? reportedVolume24h / 2 : NaN;
      const openInterest = Number(
        item && item.open_interest !== undefined ? item.open_interest : 0
      );
      const mark = Number(item && item.mark !== undefined ? item.mark : 0);

      if (Number.isFinite(volume24h)) acc.dailyVolume += volume24h;
      if (Number.isFinite(openInterest) && Number.isFinite(mark)) {
        acc.openInterestAtEnd += openInterest * mark;
      }
      return acc;
    },
    {
      dailyVolume: 0,
      openInterestAtEnd: 0,
    }
  );
}

function buildDefiLlamaVolumeRank(rows = [], limit = 100) {
  return (Array.isArray(rows) ? rows : [])
    .map((item) => {
      const symbol = String(item && item.symbol ? item.symbol : "").trim();
      const reportedVolume24h = Number(
        item && item.volume_24h !== undefined ? item.volume_24h : 0
      );
      const volume24h = Number.isFinite(reportedVolume24h) ? reportedVolume24h / 2 : NaN;
      const openInterest = Number(
        item && item.open_interest !== undefined ? item.open_interest : 0
      );
      const mark = Number(item && item.mark !== undefined ? item.mark : 0);
      return {
        symbol,
        volume_24h_usd: Number.isFinite(volume24h) ? volume24h : 0,
        open_interest_usd:
          Number.isFinite(openInterest) && Number.isFinite(mark) ? openInterest * mark : 0,
      };
    })
    .filter((row) => Boolean(row.symbol))
    .sort((a, b) => Number(b.volume_24h_usd || 0) - Number(a.volume_24h_usd || 0))
    .slice(0, Math.max(1, Number(limit || 100)));
}

function getUtcStartOfDayMs(nowMs = Date.now()) {
  const d = new Date(nowMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function formatUtcDate(ms) {
  const value = Number(ms);
  if (!Number.isFinite(value)) return null;
  return new Date(value).toISOString().slice(0, 10);
}

function parseUtcDateMs(dateLike) {
  const iso = String(dateLike || "").trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;
  const ms = Date.parse(`${iso}T00:00:00.000Z`);
  return Number.isFinite(ms) ? ms : null;
}

function toFinite(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function extractDailyVolume(entry) {
  if (entry && typeof entry === "object") {
    return toFinite(entry.dailyVolume, 0);
  }
  return toFinite(entry, 0);
}

function hasSymbolVolumes(entry) {
  if (!entry || typeof entry !== "object") return false;
  const volumes = entry.symbolVolumes;
  if (!volumes || typeof volumes !== "object" || Array.isArray(volumes)) return false;
  return Object.keys(volumes).length > 0;
}

function findNextMissingSymbolVolumeDate({ startMs, endMs, dailyByDate }) {
  const map = dailyByDate && typeof dailyByDate === "object" ? dailyByDate : {};
  const safeStart = Number.isFinite(Number(startMs)) ? Number(startMs) : null;
  const safeEnd = Number.isFinite(Number(endMs)) ? Number(endMs) : null;
  if (safeStart === null || safeEnd === null || safeEnd < safeStart) return null;
  let cursor = safeStart;
  while (cursor <= safeEnd) {
    const day = formatUtcDate(cursor);
    if (!day) break;
    const row = map[day];
    if (!row || !hasSymbolVolumes(row) || isDefillamaHistoryRowIncomplete(row)) return day;
    cursor += ONE_DAY_MS;
  }
  return null;
}

function isDefillamaHistoryRowIncomplete(row = {}) {
  if (!row || typeof row !== "object") return true;
  const symbolCount = Math.max(0, Number(row.symbolCount || 0));
  const okSymbols = Math.max(0, Number(row.okSymbols || 0));
  const missingSymbols = Math.max(0, Number(row.missingSymbols || 0));
  const failedSymbols = Math.max(0, Number(row.failedSymbols || 0));
  const symbolVolumes =
    row.symbolVolumes && typeof row.symbolVolumes === "object" && !Array.isArray(row.symbolVolumes)
      ? row.symbolVolumes
      : {};
  const populatedSymbolCount = Object.keys(symbolVolumes).length;
  if (symbolCount <= 0) return true;
  if (failedSymbols > 0 || missingSymbols > 0) return true;
  if (okSymbols > 0 && okSymbols < symbolCount) return true;
  if (populatedSymbolCount < symbolCount) return true;
  return false;
}

function buildRecentClosedRepairDays({ todayStartMs, startMs, lookbackDays = 0 }) {
  const safeTodayStartMs = Number(todayStartMs);
  const safeStartMs = Number(startMs);
  const safeLookbackDays = Math.max(0, Number(lookbackDays || 0));
  const out = [];
  if (!Number.isFinite(safeTodayStartMs) || !Number.isFinite(safeStartMs) || safeLookbackDays <= 0) {
    return out;
  }
  for (let offset = 1; offset <= safeLookbackDays; offset += 1) {
    const dayMs = safeTodayStartMs - offset * ONE_DAY_MS;
    if (dayMs < safeStartMs) break;
    out.push(dayMs);
  }
  return out;
}

function computeDefillamaContiguousProgress({ startMs, todayMs, dailyByDate }) {
  const map = dailyByDate && typeof dailyByDate === "object" ? dailyByDate : {};
  const safeStartMs = Number.isFinite(Number(startMs))
    ? Number(startMs)
    : Date.UTC(2025, 5, 9, 0, 0, 0, 0);
  const safeTodayMs = Number.isFinite(Number(todayMs)) ? Number(todayMs) : getUtcStartOfDayMs();
  const totalDays = Math.max(0, Math.floor((safeTodayMs - safeStartMs) / ONE_DAY_MS) + 1);

  let processedDays = 0;
  let cursorMs = safeStartMs;
  while (cursorMs <= safeTodayMs) {
    const day = formatUtcDate(cursorMs);
    if (!day) break;
    if (!Object.prototype.hasOwnProperty.call(map, day)) break;
    processedDays += 1;
    cursorMs += ONE_DAY_MS;
  }

  const backfillComplete = processedDays >= totalDays;
  const nextUnprocessedDate = backfillComplete ? null : formatUtcDate(safeStartMs + processedDays * ONE_DAY_MS);
  const lastProcessedDate = processedDays > 0 ? formatUtcDate(safeStartMs + (processedDays - 1) * ONE_DAY_MS) : null;
  const remainingDays = Math.max(0, totalDays - processedDays);

  let cumulative = 0;
  for (let i = 0; i < processedDays; i += 1) {
    const day = formatUtcDate(safeStartMs + i * ONE_DAY_MS);
    cumulative += extractDailyVolume(map[day]);
  }

  return {
    totalDays,
    processedDays,
    remainingDays,
    backfillComplete,
    nextUnprocessedDate,
    lastProcessedDate,
    cumulativeVolume: cumulative,
  };
}

async function fetchPacificaMarkets(restClient, options = {}) {
  const routeOptions = {
    cost: 1,
  };
  if (options && options.shardKey) {
    routeOptions.shardKey = String(options.shardKey);
  }
  const infoRes = await restClient.get("/info", routeOptions);
  const payload = infoRes && infoRes.payload ? infoRes.payload : {};
  const rows = Array.isArray(payload.data) ? payload.data : [];
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const symbol = String((row && row.symbol) || "").trim();
    if (!symbol || seen.has(symbol)) continue;
    seen.add(symbol);
    const createdAtRaw = row && row.created_at !== undefined ? Number(row.created_at) : null;
    out.push({
      symbol,
      createdAtMs: Number.isFinite(createdAtRaw) ? createdAtRaw : null,
    });
  }
  return out;
}

async function computeDefiLlamaCompatDailyVolume({
  restClient,
  symbols = [],
  startOfDayMs,
  symbolConcurrency = 1,
  logger = console,
  shardKeyPrefix = "global_kpi:kline",
}) {
  const safeSymbols = Array.isArray(symbols) ? symbols.filter(Boolean) : [];
  let dailyVolume = 0;
  const symbolVolumes = {};
  let okSymbols = 0;
  let failedSymbols = 0;
  let missingSymbols = 0;
  const errors = [];
  const concurrency = Math.max(1, Math.min(64, Number(symbolConcurrency || 1)));
  let cursor = 0;
  const workers = Array.from({ length: Math.min(concurrency, safeSymbols.length || 1) }, async () => {
    while (cursor < safeSymbols.length) {
      const symbol = safeSymbols[cursor];
      cursor += 1;
      try {
        const res = await restClient.get("/kline", {
          query: {
            symbol,
            interval: "1d",
            start_time: startOfDayMs,
            end_time: startOfDayMs + ONE_DAY_MS * 2,
          },
          cost: 3,
          shardKey: `${shardKeyPrefix}:${startOfDayMs}:${symbol}`,
        });
        const payload = res && res.payload ? res.payload : {};
        const rows = Array.isArray(payload.data) ? payload.data : [];
        const target = rows.find((row) => Number((row && row.t) || 0) === Number(startOfDayMs)) || null;
        if (!target) {
          missingSymbols += 1;
          if (errors.length < 8) {
            errors.push(`${symbol}:missing_exact_day_candle`);
          }
          continue;
        }

        const v = Number(target.v);
        const c = Number(target.c);
        if (Number.isFinite(v) && Number.isFinite(c)) {
          // Pacifica day candles include both sides of the match. Normalize to
          // single-count USD notional so daily history and rolling 24h use the
          // same convention.
          const symbolVolume = (v * c) / 2;
          dailyVolume += symbolVolume;
          symbolVolumes[symbol] = Number(symbolVolume.toFixed(8));
        } else {
          missingSymbols += 1;
          if (errors.length < 8) {
            errors.push(`${symbol}:invalid_v_or_c`);
          }
          continue;
        }
        okSymbols += 1;
      } catch (error) {
        failedSymbols += 1;
        const message = error && error.message ? error.message : "unknown_error";
        if (errors.length < 8) {
          errors.push(`${symbol}:${message}`);
        }
        logger.warn(`[global-kpi] defillama_compat symbol=${symbol} failed: ${message}`);
      }
    }
  });
  await Promise.all(workers);

  return {
    dailyVolume,
    symbolVolumes,
    symbolCount: safeSymbols.length,
    okSymbols,
    failedSymbols,
    missingSymbols,
    errors,
  };
}

async function computeDefiLlamaCompatHistoricalVolume({
  restClient,
  symbols = [],
  startMs,
  endMs,
  symbolConcurrency = 1,
  shardKeyPrefix = "global_kpi:kline_bootstrap",
}) {
  const safeSymbols = Array.isArray(symbols) ? symbols.filter(Boolean) : [];
  const safeStartMs = Number.isFinite(Number(startMs)) ? Number(startMs) : null;
  const safeEndMs = Number.isFinite(Number(endMs)) ? Number(endMs) : null;
  if (safeStartMs === null || safeEndMs === null || safeEndMs < safeStartMs) {
    return { dailyByDate: {}, symbolCount: 0, okSymbols: 0, failedSymbols: 0, errors: [] };
  }
  const dailyByDate = {};
  let okSymbols = 0;
  let failedSymbols = 0;
  const errors = [];
  const concurrency = Math.max(1, Math.min(64, Number(symbolConcurrency || 1)));
  let cursor = 0;
  const workers = Array.from({ length: Math.min(concurrency, safeSymbols.length || 1) }, async () => {
    while (cursor < safeSymbols.length) {
      const symbol = safeSymbols[cursor];
      cursor += 1;
      try {
        const res = await restClient.get("/kline", {
          query: {
            symbol,
            interval: "1d",
            start_time: safeStartMs,
          },
          cost: 3,
          shardKey: `${shardKeyPrefix}:${safeStartMs}:${symbol}`,
        });
        const payload = res && res.payload ? res.payload : {};
        const rows = Array.isArray(payload.data) ? payload.data : [];
        for (const row of rows) {
          const t = Number(row && row.t);
          if (!Number.isFinite(t) || t < safeStartMs || t > safeEndMs) continue;
          const v = Number(row && row.v);
          const c = Number(row && row.c);
          if (!Number.isFinite(v) || !Number.isFinite(c)) continue;
          const dayDate = formatUtcDate(t);
          if (!dayDate) continue;
          const bucket =
            dailyByDate[dayDate] ||
            (dailyByDate[dayDate] = {
              dailyVolume: 0,
              symbolVolumes: {},
              symbolCount: 0,
              okSymbols: 0,
              failedSymbols: 0,
              missingSymbols: 0,
              sampleErrors: [],
              updatedAt: Date.now(),
            });
          const symbolVolume = (v * c) / 2;
          bucket.dailyVolume += symbolVolume;
          bucket.symbolVolumes[symbol] = Number(symbolVolume.toFixed(8));
          bucket.updatedAt = Date.now();
        }
        okSymbols += 1;
      } catch (error) {
        failedSymbols += 1;
        if (errors.length < 8) {
          errors.push(`${symbol}:${error && error.message ? error.message : "unknown_error"}`);
        }
      }
    }
  });
  await Promise.all(workers);

  let cursorMs = safeStartMs;
  while (cursorMs <= safeEndMs) {
    const dayDate = formatUtcDate(cursorMs);
    if (dayDate) {
      const bucket =
        dailyByDate[dayDate] ||
        (dailyByDate[dayDate] = {
          dailyVolume: 0,
          symbolVolumes: {},
          symbolCount: 0,
          okSymbols: 0,
          failedSymbols: 0,
          missingSymbols: 0,
          sampleErrors: [],
          updatedAt: Date.now(),
        });
      const symbolCount = Object.keys(bucket.symbolVolumes || {}).length;
      bucket.dailyVolume = Number(Number(bucket.dailyVolume || 0).toFixed(8));
      bucket.symbolCount = symbolCount;
      bucket.okSymbols = symbolCount;
      bucket.failedSymbols = failedSymbols;
      if (failedSymbols > 0 && errors.length && (!bucket.sampleErrors || !bucket.sampleErrors.length)) {
        bucket.sampleErrors = [...errors];
      }
    }
    cursorMs += ONE_DAY_MS;
  }

  return {
    dailyByDate,
    symbolCount: safeSymbols.length,
    okSymbols,
    failedSymbols,
    errors,
  };
}

function contentTypeFor(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  if (filePath.endsWith(".svg")) return "image/svg+xml";
  if (filePath.endsWith(".png")) return "image/png";
  if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) return "image/jpeg";
  if (filePath.endsWith(".webp")) return "image/webp";
  if (filePath.endsWith(".ico")) return "image/x-icon";
  return "application/octet-stream";
}

async function readJsonBody(req, maxBytes = 4 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (Buffer.byteLength(body, "utf8") > maxBytes) {
        reject(new Error("Request body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!body.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch (_error) {
        reject(new Error("Invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

function extractPayloadData(result, fallback = null) {
  if (!result || !result.payload) return fallback;
  if (Object.prototype.hasOwnProperty.call(result.payload, "data")) {
    return result.payload.data;
  }
  return result.payload;
}

function uniqueSymbols(list = []) {
  return Array.from(
    new Set(
      (Array.isArray(list) ? list : [])
        .map((item) => String(item || "").toUpperCase().trim())
        .filter(Boolean)
    )
  );
}

function serveStatic(req, res, url) {
  if (!ENABLE_UI) return false;
  if (req.method !== "GET" && req.method !== "HEAD") return false;
  if (String(url.pathname || "").startsWith("/api/")) return false;

  if (url.pathname === "/" || url.pathname === "") {
    const indexPath = path.join(PUBLIC_DIR, "index.html");
    if (fs.existsSync(indexPath)) {
      const indexStat = fs.statSync(indexPath);
      const assetVersion = getRuntimeAssetVersion();
      const baseHtml = applyRuntimeAssetVersion(readUtf8FileCached(indexPath, indexStat), assetVersion);
      const payload = exchangeBootstrapCache.value;
      const payloadCacheKey =
        payload && (payload.overview || payload.series)
          ? `${Number((payload.overview && payload.overview.generatedAt) || 0)}:${payload.series ? Object.keys(payload.series || {}).length : 0}`
          : "nopayload";
      const renderCacheKey = `root:${Number(indexStat.mtimeMs || 0)}:${Number(indexStat.size || 0)}:${payloadCacheKey}:${assetVersion}`;
      let html = getCachedRenderedHtml(renderCacheKey);
      if (!html) {
        const bootstrapScript =
          payload && (payload.overview || payload.series)
            ? `<script>${payload.overview ? `window.__PF_BOOTSTRAP_EXCHANGE__=${JSON.stringify(payload.overview).replace(/</g, "\\u003c")};` : ""}${payload.series ? `window.__PF_BOOTSTRAP_EXCHANGE_SERIES__=${JSON.stringify(payload.series).replace(/</g, "\\u003c")};` : ""}</script>`
            : "";
        html = bootstrapScript
          ? baseHtml.replace(
              /<script\s+id="pf-app-entry"/,
              `${bootstrapScript}<script id="pf-app-entry"`
            )
          : baseHtml;
        html = setCachedRenderedHtml(renderCacheKey, html);
      }
      writeResponseBody(req, res, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
      }, html, renderCacheKey);
      return true;
    }
  }

  if (url.pathname === "/wallets" || url.pathname === "/wallets/") {
    res.writeHead(308, {
      Location: "/wallets-explorer/",
      "Cache-Control": "no-store",
    });
    res.end();
    return true;
  }

  if (url.pathname === "/social-trade" || url.pathname === "/social-trade/") {
    res.writeHead(308, {
      Location: "/social-cart/",
      "Cache-Control": "no-store",
    });
    res.end();
    return true;
  }

  if (
    url.pathname === "/token-analysis" ||
    url.pathname === "/token-analysis/" ||
    url.pathname === "/coin-analysis" ||
    url.pathname === "/coin-analysis/" ||
    url.pathname === "/token-analysis/BTC" ||
    url.pathname === "/token-analysis/btc" ||
    url.pathname === "/coin-analysis/BTC" ||
    url.pathname === "/coin-analysis/btc"
  ) {
    res.writeHead(308, {
      Location: "/coin/BTC",
      "Cache-Control": "no-store",
    });
    res.end();
    return true;
  }

  if (url.pathname === "/wallets-explorer" || url.pathname === "/wallets-explorer/") {
    const indexPath = fs.existsSync(path.join(PUBLIC_DIR, "wallets-explorer", "index.html"))
      ? path.join(PUBLIC_DIR, "wallets-explorer", "index.html")
      : path.join(PUBLIC_DIR, "index.html");
    const defaultWalletPagePath =
      walletTrackingReadModel &&
      walletTrackingReadModel.paths &&
      walletTrackingReadModel.paths.defaultWalletPage
        ? walletTrackingReadModel.paths.defaultWalletPage
        : null;
    const persistedDefaultPage =
      !MINIMAL_UI_MODE && defaultWalletPagePath ? readJson(defaultWalletPagePath, null) : null;
    const snapshot = MINIMAL_UI_MODE
      ? getMinimalUiDataset({ force: false })
      : persistedDefaultPage &&
        persistedDefaultPage.payload &&
        typeof persistedDefaultPage.payload === "object"
      ? persistedDefaultPage.payload
      : walletTrackingReadModel.getDefaultWalletPage();
      const payload =
      MINIMAL_UI_MODE && snapshot
        ? buildMinimalWalletsApiPayload(snapshot, url)
        : snapshot && typeof snapshot === "object"
        ? snapshot
        : null;
    if (fs.existsSync(indexPath)) {
      const indexStat = fs.statSync(indexPath);
      const assetVersion = getRuntimeAssetVersion();
      const baseHtml = applyRuntimeAssetVersion(readUtf8FileCached(indexPath, indexStat), assetVersion);
      const payloadCacheKey =
        payload && typeof payload === "object"
          ? `${Number(payload.generatedAt || 0)}:${Number(payload.total || 0)}:${Number((payload.counts && payload.counts.totalWallets) || 0)}`
          : "nopayload";
      const renderCacheKey = `wallets:${Number(indexStat.mtimeMs || 0)}:${Number(indexStat.size || 0)}:${payloadCacheKey}:${MINIMAL_UI_MODE ? 1 : 0}:${assetVersion}`;
      let html = getCachedRenderedHtml(renderCacheKey);
      if (!html) {
        html = renderWalletsShellHtml(baseHtml, null);
        if (MINIMAL_UI_MODE) {
          html = html.replace(
            "</head>",
            '<script>window.__PF_MINIMAL_UI__=true;</script></head>'
          );
        }
        html = setCachedRenderedHtml(renderCacheKey, html);
      }
      writeResponseBody(req, res, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
      }, html, renderCacheKey);
      return true;
    }
  }

  if (url.pathname === "/social-cart" || url.pathname === "/social-cart/") {
    const indexPath = path.join(PUBLIC_DIR, "social-trade", "index.html");
    if (fs.existsSync(indexPath)) {
      const indexStat = fs.statSync(indexPath);
      const body = applyRuntimeAssetVersion(readUtf8FileCached(indexPath, indexStat), getRuntimeAssetVersion());
      writeResponseBody(req, res, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
      }, body, `social-cart:${Number(indexStat.mtimeMs || 0)}:${Number(indexStat.size || 0)}`);
      return true;
    }
  }

  if (url.pathname === "/copy-trading" || url.pathname === "/copy-trading/") {
    const indexPath = path.join(PUBLIC_DIR, "copy-trading", "index.html");
    if (fs.existsSync(indexPath)) {
      const indexStat = fs.statSync(indexPath);
      const body = applyRuntimeAssetVersion(readUtf8FileCached(indexPath, indexStat), getRuntimeAssetVersion());
      writeResponseBody(req, res, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
      }, body, `copy-trading:${Number(indexStat.mtimeMs || 0)}:${Number(indexStat.size || 0)}`);
      return true;
    }
  }

  if (/^\/copy-trading\/(overview|settings)\/?$/.test(url.pathname)) {
    const indexPath = path.join(PUBLIC_DIR, "copy-trading", "index.html");
    if (fs.existsSync(indexPath)) {
      const indexStat = fs.statSync(indexPath);
      const body = applyRuntimeAssetVersion(readUtf8FileCached(indexPath, indexStat), getRuntimeAssetVersion());
      writeResponseBody(req, res, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
      }, body, `copy-trading:${Number(indexStat.mtimeMs || 0)}:${Number(indexStat.size || 0)}`);
      return true;
    }
  }

  if (url.pathname === "/settings" || url.pathname === "/settings/") {
    const indexPath = path.join(PUBLIC_DIR, "settings", "index.html");
    if (fs.existsSync(indexPath)) {
      const indexStat = fs.statSync(indexPath);
      const body = applyRuntimeAssetVersion(readUtf8FileCached(indexPath, indexStat), getRuntimeAssetVersion());
      writeResponseBody(req, res, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache, max-age=0, must-revalidate",
      }, body, `settings:${Number(indexStat.mtimeMs || 0)}:${Number(indexStat.size || 0)}`);
      return true;
    }
  }

  const requested = decodeURIComponent(url.pathname === "/" ? "/index.html" : url.pathname);
  const safePath = path
    .normalize(requested)
    .replace(/^\.\.(\/|\\|$)+/, "")
    .replace(/^\/+/, "");

  let filePath = path.join(PUBLIC_DIR, safePath);
  if (!filePath.startsWith(PUBLIC_DIR)) return false;
  let stat = fs.existsSync(filePath) ? fs.statSync(filePath) : null;
  if (!stat) {
    const hasExtension = path.extname(safePath) !== "";
    if (hasExtension) return false;
    filePath = path.join(PUBLIC_DIR, "index.html");
    if (!filePath.startsWith(PUBLIC_DIR)) return false;
    if (!fs.existsSync(filePath)) return false;
    stat = fs.statSync(filePath);
  }
  if (stat.isDirectory()) {
    filePath = path.join(filePath, "index.html");
    if (!filePath.startsWith(PUBLIC_DIR)) return false;
    if (!fs.existsSync(filePath)) return false;
    stat = fs.statSync(filePath);
  }
  if (!stat.isFile()) return false;

  const ext = path.extname(filePath).toLowerCase();
  const isHtml = ext === ".html";
  const isRootRuntimeAsset =
    safePath === "app.js" ||
    safePath === "home.js" ||
    safePath === "styles.css" ||
    safePath === "theme.css";
  const isLiveTradeRuntimeAsset =
    safePath.startsWith("live-trade/") && (ext === ".js" || ext === ".css");
  const isWalletPerformanceRuntimeAsset =
    safePath.startsWith("wallet-performance/") && (ext === ".js" || ext === ".css");
  const isSocialTradeRuntimeAsset =
    safePath.startsWith("social-trade/") && (ext === ".js" || ext === ".css");
  const isCopyTradingRuntimeAsset =
    safePath.startsWith("copy-trading/") && (ext === ".js" || ext === ".css");
  const isSettingsRuntimeAsset =
    safePath.startsWith("settings/") && (ext === ".js" || ext === ".css");
  const isLongLivedStaticAsset =
    safePath.startsWith("assets/") ||
    safePath === "favicon.png" ||
    safePath === "apple-touch-icon.png";
  const cacheControl = isHtml
    ? "no-cache, max-age=0, must-revalidate"
    : isRootRuntimeAsset ||
      isLiveTradeRuntimeAsset ||
      isWalletPerformanceRuntimeAsset ||
      isSocialTradeRuntimeAsset ||
      isCopyTradingRuntimeAsset ||
      isSettingsRuntimeAsset ||
      isLongLivedStaticAsset
    ? "public, max-age=31536000, immutable"
    : "public, max-age=604800, stale-while-revalidate=86400";
  const contentType = contentTypeFor(filePath);
  if (isHtml) {
    const body = applyRuntimeAssetVersion(readUtf8FileCached(filePath, stat), getRuntimeAssetVersion());
    writeResponseBody(req, res, 200, {
      "Content-Type": contentType,
      "Cache-Control": cacheControl,
    }, body, `${filePath}:${Number(stat.mtimeMs || 0)}:${Number(stat.size || 0)}`);
    return true;
  }

  if (
    isCompressibleContentType(contentType) &&
    !isRootRuntimeAsset &&
    !isLiveTradeRuntimeAsset &&
    !isWalletPerformanceRuntimeAsset &&
    !isSocialTradeRuntimeAsset &&
    !isCopyTradingRuntimeAsset &&
    !isSettingsRuntimeAsset
  ) {
    const body = readFileBufferCached(filePath, stat);
    writeResponseBody(req, res, 200, {
      "Content-Type": contentType,
      "Cache-Control": cacheControl,
    }, body, `${filePath}:${Number(stat.mtimeMs || 0)}:${Number(stat.size || 0)}`);
    return true;
  }

  res.writeHead(200, {
    "Content-Type": contentType,
    "Content-Length": stat.size,
    "Cache-Control": cacheControl,
  });

  if (req.method === "HEAD") {
    res.end();
    return true;
  }

  fs.createReadStream(filePath).pipe(res);
  return true;
}

function makeBootstrapper({ restClient, pipeline, logger = console }) {
  return async function bootstrapSnapshots(account) {
    const failures = [];
    const snapshotKey = Date.now();

    const market = {
      prices: [],
      info: [],
      fundingRatesBySymbol: {},
      orderbooksBySymbol: {},
      candlesBySymbol: {},
      markCandlesBySymbol: {},
      recentPublicTradesBySymbol: {},
    };

    const accountData = {
      overview: {},
      settings: [],
      positions: [],
      orders: [],
      orderHistory: [],
      tradeHistory: [],
      fundingHistory: [],
      balanceHistory: [],
      portfolioHistory: [],
    };

    async function pull(label, fn) {
      try {
        await fn();
      } catch (error) {
        failures.push({ label, error: error.message });
        logger.warn(`[bootstrap] ${label} failed: ${error.message}`);
      }
    }

    await pull("market_info", async () => {
      const res = await restClient.get("/info", { cost: 1 });
      market.info = extractPayloadData(res, []);
      pipeline.recordSnapshot("market_info", Array.isArray(market.info) ? market.info : [], {
        snapshotKey,
      });
    });

    await pull("prices", async () => {
      const res = await restClient.get("/info/prices", { cost: 1 });
      market.prices = extractPayloadData(res, []);
      pipeline.recordSnapshot("prices", Array.isArray(market.prices) ? market.prices : [], {
        snapshotKey,
      });
    });

    if (!account) {
      pipeline.recordSnapshot("account_info", {}, { snapshotKey });
      pipeline.recordSnapshot("account_settings", [], { snapshotKey });
      pipeline.recordSnapshot("positions", [], { snapshotKey });
      pipeline.recordSnapshot("orders", [], { snapshotKey });
      pipeline.recordSnapshot("order_history", [], { snapshotKey });
      pipeline.recordSnapshot("trades", [], { snapshotKey });
      pipeline.recordSnapshot("funding_history", [], { snapshotKey });
      pipeline.recordSnapshot("balance_history", [], { snapshotKey });
      pipeline.recordSnapshot("portfolio_history", [], { snapshotKey });
      pipeline.recordSnapshot("funding_rates", {}, { snapshotKey });
      pipeline.recordSnapshot("orderbooks", {}, { snapshotKey });
      pipeline.recordSnapshot("candles", {}, { snapshotKey });
      pipeline.recordSnapshot("mark_candles", {}, { snapshotKey });
      pipeline.markBootstrap();
      return {
        failures,
        focusSymbols: (() => {
          const symbols = market.prices
            .map((item) => String(item.symbol || "").toUpperCase())
            .filter(Boolean);
          return Number.isFinite(LIVE_FOCUS_SYMBOL_MAX)
            ? symbols.slice(0, LIVE_FOCUS_SYMBOL_MAX)
            : symbols;
        })(),
      };
    }

    await pull("account_info", async () => {
      const res = await restClient.get("/account", {
        query: { account },
        cost: 1,
      });
      accountData.overview = extractPayloadData(res, {}) || {};
      pipeline.recordSnapshot("account_info", accountData.overview, { snapshotKey });
    });

    await pull("account_settings", async () => {
      const res = await restClient.get("/account/settings", {
        query: { account },
        cost: 1,
      });
      accountData.settings = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("account_settings", accountData.settings, { snapshotKey });
    });

    await pull("positions", async () => {
      const res = await restClient.get("/positions", {
        query: { account },
        cost: 1,
      });
      accountData.positions = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("positions", accountData.positions, { snapshotKey });
    });

    await pull("orders", async () => {
      const res = await restClient.get("/orders", {
        query: { account },
        cost: 1,
      });
      accountData.orders = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("orders", accountData.orders, { snapshotKey });
    });

    await pull("order_history", async () => {
      const res = await restClient.get("/orders/history", {
        query: { account, limit: 300 },
        cost: 2,
      });
      accountData.orderHistory = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("order_history", accountData.orderHistory, { snapshotKey });
    });

    await pull("trades_history", async () => {
      const res = await restClient.get("/trades/history", {
        query: { account, limit: 500 },
        cost: 2,
      });
      accountData.tradeHistory = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("trades", accountData.tradeHistory, { snapshotKey });
    });

    await pull("funding_history", async () => {
      const res = await restClient.get("/funding/history", {
        query: { account, limit: 400 },
        cost: 2,
      });
      accountData.fundingHistory = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("funding_history", accountData.fundingHistory, { snapshotKey });
    });

    await pull("portfolio_history", async () => {
      const res = await restClient.get("/portfolio", {
        query: { account, time_range: "30d", limit: 500 },
        cost: 2,
      });
      accountData.portfolioHistory = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("portfolio_history", accountData.portfolioHistory, {
        snapshotKey,
      });
    });

    await pull("balance_history", async () => {
      const res = await restClient.get("/account/balance/history", {
        query: { account, limit: 400 },
        cost: 2,
      });
      accountData.balanceHistory = extractPayloadData(res, []) || [];
      pipeline.recordSnapshot("balance_history", accountData.balanceHistory, {
        snapshotKey,
      });
    });

    const symbolsAll = uniqueSymbols([
      ...accountData.positions.map((row) => row.symbol),
      ...accountData.orders.map((row) => row.symbol),
      ...accountData.tradeHistory.slice(0, 40).map((row) => row.symbol),
      ...market.prices.map((row) => row.symbol),
    ]);
    const focusSymbols = Number.isFinite(LIVE_FOCUS_SYMBOL_MAX)
      ? symbolsAll.slice(0, LIVE_FOCUS_SYMBOL_MAX)
      : symbolsAll;
    const bootstrapSymbols = focusSymbols.slice(0, LIVE_BOOTSTRAP_SYMBOL_MAX);

    const nowMs = Date.now();
    const start1h = nowMs - 72 * 60 * 60 * 1000;

    await Promise.all(
      bootstrapSymbols.map(async (symbol) => {
        await pull(`funding_rate_history:${symbol}`, async () => {
          const res = await restClient.get("/funding_rate/history", {
            query: { symbol, limit: 120 },
            cost: 2,
          });
          market.fundingRatesBySymbol[symbol] = extractPayloadData(res, []) || [];
        });

        await pull(`orderbook:${symbol}`, async () => {
          const res = await restClient.get("/book", {
            query: { symbol, agg_level: 1 },
            cost: 1,
          });
          market.orderbooksBySymbol[symbol] = extractPayloadData(res, {}) || {};
        });

        await pull(`kline:${symbol}`, async () => {
          const res = await restClient.get("/kline", {
            query: {
              symbol,
              interval: "1h",
              start_time: start1h,
              end_time: nowMs,
            },
            cost: 2,
          });
          market.candlesBySymbol[symbol] = extractPayloadData(res, []) || [];
        });

        await pull(`kline_mark:${symbol}`, async () => {
          const res = await restClient.get("/kline/mark", {
            query: {
              symbol,
              interval: "1h",
              start_time: start1h,
              end_time: nowMs,
            },
            cost: 2,
          });
          market.markCandlesBySymbol[symbol] = extractPayloadData(res, []) || [];
        });

        await pull(`public_trades:${symbol}`, async () => {
          const res = await restClient.get("/trades", {
            query: { symbol },
            cost: 1,
          });
          market.recentPublicTradesBySymbol[symbol] = extractPayloadData(res, []) || [];
        });
      })
    );

    pipeline.recordSnapshot("funding_rates", market.fundingRatesBySymbol, {
      snapshotKey,
    });
    pipeline.recordSnapshot("orderbooks", market.orderbooksBySymbol, {
      snapshotKey,
    });
    pipeline.recordSnapshot("candles", market.candlesBySymbol, {
      snapshotKey,
    });
    pipeline.recordSnapshot("mark_candles", market.markCandlesBySymbol, {
      snapshotKey,
    });

    // Public trades are fed via WS primarily; bootstrap rows are optional and skipped to reduce event volume.

    pipeline.markBootstrap();

    return {
      failures,
      focusSymbols,
    };
  };
}

async function main() {
  ensureDir(DATA_ROOT);
  ensureDir(PIPELINE_DATA_DIR);
  ensureDir(LIVE_POSITIONS_DATA_DIR);
  ensureDir(CREATOR_DATA_DIR);
  ensureDir(INDEXER_DATA_DIR);

  let snapshotLoop = null;
  let apiUsageTimer = null;
  let onchainLoop = null;
  let globalKpiLoop = null;
  let walletMetricsHistoryLoop = null;
  let positionLifecycleLoop = null;
  let runtimeStatusLoop = null;
  let copyTradingExecutionLoop = null;
  const copyTradingSseClients = new Map();

  const clockSync = createClockSync();
  const rateGuard = createRateLimitGuard({
    capacity: RATE_LIMIT_CAPACITY,
    refillWindowMs: RATE_LIMIT_WINDOW_MS,
    safetyMarginRatio: RATE_LIMIT_SAFETY_RATIO,
  });

  const retryPolicy = createRetryPolicy({
    maxAttempts: REST_RETRY_MAX_ATTEMPTS,
    baseDelayMs: REST_RETRY_BASE_DELAY_MS,
    maxDelayMs: REST_RETRY_MAX_DELAY_MS,
  });
  const solanaRetryPolicy = createRetryPolicy({
    maxAttempts: 6,
    baseDelayMs: 1000,
    maxDelayMs: 120000,
    jitterRatio: 0.2,
  });
  const solanaRateGuard = createRateLimitGuard({
    capacity: SOLANA_RPC_RATE_LIMIT_CAPACITY,
    refillWindowMs: SOLANA_RPC_RATE_LIMIT_WINDOW_MS,
    safetyMarginRatio: SOLANA_RPC_RATE_LIMIT_SAFETY_RATIO,
  });

  const signer = createSigner();

  const restClient = createRestClient({
    baseUrl: API_BASE,
    retryPolicy,
    rateLimitGuard: rateGuard,
    clockSync,
    defaultHeaders: API_CONFIG_KEY ? { "PF-API-KEY": API_CONFIG_KEY } : {},
    logger: console,
    transport: "fetch",
    clientId: "direct",
  });

  const indexerRestClientEntries = [];
  const pushIndexerClient = ({ id, client, rateGuard: guard, proxyUrl }) => {
    if (!client || typeof client.get !== "function") return;
    indexerRestClientEntries.push({
      id,
      client,
      rateGuard: guard,
      proxyUrl: proxyUrl || null,
    });
  };

  if (MULTI_EGRESS_ENABLED) {
    if (MULTI_EGRESS_INCLUDE_DIRECT) {
      pushIndexerClient({
        id: "direct",
        client: restClient,
        rateGuard,
      });
    }

    const proxyRows = loadProxyList(MULTI_EGRESS_PROXY_FILE);
    const selectedProxies =
      MULTI_EGRESS_MAX_PROXIES > 0 ? proxyRows.slice(0, MULTI_EGRESS_MAX_PROXIES) : proxyRows;

    selectedProxies.forEach((proxy, idx) => {
      const proxyRateCap = Math.max(
        0.1,
        Number(((MULTI_EGRESS_RPM_CAP_PER_IP * RATE_LIMIT_WINDOW_MS) / 60000).toFixed(3))
      );
      const proxyRateGuard = createRateLimitGuard({
        capacity: proxyRateCap,
        refillWindowMs: RATE_LIMIT_WINDOW_MS,
        safetyMarginRatio: RATE_LIMIT_SAFETY_RATIO,
      });

      const proxyClient = createRestClient({
        baseUrl: API_BASE,
        retryPolicy,
        rateLimitGuard: proxyRateGuard,
        clockSync,
        defaultHeaders: API_CONFIG_KEY ? { "PF-API-KEY": API_CONFIG_KEY } : {},
        logger: console,
        transport: MULTI_EGRESS_TRANSPORT,
        proxyUrl: proxy,
        clientId: `proxy_${idx + 1}`,
      });

      pushIndexerClient({
        id: `proxy_${idx + 1}`,
        client: proxyClient,
        rateGuard: proxyRateGuard,
        proxyUrl: proxy,
      });
    });
  }

  if (!indexerRestClientEntries.length) {
    pushIndexerClient({
      id: "direct",
      client: restClient,
      rateGuard,
    });
  }

  const indexerRestClients = indexerRestClientEntries.map((row) => row.client);
  const indexerRestClientIds = indexerRestClientEntries.map((row) => row.id);
  const indexerRoundRobinRestClient = createRoundRobinRestClient(indexerRestClients);
  const effectiveWalletScanConcurrency = Math.max(
    1,
    Math.min(INDEXER_WALLET_SCAN_CONCURRENCY, Math.max(1, indexerRestClients.length))
  );
  const effectiveIndexerBatchSize = Math.max(
    1,
    Math.max(INDEXER_BATCH_SIZE, effectiveWalletScanConcurrency)
  );
  const globalKpiRestClientEntries = (() => {
    if (!GLOBAL_KPI_USE_MULTI_EGRESS) return [];
    const baseRows = Array.isArray(indexerRestClientEntries) ? indexerRestClientEntries : [];
    const hasExternal = baseRows.some((entry) => String(entry && entry.id) !== "direct");
    if (GLOBAL_KPI_EXTERNAL_SHARDS_ONLY && hasExternal) {
      const externalRows = baseRows.filter((entry) => String(entry && entry.id) !== "direct");
      if (externalRows.length) return externalRows;
    }
    return baseRows;
  })();
  const globalKpiRestClient = GLOBAL_KPI_USE_MULTI_EGRESS
    ? createShardRoutedRestClient(globalKpiRestClientEntries, {
        fallbackClient: restClient,
      }) || restClient
    : restClient;
  const globalKpiRoundRobinRestClient = GLOBAL_KPI_USE_MULTI_EGRESS
    ? createRoundRobinRestClient(
        globalKpiRestClientEntries.map((entry) => (entry && entry.client ? entry.client : null))
      ) || restClient
    : restClient;

  copyTradingExecutionWorker = createCopyTradingExecutionWorker({
    restClient,
    loadStore: loadCopyTradingStore,
    saveStore: saveCopyTradingStore,
    getLeaderWalletsForUser,
    getApiKeysForUser,
    getCopyProfileForUser,
    getCopyRiskRulesForUser,
    getCopyRuntimeStatusForUser,
    fetchLeaderActivityPayload,
    createPacificaSignedRequest,
    onEvent: handleCopyTradingWorkerEvent,
    logger: console,
    pollIntervalMs: Math.max(3000, Number(process.env.PACIFICA_COPY_EXECUTION_POLL_MS || 5000)),
    dryRunDefault:
      String(process.env.PACIFICA_COPY_EXECUTION_DRY_RUN_DEFAULT || "true").trim().toLowerCase() !==
      "false",
    slippagePercentDefault: Math.max(
      0,
      Number(process.env.PACIFICA_COPY_EXECUTION_SLIPPAGE_PERCENT || 0.5)
    ),
    maxConcurrentLeaders: Math.max(
      1,
      Number(process.env.PACIFICA_COPY_EXECUTION_MAX_LEADER_CONCURRENCY || 4)
    ),
    maxEventsPerUser: Math.max(
      20,
      Number(process.env.PACIFICA_COPY_EXECUTION_MAX_EVENTS_PER_USER || 100)
    ),
  });

  copyTradingExecutionLoop = createManagedLoop({
    name: "copy-trading-execution",
    logger: console,
    runTask: async () => (copyTradingExecutionWorker ? copyTradingExecutionWorker.runCycle() : null),
    getDelayMs: () => Math.max(3000, Number(process.env.PACIFICA_COPY_EXECUTION_POLL_MS || 5000)),
  });

  let globalKpiState = readJson(GLOBAL_KPI_PATH, null) || null;
  let globalKpiRefreshInFlight = null;

  async function doRefreshGlobalKpi() {
    const startedAt = Date.now();
    const nowMs = Date.now();
    const todayStartMs = getUtcStartOfDayMs(nowMs);
    const todayUtcDate = formatUtcDate(todayStartMs);

    const fetchGlobalKpiMetadata = async (pathname, routeOptions = {}, label = pathname) => {
      const candidates = [
        {
          name: "direct_primary",
          client: restClient,
          options: { ...(routeOptions || {}) },
        },
        {
          name: "round_robin_fallback",
          client: globalKpiRoundRobinRestClient,
          options: { ...(routeOptions || {}) },
        },
      ];
      const seen = new Set();
      const failures = [];
      for (const candidate of candidates) {
        const client = candidate && candidate.client;
        if (!client || typeof client.get !== "function") continue;
        if (seen.has(client)) continue;
        seen.add(client);
        try {
          return await client.get(pathname, candidate.options || {});
        } catch (error) {
          const message = error && error.message ? error.message : String(error || "unknown_error");
          failures.push(`${candidate.name}:${message}`);
        }
      }
      throw new Error(`[global-kpi] ${label} failed ${failures.join(" | ") || "no_attempts"}`);
    };

    const cachedPriceRows =
      globalKpiState && Array.isArray(globalKpiState.prices) ? globalKpiState.prices : [];
    let rows = [];
    try {
      const res = await fetchGlobalKpiMetadata(
        "/info/prices",
        {
          cost: 1,
        },
        "info_prices"
      );
      const payload = res && res.payload ? res.payload : {};
      rows = Array.isArray(payload.data) ? payload.data : [];
    } catch (error) {
      if (cachedPriceRows.length) {
        rows = cachedPriceRows;
        console.warn(
          `[global-kpi] info_prices degraded; using cached prices snapshot rows=${cachedPriceRows.length} reason=${
            error && error.message ? error.message : error
          }`
        );
      } else {
        throw error;
      }
    }
    const rankedRolling24h = buildDefiLlamaVolumeRank(rows, Array.isArray(rows) ? rows.length : 0);
    const totalsFromPrices = computeDefiLlamaV2FromPrices(rows);
    const dailyVolumeFromPrices24h = rankedRolling24h.reduce(
      (sum, row) => sum + Number((row && row.volume_24h_usd) || 0),
      0
    );
    let dailyVolume = dailyVolumeFromPrices24h;
    let totalHistoricalVolume = dailyVolumeFromPrices24h;
    let dailyVolumeDefillamaCompat = null;
    let volumeSource = "/api/v1/info/prices:sum(volume_24h)/2";
    let volumeMethod = "prices_rolling_24h";
    let volumeMeta = null;
    let historyState = null;

    if (GLOBAL_KPI_VOLUME_METHOD === "defillama_compat") {
      const configuredStartMs = parseUtcDateMs(GLOBAL_KPI_DEFILLAMA_START_DATE);
      const startMs = Number.isFinite(configuredStartMs)
        ? configuredStartMs
        : Date.UTC(2025, 8, 9, 0, 0, 0, 0);
      const startDate = formatUtcDate(startMs);
      const previousCompatVersion = Number(
        globalKpiState &&
          globalKpiState.volumeMeta &&
          Number(globalKpiState.volumeMeta.compatVersion || 0)
      );
      const previousStartDate = String(
        (globalKpiState &&
          globalKpiState.volumeMeta &&
          globalKpiState.volumeMeta.trackingStartDate) ||
          (globalKpiState && globalKpiState.history && globalKpiState.history.startDate) ||
          ""
      )
        .trim()
        .slice(0, 10);
      const resetForCompatUpgrade = previousCompatVersion < GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION;
      const resetForStartDateChange =
        Boolean(previousStartDate) && previousStartDate !== startDate;
      const prevHistory =
        globalKpiState &&
        globalKpiState.history &&
        typeof globalKpiState.history === "object" &&
        !Array.isArray(globalKpiState.history)
          ? globalKpiState.history
          : {};
      const canScaleLegacyCompatHistoryV7 =
        resetForCompatUpgrade &&
        !resetForStartDateChange &&
        previousCompatVersion === 6 &&
        GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION === 7 &&
        prevHistory.dailyByDate &&
        typeof prevHistory.dailyByDate === "object" &&
        !Array.isArray(prevHistory.dailyByDate);
      const canScaleLegacyCompatHistoryV8 =
        resetForCompatUpgrade &&
        !resetForStartDateChange &&
        previousCompatVersion === 7 &&
        GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION === 8 &&
        prevHistory.dailyByDate &&
        typeof prevHistory.dailyByDate === "object" &&
        !Array.isArray(prevHistory.dailyByDate);
      if (
        resetForCompatUpgrade &&
        !canScaleLegacyCompatHistoryV7 &&
        !canScaleLegacyCompatHistoryV8
      ) {
        console.warn(
          `[global-kpi] resetting history for compat_version_upgrade old=${Number.isFinite(previousCompatVersion) ? previousCompatVersion : 0} new=${GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION}`
        );
      }
      if (resetForStartDateChange) {
        console.warn(
          `[global-kpi] resetting history for start_date_change old=${previousStartDate} new=${startDate}`
        );
      }
      let dailyByDate = {};
      let currentDaySymbolCursor = Math.max(
        0,
        Number(
          globalKpiState &&
            globalKpiState.volumeMeta &&
            globalKpiState.volumeMeta.currentDaySymbolCursor
        ) || 0
      );
      if (
        !resetForCompatUpgrade &&
        !resetForStartDateChange &&
        prevHistory.dailyByDate &&
        typeof prevHistory.dailyByDate === "object" &&
        !Array.isArray(prevHistory.dailyByDate)
      ) {
        dailyByDate = { ...prevHistory.dailyByDate };
      } else if (canScaleLegacyCompatHistoryV7) {
        for (const [dayKey, value] of Object.entries(prevHistory.dailyByDate)) {
          const row = value && typeof value === "object" ? value : {};
          const scaledVolume = Number(row.dailyVolume) * 2;
          dailyByDate[dayKey] = {
            ...row,
            dailyVolume: Number.isFinite(scaledVolume) ? scaledVolume : 0,
            migratedFromCompatVersion: previousCompatVersion,
            migratedAt: Date.now(),
          };
        }
        console.warn(
          `[global-kpi] migrated history for compat_version_upgrade old=${previousCompatVersion} new=${GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION} transform=sum(v*c)`
        );
      } else if (canScaleLegacyCompatHistoryV8) {
        for (const [dayKey, value] of Object.entries(prevHistory.dailyByDate)) {
          const row = value && typeof value === "object" ? value : {};
          const rawSymbolVolumes =
            row.symbolVolumes && typeof row.symbolVolumes === "object" && !Array.isArray(row.symbolVolumes)
              ? row.symbolVolumes
              : {};
          const normalizedSymbolVolumes = {};
          for (const [symbolRaw, volumeRaw] of Object.entries(rawSymbolVolumes)) {
            const symbol = String(symbolRaw || "").trim();
            const volume = Number(volumeRaw);
            if (!symbol || !Number.isFinite(volume)) continue;
            normalizedSymbolVolumes[symbol] = Number((volume / 2).toFixed(8));
          }
          const scaledVolume = Number(row.dailyVolume) / 2;
          dailyByDate[dayKey] = {
            ...row,
            dailyVolume: Number.isFinite(scaledVolume) ? scaledVolume : 0,
            symbolVolumes: normalizedSymbolVolumes,
            migratedFromCompatVersion: previousCompatVersion,
            migratedAt: Date.now(),
          };
        }
        console.warn(
          `[global-kpi] migrated history for compat_version_upgrade old=${previousCompatVersion} new=${GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION} transform=sum((v*c)/2)`
        );
      }

      let markets = [];
      try {
        const marketsPayloadRes = await fetchGlobalKpiMetadata(
          "/info",
          {
            cost: 1,
          },
          "markets"
        );
        const marketsPayload =
          marketsPayloadRes && marketsPayloadRes.payload ? marketsPayloadRes.payload : {};
        const marketRows = Array.isArray(marketsPayload.data) ? marketsPayload.data : [];
        const seenMarkets = new Set();
        for (const row of marketRows) {
          const symbol = String((row && row.symbol) || "").trim();
          if (!symbol || seenMarkets.has(symbol)) continue;
          seenMarkets.add(symbol);
          const createdAtRaw = row && row.created_at !== undefined ? Number(row.created_at) : null;
          markets.push({
            symbol,
            createdAtMs: Number.isFinite(createdAtRaw) ? createdAtRaw : null,
          });
        }
      } catch (error) {
        const seenMarkets = new Set();
        for (const row of rows) {
          const symbol = String((row && row.symbol) || "").trim();
          if (!symbol || seenMarkets.has(symbol)) continue;
          seenMarkets.add(symbol);
          markets.push({
            symbol,
            createdAtMs: null,
          });
        }
        if (!markets.length) {
          throw error;
        }
        console.warn(
          `[global-kpi] markets degraded; using cached symbol universe count=${markets.length} reason=${
            error && error.message ? error.message : error
          }`
        );
      }
      if (!Object.keys(dailyByDate).length && markets.length) {
        const bootstrap = await computeDefiLlamaCompatHistoricalVolume({
          restClient: globalKpiRestClient,
          symbols: markets.map((market) => market.symbol).filter(Boolean),
          startMs,
          endMs: todayStartMs,
          symbolConcurrency: GLOBAL_KPI_SYMBOL_CONCURRENCY,
          shardKeyPrefix: `global_kpi:kline_bootstrap:${todayUtcDate}`,
        });
        if (bootstrap && bootstrap.dailyByDate && Object.keys(bootstrap.dailyByDate).length) {
          dailyByDate = bootstrap.dailyByDate;
          console.warn(
            `[global-kpi] bootstrapped full historical volume start=${startDate} end=${todayUtcDate} symbols=${markets.length} ok_symbols=${bootstrap.okSymbols} failed_symbols=${bootstrap.failedSymbols}`
          );
        }
      }
      let progress = computeDefillamaContiguousProgress({
        startMs,
        todayMs: todayStartMs,
        dailyByDate,
      });

      const persistCompatSnapshot = ({ progressState, processedDatesState = [] }) => {
        const todayEntry = dailyByDate[todayUtcDate] || null;
        const currentDailyCompat = toFinite(todayEntry ? extractDailyVolume(todayEntry) : 0, 0);
        const nextVolumeMeta = {
          trackingStartDate: startDate,
          todayUtcDate,
          latestDayDate: todayUtcDate,
          latestDayStartMs: todayStartMs,
          latestDayStartIso: new Date(todayStartMs).toISOString(),
          symbolCount: Number(markets.length || 0),
          lastProcessedDate: progressState.lastProcessedDate,
          nextUnprocessedDate: progressState.nextUnprocessedDate,
          remainingDaysToToday: progressState.remainingDays,
          processedDays: progressState.processedDays,
          totalDaysToToday: progressState.totalDays,
          backfillComplete: progressState.backfillComplete,
          cumulativeVolume: Number(progressState.cumulativeVolume || 0),
          currentDaySymbolCursor,
          processedDates: Array.isArray(processedDatesState) ? processedDatesState : [],
          strictDaily: GLOBAL_KPI_STRICT_DAILY,
          backfillDaysPerRun: GLOBAL_KPI_BACKFILL_DAYS_PER_RUN,
          compatVersion: GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION,
        };
        const nextHistoryState = {
          startDate,
          todayUtcDate,
          lastProcessedDate: progressState.lastProcessedDate,
          nextUnprocessedDate: progressState.nextUnprocessedDate,
          remainingDaysToToday: progressState.remainingDays,
          processedDays: progressState.processedDays,
          totalDaysToToday: progressState.totalDays,
          backfillComplete: progressState.backfillComplete,
          cumulativeVolume: Number(progressState.cumulativeVolume || 0),
          compatVersion: GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION,
          dailyByDate,
          lastRunAt: Date.now(),
        };

        globalKpiState = {
          source: "/api/v1/info/prices",
          volumeSource: "/api/v1/kline?interval=1d&start_time=<utc_day_start>:sum((v*c)/2)",
          volumeMethod: "defillama_compat",
          volumeMeta: nextVolumeMeta,
          fetchedAt: Date.now(),
          fetchDurationMs: Date.now() - startedAt,
          dailyVolume: currentDailyCompat,
          totalHistoricalVolume: Number(progressState.cumulativeVolume || 0),
          dailyVolumeFromPrices24h,
          dailyVolumeDefillamaCompat: currentDailyCompat,
          openInterestAtEnd: Number(totalsFromPrices.openInterestAtEnd || 0),
          prices: rows,
          volumeRank: rankedRolling24h,
          history: nextHistoryState,
          updatedAt: Date.now(),
        };
        ensureDir(path.dirname(GLOBAL_KPI_PATH));
        writeJsonAtomic(GLOBAL_KPI_PATH, globalKpiState);
      };

      // Persist a reset/bootstrapped history immediately so the API can serve the
      // corrected historical baseline even if the follow-up current-day reconcile
      // is delayed by rate limiting.
      if (resetForCompatUpgrade || resetForStartDateChange || !Object.keys(prevHistory.dailyByDate || {}).length) {
        persistCompatSnapshot({
          progressState: progress,
          processedDatesState: [],
        });
      }

      const daysToProcessMs = [];
      if (progress.nextUnprocessedDate) {
        let cursorMs = parseUtcDateMs(progress.nextUnprocessedDate);
        while (
          Number.isFinite(cursorMs) &&
          cursorMs <= todayStartMs &&
          daysToProcessMs.length < GLOBAL_KPI_BACKFILL_DAYS_PER_RUN
        ) {
          daysToProcessMs.push(cursorMs);
          cursorMs += ONE_DAY_MS;
        }
      } else {
        // Backfill complete: if old rows are missing symbol-level volumes, repair those first.
        const nextMissingSymbolDate = findNextMissingSymbolVolumeDate({
          startMs,
          endMs: todayStartMs,
          dailyByDate,
        });
        if (nextMissingSymbolDate) {
          let cursorMs = parseUtcDateMs(nextMissingSymbolDate);
          while (
            Number.isFinite(cursorMs) &&
            cursorMs <= todayStartMs &&
            daysToProcessMs.length < GLOBAL_KPI_BACKFILL_DAYS_PER_RUN
          ) {
            daysToProcessMs.push(cursorMs);
            cursorMs += ONE_DAY_MS;
          }
        } else {
          // Keep current UTC day fresh while backfill is complete.
          daysToProcessMs.push(todayStartMs);
        }
      }

      // Recompute the most recent closed days in full so partial current-day
      // batches can never become permanent historical rows.
      const recentRepairDaysMs = buildRecentClosedRepairDays({
        todayStartMs,
        startMs,
        lookbackDays: GLOBAL_KPI_RECENT_REPAIR_DAYS,
      });
      for (const dayMs of recentRepairDaysMs) {
        if (!Number.isFinite(dayMs)) continue;
        daysToProcessMs.push(dayMs);
      }

      const dedupedDaysToProcessMs = Array.from(
        new Set(
          daysToProcessMs
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value) && value >= startMs && value <= todayStartMs)
        )
      ).sort((left, right) => left - right);

      const processedDates = [];
      for (const dayMs of dedupedDaysToProcessMs) {
        const dayDate = formatUtcDate(dayMs);
        const dayEndMs = dayMs + ONE_DAY_MS - 1;
        const useDirectForCurrentDay =
          Boolean(progress.backfillComplete) && Number(dayMs) === Number(todayStartMs);
        const fullDaySymbols = markets
          .filter((market) => {
            const listedAt = Number(market && market.createdAtMs);
            if (!Number.isFinite(listedAt)) return true;
            return listedAt <= dayEndMs;
          })
          .map((market) => market.symbol)
          .filter(Boolean);
        let daySymbols = fullDaySymbols;
        let compatSymbolConcurrency = GLOBAL_KPI_SYMBOL_CONCURRENCY;
        if (useDirectForCurrentDay && fullDaySymbols.length > 0) {
          const batchSize = Math.max(
            1,
            Math.min(
              20,
              fullDaySymbols.length,
              Number(process.env.PACIFICA_GLOBAL_KPI_CURRENT_DAY_BATCH_SIZE || 20)
            )
          );
          const startIndex = currentDaySymbolCursor % fullDaySymbols.length;
          const subset = [];
          for (let idx = 0; idx < batchSize; idx += 1) {
            subset.push(fullDaySymbols[(startIndex + idx) % fullDaySymbols.length]);
          }
          daySymbols = subset;
          compatSymbolConcurrency = Math.max(1, Math.min(2, subset.length));
        }
        const compat = await computeDefiLlamaCompatDailyVolume({
          restClient: useDirectForCurrentDay ? restClient : globalKpiRestClient,
          symbols: daySymbols,
          startOfDayMs: dayMs,
          symbolConcurrency: compatSymbolConcurrency,
          logger: console,
          shardKeyPrefix: `global_kpi:kline:${dayDate}`,
        });

        // Guard against writing bad day snapshots when transport is degraded.
        // Missing symbols are expected for days with no trades/listings, but failed symbols
        // indicate request-level errors (timeouts/429/proxy failures).
        const failedRatio =
          Number(compat.symbolCount || 0) > 0
            ? Number(compat.failedSymbols || 0) / Number(compat.symbolCount || 1)
            : 0;
        if (GLOBAL_KPI_STRICT_DAILY && Number(compat.failedSymbols || 0) > 0) {
          throw new Error(
            `defillama_compat day=${dayDate} incomplete: failed_symbols=${compat.failedSymbols}`
          );
        }

        if (
          Number(compat.failedSymbols || 0) > 0 &&
          (Number(compat.okSymbols || 0) === 0 || failedRatio > 0.25)
        ) {
          // In non-strict mode we keep moving forward with explicit quality markers.
          // This prevents full KPI starvation when a subset of symbols/proxies fail.
          console.warn(
            `[global-kpi] defillama_compat day=${dayDate} degraded: failed_symbols=${compat.failedSymbols} ok_symbols=${compat.okSymbols} symbol_count=${compat.symbolCount} strict=false`
          );
          if (Number(compat.okSymbols || 0) === 0) {
            const existing = dailyByDate[dayDate];
            if (existing && typeof existing === "object") {
              processedDates.push(dayDate);
              continue;
            }
          }
        }

        const normalizedSymbolVolumes = {};
        if (
          compat.symbolVolumes &&
          typeof compat.symbolVolumes === "object" &&
          !Array.isArray(compat.symbolVolumes)
        ) {
          Object.entries(compat.symbolVolumes).forEach(([symbolRaw, volumeRaw]) => {
            const symbol = String(symbolRaw || "").trim();
            if (!symbol) return;
            const volume = Number(volumeRaw);
            if (!Number.isFinite(volume) || volume <= 0) return;
            normalizedSymbolVolumes[symbol] = Number(volume.toFixed(8));
          });
        }

        if (useDirectForCurrentDay) {
          const previousDayEntry =
            dailyByDate[dayDate] && typeof dailyByDate[dayDate] === "object" ? dailyByDate[dayDate] : {};
          const previousSymbolVolumes =
            previousDayEntry.symbolVolumes &&
            typeof previousDayEntry.symbolVolumes === "object" &&
            !Array.isArray(previousDayEntry.symbolVolumes)
              ? previousDayEntry.symbolVolumes
              : {};
          const mergedSymbolVolumes = {
            ...previousSymbolVolumes,
            ...normalizedSymbolVolumes,
          };
          const mergedDailyVolume = Object.values(mergedSymbolVolumes).reduce(
            (sum, value) => sum + toFinite(value, 0),
            0
          );
          dailyByDate[dayDate] = {
            dailyVolume: Number(mergedDailyVolume || 0),
            symbolVolumes: mergedSymbolVolumes,
            symbolCount: Number(fullDaySymbols.length || 0),
            okSymbols: Object.keys(mergedSymbolVolumes).length,
            failedSymbols: Number(compat.failedSymbols || 0),
            missingSymbols: Math.max(0, Number(fullDaySymbols.length || 0) - Object.keys(mergedSymbolVolumes).length),
            sampleErrors: Array.isArray(compat.errors) ? compat.errors : [],
            updatedAt: Date.now(),
            refreshBatchSize: Number(daySymbols.length || 0),
          };
          currentDaySymbolCursor = (currentDaySymbolCursor + Number(daySymbols.length || 0)) % Math.max(1, fullDaySymbols.length);
        } else {
          dailyByDate[dayDate] = {
            dailyVolume: Number(compat.dailyVolume || 0),
            symbolVolumes: normalizedSymbolVolumes,
            symbolCount: Number(compat.symbolCount || 0),
            okSymbols: Number(compat.okSymbols || 0),
            failedSymbols: Number(compat.failedSymbols || 0),
            missingSymbols: Number(compat.missingSymbols || 0),
            sampleErrors: Array.isArray(compat.errors) ? compat.errors : [],
            updatedAt: Date.now(),
          };
        }
        processedDates.push(dayDate);

        // Persist incremental progress so UI can reflect live backfill advancement.
        const incrementalProgress = computeDefillamaContiguousProgress({
          startMs,
          todayMs: todayStartMs,
          dailyByDate,
        });
        persistCompatSnapshot({
          progressState: incrementalProgress,
          processedDatesState: [...processedDates],
        });
      }

      progress = computeDefillamaContiguousProgress({
        startMs,
        todayMs: todayStartMs,
        dailyByDate,
      });
      persistCompatSnapshot({
        progressState: progress,
        processedDatesState: [...processedDates],
      });

      const latestState = globalKpiState || {};
      dailyVolumeDefillamaCompat = Number(latestState.dailyVolume || 0);
      dailyVolume = dailyVolumeDefillamaCompat;
      totalHistoricalVolume = Number(latestState.totalHistoricalVolume || 0);
      volumeMethod = latestState.volumeMethod || "defillama_compat";
      volumeSource =
        latestState.volumeSource ||
        "/api/v1/kline?interval=1d&start_time=<utc_day_start>:sum((v*c)/2)";
      volumeMeta = latestState.volumeMeta || null;
      historyState = latestState.history || null;
    }

    const ranked = buildDefiLlamaVolumeRank(rows, Array.isArray(rows) ? rows.length : 0);
    globalKpiState = {
      source: "/api/v1/info/prices",
      volumeSource,
      volumeMethod,
      volumeMeta,
      fetchedAt: Date.now(),
      fetchDurationMs: Date.now() - startedAt,
      dailyVolume,
      totalHistoricalVolume,
      dailyVolumeFromPrices24h,
      dailyVolumeDefillamaCompat,
      openInterestAtEnd: Number(totalsFromPrices.openInterestAtEnd || 0),
      prices: rows,
      volumeRank: ranked,
      history: historyState,
      updatedAt: Date.now(),
    };
    ensureDir(path.dirname(GLOBAL_KPI_PATH));
    writeJsonAtomic(GLOBAL_KPI_PATH, globalKpiState);
    return globalKpiState;
  }

  async function refreshGlobalKpi() {
    if (globalKpiRefreshInFlight) {
      return globalKpiRefreshInFlight;
    }
    globalKpiRefreshInFlight = doRefreshGlobalKpi()
      .catch((error) => {
        throw error;
      })
      .finally(() => {
        globalKpiRefreshInFlight = null;
      });
    return globalKpiRefreshInFlight;
  }

  function getGlobalKpiState() {
    const fromDisk = readJson(GLOBAL_KPI_PATH, null);
    if (fromDisk && typeof fromDisk === "object") {
      globalKpiState = fromDisk;
    }
    return globalKpiState;
  }

  const wsClient = createWsClient({
    url: WS_URL,
    logger: console,
    reconnectMs: 2000,
    pingIntervalMs: 25000,
  });
  const solanaRpcAdaptiveConfig = {
    globalBucketCapacity: SOLANA_RPC_GLOBAL_BUCKET_CAPACITY,
    globalRefillPerSec: SOLANA_RPC_GLOBAL_REFILL_PER_SEC,
    sigRefillPerSec: SOLANA_RPC_SIG_REFILL_PER_SEC,
    txRefillPerSec: SOLANA_RPC_TX_REFILL_PER_SEC,
    sigMaxConcurrency: SOLANA_RPC_SIG_MAX_CONCURRENCY,
    txMaxConcurrency: SOLANA_RPC_TX_MAX_CONCURRENCY,
    sigInitialConcurrency: SOLANA_RPC_SIG_INITIAL_CONCURRENCY,
    txInitialConcurrency: SOLANA_RPC_TX_INITIAL_CONCURRENCY,
    base429DelayMs: SOLANA_RPC_429_BASE_DELAY_MS,
    max429DelayMs: SOLANA_RPC_429_MAX_DELAY_MS,
    rampQuietMs: SOLANA_RPC_RAMP_QUIET_MS,
    rampStepMs: SOLANA_RPC_RAMP_STEP_MS,
  };
  const solanaRpcClientEntries =
    HELIUS_API_KEYS.length > 0
      ? HELIUS_API_KEYS.map((key, idx) => {
          const heliusRateGuard = createRateLimitGuard({
            capacity: SOLANA_RPC_RATE_LIMIT_CAPACITY,
            refillWindowMs: SOLANA_RPC_RATE_LIMIT_WINDOW_MS,
            safetyMarginRatio: SOLANA_RPC_RATE_LIMIT_SAFETY_RATIO,
          });
          const rpcUrl = HELIUS_RPC_URL_TEMPLATE.replace("{key}", encodeURIComponent(key));
          return {
            id: `helius_${idx + 1}`,
            rpcUrl,
            client: createSolanaRpcClient({
              rpcUrl,
              retryPolicy: solanaRetryPolicy,
              rateLimitGuard: heliusRateGuard,
              adaptive: solanaRpcAdaptiveConfig,
              logger: console,
            }),
          };
        })
      : [
          {
            id: "solana_direct",
            rpcUrl: SOLANA_RPC_URL,
            client: createSolanaRpcClient({
              rpcUrl: SOLANA_RPC_URL,
              retryPolicy: solanaRetryPolicy,
              rateLimitGuard: solanaRateGuard,
              adaptive: solanaRpcAdaptiveConfig,
              logger: console,
            }),
          },
        ];
  const solanaRpcClient =
    solanaRpcClientEntries.length > 1
      ? createShardedSolanaRpcClient({
          clients: solanaRpcClientEntries,
          logger: console,
        })
      : solanaRpcClientEntries[0].client;
  const liveWalletTriggerStore = new LiveWalletTriggerStore({
    filePath: LIVE_WALLET_TRIGGER_FILE,
    maxEntries: Math.max(
      5000,
      Number(process.env.PACIFICA_LIVE_WALLET_TRIGGER_MAX_ENTRIES || 50000)
    ),
  });
  const solanaLogTriggerMonitor = new SolanaLogsTriggerMonitor({
    enabled:
      String(process.env.PACIFICA_SOLANA_LOG_TRIGGER_ENABLED || "true").toLowerCase() !== "false",
    rpcUrl: deriveRpcWsUrl(process.env.SOLANA_WS_URL || solanaRpcClientEntries[0].rpcUrl || SOLANA_RPC_URL),
    rpcClient: solanaRpcClient,
    triggerStore: liveWalletTriggerStore,
    logger: console,
    programIds: ONCHAIN_PROGRAM_IDS,
    mentionAddresses: TRIGGER_MENTION_ADDRESSES,
    maxConcurrentTx: Math.max(1, Number(process.env.PACIFICA_SOLANA_LOG_TRIGGER_MAX_CONCURRENCY || 4)),
    maxWalletCandidates: Math.max(
      1,
      Number(process.env.PACIFICA_SOLANA_LOG_TRIGGER_MAX_WALLET_CANDIDATES || 6)
    ),
    txTimeoutMs: Math.max(1000, Number(process.env.PACIFICA_SOLANA_LOG_TRIGGER_TX_TIMEOUT_MS || 10000)),
    includeErroredTx:
      String(process.env.PACIFICA_SOLANA_LOG_TRIGGER_INCLUDE_ERRORED_TX || "false").toLowerCase() ===
      "true",
  });

  const startupState = {
    phase: "booting",
    startedAt: Date.now(),
    listeningAt: null,
    readyAt: null,
    lastError: null,
    steps: {
      pipeline: { status: "pending", startedAt: null, finishedAt: null, skippedReplay: PIPELINE_SKIP_REPLAY },
      collectionStore: { status: "pending", startedAt: null, finishedAt: null },
      walletStore: { status: "pending", startedAt: null, finishedAt: null },
      walletMetricsHistoryStore: { status: "pending", startedAt: null, finishedAt: null },
      positionLifecycleStore: { status: "pending", startedAt: null, finishedAt: null },
      walletIndexerState: { status: "pending", startedAt: null, finishedAt: null },
      onchainState: { status: "pending", startedAt: null, finishedAt: null },
    },
  };

  function setStartupStep(stepName, patch = {}) {
    const current =
      startupState.steps && startupState.steps[stepName] && typeof startupState.steps[stepName] === "object"
        ? startupState.steps[stepName]
        : {};
    startupState.steps[stepName] = {
      ...current,
      ...patch,
    };
  }

  function getStartupStatus() {
    const steps = {};
    Object.entries(startupState.steps || {}).forEach(([name, value]) => {
      steps[name] = value && typeof value === "object" ? { ...value } : value;
    });
    const readyCount = Object.values(steps).filter((row) => row && row.status === "ready").length;
    const pendingCount = Object.values(steps).filter((row) => row && row.status === "pending").length;
    const erroredCount = Object.values(steps).filter((row) => row && row.status === "error").length;
    return {
      phase: startupState.phase,
      startedAt: startupState.startedAt,
      listeningAt: startupState.listeningAt,
      readyAt: startupState.readyAt,
      lastError: startupState.lastError,
      readyCount,
      pendingCount,
      erroredCount,
      steps,
    };
  }

  const pipeline = new PacificaPipelineService({
    dataDir: PIPELINE_DATA_DIR,
    apiBase: API_BASE,
    wsUrl: WS_URL,
    serviceName: SERVICE,
    persistEveryMs: PIPELINE_PERSIST_EVERY_MS,
    skipReplay: PIPELINE_SKIP_REPLAY,
  });

  if (ACCOUNT_ENV_PRESENT) {
    pipeline.setAccount(INITIAL_ACCOUNT || null);
  } else if (INDEXER_ENABLED) {
    // In global indexer mode, default to no single tracked account unless explicitly set.
    pipeline.setAccount(null);
  }

  const collectionStore = new CollectionStore({
    dataDir: CREATOR_DATA_DIR,
    filePath: path.join(CREATOR_DATA_DIR, "collections.json"),
  });

  const walletStore = new WalletExplorerStore({
    dataDir: INDEXER_DATA_DIR,
    filePath: path.join(INDEXER_DATA_DIR, "wallets.json"),
    externalShardsEnabled: INDEXER_EXTERNAL_SHARDS_ENABLED,
    externalShardsDir: INDEXER_EXTERNAL_SHARDS_DIR,
    ownerShardCount: INDEXER_WORKER_SHARD_COUNT,
    ownerShardIndex: INDEXER_WORKER_SHARD_INDEX,
    enforceOwnerOnly: INDEXER_WORKER_SHARD_COUNT > 1 && !INDEXER_EXTERNAL_SHARDS_ENABLED,
  });
  const UI_READ_MODEL_ONLY_MODE =
    !INDEXER_ENABLED &&
    !ONCHAIN_ENABLED &&
    !GLOBAL_KPI_ENABLED &&
    !SNAPSHOT_ENABLED &&
    Boolean(walletStore && walletStore.externalShardsEnabled);
  const walletMetricsHistoryStore = new WalletMetricsHistoryStore({
    baseDir: path.join(DATA_ROOT, "indexer", "wallet_metrics_history"),
    retentionHourly: Math.max(
      24,
      Number(process.env.PACIFICA_WALLET_METRICS_HISTORY_RETENTION_HOURLY || 24 * 14)
    ),
    retentionDaily: Math.max(
      14,
      Number(process.env.PACIFICA_WALLET_METRICS_HISTORY_RETENTION_DAILY || 180)
    ),
  });
  const positionLifecycleStore = new PositionLifecycleStore({
    dataDir: path.join(DATA_ROOT, "indexer"),
    filePath: path.join(DATA_ROOT, "indexer", "position_lifecycle_state.json"),
    flushIntervalMs: Math.max(
      1000,
      Number(process.env.PACIFICA_POSITION_LIFECYCLE_FLUSH_MS || 15000)
    ),
    maxBufferedEvents: Math.max(
      1,
      Number(process.env.PACIFICA_POSITION_LIFECYCLE_MAX_BUFFERED || 200)
    ),
    retentionClosedMs: Math.max(
      24 * 60 * 60 * 1000,
      Number(
        process.env.PACIFICA_POSITION_LIFECYCLE_RETENTION_MS ||
          180 * 24 * 60 * 60 * 1000
      )
    ),
  });

  const walletSource = createWalletSource({
    filePath: WALLET_SOURCE_FILE,
    url: WALLET_SOURCE_URL || null,
    inlineWallets: uniqWalletSeeds([INITIAL_ACCOUNT, ...INLINE_WALLET_SEEDS]),
    logger: console,
  });
  const onchainDiscovery =
    ONCHAIN_ENABLED
      ? new OnChainWalletDiscovery({
          rpcClient: solanaRpcClient,
          restClient: indexerRoundRobinRestClient || restClient,
          dataDir: INDEXER_DATA_DIR,
          statePath: path.join(INDEXER_DATA_DIR, "onchain_state.json"),
          walletsPath: path.join(INDEXER_DATA_DIR, "wallet_discovery.json"),
          rawSignaturesPath: path.join(INDEXER_DATA_DIR, "raw_signatures.ndjson"),
          rawTransactionsPath: path.join(INDEXER_DATA_DIR, "raw_transactions.ndjson"),
          programIds: ONCHAIN_PROGRAM_IDS,
          excludeAddresses: ONCHAIN_EXCLUDE_ADDRESSES,
          depositVaults: ONCHAIN_DEPOSIT_VAULTS,
          discoveryType: ONCHAIN_DISCOVERY_TYPE,
          mode: ONCHAIN_MODE,
          startTimeMs: ONCHAIN_START_TIME_MS,
          endTimeMs: ONCHAIN_END_TIME_MS,
          signaturePageLimit: ONCHAIN_SIGNATURE_PAGE_LIMIT,
          scanIntervalMs: ONCHAIN_SCAN_INTERVAL_MS,
          scanPagesPerCycle: ONCHAIN_SCAN_PAGES_PER_CYCLE,
          txConcurrency: ONCHAIN_TX_CONCURRENCY,
          maxTransactionsPerCycle: ONCHAIN_MAX_TX_PER_CYCLE,
          checkpointIntervalMs: ONCHAIN_CHECKPOINT_INTERVAL_MS,
          logIntervalMs: ONCHAIN_LOG_INTERVAL_MS,
          logFormat: ONCHAIN_LOG_FORMAT,
          pendingMaxAttempts: ONCHAIN_PENDING_MAX_ATTEMPTS,
          validationBatchSize: ONCHAIN_VALIDATE_BATCH,
          logger: console,
        })
      : null;

  const walletIndexer = INDEXER_ENABLED
      ? new ExchangeWalletIndexer({
        restClient: indexerRoundRobinRestClient || restClient,
        restClients: indexerRestClients,
        restClientIds: indexerRestClientIds,
        restClientEntries: indexerRestClientEntries.map((entry) => ({
          id: entry.id,
          client: entry.client,
          proxyUrl: entry.proxyUrl || null,
          rateGuard: entry.rateGuard || null,
        })),
        walletStore,
        walletSource,
        onchainDiscovery,
        dataDir: INDEXER_DATA_DIR,
        statePath: path.join(INDEXER_DATA_DIR, "indexer_state.json"),
        onchainStatePath: path.join(INDEXER_DATA_DIR, "onchain_state.json"),
        depositWalletsPath: DEPOSIT_WALLETS_PATH,
        seedWallets: uniqWalletSeeds([INITIAL_ACCOUNT, ...INLINE_WALLET_SEEDS]),
        scanIntervalMs: INDEXER_SCAN_INTERVAL_MS,
        discoveryIntervalMs: INDEXER_DISCOVERY_INTERVAL_MS,
        maxWalletsPerScan: effectiveIndexerBatchSize,
        maxPagesPerWallet: INDEXER_MAX_PAGES_PER_WALLET,
        fullHistoryPerWallet: INDEXER_FULL_HISTORY_PER_WALLET,
        tradesPageLimit: INDEXER_TRADES_PAGE_LIMIT,
        fundingPageLimit: INDEXER_FUNDING_PAGE_LIMIT,
        walletScanConcurrency: effectiveWalletScanConcurrency,
        restShardParallelismMax: INDEXER_REST_SHARD_PARALLELISM_MAX,
        liveWalletsPerScan: INDEXER_LIVE_WALLETS_PER_SCAN,
        liveWalletsPerScanMin: INDEXER_LIVE_WALLETS_PER_SCAN_MIN,
        liveWalletsPerScanMax: INDEXER_LIVE_WALLETS_PER_SCAN_MAX,
        liveRefreshTargetMs: INDEXER_LIVE_REFRESH_TARGET_MS,
        liveRefreshConcurrency: INDEXER_LIVE_REFRESH_CONCURRENCY,
        liveTriggerStore: liveWalletTriggerStore,
        liveTriggerPollMs: INDEXER_LIVE_TRIGGER_POLL_MS,
        liveTriggerReadLimit: INDEXER_LIVE_TRIGGER_READ_LIMIT,
        liveTriggerTtlMs: INDEXER_LIVE_TRIGGER_TTL_MS,
        liveMaxPagesPerWallet: INDEXER_LIVE_MAX_PAGES_PER_WALLET,
        fullHistoryPagesPerScan: INDEXER_FULL_HISTORY_PAGES_PER_SCAN,
        deepHistoryPagesPerScan: INDEXER_DEEP_HISTORY_PAGES_PER_SCAN,
        activationTradesPageLimit: INDEXER_ACTIVATION_TRADES_PAGE_LIMIT,
        activationFundingPageLimit: INDEXER_ACTIVATION_FUNDING_PAGE_LIMIT,
        deepHistoryTradesPageLimit: INDEXER_DEEP_HISTORY_TRADES_PAGE_LIMIT,
        deepHistoryFundingPageLimit: INDEXER_DEEP_HISTORY_FUNDING_PAGE_LIMIT,
        historyAuditRepairBatchSize: INDEXER_HISTORY_AUDIT_REPAIR_BATCH_SIZE,
        historyAuditRepairHotWalletLimit: INDEXER_HISTORY_AUDIT_REPAIR_HOT_WALLET_LIMIT,
        topCorrectionCohortSize: INDEXER_TOP_CORRECTION_COHORT_SIZE,
        historyRequestMaxAttempts: INDEXER_HISTORY_REQUEST_MAX_ATTEMPTS,
        activationReserveMin: INDEXER_ACTIVATION_RESERVE_MIN,
        activationReserveMax: INDEXER_ACTIVATION_RESERVE_MAX,
        continuationReserveMin: INDEXER_CONTINUATION_RESERVE_MIN,
        continuationReserveMax: INDEXER_CONTINUATION_RESERVE_MAX,
        deepBackfillMinPerScan: INDEXER_DEEP_BACKFILL_MIN_PER_SCAN,
        backfillProxyStrictContinuationThreshold:
          INDEXER_BACKFILL_PROXY_STRICT_CONTINUATION_THRESHOLD,
        backfillProxyStrictMinWeight: INDEXER_BACKFILL_PROXY_STRICT_MIN_WEIGHT,
        backfillProxyStrictMaxLatencyMs: INDEXER_BACKFILL_PROXY_STRICT_MAX_LATENCY_MS,
        backfillProxyStrictMaxFailureRatio:
          INDEXER_BACKFILL_PROXY_STRICT_MAX_FAILURE_RATIO,
        backfillProxyStrictMaxTimeoutRatio:
          INDEXER_BACKFILL_PROXY_STRICT_MAX_TIMEOUT_RATIO,
        backfillProxyStrictMaxNetworkRatio:
          INDEXER_BACKFILL_PROXY_STRICT_MAX_NETWORK_RATIO,
        backfillQualityWindowMs: INDEXER_BACKFILL_QUALITY_WINDOW_MS,
        backfillQualityWindowCount: INDEXER_BACKFILL_QUALITY_WINDOW_COUNT,
        backfillWindowHighLatencyCountThreshold:
          INDEXER_BACKFILL_WINDOW_HIGH_LATENCY_COUNT_THRESHOLD,
        backfillWindowBadCountThreshold:
          INDEXER_BACKFILL_WINDOW_BAD_COUNT_THRESHOLD,
        backfillWindowCooldownMs: INDEXER_BACKFILL_WINDOW_COOLDOWN_MS,
        deepBackfillMaxSharePerProxy: INDEXER_DEEP_BACKFILL_MAX_SHARE_PER_PROXY,
        deepBackfillMinTasksPerProxy: INDEXER_DEEP_BACKFILL_MIN_TASKS_PER_PROXY,
        deepBackfillMaxTasksPerProxy: INDEXER_DEEP_BACKFILL_MAX_TASKS_PER_PROXY,
        backfillPageBudgetWhenLivePressure:
          INDEXER_BACKFILL_PAGE_BUDGET_WHEN_LIVE_PRESSURE,
        activateTaskTimeoutMs: INDEXER_ACTIVATE_TIMEOUT_MS,
        recentTaskTimeoutMs: INDEXER_RECENT_TIMEOUT_MS,
        backfillTaskTimeoutMs: INDEXER_BACKFILL_TIMEOUT_MS,
        liveTaskTimeoutMs: INDEXER_LIVE_TIMEOUT_MS,
        activateRequestTimeoutMs: INDEXER_ACTIVATE_REQUEST_TIMEOUT_MS,
        recentRequestTimeoutMs: INDEXER_RECENT_REQUEST_TIMEOUT_MS,
        backfillRequestTimeoutMs: INDEXER_BACKFILL_REQUEST_TIMEOUT_MS,
        liveRequestTimeoutMs: INDEXER_LIVE_REQUEST_TIMEOUT_MS,
        stateSaveMinIntervalMs: INDEXER_STATE_SAVE_MIN_INTERVAL_MS,
        cacheEntriesPerEndpoint: INDEXER_CACHE_ENTRIES_PER_ENDPOINT,
        backlogModeEnabled: INDEXER_BACKLOG_MODE_ENABLED,
        backlogWalletThreshold: INDEXER_BACKLOG_WALLETS_THRESHOLD,
        backlogAvgWaitMsThreshold: INDEXER_BACKLOG_AVG_WAIT_MS_THRESHOLD,
        backlogDiscoverEveryCycles: INDEXER_BACKLOG_DISCOVER_EVERY_CYCLES,
        backlogRefillBatch: INDEXER_BACKLOG_REFILL_BATCH,
        scanRampQuietMs: INDEXER_SCAN_RAMP_QUIET_MS,
        scanRampStepMs: INDEXER_SCAN_RAMP_STEP_MS,
        client429CooldownBaseMs: INDEXER_CLIENT_429_COOLDOWN_BASE_MS,
        client429CooldownMaxMs: INDEXER_CLIENT_429_COOLDOWN_MAX_MS,
        clientServerErrorCooldownBaseMs: INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_BASE_MS,
        clientServerErrorCooldownMaxMs: INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_MAX_MS,
        clientTimeoutCooldownBaseMs: INDEXER_CLIENT_TIMEOUT_COOLDOWN_BASE_MS,
        clientTimeoutCooldownMaxMs: INDEXER_CLIENT_TIMEOUT_COOLDOWN_MAX_MS,
        clientDefaultCooldownMs: INDEXER_CLIENT_DEFAULT_COOLDOWN_MS,
        restShardStrategy: String(
          process.env.PACIFICA_INDEXER_REST_SHARD_STRATEGY || "wallet_hash"
        ).trim(),
        walletLeaseMs: Math.max(
          INDEXER_SCAN_INTERVAL_MS * 2,
          Number(process.env.PACIFICA_INDEXER_WALLET_LEASE_MS || 2 * 60 * 1000)
        ),
        discoveryOnly: INDEXER_DISCOVERY_ONLY,
        onchainPagesPerDiscoveryCycle: ONCHAIN_SCAN_PAGES_PER_CYCLE,
        onchainPagesMaxPerCycle: ONCHAIN_SCAN_PAGES_MAX,
        onchainValidatePerCycle: INDEXER_DISCOVERY_ONLY ? 0 : ONCHAIN_VALIDATE_BATCH,
        logger: console,
        workerShardIndex: INDEXER_WORKER_SHARD_INDEX,
        workerShardCount: INDEXER_WORKER_SHARD_COUNT,
        workerShardId:
          INDEXER_WORKER_SHARD_COUNT > 1
            ? `indexer_shard_${INDEXER_WORKER_SHARD_INDEX}`
            : "indexer_primary",
      })
    : null;
  if (!onchainDiscovery) {
    setStartupStep("onchainState", {
      status: "disabled",
      startedAt: Date.now(),
      finishedAt: Date.now(),
    });
  }
  if (!walletIndexer) {
    setStartupStep("walletIndexerState", {
      status: "disabled",
      startedAt: Date.now(),
      finishedAt: Date.now(),
    });
  }

  const liveHost = MINIMAL_UI_SERVER || UI_READ_MODEL_ONLY_MODE
    ? {
        start() {},
        stop() {},
        setFocusSymbols() {},
        setAccount() {},
      }
    : (() => {
        ensureUiComponentFactories();
        return createLiveTradesHostComponent({
          wsClient,
          pipeline,
          account: pipeline.getAccount(),
          logger: console,
        });
      })();

  const bootstrapSnapshots = makeBootstrapper({
    restClient,
    pipeline,
    logger: console,
  });

  async function refreshSnapshots() {
    const result = await bootstrapSnapshots(pipeline.getAccount());
    const wallet = pipeline.getAccount();
    if (wallet) {
      walletStore.upsert(
        buildWalletRecordFromState({
          wallet,
          state: pipeline.getState(),
        }),
        { force: true }
      );
      if (walletIndexer && typeof walletIndexer.addWallets === "function") {
        walletIndexer.addWallets([wallet], "tracked_account");
      }
    }
    liveHost.setFocusSymbols(result.focusSymbols || []);
    return result;
  }

  async function onAccountChanged(nextAccount) {
    pipeline.setAccount(nextAccount || null);
    liveHost.setAccount(nextAccount || null);
    if (nextAccount && walletIndexer && typeof walletIndexer.addWallets === "function") {
      walletIndexer.addWallets([nextAccount], "ui_account");
    }
    return refreshSnapshots();
  }

  let generalDataComponent = null;
  function getGeneralDataComponent() {
    if (generalDataComponent) return generalDataComponent;
    if (MINIMAL_UI_SERVER) return null;
    ensureUiComponentFactories();
    generalDataComponent = createGeneralDataComponent({
      sendJson,
      pipeline,
      restClient,
      clockSync,
      rateGuard,
      readJsonBody,
      onAccountChanged,
      refreshSnapshots,
      liveHost,
      walletIndexer,
      onchainDiscovery,
      getEgressUsage: () => buildEgressUsageSnapshot(indexerRestClientEntries),
      getGlobalKpiState,
      refreshGlobalKpi: GLOBAL_KPI_ENABLED ? refreshGlobalKpi : null,
    });
    return generalDataComponent;
  }

  let runtimeLoopStates = {};
  const nextgenRuntime = createNextgenRuntime({
    restClient,
    logger: console,
  });

  let walletTrackingComponent = null;
  function getWalletTrackingComponent() {
    if (MINIMAL_UI_SERVER) return null;
    if (walletTrackingComponent) return walletTrackingComponent;
    ensureUiComponentFactories();
    walletTrackingComponent = createWalletTrackingComponent({
      sendJson,
      pipeline,
      walletStore,
      walletIndexer,
      onchainDiscovery,
      restClient,
      solanaLogTriggerMonitor,
      walletMetricsHistoryStore,
      positionLifecycleStore,
      liveRestClientEntries: indexerRestClientEntries.map((entry) => ({
        id: entry.id,
        client: entry.client,
        proxyUrl: entry.proxyUrl || null,
      })),
      globalKpiProvider: getGlobalKpiState,
      runtimeStatusDir: RUNTIME_STATUS_DIR,
      runtimeStatusStaleMs: RUNTIME_STATUS_STALE_MS,
      runtimeStatusProvider: LOCAL_RUNTIME_STATUS_PUBLISH_ENABLED
        ? () => buildLocalRuntimeSnapshot(runtimeLoopStates)
        : null,
      startupStatusProvider: () => getStartupStatus(),
    });
    return walletTrackingComponent;
  }

  let walletTrackingNextgenComponent = null;
  function getWalletTrackingNextgenComponent() {
    if (MINIMAL_UI_SERVER) return null;
    if (walletTrackingNextgenComponent) return walletTrackingNextgenComponent;
    ensureUiComponentFactories();
    nextgenRuntime.start();
    walletTrackingNextgenComponent = createWalletTrackingNextgenComponent({
      sendJson,
      runtime: nextgenRuntime,
    });
    return walletTrackingNextgenComponent;
  }

  let creatorStudioComponent = null;
  function getCreatorStudioComponent() {
    if (creatorStudioComponent) return creatorStudioComponent;
    if (MINIMAL_UI_SERVER) return null;
    ensureUiComponentFactories();
    creatorStudioComponent = createCreatorStudioComponent({
      sendJson,
      readJsonBody,
      collectionStore,
      pipeline,
    });
    return creatorStudioComponent;
  }

  function deriveRuntimeServiceName() {
    if (INDEXER_ENABLED && INDEXER_RUNNER_ENABLED && !ONCHAIN_ENABLED && !GLOBAL_KPI_ENABLED) {
      return "indexer";
    }
    if (ONCHAIN_ENABLED && ONCHAIN_RUNNER_ENABLED && !INDEXER_ENABLED && !GLOBAL_KPI_ENABLED) {
      return "onchain";
    }
    if (GLOBAL_KPI_ENABLED && !INDEXER_ENABLED && !ONCHAIN_ENABLED) {
      return "global-kpi";
    }
    if (ENABLE_UI && !INDEXER_RUNNER_ENABLED && !ONCHAIN_RUNNER_ENABLED) {
      return "ui-api";
    }
    if (ENABLE_LIVE && !ENABLE_UI && !ENABLE_API) {
      return "live";
    }
    return SERVICE;
  }

  const runtimeStatusStore =
    RUNTIME_STATUS_ENABLED
      ? new RuntimeStatusStore({
          baseDir: RUNTIME_STATUS_DIR,
          serviceName: deriveRuntimeServiceName(),
          instanceId: `${HOST.replace(/[^a-z0-9.-]+/gi, "-")}-${PORT}`,
          staleAfterMs: RUNTIME_STATUS_STALE_MS,
          logger: console,
        })
      : null;

  function buildWalletStoreCoverageSummary() {
    const rows = walletStore && typeof walletStore.list === "function" ? walletStore.list() : [];
    let oldestProcessedAt = null;
    let newestProcessedAt = null;
    let walletsWithOpenPositions = 0;
    rows.forEach((row) => {
      const firstTrade = Number(row && row.firstTrade ? row.firstTrade : 0);
      const lastTrade = Number(
        row && (row.lastTrade || row.lastActivity || row.updatedAt)
          ? row.lastTrade || row.lastActivity || row.updatedAt
          : 0
      );
      if (firstTrade > 0 && (!oldestProcessedAt || firstTrade < oldestProcessedAt)) {
        oldestProcessedAt = firstTrade;
      }
      if (lastTrade > 0 && (!newestProcessedAt || lastTrade > newestProcessedAt)) {
        newestProcessedAt = lastTrade;
      }
      if (Number(row && row.openPositions ? row.openPositions : 0) > 0) {
        walletsWithOpenPositions += 1;
      }
    });
    return {
      totalWalletRows: rows.length,
      walletsWithOpenPositions,
      oldestProcessedAt,
      newestProcessedAt,
      updatedAt:
        walletStore && walletStore.data ? Number(walletStore.data.updatedAt || 0) || null : null,
    };
  }

  function buildGlobalKpiProgressSummary(globalState = null) {
    const state = globalState && typeof globalState === "object" ? globalState : null;
    const meta = state && state.volumeMeta && typeof state.volumeMeta === "object"
      ? state.volumeMeta
      : {};
    const history = state && state.history && typeof state.history === "object" ? state.history : {};
    const dailyByDate =
      history.dailyByDate && typeof history.dailyByDate === "object" ? history.dailyByDate : {};
    const dailyRows = Object.values(dailyByDate);
    const symbolRollupCompleteDays = dailyRows.filter((row) => {
      const safe = row && typeof row === "object" ? row : {};
      return (
        safe.symbolVolumes &&
        typeof safe.symbolVolumes === "object" &&
        Number(safe.failedSymbols || 0) <= 0
      );
    }).length;
    const totalDaysToToday = Number(
      meta.totalDaysToToday !== undefined ? meta.totalDaysToToday : history.totalDays || 0
    );
    const processedDays = Number(
      meta.processedDays !== undefined ? meta.processedDays : history.processedDays || 0
    );
    const pct =
      totalDaysToToday > 0
        ? Number(((processedDays / totalDaysToToday) * 100).toFixed(2))
        : null;
    return {
      startDate: meta.trackingStartDate || history.startDate || null,
      currentDate: meta.lastProcessedDate || history.lastProcessedDate || null,
      nextDate: meta.nextUnprocessedDate || history.nextUnprocessedDate || null,
      endDate: meta.todayUtcDate || history.todayUtcDate || formatUtcDate(Date.now()),
      percentComplete: pct,
      daysProcessed: processedDays,
      daysRemaining:
        meta.remainingDaysToToday !== undefined ? Number(meta.remainingDaysToToday) : null,
      backfillComplete:
        meta.backfillComplete !== undefined ? Boolean(meta.backfillComplete) : null,
      symbolRollupCompleteDays,
      symbolRollupPendingDays:
        totalDaysToToday > 0 ? Math.max(0, totalDaysToToday - symbolRollupCompleteDays) : null,
      totalFeesStatus: null,
      totalVolumeStatus:
        meta.backfillComplete !== undefined
          ? Boolean(meta.backfillComplete)
            ? "complete"
            : "partial"
          : "unknown",
      lastSuccessfulAt: Number(state && state.updatedAt ? state.updatedAt : state && state.fetchedAt ? state.fetchedAt : 0) || null,
    };
  }

  function buildSolanaDiscoveryTimeline(onchainStatus = null) {
    const progress =
      onchainStatus && onchainStatus.progress && typeof onchainStatus.progress === "object"
        ? onchainStatus.progress
        : null;
    if (!progress) return null;
    const programRows =
      onchainStatus && Array.isArray(onchainStatus.programs) ? onchainStatus.programs : [];
    const oldestProgramTimeMs = programRows
      .map((row) => Number((row && row.firstBlockTimeMs) || 0))
      .filter((value) => Number.isFinite(value) && value > 0)
      .sort((a, b) => a - b)[0] || null;
    const latestProgramTimeMs = programRows
      .map((row) => Number((row && row.lastBlockTimeMs) || 0))
      .filter((value) => Number.isFinite(value) && value > 0)
      .sort((a, b) => b - a)[0] || null;
    const currentTimeMs =
      Number(
        (progress.cursor && progress.cursor.blockTimeMs) ||
          progress.oldestReached ||
          oldestProgramTimeMs ||
          progress.latestReached ||
          0
      ) || null;
    const percentComplete = Number.isFinite(Number(progress.pct))
      ? Number(progress.pct)
      : null;
    const remainingMs = Number.isFinite(Number(progress.remainingMs))
      ? Number(progress.remainingMs)
      : null;
    return {
      timelineId: "solana_deposit_discovery",
      label: "Solana Deposit Discovery",
      currentDayLabel: "discovery_cursor",
      startTimeMs: Number(progress.startTimeMs || onchainStatus.startTimeMs || 0) || null,
      currentTimeMs,
      endTimeMs:
        Number(progress.latestReached || latestProgramTimeMs || onchainStatus.endTimeMs || 0) || null,
      remainingMs:
        remainingMs !== null
          ? remainingMs
          : Number(percentComplete || 0) >= 100
          ? 0
          : null,
      percentComplete,
      etaMs: Number(onchainStatus && onchainStatus.etaMs ? onchainStatus.etaMs : 0) || null,
      mode: onchainStatus && onchainStatus.mode ? onchainStatus.mode : progress.mode || null,
      completed:
        Number(percentComplete || 0) >= 100 &&
        Number(progress.pendingTransactions || 0) <= 0 &&
        Boolean(progress.historyExhausted),
      exactness: "exact",
      method: "direct_deposit_vault_history_scan",
    };
  }

  function buildWalletHistoryTimeline(indexerStatus = null, walletCoverage = null, onchainStatus = null) {
    const coverage =
      indexerStatus && indexerStatus.coverage && typeof indexerStatus.coverage === "object"
        ? indexerStatus.coverage
        : walletCoverage && typeof walletCoverage === "object"
        ? walletCoverage
        : {};
    const lifecycle =
      indexerStatus && indexerStatus.lifecycle && typeof indexerStatus.lifecycle === "object"
        ? indexerStatus.lifecycle
        : {};
    const startTimeMs =
      Number(
        coverage.referenceStartTimeMs ||
          coverage.oldestProcessedAt ||
          (onchainStatus && onchainStatus.firstDepositAtMs) ||
          0
      ) || null;
    const rawCurrentTimeMs =
      Number(coverage.frontierTimeMs || coverage.oldestProcessedAt || 0) || null;
    const endTimeMs =
      Number(coverage.newestProcessedAt || (walletCoverage && walletCoverage.newestProcessedAt) || 0) ||
      Date.now();
    const percentComplete = Number.isFinite(
      Number(indexerStatus && indexerStatus.completionPct !== undefined ? indexerStatus.completionPct : NaN)
    )
      ? Number(indexerStatus.completionPct)
      : null;
    const currentTimeMs =
      rawCurrentTimeMs || (Number(percentComplete || 0) < 100 ? endTimeMs : null);
    const backlogWallets = Number((indexerStatus && indexerStatus.walletBacklog) || 0);
    const completed =
      Number(percentComplete || 0) >= 100 && backlogWallets <= 0;
    const derivedRemainingMs =
      !completed &&
      Number.isFinite(startTimeMs) &&
      Number.isFinite(currentTimeMs) &&
      currentTimeMs > startTimeMs
        ? Math.max(0, currentTimeMs - startTimeMs)
        : null;
    const remainingMs = completed
      ? 0
      : Number.isFinite(Number(coverage.remainingHistoryMs)) && Number(coverage.remainingHistoryMs) > 0
      ? Number(coverage.remainingHistoryMs)
      : derivedRemainingMs;

    return {
      timelineId: "pacifica_wallet_history",
      label: "Pacifica Wallet History",
      currentDayLabel: "wallet_history_frontier",
      startTimeMs,
      currentTimeMs,
      endTimeMs,
      remainingMs,
      percentComplete,
      mode: completed ? "live" : "catch_up",
      completed,
      exactness:
        String(coverage.frontierMethod || "").trim().toLowerCase() === "history_complete"
          ? "exact"
          : "derived",
      method: coverage.frontierMethod || "earliest_loaded_trade_among_incomplete_wallets",
      frontierWallet: coverage.frontierWallet || null,
      walletCounts: {
        activationOnly: Number(coverage.activationOnlyWallets || 0),
        partialHistory: Number(coverage.partialHistoryWallets || 0),
        recentHistoryOnly: Number(coverage.recentHistoryWallets || 0),
        historyComplete: Number(lifecycle.backfillComplete || coverage.completeWallets || 0),
        trackedLite: Number(lifecycle.liveTrackingPartial || 0),
      },
    };
  }

  function buildStorageSummary({
    datasets = null,
    indexerStatus = null,
    onchainStatus = null,
    walletCoverage = null,
    walletMetricsHistoryStatus = null,
    positionLifecycleStatus = null,
  } = {}) {
    const safeDatasets = datasets && typeof datasets === "object" ? datasets : {};
    return {
      model: "durable_file_backed_materialized_state",
      durability: "persistent",
      checkpoints: {
        discoveryMode: onchainStatus && onchainStatus.mode ? onchainStatus.mode : null,
        walletHistoryCompletionPct:
          Number(indexerStatus && indexerStatus.completionPct !== undefined ? indexerStatus.completionPct : 0) ||
          0,
        exchangeOverviewDataset: safeDatasets.globalKpi || null,
      },
      wallets: {
        discovered: Number((indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.discovered) || 0),
        tracked: Number((indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.liveTracking) || 0),
        walletRowsPersisted: Number((walletCoverage && walletCoverage.totalWalletRows) || 0),
        explorerStore: safeDatasets.walletExplorer || null,
        historyStore: {
          path: path.join(INDEXER_DATA_DIR, "wallet_history"),
          exists: fs.existsSync(path.join(INDEXER_DATA_DIR, "wallet_history")),
          persistedWallets: Number((indexerStatus && indexerStatus.knownWallets) || 0),
          historyCompleteWallets: Number(
            (indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.backfillComplete) || 0
          ),
          updatedAt: Math.max(
            Number((safeDatasets.walletExplorer && safeDatasets.walletExplorer.updatedAt) || 0),
            Number((safeDatasets.walletIndexerState && safeDatasets.walletIndexerState.updatedAt) || 0)
          ) || null,
        },
      },
      materializedViews: {
        walletMetricsHistory: walletMetricsHistoryStatus || null,
        positionLifecycle: positionLifecycleStatus || null,
      },
      files: safeDatasets,
    };
  }

  function buildSystemMode(indexerStatus = null, onchainStatus = null, globalProgress = null) {
    const onchainPct = Number(
      onchainStatus && onchainStatus.progress && onchainStatus.progress.pct !== undefined
        ? onchainStatus.progress.pct
        : NaN
    );
    const walletPct = Number(indexerStatus && indexerStatus.completionPct !== undefined ? indexerStatus.completionPct : NaN);
    const globalPct = Number(globalProgress && globalProgress.percentComplete !== undefined ? globalProgress.percentComplete : NaN);
    const anyIncomplete =
      (Number.isFinite(onchainPct) && onchainPct < 100) ||
      (Number.isFinite(walletPct) && walletPct < 100) ||
      (Number.isFinite(globalPct) && globalPct < 100);
    if (
      anyIncomplete &&
      ((Number.isFinite(onchainPct) && onchainPct < 15) ||
        (Number.isFinite(walletPct) && walletPct < 15))
    ) {
      return "bootstrap";
    }
    if (anyIncomplete) return "catch_up";
    return "maintenance";
  }

  function buildLocalRuntimeSnapshot(loopStates = {}) {
    const now = Date.now();
    const indexerStatus =
      walletIndexer && typeof walletIndexer.getStatus === "function"
        ? walletIndexer.getStatus()
        : null;
    const onchainStatus =
      onchainDiscovery && typeof onchainDiscovery.getStatus === "function"
        ? onchainDiscovery.getStatus()
        : null;
    const globalState = getGlobalKpiState();
    const globalProgress = buildGlobalKpiProgressSummary(globalState);
    const trackingComponent = walletTrackingComponent;
    const liveSnapshot =
      trackingComponent &&
      typeof trackingComponent.getLiveWalletSnapshot === "function"
        ? trackingComponent.getLiveWalletSnapshot(100)
        : null;
    const liveStatus =
      liveSnapshot && liveSnapshot.status && typeof liveSnapshot.status === "object"
        ? liveSnapshot.status
        : null;
    const egress = buildEgressUsageSnapshot(indexerRestClientEntries);
    const restRate = rateGuard.getState();
    const solanaRpcStats =
      solanaRpcClient && typeof solanaRpcClient.getStats === "function"
        ? solanaRpcClient.getStats()
        : null;
    const walletCoverage = buildWalletStoreCoverageSummary();
    const discoveryTimeline = buildSolanaDiscoveryTimeline(onchainStatus);
    const walletHistoryTimeline = buildWalletHistoryTimeline(
      indexerStatus,
      walletCoverage,
      onchainStatus
    );
    const walletMetricsHistoryStatus =
      walletMetricsHistoryStore &&
      typeof walletMetricsHistoryStore.getStatus === "function"
        ? walletMetricsHistoryStore.getStatus()
        : null;
    const positionLifecycleStatus =
      positionLifecycleStore &&
      typeof positionLifecycleStore.getStatus === "function"
        ? positionLifecycleStore.getStatus()
        : null;
    const datasets = {
      walletExplorer: describeFileDataset(path.join(INDEXER_DATA_DIR, "wallets.json")),
      walletIndexerState: describeFileDataset(path.join(INDEXER_DATA_DIR, "indexer_state.json")),
      walletDiscoveryState: describeFileDataset(path.join(INDEXER_DATA_DIR, "wallet_discovery.json")),
      onchainState: describeFileDataset(path.join(INDEXER_DATA_DIR, "onchain_state.json")),
      depositWallets: describeFileDataset(DEPOSIT_WALLETS_PATH),
      globalKpi: describeFileDataset(GLOBAL_KPI_PATH),
      pipelineState: describeFileDataset(path.join(PIPELINE_DATA_DIR, "snapshots", "state.json")),
      walletMetricsHistory: {
        path: walletMetricsHistoryStore ? walletMetricsHistoryStore.baseDir : null,
        exists: Boolean(walletMetricsHistoryStore && fs.existsSync(walletMetricsHistoryStore.baseDir)),
        ...(walletMetricsHistoryStatus && typeof walletMetricsHistoryStatus === "object"
          ? walletMetricsHistoryStatus
          : {}),
      },
      positionLifecycle: {
        path: positionLifecycleStore ? positionLifecycleStore.filePath : null,
        exists: Boolean(positionLifecycleStore && fs.existsSync(positionLifecycleStore.filePath)),
        ...(positionLifecycleStatus && typeof positionLifecycleStatus === "object"
          ? positionLifecycleStatus
          : {}),
      },
    };
    const walletProgress = {
      discovered: Math.max(
        Number((indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.discovered) || 0),
        Number((onchainStatus && onchainStatus.walletCount) || 0)
      ),
      validated: Number((onchainStatus && onchainStatus.confirmedWallets) || 0),
      backfilled: Number((indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.backfillComplete) || 0),
      currentlyTracked: Number((indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.liveTracking) || 0),
      trackedLite: Number(
        (indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.liveTrackingPartial) || 0
      ),
      trackedFull: Math.max(
        0,
        Number((indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.liveTracking) || 0) -
          Number(
            (indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.liveTrackingPartial) || 0
          )
      ),
      tradeHistoryComplete: Number(
        (indexerStatus && indexerStatus.lifecycle && indexerStatus.lifecycle.backfillComplete) || 0
      ),
      pending: Number((indexerStatus && indexerStatus.walletBacklog) || 0),
      discoveryMetricType: "solana_deposit_vault_history",
      oldestProcessedAt: walletCoverage.oldestProcessedAt,
      newestProcessedAt: walletCoverage.newestProcessedAt,
      discoveryWindow: discoveryTimeline,
      solanaDiscoveryTimeline: discoveryTimeline,
      walletHistoryTimeline,
    };
    const exchangeProgress = {
      ...globalProgress,
      timelineId: "exchange_overview_historical_rollup",
      currentDayLabel: "exchange_rollup_day",
      completed: Boolean(globalProgress && globalProgress.backfillComplete),
      exactness: "exact",
      method: "daily_exchange_rollup",
      totalFeesStatus:
        indexerStatus && Number(indexerStatus.completionPct || 0) >= 99.9
          ? "complete"
          : indexerStatus
          ? "partial"
          : "unknown",
      feesPartial:
        indexerStatus ? Number(indexerStatus.completionPct || 0) < 99.9 : true,
    };
    const mode = buildSystemMode(indexerStatus, onchainStatus, globalProgress);
    const systemSummary = {
      mode,
      activeShardCount:
        indexerStatus && indexerStatus.restClients && Array.isArray(indexerStatus.restClients.shards)
          ? indexerStatus.restClients.shards.length
          : 0,
      proxyHealth: {
        total: Number(egress.clients || 0),
        active: Number(egress.active || 0),
        cooling: Array.isArray(egress.rows)
          ? egress.rows.filter((row) => Number(row.pauseRemainingMs || 0) > 0).length
          : 0,
      },
      heliusHealth: (() => {
        const shards =
          solanaRpcStats && Array.isArray(solanaRpcStats.shards) ? solanaRpcStats.shards : [];
        return {
          total: shards.length,
          active: shards.filter((row) => row && row.stats).length,
          rate429PerMin: shards.reduce(
            (sum, row) => sum + Number((row && row.stats && row.stats.rate429PerMin) || 0),
            0
          ),
        };
      })(),
      restPace: {
        used1m: Number(restRate.used1m || 0),
        rpmCap: Number(restRate.rpmCap || 0),
        headroomPct:
          Number(restRate.rpmCap || 0) > 0
            ? Number(
                (
                  (1 - Number(restRate.used1m || 0) / Math.max(1, Number(restRate.rpmCap || 0))) *
                  100
                ).toFixed(2)
              )
            : null,
      },
      wsHealth: liveStatus
        ? {
            enabled: Boolean(liveStatus.enabled),
            activeWallets: Number(liveStatus.hotWsActiveWallets || 0),
            openConnections: Number(liveStatus.hotWsOpenConnections || 0),
            promotionBacklog: Number(liveStatus.hotWsPromotionBacklog || 0),
          }
        : { enabled: false },
    };
    const storageSummary = buildStorageSummary({
      datasets,
      indexerStatus,
      onchainStatus,
      walletCoverage,
      walletMetricsHistoryStatus,
      positionLifecycleStatus,
    });

    return {
      startup: getStartupStatus(),
      service: {
        logicalName: deriveRuntimeServiceName(),
        mode: SERVICE,
        host: HOST,
        port: PORT,
        enableApi: ENABLE_API,
        enableUi: ENABLE_UI,
        enableLive: ENABLE_LIVE,
      },
      summary: {
        systemMode: mode,
        walletExplorerProgress: walletProgress,
        exchangeOverviewProgress: exchangeProgress,
        systemStatus: systemSummary,
        storage: storageSummary,
        dataFreshness: {
          lastSuccessfulSyncAt: Number((indexerStatus && indexerStatus.lastScanAt) || 0) || null,
          lastSuccessfulWalletDiscoveryAt:
            Number((onchainStatus && onchainStatus.lastScanAt) || (indexerStatus && indexerStatus.lastDiscoveryAt) || 0) ||
            null,
          lastSuccessfulExchangeRollupAt: globalProgress.lastSuccessfulAt,
          lagBehindNowMs:
            Number((globalProgress && globalProgress.lastSuccessfulAt) || 0) > 0
              ? Math.max(0, now - Number(globalProgress.lastSuccessfulAt || 0))
              : null,
        },
      },
      workers: {
        indexer: indexerStatus,
        onchain: onchainStatus,
        globalKpi:
          globalState && typeof globalState === "object"
            ? {
                updatedAt: Number(globalState.updatedAt || globalState.fetchedAt || 0) || null,
                progress: globalProgress,
              }
            : null,
        live: liveStatus,
      },
      transport: {
        pacificaApi: restRate,
        pacificaEgress: egress,
        solanaRpc: solanaRpcStats,
      },
      schedulers: loopStates,
      datasets,
    };
  }

  pipeline.setTransportState({
    apiBase: API_BASE,
    wsUrl: WS_URL,
    wsStatus: WS_ENABLED && ENABLE_LIVE ? "connecting" : "disabled",
  });

  const server = http.createServer(async (req, res) => {
    res.__pfRequest = req;
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    try {
      if (serveStatic(req, res, url)) return;

      if (ENABLE_API && url.pathname.startsWith("/api/auth")) {
        const authStore = loadCopyTradingStore();
        if (req.method === "GET" && url.pathname === "/api/auth/access") {
          const allowed = isAuthRequestIpAllowed(req);
          sendJson(res, 200, {
            ok: true,
            allowed,
            reason: allowed ? null : "account access is restricted to an approved IP",
          });
          return;
        }
        if (
          !isAuthRequestIpAllowed(req) &&
          url.pathname !== "/api/auth/access" &&
          url.pathname !== "/api/auth/twitter/status"
        ) {
          sendJson(res, 403, {
            ok: false,
            error: "account access is restricted to an approved IP",
          });
          return;
        }
        if (req.method === "GET" && url.pathname === "/api/auth/twitter/status") {
          sendJson(res, 200, {
            ok: true,
            configured: isTwitterOAuthConfigured(),
          });
          return;
        }

        if (req.method === "GET" && url.pathname === "/api/auth/twitter/start") {
          if (!isTwitterOAuthConfigured()) {
            sendJson(res, 503, {
              ok: false,
              error: "twitter login is not configured on this server",
            });
            return;
          }
          const returnTo = buildTwitterReturnPath(url.searchParams.get("return") || "/");
          const stateToken = createTwitterStateRecord({ returnTo });
          const record = getTwitterStateRecord(stateToken);
          const location = buildTwitterAuthorizeUrl(stateToken, createTwitterCodeChallenge(record && record.codeVerifier ? record.codeVerifier : ""));
          res.writeHead(302, {
            Location: location,
            "Cache-Control": "no-store",
          });
          res.end();
          return;
        }

        if (req.method === "GET" && url.pathname === "/api/auth/twitter/callback") {
          if (!isTwitterOAuthConfigured()) {
            sendJson(res, 503, {
              ok: false,
              error: "twitter login is not configured on this server",
            });
            return;
          }
          const code = String(url.searchParams.get("code") || "").trim();
          const stateToken = String(url.searchParams.get("state") || "").trim();
          if (!code || !stateToken) {
            sendJson(res, 400, { ok: false, error: "missing twitter callback parameters" });
            return;
          }
          const stateRecord = getTwitterStateRecord(stateToken);
          if (!stateRecord) {
            sendJson(res, 410, { ok: false, error: "twitter login state expired" });
            return;
          }
          try {
            const tokenPayload = await exchangeTwitterCodeForToken(code, stateRecord.codeVerifier);
            const profile = await fetchTwitterIdentity(tokenPayload && tokenPayload.access_token ? tokenPayload.access_token : "");
            const twitterId = String(profile && profile.id ? profile.id : "").trim();
            if (!twitterId) {
              throw new Error("twitter_profile_missing_id");
            }
            const user = findOrCreateTwitterUser(profile);
            const challengeReturnTo = buildTwitterReturnPath(stateRecord.returnTo || "/");
            delete authStore.pendingTwitterStatesByToken[stateToken];
            if (user && user.mfaEnabled && user.mfaSecret) {
              const mfaToken = crypto.randomUUID().replace(/-/g, "");
              authStore.pendingSigninChallengesByToken[mfaToken] = {
                token: mfaToken,
                userId: user.id,
                createdAt: Date.now(),
                expiresAt: Date.now() + 1000 * 60 * 10,
                attempts: 0,
                provider: "twitter",
                label: user.displayName || user.twitterUsername || user.email || "Twitter account",
                returnTo: challengeReturnTo,
              };
              saveCopyTradingStore();
              const redirectTo = new URL(challengeReturnTo, "http://localhost");
              redirectTo.searchParams.set("auth", "twitter-mfa");
              redirectTo.searchParams.set("mfaToken", mfaToken);
              redirectTo.searchParams.set("label", user.displayName || user.twitterUsername || user.email || "Twitter account");
              res.writeHead(302, {
                Location: `${redirectTo.pathname}${redirectTo.search}${redirectTo.hash}`,
                "Cache-Control": "no-store",
              });
              res.end();
              return;
            }
            const token = crypto.randomUUID().replace(/-/g, "");
            const expiresAt = Date.now() + 1000 * 60 * 60 * 24 * 30;
            authStore.sessionsByToken[token] = {
              token,
              userId: user.id,
              createdAt: Date.now(),
              expiresAt,
            };
            saveCopyTradingStore();
            const fallbackReturn = user.mfaEnabled ? challengeReturnTo : "/settings/?welcome=1";
            const redirectUrl = new URL(fallbackReturn, "http://localhost");
            res.writeHead(302, {
              Location: `${redirectUrl.pathname}${redirectUrl.search}${redirectUrl.hash}`,
              "Set-Cookie": buildCookie("pf_session", token, {
                maxAge: expiresAt - Date.now(),
                httpOnly: true,
                path: "/",
                secure: false,
              }),
              "Cache-Control": "no-store",
            });
            res.end();
            return;
          } catch (error) {
            delete authStore.pendingTwitterStatesByToken[stateToken];
            saveCopyTradingStore();
            sendJson(res, 503, {
              ok: false,
              error:
                error && error.message
                    ? String(error.message)
                    : "twitter login failed",
            });
            return;
          }
        }

        if (req.method === "GET" && url.pathname === "/api/auth/me") {
          const session = getAuthSessionFromRequest(req);
          if (!session) {
            sendJson(res, 200, { ok: true, authenticated: false, user: null });
            return;
          }
          const leaders = getLeaderWalletsForUser(session.user.id);
          sendJson(res, 200, {
            ok: true,
            authenticated: true,
            user: serializeAuthUser(session.user),
            copyTradingEligible: Boolean(session.user.mfaEnabled),
            leaders: leaders.map((row) => ({
              wallet: row.wallet,
              addedAt: row.addedAt,
              note: row.note || "",
            })),
          });
          return;
        }

        if (req.method === "GET" && url.pathname === "/api/auth/settings") {
          const session = getAuthSessionFromRequest(req);
          if (!session) {
            sendJson(res, 401, { ok: false, error: "sign in required" });
            return;
          }
          sendJson(res, 200, {
            ok: true,
            user: serializeAuthUser(session.user),
            copyTradingEligible: Boolean(session.user.mfaEnabled),
            apiKeys: getApiKeysForUser(session.user.id).map(serializeApiKeyRecord).filter(Boolean),
          });
          return;
        }

        if (req.method === "GET" && url.pathname === "/api/auth/api-keys") {
          const session = getAuthSessionFromRequest(req);
          if (!session) {
            sendJson(res, 401, { ok: false, error: "sign in required" });
            return;
          }
          sendJson(res, 200, {
            ok: true,
            apiKeys: getApiKeysForUser(session.user.id).map(serializeApiKeyRecord).filter(Boolean),
          });
          return;
        }

        if ((req.method === "GET" || req.method === "POST") && /^\/api\/auth\/api-keys\/[^/]+\/validate$/i.test(url.pathname)) {
          const session = getAuthSessionFromRequest(req);
          if (!session) {
            sendJson(res, 401, { ok: false, error: "sign in required" });
            return;
          }
          if (!session.user || !session.user.mfaEnabled || !session.user.mfaSecret) {
            sendJson(res, 403, { ok: false, error: "google authenticator required" });
            return;
          }
          const keyId = String(url.pathname.replace(/^\/api\/auth\/api-keys\//i, "").replace(/\/validate$/i, "")).trim();
          const store = loadCopyTradingStore();
          const rawRows = Array.isArray(store.apiKeysByUserId && store.apiKeysByUserId[session.user.id])
            ? store.apiKeysByUserId[session.user.id]
            : [];
          const storedRow = rawRows.find((row) => String(row.id || "") === keyId) || null;
          const accountWallet = String(storedRow && (storedRow.accountWallet || storedRow.account || "")).trim();
          const agentWallet = String(storedRow && (storedRow.agentWallet || storedRow.apiKey || "")).trim();
          const agentPrivateKey = String(storedRow && (storedRow.agentPrivateKey || storedRow.apiSecret || "")).trim();
          if (!storedRow || !accountWallet || !agentWallet || !agentPrivateKey) {
            sendJson(res, 404, { ok: false, error: "pacifica access set not found" });
            return;
          }
          try {
            const signed = createPacificaSignedRequest({
              accountWallet,
              agentWallet,
              agentPrivateKey,
              type: "list_api_keys",
              operationData: null,
              expiryWindow: 5000,
            });
            const accountRes = await restClient.get("/account", {
              query: { account: accountWallet },
              timeoutMs: 15_000,
              retryMaxAttempts: 2,
            });
            const signedListRes = await restClient.post("/account/api_keys", {
              timeoutMs: 15_000,
              retryMaxAttempts: 2,
              body: signed.body,
            });
            sendJson(res, 200, {
              ok: true,
              accountWallet,
              agentWallet,
              derivedAgentWallet: signed.derivedAgentWallet,
              account: extractPayloadData(accountRes, null),
              apiKeys: extractPayloadData(signedListRes, []),
              method: req.method,
            });
          } catch (error) {
            const status = Number(error && error.status ? error.status : 0);
            sendJson(
              res,
              status >= 400 && status < 600 ? status : 500,
              {
                ok: false,
                error: error && error.message ? error.message : "pacifica_validation_failed",
              }
            );
          }
          return;
        }

        if (req.method === "POST" && url.pathname === "/api/auth/profile") {
          const session = getAuthSessionFromRequest(req);
          if (!session) {
            sendJson(res, 401, { ok: false, error: "sign in required" });
            return;
          }
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const current = getUserById(session.user.id);
          if (!current) {
            sendJson(res, 404, { ok: false, error: "account not found" });
            return;
          }
          const nextDisplayName = normalizeAuthDisplayName(body && body.displayName, current.email);
          current.displayName = nextDisplayName;
          current.updatedAt = Date.now();
          saveCopyTradingStore();
          sendJson(res, 200, {
            ok: true,
            user: serializeAuthUser(current),
          });
          return;
        }

        if (
          req.method === "POST" &&
          (url.pathname === "/api/auth/signup" ||
            url.pathname === "/api/auth/signup/verify" ||
            url.pathname === "/api/auth/signup/resend" ||
            url.pathname === "/api/auth/signin" ||
            url.pathname === "/api/auth/signin/verify" ||
            url.pathname === "/api/auth/mfa/setup" ||
            url.pathname === "/api/auth/mfa/verify" ||
            url.pathname === "/api/auth/mfa/disable" ||
            url.pathname === "/api/auth/mfa/backup-codes" ||
            url.pathname === "/api/auth/api-keys")
        ) {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          if (url.pathname === "/api/auth/api-keys") {
            const session = getAuthSessionFromRequest(req);
            if (!session) {
              sendJson(res, 401, { ok: false, error: "sign in required" });
              return;
            }
            const label = normalizeApiKeyLabel(body && body.label) || "Pacifica access";
            const accountWallet = normalizeApiKeyValue(body && (body.accountWallet || body.account || ""));
            const agentWallet = normalizeApiKeyValue(body && (body.agentWallet || body.apiKey || ""));
            const agentPrivateKey = normalizeApiKeyValue(body && (body.agentPrivateKey || body.apiSecret || ""));
            if (accountWallet.length < 8) {
              sendJson(res, 400, { ok: false, error: "account wallet required" });
              return;
            }
            if (agentWallet.length < 8) {
              sendJson(res, 400, { ok: false, error: "agent wallet required" });
              return;
            }
            if (agentPrivateKey.length < 8) {
              sendJson(res, 400, { ok: false, error: "agent private key required" });
              return;
            }
            const store = loadCopyTradingStore();
            if (!store.apiKeysByUserId || typeof store.apiKeysByUserId !== "object") {
              store.apiKeysByUserId = {};
            }
            const row = {
              id: crypto.randomUUID(),
              label,
              accountWallet,
              agentWallet,
              agentPrivateKey,
              apiKey: agentWallet,
              apiSecret: agentPrivateKey,
              createdAt: Date.now(),
              updatedAt: Date.now(),
            };
            const next = Array.isArray(store.apiKeysByUserId[session.user.id])
              ? store.apiKeysByUserId[session.user.id].slice()
              : [];
            next.unshift(row);
            store.apiKeysByUserId[session.user.id] = next;
            saveCopyTradingStore();
            sendJson(res, 200, {
              ok: true,
              apiKeys: next.map(serializeApiKeyRecord).filter(Boolean),
            });
            return;
          }
          if (url.pathname === "/api/auth/signin/verify") {
            const bodyCode = String(body && body.code ? body.code : "").trim();
            const mfaToken = String(body && body.mfaToken ? body.mfaToken : "").trim();
            const challenge = mfaToken
              ? authStore.pendingSigninChallengesByToken && authStore.pendingSigninChallengesByToken[mfaToken]
              : null;
            if (!challenge) {
              sendJson(res, 404, { ok: false, error: "mfa challenge not found" });
              return;
            }
            if (Number(challenge.expiresAt || 0) <= Date.now()) {
              delete authStore.pendingSigninChallengesByToken[mfaToken];
              saveCopyTradingStore();
              sendJson(res, 410, { ok: false, error: "mfa challenge expired" });
              return;
            }
            const challengeUser = getUserById(challenge.userId);
            if (!challengeUser || !challengeUser.mfaEnabled || !challengeUser.mfaSecret) {
              delete authStore.pendingSigninChallengesByToken[mfaToken];
              saveCopyTradingStore();
              sendJson(res, 400, { ok: false, error: "mfa not enabled for this account" });
              return;
            }
            if (!verifyMfaCodeOrBackup(challengeUser, bodyCode)) {
              challenge.attempts = Math.max(0, Number(challenge.attempts || 0)) + 1;
              saveCopyTradingStore();
              sendJson(res, 401, { ok: false, error: "invalid authentication code" });
              return;
            }
            delete authStore.pendingSigninChallengesByToken[mfaToken];
            const token = crypto.randomUUID().replace(/-/g, "");
            const expiresAt = Date.now() + 1000 * 60 * 60 * 24 * 30;
            authStore.sessionsByToken[token] = {
              token,
              userId: challengeUser.id,
              createdAt: Date.now(),
              expiresAt,
            };
            saveCopyTradingStore();
            sendJson(
              res,
              200,
              {
                ok: true,
                authenticated: true,
                mfaVerified: true,
                user: serializeAuthUser(challengeUser),
              },
              {
                "Set-Cookie": buildCookie("pf_session", token, {
                  maxAge: expiresAt - Date.now(),
                  httpOnly: true,
                  path: "/",
                  secure: false,
                }),
              }
            );
            return;
          }
          if (
            url.pathname === "/api/auth/mfa/setup" ||
            url.pathname === "/api/auth/mfa/verify" ||
            url.pathname === "/api/auth/mfa/disable" ||
            url.pathname === "/api/auth/mfa/backup-codes"
          ) {
            const session = getAuthSessionFromRequest(req);
            if (!session) {
              sendJson(res, 401, { ok: false, error: "sign in required" });
              return;
            }
            const user = session.user;
            if (url.pathname === "/api/auth/mfa/setup") {
              const existingPending =
                authStore.pendingMfaSetupsByUserId && authStore.pendingMfaSetupsByUserId[user.id]
                  ? authStore.pendingMfaSetupsByUserId[user.id]
                  : null;
              const pending =
                existingPending && Number(existingPending.expiresAt || 0) > Date.now()
                  ? existingPending
                  : null;
              if (user.mfaEnabled && user.mfaSecret) {
                const qrDataUrl = await buildMfaQrDataUrl(
                  buildMfaOtpAuthUrl("PacificaFlow", user.displayName || user.email, user.mfaSecret)
                );
                sendJson(res, 200, {
                  ok: true,
                  mfaEnabled: true,
                  issuer: "PacificaFlow",
                  accountName: user.displayName || user.email,
                  secret: null,
                  qrDataUrl,
                  otpauthUrl: buildMfaOtpAuthUrl("PacificaFlow", user.displayName || user.email, user.mfaSecret),
                  mfaEnabledAt: user.mfaEnabledAt || null,
                  user: serializeAuthUser(user),
                });
                return;
              }
              const setup = pending || (() => {
                const next = createMfaSecret(user.email, user.displayName || user.email);
                authStore.pendingMfaSetupsByUserId[user.id] = {
                  secret: next.secret,
                  otpauthUrl: next.otpauth,
                  issuer: next.issuer,
                  accountName: next.accountName,
                  createdAt: Date.now(),
                  expiresAt: Date.now() + 1000 * 60 * 15,
                };
                return authStore.pendingMfaSetupsByUserId[user.id];
              })();
              const qrDataUrl = await buildMfaQrDataUrl(setup.otpauthUrl);
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                mfaEnabled: false,
                issuer: setup.issuer,
                accountName: setup.accountName,
                secret: setup.secret,
                qrDataUrl,
                otpauthUrl: setup.otpauthUrl,
                expiresAt: setup.expiresAt,
                user: serializeAuthUser(user),
              });
              return;
            }
            if (url.pathname === "/api/auth/mfa/verify") {
              const code = String(body && body.code ? body.code : "").trim();
              const pendingSetup = authStore.pendingMfaSetupsByUserId && authStore.pendingMfaSetupsByUserId[user.id];
              if (!pendingSetup) {
                sendJson(res, 404, { ok: false, error: "mfa setup not found" });
                return;
              }
              if (Number(pendingSetup.expiresAt || 0) <= Date.now()) {
                delete authStore.pendingMfaSetupsByUserId[user.id];
                saveCopyTradingStore();
                sendJson(res, 410, { ok: false, error: "mfa setup expired" });
                return;
              }
              if (!verifyTotpCode(pendingSetup.secret, code)) {
                sendJson(res, 401, { ok: false, error: "invalid authentication code" });
                return;
              }
              const current = getUserById(user.id);
              if (!current) {
                sendJson(res, 404, { ok: false, error: "account not found" });
                return;
              }
              current.mfaEnabled = true;
              current.mfaEnabledAt = Date.now();
              current.mfaSecret = pendingSetup.secret;
              const backupCodes = createMfaBackupCodes(current);
              delete authStore.pendingMfaSetupsByUserId[user.id];
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                mfaEnabled: true,
                mfaEnabledAt: current.mfaEnabledAt,
                backupCodes,
                backupCodesRemaining: getBackupCodeStats(current).remaining,
                user: serializeAuthUser(current),
              });
              return;
            }
            if (url.pathname === "/api/auth/mfa/disable") {
              const code = String(body && body.code ? body.code : "").trim();
              const current = getUserById(user.id);
              if (!current || !current.mfaEnabled || !current.mfaSecret) {
                sendJson(res, 400, { ok: false, error: "mfa is not enabled" });
                return;
              }
              if (!verifyMfaCodeOrBackup(current, code)) {
                sendJson(res, 401, { ok: false, error: "invalid authentication code" });
                return;
              }
              current.mfaEnabled = false;
              current.mfaEnabledAt = null;
              delete current.mfaSecret;
              delete current.mfaBackupCodes;
              delete current.mfaBackupCodesGeneratedAt;
              delete current.mfaBackupCodesLastUsedAt;
              if (authStore.pendingMfaSetupsByUserId && authStore.pendingMfaSetupsByUserId[user.id]) {
                delete authStore.pendingMfaSetupsByUserId[user.id];
              }
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                mfaEnabled: false,
                user: serializeAuthUser(current),
              });
              return;
            }
          if (url.pathname === "/api/auth/mfa/backup-codes") {
              const code = String(body && body.code ? body.code : "").trim();
              const current = getUserById(user.id);
              if (!current || !current.mfaEnabled || !current.mfaSecret) {
                sendJson(res, 400, { ok: false, error: "mfa is not enabled" });
                return;
              }
              if (!verifyMfaCodeOrBackup(current, code)) {
                sendJson(res, 401, { ok: false, error: "invalid authentication code" });
                return;
              }
              const backupCodes = createMfaBackupCodes(current);
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                backupCodes,
                backupCodesRemaining: getBackupCodeStats(current).remaining,
                backupCodesGeneratedAt: current.mfaBackupCodesGeneratedAt || null,
                user: serializeAuthUser(current),
              });
            return;
          }
        }

        if (req.method === "DELETE" && /^\/api\/auth\/api-keys\/[^/]+$/i.test(url.pathname)) {
          const session = getAuthSessionFromRequest(req);
          if (!session) {
            sendJson(res, 401, { ok: false, error: "sign in required" });
            return;
          }
          const keyId = String(url.pathname.replace(/^\/api\/auth\/api-keys\//i, "")).trim();
          const store = loadCopyTradingStore();
          const rows = Array.isArray(store.apiKeysByUserId && store.apiKeysByUserId[session.user.id])
            ? store.apiKeysByUserId[session.user.id]
            : [];
          const next = rows.filter((row) => String(row.id || "") !== keyId);
          store.apiKeysByUserId[session.user.id] = next;
          saveCopyTradingStore();
          sendJson(res, 200, {
            ok: true,
            apiKeys: next.map(serializeApiKeyRecord).filter(Boolean),
          });
          return;
        }
          const email = normalizeEmailAddress(body && body.email);
          const password = String(body && body.password ? body.password : "");
          const displayName = normalizeAuthDisplayName(body && body.displayName, email);
          const pending = email && authStore.pendingSignupsByEmail ? authStore.pendingSignupsByEmail[email] || null : null;
          if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
            sendJson(res, 400, { ok: false, error: "valid email required" });
            return;
          }
          if (url.pathname === "/api/auth/signup/verify" || url.pathname === "/api/auth/signup/resend") {
            if (!pending) {
              sendJson(res, 404, { ok: false, error: "pending signup not found" });
              return;
            }
          }
          if (url.pathname === "/api/auth/signin" && password.length < 8) {
            sendJson(res, 400, { ok: false, error: "password must be at least 8 characters" });
            return;
          }
          if (url.pathname === "/api/auth/signin/verify") {
            const bodyCode = String(body && body.code ? body.code : "").trim();
            const mfaToken = String(body && body.mfaToken ? body.mfaToken : "").trim();
            const challenge = mfaToken
              ? authStore.pendingSigninChallengesByToken && authStore.pendingSigninChallengesByToken[mfaToken]
              : null;
            if (!challenge) {
              sendJson(res, 404, { ok: false, error: "mfa challenge not found" });
              return;
            }
            if (Number(challenge.expiresAt || 0) <= Date.now()) {
              delete authStore.pendingSigninChallengesByToken[mfaToken];
              saveCopyTradingStore();
              sendJson(res, 410, { ok: false, error: "mfa challenge expired" });
              return;
            }
            const challengeUser = getUserById(challenge.userId);
            if (!challengeUser || !challengeUser.mfaEnabled || !challengeUser.mfaSecret) {
              delete authStore.pendingSigninChallengesByToken[mfaToken];
              saveCopyTradingStore();
              sendJson(res, 400, { ok: false, error: "mfa not enabled for this account" });
              return;
            }
            if (!verifyTotpCode(challengeUser.mfaSecret, bodyCode)) {
              challenge.attempts = Math.max(0, Number(challenge.attempts || 0)) + 1;
              saveCopyTradingStore();
              sendJson(res, 401, { ok: false, error: "invalid authentication code" });
              return;
            }
            delete authStore.pendingSigninChallengesByToken[mfaToken];
            const token = crypto.randomUUID().replace(/-/g, "");
            const expiresAt = Date.now() + 1000 * 60 * 60 * 24 * 30;
            authStore.sessionsByToken[token] = {
              token,
              userId: challengeUser.id,
              createdAt: Date.now(),
              expiresAt,
            };
            saveCopyTradingStore();
            sendJson(
              res,
              200,
              {
                ok: true,
                authenticated: true,
                mfaVerified: true,
                user: serializeAuthUser(challengeUser),
              },
              {
                "Set-Cookie": buildCookie("pf_session", token, {
                  maxAge: expiresAt - Date.now(),
                  httpOnly: true,
                  path: "/",
                  secure: false,
                }),
              }
            );
            return;
          }
          if (url.pathname === "/api/auth/mfa/setup" || url.pathname === "/api/auth/mfa/verify" || url.pathname === "/api/auth/mfa/disable") {
            const session = getAuthSessionFromRequest(req);
            if (!session) {
              sendJson(res, 401, { ok: false, error: "sign in required" });
              return;
            }
            const user = session.user;
            if (url.pathname === "/api/auth/mfa/setup") {
              const existingPending =
                authStore.pendingMfaSetupsByUserId && authStore.pendingMfaSetupsByUserId[user.id]
                  ? authStore.pendingMfaSetupsByUserId[user.id]
                  : null;
              const pending =
                existingPending && Number(existingPending.expiresAt || 0) > Date.now()
                  ? existingPending
                  : null;
              if (user.mfaEnabled && user.mfaSecret) {
                const qrDataUrl = await buildMfaQrDataUrl(
                  buildMfaOtpAuthUrl("PacificaFlow", user.displayName || user.email, user.mfaSecret)
                );
                sendJson(res, 200, {
                  ok: true,
                  mfaEnabled: true,
                  issuer: "PacificaFlow",
                  accountName: user.displayName || user.email,
                  secret: null,
                  qrDataUrl,
                  otpauthUrl: buildMfaOtpAuthUrl("PacificaFlow", user.displayName || user.email, user.mfaSecret),
                  mfaEnabledAt: user.mfaEnabledAt || null,
                  user: serializeAuthUser(user),
                });
                return;
              }
              const setup = pending || (() => {
                const next = createMfaSecret(user.email, user.displayName || user.email);
                authStore.pendingMfaSetupsByUserId[user.id] = {
                  secret: next.secret,
                  otpauthUrl: next.otpauth,
                  issuer: next.issuer,
                  accountName: next.accountName,
                  createdAt: Date.now(),
                  expiresAt: Date.now() + 1000 * 60 * 15,
                };
                return authStore.pendingMfaSetupsByUserId[user.id];
              })();
              const qrDataUrl = await buildMfaQrDataUrl(setup.otpauthUrl);
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                mfaEnabled: false,
                issuer: setup.issuer,
                accountName: setup.accountName,
                secret: setup.secret,
                qrDataUrl,
                otpauthUrl: setup.otpauthUrl,
                expiresAt: setup.expiresAt,
                user: serializeAuthUser(user),
              });
              return;
            }
            if (url.pathname === "/api/auth/mfa/verify") {
              const body = await parseJsonRequestBody(req).catch((error) => {
                throw new Error(error && error.message ? error.message : "invalid_request_body");
              });
              const code = String(body && body.code ? body.code : "").trim();
              const pendingSetup = authStore.pendingMfaSetupsByUserId && authStore.pendingMfaSetupsByUserId[user.id];
              if (!pendingSetup) {
                sendJson(res, 404, { ok: false, error: "mfa setup not found" });
                return;
              }
              if (Number(pendingSetup.expiresAt || 0) <= Date.now()) {
                delete authStore.pendingMfaSetupsByUserId[user.id];
                saveCopyTradingStore();
                sendJson(res, 410, { ok: false, error: "mfa setup expired" });
                return;
              }
              if (!verifyTotpCode(pendingSetup.secret, code)) {
                sendJson(res, 401, { ok: false, error: "invalid authentication code" });
                return;
              }
              const current = getUserById(user.id);
              if (!current) {
                sendJson(res, 404, { ok: false, error: "account not found" });
                return;
              }
              current.mfaEnabled = true;
              current.mfaEnabledAt = Date.now();
              current.mfaSecret = pendingSetup.secret;
              delete authStore.pendingMfaSetupsByUserId[user.id];
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                mfaEnabled: true,
                mfaEnabledAt: current.mfaEnabledAt,
                user: serializeAuthUser(current),
              });
              return;
            }
            if (url.pathname === "/api/auth/mfa/disable") {
              const body = await parseJsonRequestBody(req).catch((error) => {
                throw new Error(error && error.message ? error.message : "invalid_request_body");
              });
              const code = String(body && body.code ? body.code : "").trim();
              const current = getUserById(user.id);
              if (!current || !current.mfaEnabled || !current.mfaSecret) {
                sendJson(res, 400, { ok: false, error: "mfa is not enabled" });
                return;
              }
              if (!verifyTotpCode(current.mfaSecret, code)) {
                sendJson(res, 401, { ok: false, error: "invalid authentication code" });
                return;
              }
              current.mfaEnabled = false;
              current.mfaEnabledAt = null;
              delete current.mfaSecret;
              if (authStore.pendingMfaSetupsByUserId && authStore.pendingMfaSetupsByUserId[user.id]) {
                delete authStore.pendingMfaSetupsByUserId[user.id];
              }
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                mfaEnabled: false,
                user: serializeAuthUser(current),
              });
              return;
            }
            if (url.pathname === "/api/auth/mfa/backup-codes") {
              const code = String(body && body.code ? body.code : "").trim();
              const current = getUserById(user.id);
              if (!current || !current.mfaEnabled || !current.mfaSecret) {
                sendJson(res, 400, { ok: false, error: "mfa is not enabled" });
                return;
              }
              if (!verifyMfaCodeOrBackup(current, code)) {
                sendJson(res, 401, { ok: false, error: "invalid authentication code" });
                return;
              }
              const backupCodes = createMfaBackupCodes(current);
              saveCopyTradingStore();
              sendJson(res, 200, {
                ok: true,
                backupCodes,
                backupCodesRemaining: getBackupCodeStats(current).remaining,
                backupCodesGeneratedAt: current.mfaBackupCodesGeneratedAt || null,
                user: serializeAuthUser(current),
              });
              return;
            }
          }
          if (
            url.pathname === "/api/auth/signup" ||
            url.pathname === "/api/auth/signup/verify" ||
            url.pathname === "/api/auth/signup/resend" ||
            url.pathname === "/api/auth/signin"
          ) {
            sendJson(res, 410, {
              ok: false,
              error: "email-based authentication is disabled; use Twitter login instead",
            });
            return;
          }
          if (url.pathname === "/api/auth/signup/verify") {
            const code = String(body && body.code ? body.code : "").trim();
            if (!code || !/^\d{6}$/.test(code)) {
              sendJson(res, 400, { ok: false, error: "valid verification code required" });
              return;
            }
            if (Number(pending.expiresAt || 0) <= Date.now()) {
              delete authStore.pendingSignupsByEmail[email];
              saveCopyTradingStore();
              sendJson(res, 410, { ok: false, error: "verification code expired" });
              return;
            }
            const codeOk = verifyVerificationCode(code, pending.codeSalt, pending.codeHash);
            if (!codeOk) {
              pending.attempts = Math.max(0, Number(pending.attempts || 0)) + 1;
              saveCopyTradingStore();
              sendJson(res, 401, { ok: false, error: "invalid verification code" });
              return;
            }
            const existing = authStore.usersByEmail[email] || null;
            if (existing) {
              delete authStore.pendingSignupsByEmail[email];
              saveCopyTradingStore();
              sendJson(res, 409, { ok: false, error: "account already exists" });
              return;
            }
            const user = {
              id: crypto.randomUUID(),
              email,
              displayName: pending.displayName || displayName,
              passwordSalt: pending.passwordSalt,
              passwordHash: pending.passwordHash,
              emailVerifiedAt: Date.now(),
              mfaEnabled: false,
              mfaEnabledAt: null,
              createdAt: Date.now(),
              updatedAt: Date.now(),
            };
            authStore.usersByEmail[email] = user;
            delete authStore.pendingSignupsByEmail[email];
            const token = crypto.randomUUID().replace(/-/g, "");
            const expiresAt = Date.now() + 1000 * 60 * 60 * 24 * 30;
            authStore.sessionsByToken[token] = {
              token,
              userId: user.id,
              createdAt: Date.now(),
              expiresAt,
            };
            saveCopyTradingStore();
            sendJson(
              res,
              200,
              {
                ok: true,
                authenticated: true,
                verified: true,
                user: serializeAuthUser(user),
              },
              {
                "Set-Cookie": buildCookie("pf_session", token, {
                  maxAge: expiresAt - Date.now(),
                  httpOnly: true,
                  path: "/",
                  secure: false,
                }),
              }
            );
            return;
          }
          if (url.pathname === "/api/auth/signup/resend") {
            if (Number(pending.expiresAt || 0) > Date.now() && Number(pending.lastSentAt || 0) + 15000 > Date.now()) {
              sendJson(res, 429, { ok: false, error: "please wait before resending" });
              return;
            }
            pending.code = createVerificationCode();
            pending.codeSalt = null;
            const codeRecord = createVerificationCodeHash(pending.code);
            pending.codeSalt = codeRecord.salt;
            pending.codeHash = codeRecord.hash;
            pending.createdAt = Date.now();
            pending.expiresAt = Date.now() + 1000 * 60 * 10;
            pending.lastSentAt = Date.now();
            try {
              await sendAuthVerificationEmail({
                email,
                code: pending.code,
                displayName: pending.displayName || displayName,
              });
            } catch (error) {
              saveCopyTradingStore();
              sendJson(res, 503, {
                ok: false,
                error:
                  error && error.message === "email_transport_not_configured"
                    ? "email transport is not configured on this server"
                    : "failed to send verification email",
              });
              return;
            }
            saveCopyTradingStore();
            sendJson(res, 200, {
              ok: true,
              verificationRequired: true,
              email,
              expiresAt: pending.expiresAt,
            });
            return;
          }
          if (password.length < 8) {
            sendJson(res, 400, { ok: false, error: "password must be at least 8 characters" });
            return;
          }
          const existing = authStore.usersByEmail[email] || null;
          let user = existing;
          if (url.pathname === "/api/auth/signup") {
            if (existing) {
              sendJson(res, 409, { ok: false, error: "account already exists" });
              return;
            }
            const passwordRecord = createPasswordHash(password);
            const code = createVerificationCode();
            const codeRecord = createVerificationCodeHash(code);
            authStore.pendingSignupsByEmail[email] = {
              email,
              displayName,
              passwordSalt: passwordRecord.salt,
              passwordHash: passwordRecord.hash,
              codeSalt: codeRecord.salt,
              codeHash: codeRecord.hash,
              attempts: 0,
              createdAt: Date.now(),
              expiresAt: Date.now() + 1000 * 60 * 10,
              lastSentAt: Date.now(),
            };
            try {
              await sendAuthVerificationEmail({
                email,
                code,
                displayName,
              });
            } catch (error) {
              delete authStore.pendingSignupsByEmail[email];
              saveCopyTradingStore();
              sendJson(res, 503, {
                ok: false,
                error:
                  error && error.message === "email_transport_not_configured"
                    ? "email transport is not configured on this server"
                    : "failed to send verification email",
              });
              return;
            }
            saveCopyTradingStore();
            sendJson(res, 200, {
              ok: true,
              verificationRequired: true,
              email,
              expiresAt: authStore.pendingSignupsByEmail[email].expiresAt,
            });
            return;
          } else {
            if (!existing || !verifyPassword(password, existing.passwordSalt, existing.passwordHash)) {
              sendJson(res, 401, { ok: false, error: "invalid email or password" });
              return;
            }
            user = existing;
          }
          if (url.pathname === "/api/auth/signin" && user && user.mfaEnabled && user.mfaSecret) {
            const mfaToken = crypto.randomUUID().replace(/-/g, "");
            authStore.pendingSigninChallengesByToken[mfaToken] = {
              token: mfaToken,
              userId: user.id,
              createdAt: Date.now(),
              expiresAt: Date.now() + 1000 * 60 * 10,
              attempts: 0,
            };
            saveCopyTradingStore();
            sendJson(res, 200, {
              ok: true,
              mfaRequired: true,
              mfaToken,
              user: serializeAuthUser(user),
            });
            return;
          }
          const token = crypto.randomUUID().replace(/-/g, "");
          const expiresAt = Date.now() + 1000 * 60 * 60 * 24 * 30;
          authStore.sessionsByToken[token] = {
            token,
            userId: user.id,
            createdAt: Date.now(),
            expiresAt,
          };
          saveCopyTradingStore();
          sendJson(
            res,
            200,
            {
              ok: true,
              authenticated: true,
              user: serializeAuthUser(user),
            },
            {
              "Set-Cookie": buildCookie("pf_session", token, {
                maxAge: expiresAt - Date.now(),
                httpOnly: true,
                path: "/",
                secure: false,
              }),
            }
          );
          return;
        }

        if (req.method === "POST" && url.pathname === "/api/auth/signout") {
          const session = getAuthSessionFromRequest(req);
          const cookieHeader = buildCookie("pf_session", "", {
            maxAge: 0,
            expires: new Date(0),
            httpOnly: true,
            path: "/",
            secure: false,
          });
          if (session && session.token && authStore.sessionsByToken) {
            delete authStore.sessionsByToken[session.token];
            saveCopyTradingStore();
          }
          sendJson(res, 200, { ok: true, authenticated: false }, { "Set-Cookie": cookieHeader });
          return;
        }

        sendJson(res, 405, { ok: false, error: "method not allowed" });
        return;
      }

      if (ENABLE_API && url.pathname.startsWith("/api/copy")) {
        const session = getAuthSessionFromRequest(req);
        if (!session) {
          sendJson(res, 401, { ok: false, error: "sign in required" });
          return;
        }
        if (!session.user || !session.user.mfaEnabled || !session.user.mfaSecret) {
          sendJson(res, 403, { ok: false, error: "google authenticator required" });
          return;
        }
        const userId = session.user.id;
        const routeSuffix = String(url.pathname.slice("/api/copy".length) || "").trim() || "/";
        const store = loadCopyTradingStore();
        const leadersForUser = () => getLeaderWalletsForUser(userId).map(serializeCopyLeaderWallet).filter(Boolean);
        const apiKeysForUser = () => getApiKeysForUser(userId).map(serializeApiKeyRecord).filter(Boolean);
        const runtimeSnapshot = () => buildCopyDashboardStatusSnapshot(userId);
        if (req.method === "GET" && routeSuffix === "/stream") {
          registerCopyTradingSseClient(userId, req, res);
          return;
        }
        if (req.method === "GET" && routeSuffix === "/settings") {
          sendJson(res, 200, {
            ok: true,
            ...runtimeSnapshot(),
          });
          return;
        }
        if (req.method === "PATCH" && routeSuffix === "/settings") {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const profilePatch = {
            copyMode: body && body.copyMode !== undefined ? String(body.copyMode || "").trim() : undefined,
            copyOnlyNewTrades: body && body.copyOnlyNewTrades !== undefined ? Boolean(body.copyOnlyNewTrades) : undefined,
            copyExistingTrades: body && body.copyExistingTrades !== undefined ? Boolean(body.copyExistingTrades) : undefined,
            allowReopenAfterManualClose:
              body && body.allowReopenAfterManualClose !== undefined ? Boolean(body.allowReopenAfterManualClose) : undefined,
            pauseNewOpensOnly: body && body.pauseNewOpensOnly !== undefined ? Boolean(body.pauseNewOpensOnly) : undefined,
            stopMode: body && body.stopMode !== undefined ? String(body.stopMode || "").trim() : undefined,
          };
          const savedProfile = saveCopyProfileForUser(userId, profilePatch);
          const executionPatch = {};
          ["enabled", "dryRun", "copyRatio", "maxAllocationPct", "minNotionalUsd", "slippagePercent", "fixedNotionalUsd", "accessKeyId", "leveragePolicy", "retryPolicy", "syncBehavior"].forEach((key) => {
            if (body && body[key] !== undefined) executionPatch[key] = body[key];
          });
          if (Object.keys(executionPatch).length) {
            await copyTradingExecutionWorker.updateUserConfig(userId, executionPatch);
          }
          appendCopyActivityLog(userId, {
            userId,
            action: "update_settings",
            scope: "settings",
            source: "api",
            status: "saved",
            message: "Copy settings updated.",
            payload: { profile: savedProfile, execution: executionPatch },
          });
          broadcastCopyTradingEvent(userId, { type: "settings-updated", profile: savedProfile });
          sendJson(res, 200, {
            ok: true,
            settings: savedProfile,
            status: runtimeSnapshot(),
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/risk") {
          sendJson(res, 200, {
            ok: true,
            risk: serializeCopyRiskRules(store.copyRiskRulesByUserId && store.copyRiskRulesByUserId[userId]),
          });
          return;
        }
        if (req.method === "PATCH" && routeSuffix === "/risk") {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const nextRisk = saveCopyRiskRulesForUser(userId, {
            global: {
              totalCopiedExposureCapUsd: body && body.global && body.global.totalCopiedExposureCapUsd !== undefined ? Number(body.global.totalCopiedExposureCapUsd) : undefined,
              totalDailyLossCapUsd: body && body.global && body.global.totalDailyLossCapUsd !== undefined ? Number(body.global.totalDailyLossCapUsd) : undefined,
              leverageCap: body && body.global && body.global.leverageCap !== undefined ? Number(body.global.leverageCap) : undefined,
              maxOpenPositions: body && body.global && body.global.maxOpenPositions !== undefined ? Number(body.global.maxOpenPositions) : undefined,
              maxAllocationPct: body && body.global && body.global.maxAllocationPct !== undefined ? Number(body.global.maxAllocationPct) : undefined,
              autoPauseAfterFailures: body && body.global && body.global.autoPauseAfterFailures !== undefined ? Number(body.global.autoPauseAfterFailures) : undefined,
              allowedSymbols: body && body.global && body.global.allowedSymbols !== undefined ? body.global.allowedSymbols : undefined,
              blockedSymbols: body && body.global && body.global.blockedSymbols !== undefined ? body.global.blockedSymbols : undefined,
            },
            leaders: body && body.leaders && typeof body.leaders === "object" ? body.leaders : {},
          });
          appendCopyActivityLog(userId, {
            userId,
            action: "update_risk",
            scope: "risk",
            source: "api",
            status: "saved",
            message: "Copy risk updated.",
            payload: nextRisk,
          });
          sendJson(res, 200, { ok: true, risk: nextRisk });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/leaders") {
          const leaders = getLeaderWalletsForUser(userId);
          const profile = getCopyProfileForUser(userId);
          const risk = getCopyRiskRulesForUser(userId);
          sendJson(res, 200, {
            ok: true,
            leaders: leaders.map((row) => {
              const wallet = String(row.wallet || "").trim();
              const leaderRisk = (risk.leaders && risk.leaders[String(wallet).toUpperCase()]) || {};
              return serializeCopyLeaderWallet({
                ...row,
                allocationPct:
                  Number.isFinite(Number(row.allocationPct)) ? Number(row.allocationPct) : leaderRisk.maxAllocationPct || null,
                paused: Boolean(row.paused || profile.pauseNewOpensOnly),
                stopMode: row.stopMode || profile.stopMode || "keep",
                syncState: row.syncState || "idle",
              });
            }),
          });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/leaders") {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const wallet = validateLeaderWallet(body && body.wallet);
          if (!wallet) {
            sendJson(res, 400, { ok: false, error: "valid wallet required" });
            return;
          }
          if (!store.leadersByUserId || typeof store.leadersByUserId !== "object") {
            store.leadersByUserId = {};
          }
          if (!Array.isArray(store.leadersByUserId[userId])) {
            store.leadersByUserId[userId] = [];
          }
          const existing = store.leadersByUserId[userId].find((row) => String(row.wallet || "").toUpperCase() === wallet.toUpperCase());
          if (existing) {
            sendJson(res, 200, { ok: true, leader: existing, leaders: leadersForUser() });
            return;
          }
          const leader = {
            id: crypto.randomUUID(),
            wallet,
            label: String(body && body.label ? body.label : "").trim(),
            note: String(body && body.note ? body.note : "").trim(),
            allocationPct: Number.isFinite(Number(body && body.allocationPct)) ? Number(body.allocationPct) : null,
            allocationUsd: Number.isFinite(Number(body && body.allocationUsd)) ? Number(body.allocationUsd) : null,
            paused: Boolean(body && body.paused),
            stopMode: String(body && body.stopMode ? body.stopMode : "keep").trim() === "close_all" ? "close_all" : "keep",
            syncState: "idle",
            addedAt: Date.now(),
            updatedAt: Date.now(),
          };
          store.leadersByUserId[userId].unshift(leader);
          store.leadersByUserId[userId] = store.leadersByUserId[userId].slice(0, 100);
          saveCopyTradingStore();
          appendCopyActivityLog(userId, {
            userId,
            action: "add_leader",
            scope: "leader",
            leaderWallet: wallet,
            source: "api",
            status: "saved",
            message: `Leader ${shortWalletLabel(wallet)} added.`,
          });
          broadcastCopyTradingEvent(userId, { type: "leaders-changed", leaders: leadersForUser() });
          sendJson(res, 200, { ok: true, leader, leaders: leadersForUser() });
          return;
        }
        if (req.method === "PATCH" && /^\/api\/copy\/leaders\/[^/]+$/i.test(url.pathname)) {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const leaderId = String(url.pathname.replace(/^\/api\/copy\/leaders\//i, "")).trim();
          const rows = Array.isArray(store.leadersByUserId && store.leadersByUserId[userId])
            ? store.leadersByUserId[userId]
            : [];
          const next = rows.map((row) => {
            const rowId = String(row.id || row.wallet || "").trim();
            if (rowId !== leaderId && String(row.wallet || "") !== leaderId) return row;
            return {
              ...row,
              label: body && body.label !== undefined ? String(body.label || "").trim() : row.label,
              note: body && body.note !== undefined ? String(body.note || "").trim() : row.note,
              allocationPct:
                body && body.allocationPct !== undefined
                  ? Number.isFinite(Number(body.allocationPct))
                    ? Number(body.allocationPct)
                    : null
                  : row.allocationPct ?? null,
              allocationUsd:
                body && body.allocationUsd !== undefined
                  ? Number.isFinite(Number(body.allocationUsd))
                    ? Number(body.allocationUsd)
                    : null
                  : row.allocationUsd ?? null,
              paused: body && body.paused !== undefined ? Boolean(body.paused) : Boolean(row.paused),
              stopMode:
                body && body.stopMode !== undefined
                  ? String(body.stopMode || "").trim() === "close_all"
                    ? "close_all"
                    : "keep"
                  : row.stopMode || "keep",
              updatedAt: Date.now(),
            };
          });
          store.leadersByUserId[userId] = next;
          saveCopyTradingStore();
          appendCopyActivityLog(userId, {
            userId,
            action: "update_leader",
            scope: "leader",
            leaderWallet: leaderId,
            source: "api",
            status: "saved",
            message: `Leader ${shortWalletLabel(leaderId)} updated.`,
            payload: body,
          });
          broadcastCopyTradingEvent(userId, { type: "leaders-changed", leaders: leadersForUser() });
          sendJson(res, 200, { ok: true, leaders: leadersForUser() });
          return;
        }
        if (req.method === "DELETE" && /^\/api\/copy\/leaders\/[^/]+$/i.test(url.pathname)) {
          const leaderId = String(url.pathname.replace(/^\/api\/copy\/leaders\//i, "")).trim();
          const rows = Array.isArray(store.leadersByUserId && store.leadersByUserId[userId])
            ? store.leadersByUserId[userId]
            : [];
          const next = rows.filter((row) => String(row.id || row.wallet || "").trim() !== leaderId && String(row.wallet || "").trim() !== leaderId);
          store.leadersByUserId[userId] = next;
          if (store.executionStateByUserId && store.executionStateByUserId[userId]) {
            const state = store.executionStateByUserId[userId];
            if (state.leaderPositionBaselinesByWallet && typeof state.leaderPositionBaselinesByWallet === "object") {
              delete state.leaderPositionBaselinesByWallet[leaderId];
              delete state.leaderPositionBaselinesByWallet[String(leaderId).toUpperCase()];
              delete state.leaderPositionBaselinesByWallet[String(leaderId).toLowerCase()];
            }
            if (state.leaderSnapshotHashes && typeof state.leaderSnapshotHashes === "object") {
              delete state.leaderSnapshotHashes[leaderId];
              delete state.leaderSnapshotHashes[String(leaderId).toUpperCase()];
              delete state.leaderSnapshotHashes[String(leaderId).toLowerCase()];
            }
          }
          saveCopyTradingStore();
          appendCopyActivityLog(userId, {
            userId,
            action: "remove_leader",
            scope: "leader",
            leaderWallet: leaderId,
            source: "api",
            status: "saved",
            message: `Leader ${shortWalletLabel(leaderId)} removed.`,
          });
          broadcastCopyTradingEvent(userId, { type: "leaders-changed", leaders: leadersForUser() });
          sendJson(res, 200, { ok: true, leaders: leadersForUser() });
          return;
        }
        if (req.method === "GET" && /^\/api\/copy\/leaders\/[^/]+\/activity$/i.test(url.pathname)) {
          const leaderId = String(url.pathname.replace(/^\/api\/copy\/leaders\//i, "").replace(/\/activity$/i, "")).trim();
          if (!leaderId) {
            sendJson(res, 400, { ok: false, error: "leader wallet required" });
            return;
          }
          try {
            const payload = await fetchLeaderActivityPayload(leaderId, { timeoutMs: 20000 });
            sendJson(res, 200, { ok: true, ...payload });
          } catch (error) {
            sendJson(res, Number(error && error.status ? error.status : 0) || 500, {
              ok: false,
              error: error && error.message ? error.message : "leader_activity_fetch_failed",
            });
          }
          return;
        }
        if (req.method === "POST" && routeSuffix === "/enable") {
          const config = await copyTradingExecutionWorker.updateUserConfig(userId, { enabled: true });
          saveCopyRuntimeStatusForUser(userId, { state: "running", health: "healthy", lastStateChangeAt: Date.now() });
          appendCopyActivityLog(userId, { userId, action: "enable", scope: "runtime", source: "api", status: "enabled", message: "Copy trading enabled." });
          sendJson(res, 200, { ok: true, runtime: getCopyRuntimeStatusForUser(userId), config });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/disable") {
          const config = await copyTradingExecutionWorker.updateUserConfig(userId, { enabled: false });
          saveCopyRuntimeStatusForUser(userId, { state: "stopped_keep", health: "paused", lastStateChangeAt: Date.now() });
          appendCopyActivityLog(userId, { userId, action: "disable", scope: "runtime", source: "api", status: "disabled", message: "Copy trading disabled." });
          sendJson(res, 200, { ok: true, runtime: getCopyRuntimeStatusForUser(userId), config });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/pause") {
          saveCopyRuntimeStatusForUser(userId, { state: "paused", health: "paused", pausedReason: "paused by user", lastStateChangeAt: Date.now() });
          appendCopyActivityLog(userId, { userId, action: "pause", scope: "runtime", source: "api", status: "paused", message: "Copy trading paused." });
          sendJson(res, 200, { ok: true, runtime: getCopyRuntimeStatusForUser(userId) });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/stop-keep") {
          await copyTradingExecutionWorker.updateUserConfig(userId, { enabled: false });
          saveCopyRuntimeStatusForUser(userId, { state: "stopped_keep", health: "paused", lastStateChangeAt: Date.now() });
          appendCopyActivityLog(userId, { userId, action: "stop_keep", scope: "runtime", source: "api", status: "stopped", message: "Copy trading stopped; positions kept." });
          sendJson(res, 200, { ok: true, runtime: getCopyRuntimeStatusForUser(userId) });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/stop-close-all") {
          await copyTradingExecutionWorker.updateUserConfig(userId, { enabled: true });
          saveCopyRuntimeStatusForUser(userId, { state: "close_all", closeAllRequested: true, health: "paused", lastStateChangeAt: Date.now() });
          appendCopyActivityLog(userId, { userId, action: "stop_close_all", scope: "runtime", source: "api", status: "requested", message: "Close-all requested." });
          const result = await copyTradingExecutionWorker.runCycle();
          sendJson(res, 200, { ok: true, runtime: getCopyRuntimeStatusForUser(userId), result });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/manual-override") {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const token = String(body && body.symbol ? body.symbol : body && body.token ? body.token : "").trim().toUpperCase();
          const side = String(body && body.side ? body.side : "").trim().toLowerCase();
          if (!token || !side) {
            sendJson(res, 400, { ok: false, error: "symbol and side required" });
            return;
          }
          const key = `${token}:${side}`;
          if (!store.manualOverridesByUserId || typeof store.manualOverridesByUserId !== "object") {
            store.manualOverridesByUserId = {};
          }
          if (!store.manualOverridesByUserId[userId]) {
            store.manualOverridesByUserId[userId] = {};
          }
          store.manualOverridesByUserId[userId][key] = {
            key,
            token,
            side,
            leaderWallet: body && body.leaderWallet ? String(body.leaderWallet).trim() : null,
            closeOnce: Boolean(body && body.closeOnce),
            protectFromReopen: Boolean(body && body.protectFromReopen),
            reason: body && body.reason ? String(body.reason).trim() : "",
            createdAt: Date.now(),
            updatedAt: Date.now(),
          };
          saveCopyTradingStore();
          appendCopyActivityLog(userId, {
            userId,
            action: "manual_override",
            scope: "manual",
            source: "api",
            status: "saved",
            symbol: token,
            side,
            reason: body && body.reason ? String(body.reason).trim() : "manual override",
            payload: store.manualOverridesByUserId[userId][key],
          });
          sendJson(res, 200, { ok: true, manualOverrides: store.manualOverridesByUserId[userId] });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/preview") {
          const preview = await copyTradingExecutionWorker.previewExecution(userId);
          appendCopyActivityLog(userId, {
            userId,
            action: "preview",
            scope: "execution",
            source: "api",
            status: preview && preview.ok ? "ok" : "blocked",
            message: preview && preview.ok ? "Preview generated." : preview && preview.error ? preview.error : "Preview failed.",
            payload: preview,
          });
          if (preview && Array.isArray(preview.previewOrders)) {
            if (!store.copyIntentsByUserId || typeof store.copyIntentsByUserId !== "object") {
              store.copyIntentsByUserId = {};
            }
            store.copyIntentsByUserId[userId] = preview.previewOrders.map((row) => ({
              id: row.clientOrderId || crypto.randomUUID(),
              createdAt: Date.now(),
              action: row.action,
              token: row.token,
              side: row.side,
              amount: row.amount,
              reduceOnly: Boolean(row.reduceOnly),
              status: "preview",
              payload: row,
            }));
            saveCopyTradingStore();
          }
          sendJson(res, 200, { ok: true, preview, status: runtimeSnapshot() });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/run") {
          const result = await copyTradingExecutionWorker.runCycle();
          appendCopyActivityLog(userId, {
            userId,
            action: "run_cycle",
            scope: "execution",
            source: "api",
            status: result && result.ok ? "ok" : "failed",
            message: result && result.ok ? "Execution cycle completed." : result && result.error ? result.error : "Execution cycle failed.",
            payload: result,
          });
          broadcastCopyTradingEvent(userId, {
            type: "copy-event",
            event: serializeCopyActivityRow({
              userId,
              action: "run_cycle",
              scope: "execution",
              source: "api",
              status: result && result.ok ? "ok" : "failed",
              message: result && result.ok ? "Execution cycle completed." : result && result.error ? result.error : "Execution cycle failed.",
              payload: result,
            }),
          });
          sendJson(res, 200, { ok: true, result, status: runtimeSnapshot() });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/activity") {
          sendJson(res, 200, {
            ok: true,
            activity: getCopyActivityLogForUser(userId),
            alerts: getCopyAlertsForUser(userId),
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/execution-log") {
          sendJson(res, 200, {
            ok: true,
            status: copyTradingExecutionWorker && typeof copyTradingExecutionWorker.getUserExecutionView === "function"
              ? copyTradingExecutionWorker.getUserExecutionView(userId)
              : null,
            events: copyTradingExecutionWorker && typeof copyTradingExecutionWorker.getUserExecutionView === "function"
              ? copyTradingExecutionWorker.getUserExecutionView(userId).status.events || []
              : [],
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/positions") {
          const accessSets = apiKeysForUser();
          const accessSet =
            accessSets.find((row) => row.accountWallet && row.agentWallet && row.hasSecret) ||
            accessSets.find((row) => row.accountWallet && row.agentWallet) ||
            accessSets.find((row) => row.accountWallet) ||
            accessSets[0] ||
            null;
          const payload = {
            ok: true,
            apiKeyCount: accessSets.length,
            hasApiKeys: accessSets.length > 0,
            hasConnectedAccessSet: Boolean(accessSet && accessSet.accountWallet && accessSet.agentWallet),
            accessSets,
            summary: null,
            positions: [],
          };
          if (accessSet && accessSet.accountWallet) {
            try {
              const positionsRes = await restClient.get("/positions", {
                query: { account: accessSet.accountWallet },
                timeoutMs: 15000,
                retryMaxAttempts: 2,
              });
              payload.positions = Array.isArray(extractPayloadData(positionsRes, [])) ? extractPayloadData(positionsRes, []) : [];
            } catch (_error) {}
            try {
              const accountRes = await restClient.get("/account", {
                query: { account: accessSet.accountWallet },
                timeoutMs: 15000,
                retryMaxAttempts: 2,
              });
              payload.summary = extractPayloadData(accountRes, null);
            } catch (_error) {}
          }
          payload.copiedPositions = getCopyCopiedPositionsForUser(userId);
          sendJson(res, 200, payload);
          return;
        }
        if (req.method === "GET" && routeSuffix === "/reconciliation") {
          const status = runtimeSnapshot();
          const currentPositions = getCopyCopiedPositionsForUser(userId);
          const executionView = copyTradingExecutionWorker && typeof copyTradingExecutionWorker.getUserExecutionView === "function"
            ? copyTradingExecutionWorker.getUserExecutionView(userId)
            : null;
          sendJson(res, 200, {
            ok: true,
            status,
            copiedPositions: currentPositions,
            execution: executionView,
            alerts: getCopyAlertsForUser(userId),
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/status") {
          sendJson(res, 200, runtimeSnapshot());
          return;
        }
      }

      if (ENABLE_API && url.pathname.startsWith("/api/copy-trading")) {
        const session = getAuthSessionFromRequest(req);
        if (!session) {
          sendJson(res, 401, { ok: false, error: "sign in required" });
          return;
        }
        if (!session.user || !session.user.mfaEnabled || !session.user.mfaSecret) {
          sendJson(res, 403, { ok: false, error: "google authenticator required" });
          return;
        }
        const store = loadCopyTradingStore();
        const leadersForUser = () => getLeaderWalletsForUser(session.user.id);
        const routeSuffix = String(url.pathname.slice("/api/copy-trading".length) || "").trim();
        if (req.method === "GET" && routeSuffix === "/balance") {
          const accessSets = getApiKeysForUser(session.user.id).map(serializeApiKeyRecord).filter(Boolean);
          const accessSet =
            accessSets.find((row) => row.accountWallet && row.agentWallet && row.hasSecret) ||
            accessSets.find((row) => row.accountWallet && row.agentWallet) ||
            accessSets.find((row) => row.accountWallet) ||
            accessSets[0] ||
            null;
          const hasSavedAccessSets = accessSets.length > 0;
          const hasConnectedAccessSet = Boolean(accessSet && accessSet.accountWallet && accessSet.agentWallet);
          let latestBalance = null;
          let history = [];
          if (accessSet && accessSet.accountWallet) {
            try {
              const accountRes = await restClient.get("/account", {
                query: { account: accessSet.accountWallet },
                timeoutMs: 15_000,
                retryMaxAttempts: 2,
              });
              const accountData = extractPayloadData(accountRes, null) || {};
              latestBalance = {
                balance:
                  accountData.balance !== undefined
                    ? accountData.balance
                    : accountData.account_equity !== undefined
                    ? accountData.account_equity
                    : null,
                pendingBalance:
                  accountData.pending_balance !== undefined
                    ? accountData.pending_balance
                    : accountData.pendingBalance !== undefined
                    ? accountData.pendingBalance
                    : null,
                amount:
                  accountData.account_equity !== undefined
                    ? accountData.account_equity
                    : accountData.balance !== undefined
                    ? accountData.balance
                    : null,
                eventType: "pacifica_account",
                createdAt: accountData.updated_at || accountData.updatedAt || null,
              };
            } catch (error) {
              console.warn(
                `[copy-trading] pacifica balance fetch failed for ${accessSet.accountWallet}: ${error && error.message ? error.message : error}`
              );
            }
            try {
              const portfolioRes = await restClient.get("/portfolio", {
                query: { account: accessSet.accountWallet, time_range: "7d" },
                timeoutMs: 20_000,
                retryMaxAttempts: 1,
              });
              const portfolioDataRaw = extractPayloadData(portfolioRes, []);
              const portfolioData = Array.isArray(portfolioDataRaw) ? portfolioDataRaw : [];
              history = portfolioData.slice(0, 10).map((row) => ({
                balance: row.account_equity !== undefined ? row.account_equity : row.balance,
                pendingBalance: null,
                amount: row.account_equity !== undefined ? row.account_equity : row.balance,
                eventType: "pacifica_portfolio",
                createdAt: row.timestamp || row.created_at || row.createdAt || null,
              }));
            } catch (error) {
              console.warn(
                `[copy-trading] pacifica portfolio history fetch failed for ${accessSet.accountWallet}: ${error && error.message ? error.message : error}`
              );
            }
          }
          sendJson(res, 200, {
            ok: true,
            apiKeyCount: accessSets.length,
            hasApiKeys: hasSavedAccessSets,
            hasConnectedAccessSet,
            accessSets,
            latestBalance: latestBalance ? serializeBalanceSnapshot(latestBalance) : null,
            history,
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/positions") {
          const accessSets = getApiKeysForUser(session.user.id).map(serializeApiKeyRecord).filter(Boolean);
          const accessSet =
            accessSets.find((row) => row.accountWallet && row.agentWallet && row.hasSecret) ||
            accessSets.find((row) => row.accountWallet && row.agentWallet) ||
            accessSets.find((row) => row.accountWallet) ||
            accessSets[0] ||
            null;
          const hasSavedAccessSets = accessSets.length > 0;
          const hasConnectedAccessSet = Boolean(accessSet && accessSet.accountWallet && accessSet.agentWallet);
          let positions = [];
          let summary = null;
          if (accessSet && accessSet.accountWallet) {
            try {
              const positionsRes = await restClient.get("/positions", {
                query: { account: accessSet.accountWallet },
                timeoutMs: 15000,
                retryMaxAttempts: 2,
              });
              positions = Array.isArray(extractPayloadData(positionsRes, [])) ? extractPayloadData(positionsRes, []) : [];
            } catch (error) {
              console.warn(
                `[copy-trading] pacifica positions fetch failed for ${accessSet.accountWallet}: ${error && error.message ? error.message : error}`
              );
            }
            try {
              const accountRes = await restClient.get("/account", {
                query: { account: accessSet.accountWallet },
                timeoutMs: 15000,
                retryMaxAttempts: 2,
              });
              summary = extractPayloadData(accountRes, null);
            } catch (error) {
              console.warn(
                `[copy-trading] pacifica account fetch failed for ${accessSet.accountWallet}: ${error && error.message ? error.message : error}`
              );
            }
          }
          sendJson(res, 200, {
            ok: true,
            apiKeyCount: accessSets.length,
            hasApiKeys: hasSavedAccessSets,
            hasConnectedAccessSet,
            accessSets,
            summary,
            positions,
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/leaders") {
          sendJson(res, 200, {
            ok: true,
            leaders: leadersForUser(),
          });
          return;
        }
        if (req.method === "GET" && routeSuffix === "/execution") {
          const view =
            copyTradingExecutionWorker && typeof copyTradingExecutionWorker.getUserExecutionView === "function"
              ? copyTradingExecutionWorker.getUserExecutionView(session.user.id)
              : null;
          sendJson(res, 200, {
            ok: true,
            apiKeys: getApiKeysForUser(session.user.id).map(serializeApiKeyRecord).filter(Boolean),
            ...view,
          });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/execution") {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          if (!copyTradingExecutionWorker || typeof copyTradingExecutionWorker.updateUserConfig !== "function") {
            sendJson(res, 503, { ok: false, error: "execution worker unavailable" });
            return;
          }
          const accessKeyId = String(body && body.accessKeyId ? body.accessKeyId : "").trim();
          const enabled = body && body.enabled !== undefined ? Boolean(body.enabled) : undefined;
          const dryRun = body && body.dryRun !== undefined ? Boolean(body.dryRun) : undefined;
          const copyRatio = body && body.copyRatio !== undefined ? Number(body.copyRatio) : undefined;
          const maxAllocationPct = body && body.maxAllocationPct !== undefined ? Number(body.maxAllocationPct) : undefined;
          const minNotionalUsd = body && body.minNotionalUsd !== undefined ? Number(body.minNotionalUsd) : undefined;
          const slippagePercent = body && body.slippagePercent !== undefined ? Number(body.slippagePercent) : undefined;
          const leveragePolicy = body && body.leveragePolicy !== undefined ? String(body.leveragePolicy || "").trim() : undefined;
          const fixedNotionalUsd = body && body.fixedNotionalUsd !== undefined ? Number(body.fixedNotionalUsd) : undefined;
          const retryPolicy = body && body.retryPolicy !== undefined ? String(body.retryPolicy || "").trim() : undefined;
          const syncBehavior = body && body.syncBehavior !== undefined ? String(body.syncBehavior || "").trim() : undefined;
          const config = await copyTradingExecutionWorker.updateUserConfig(session.user.id, {
            accessKeyId,
            enabled,
            dryRun,
            copyRatio,
            maxAllocationPct,
            minNotionalUsd,
            slippagePercent,
            leveragePolicy,
            fixedNotionalUsd,
            retryPolicy,
            syncBehavior,
          });
          sendJson(res, 200, {
            ok: true,
            config,
            view: copyTradingExecutionWorker.getUserExecutionView(session.user.id),
          });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/execution/run") {
          if (!copyTradingExecutionWorker || typeof copyTradingExecutionWorker.runCycle !== "function") {
            sendJson(res, 503, { ok: false, error: "execution worker unavailable" });
            return;
          }
          const result = await copyTradingExecutionWorker.runCycle();
          sendJson(res, 200, {
            ok: true,
            result,
            view: copyTradingExecutionWorker.getUserExecutionView(session.user.id),
          });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/execution/preview") {
          if (!copyTradingExecutionWorker || typeof copyTradingExecutionWorker.previewExecution !== "function") {
            sendJson(res, 503, { ok: false, error: "execution worker unavailable" });
            return;
          }
          const preview = await copyTradingExecutionWorker.previewExecution(session.user.id);
          sendJson(res, 200, {
            ok: true,
            preview,
            view: copyTradingExecutionWorker.getUserExecutionView(session.user.id),
          });
          return;
        }
        if (req.method === "POST" && routeSuffix === "/leaders") {
          const body = await parseJsonRequestBody(req).catch((error) => {
            throw new Error(error && error.message ? error.message : "invalid_request_body");
          });
          const wallet = validateLeaderWallet(body && body.wallet);
          if (!wallet) {
            sendJson(res, 400, { ok: false, error: "valid wallet required" });
            return;
          }
          const leaders = Array.isArray(store.leadersByUserId[session.user.id])
            ? store.leadersByUserId[session.user.id]
            : [];
          const existing = leaders.find((row) => String(row.wallet || "").toUpperCase() === wallet.toUpperCase());
          if (existing) {
            sendJson(res, 200, {
              ok: true,
              leader: existing,
              leaders: leadersForUser(),
            });
            return;
          }
          if (!store.leadersByUserId[session.user.id]) store.leadersByUserId[session.user.id] = [];
          store.leadersByUserId[session.user.id].unshift({
            wallet,
            addedAt: Date.now(),
            note: "",
          });
          store.leadersByUserId[session.user.id] = store.leadersByUserId[session.user.id].slice(0, 100);
          saveCopyTradingStore();
          sendJson(res, 200, {
            ok: true,
            leader: {
              wallet,
              addedAt: Date.now(),
              note: "",
            },
            leaders: leadersForUser(),
          });
          return;
        }
        if (req.method === "DELETE" && /^\/leaders\/[^/]+$/i.test(routeSuffix)) {
          const wallet = validateLeaderWallet(routeSuffix.replace(/^\/leaders\//i, ""));
          if (!wallet) {
            sendJson(res, 400, { ok: false, error: "valid wallet required" });
            return;
          }
          const next = leadersForUser().filter(
            (row) => String(row.wallet || "").toUpperCase() !== wallet.toUpperCase()
          );
          store.leadersByUserId[session.user.id] = next;
          saveCopyTradingStore();
          sendJson(res, 200, { ok: true, leaders: next });
          return;
        }
        if (req.method === "GET" && /^\/leaders\/[^/]+\/activity$/i.test(routeSuffix)) {
          const wallet = validateLeaderWallet(routeSuffix.replace(/^\/leaders\//i, "").replace(/\/activity$/i, ""));
          if (!wallet) {
            sendJson(res, 400, { ok: false, error: "valid wallet required" });
            return;
          }
          const activity = await fetchLeaderActivityPayload(wallet, { timeoutMs: 20000 });
          sendJson(res, 200, {
            ok: true,
            wallet,
            ...activity,
          });
          return;
        }
        sendJson(res, 405, { ok: false, error: "method not allowed" });
        return;
      }

      if (
        ENABLE_API &&
        (url.pathname === "/api/wallet-performance" ||
          /^\/api\/wallet-performance\/wallet\/[^/]+$/i.test(String(url.pathname || "")))
      ) {
        const isWalletDetailRoute = /^\/api\/wallet-performance\/wallet\/[^/]+$/i.test(
          String(url.pathname || "")
        );
        let upstreamRequestPath = String(req.url || "/api/wallet-performance");
        if (isWalletDetailRoute) {
          try {
            const localParsed = new URL(upstreamRequestPath, "http://local");
            if (!localParsed.searchParams.has("force")) {
              localParsed.searchParams.set("force", "1");
            }
            upstreamRequestPath = `${localParsed.pathname}${localParsed.search}`;
          } catch (_ignoreUrlParseError) {
            if (upstreamRequestPath.indexOf("?") >= 0) {
              if (!/[?&]force=/.test(upstreamRequestPath)) {
                upstreamRequestPath += "&force=1";
              }
            } else {
              upstreamRequestPath += "?force=1";
            }
          }
        }
        const walletPerformanceUpstreamUrl = buildWalletPerformanceUpstreamUrl(
          upstreamRequestPath
        );
        const exactWalletToVerify = !isWalletDetailRoute
          ? extractWalletPerformanceExactQueryWallet(upstreamRequestPath)
          : null;
        await proxyUpstreamWalletPerformanceJson(req, res, walletPerformanceUpstreamUrl, {
          timeoutMs: 65000,
          retries: 2,
          retryDelayMs: 250,
          isWalletDetailRoute,
          exactWalletToVerify,
        });
        return;
      }

      if (ENABLE_API) {
        if (await serveUiSnapshotFastPath(req, res, url, walletStore)) return;
        if (MINIMAL_UI_SERVER) {
          sendJson(res, 404, {
            error: "Not found",
            path: url.pathname,
          });
          return;
        }
        const generalDataComponent = getGeneralDataComponent();
        if (generalDataComponent && (await generalDataComponent.handleRequest(req, res, url))) return;
        const needsTrackingNextgenComponent =
          String(url.pathname || "").startsWith("/api/nextgen/") ||
          (NEXTGEN_WALLET_API_SHADOW_ENABLED &&
            (url.pathname === "/api/wallets" ||
              url.pathname === "/api/wallets/profile" ||
              url.pathname === "/api/wallets/sync-health"));
        if (needsTrackingNextgenComponent) {
          const trackingNextgenComponent = getWalletTrackingNextgenComponent();
          if (trackingNextgenComponent && (await trackingNextgenComponent.handleRequest(req, res, url))) return;
        }
        const trackingComponent = getWalletTrackingComponent();
        if (trackingComponent && (await trackingComponent.handleRequest(req, res, url))) return;
        const creatorStudioComponent = getCreatorStudioComponent();
        if (creatorStudioComponent && (await creatorStudioComponent.handleRequest(req, res, url))) return;
      }

      sendJson(res, 404, {
        error: "Not found",
        path: url.pathname,
      });
    } catch (error) {
      sendJson(res, 500, {
        error: error.message || "Unexpected server error",
      });
    }
  });

  function loadWalletRowsForMetricsHistory() {
    const liveRows =
      walletStore && typeof walletStore.list === "function" ? walletStore.list() : [];
    if (Array.isArray(liveRows) && liveRows.length > 0) return liveRows;
    const compact = readJson(WALLET_DATASET_COMPACT_SOURCE_PATH, null);
    const compactRows = compact && Array.isArray(compact.rows) ? compact.rows : [];
    return compactRows;
  }

  async function captureWalletMetricsHistory(force = false) {
    if (
      !ENABLE_API ||
      !walletMetricsHistoryStore ||
      !walletTrackingComponent ||
      typeof walletTrackingComponent.getLiveWalletSnapshot !== "function"
    ) {
      return;
    }
    const snapshot = walletTrackingComponent.getLiveWalletSnapshot(400);
    walletMetricsHistoryStore.capture({
      walletRows: loadWalletRowsForMetricsHistory(),
      positions:
        snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [],
      nowMs: Date.now(),
      force,
    });
  }

  async function ingestPositionLifecycle(force = false) {
    if (
      !ENABLE_API ||
      !positionLifecycleStore ||
      !walletTrackingComponent ||
      typeof walletTrackingComponent.getLiveWalletSnapshot !== "function"
    ) {
      return;
    }
    const snapshot = walletTrackingComponent.getLiveWalletSnapshot(400);
    positionLifecycleStore.ingest({
      positions:
        snapshot && Array.isArray(snapshot.positions) ? snapshot.positions : [],
      nowMs: Date.now(),
      force,
    });
  }

  function refreshRuntimeLoopStates() {
    runtimeLoopStates = {
      snapshot: snapshotLoop ? snapshotLoop.getState() : null,
      globalKpi: globalKpiLoop ? globalKpiLoop.getState() : null,
      positionLifecycle: positionLifecycleLoop ? positionLifecycleLoop.getState() : null,
      walletMetricsHistory: walletMetricsHistoryLoop ? walletMetricsHistoryLoop.getState() : null,
      onchainRunner: onchainLoop ? onchainLoop.getState() : null,
      runtimeStatus: runtimeStatusLoop ? runtimeStatusLoop.getState() : null,
      copyTradingExecution: copyTradingExecutionLoop ? copyTradingExecutionLoop.getState() : null,
    };
    return runtimeLoopStates;
  }

  function publishRuntimeStatus() {
    if (!runtimeStatusStore) return null;
    refreshRuntimeLoopStates();
    return runtimeStatusStore.publish(buildLocalRuntimeSnapshot(runtimeLoopStates));
  }

  snapshotLoop = createManagedLoop({
    name: "snapshot-refresh",
    logger: console,
    runTask: async () => refreshSnapshots(),
    getDelayMs: () => {
      const account = pipeline.getAccount();
      if (!account) return Math.max(SNAPSHOT_REFRESH_MS, 120000);
      return SNAPSHOT_REFRESH_MS;
    },
  });

  globalKpiLoop = createManagedLoop({
    name: "global-kpi",
    logger: console,
    runTask: async () => {
      const data = await refreshGlobalKpi();
      const meta = data && data.volumeMeta && typeof data.volumeMeta === "object" ? data.volumeMeta : {};
      console.log(
        `[global-kpi] refresh cumulative_volume=${Number(data.totalHistoricalVolume || 0).toFixed(
          2
        )} tracked_to=${meta.lastProcessedDate || "n/a"} remaining_days=${
          meta.remainingDaysToToday !== undefined ? meta.remainingDaysToToday : "n/a"
        } progress_days=${
          meta.processedDays !== undefined && meta.totalDaysToToday !== undefined
            ? `${meta.processedDays}/${meta.totalDaysToToday}`
            : "n/a"
        }`
      );
      return data;
    },
    getDelayMs: ({ result }) => {
      const meta = result && result.volumeMeta && typeof result.volumeMeta === "object" ? result.volumeMeta : {};
      const backfillComplete =
        meta.backfillComplete !== undefined ? Boolean(meta.backfillComplete) : false;
      return backfillComplete
        ? GLOBAL_KPI_REFRESH_MS
        : Math.min(GLOBAL_KPI_REFRESH_MS, 60000);
    },
  });

  positionLifecycleLoop = createManagedLoop({
    name: "position-lifecycle",
    logger: console,
    runTask: async () => ingestPositionLifecycle(false),
    getDelayMs: () => Math.max(5000, Number(process.env.PACIFICA_POSITION_LIFECYCLE_INGEST_MS || 10000)),
  });

  walletMetricsHistoryLoop = createManagedLoop({
    name: "wallet-metrics-history",
    logger: console,
    runTask: async () => captureWalletMetricsHistory(false),
    getDelayMs: () => {
      const liveSnapshot =
        walletTrackingComponent &&
        typeof walletTrackingComponent.getLiveWalletSnapshot === "function"
          ? walletTrackingComponent.getLiveWalletSnapshot(50)
          : null;
      const liveStatus =
        liveSnapshot && liveSnapshot.status && typeof liveSnapshot.status === "object"
          ? liveSnapshot.status
          : null;
      if (liveStatus && Number(liveStatus.recentEvents1m || 0) > 0) {
        return Math.max(
          60000,
          Math.floor(Number(process.env.PACIFICA_WALLET_METRICS_HISTORY_CAPTURE_MS || 300000) / 2)
        );
      }
      return Math.max(60000, Number(process.env.PACIFICA_WALLET_METRICS_HISTORY_CAPTURE_MS || 300000));
    },
  });

  onchainLoop = createManagedLoop({
    name: "onchain-runner",
    logger: console,
    runTask: async () => {
      await runOnchainStep();
      return onchainDiscovery && typeof onchainDiscovery.getStatus === "function"
        ? onchainDiscovery.getStatus()
        : null;
    },
    getDelayMs: ({ result }) => {
      const status = result && result.progress ? result : onchainDiscovery && typeof onchainDiscovery.getStatus === "function"
        ? onchainDiscovery.getStatus()
        : null;
      const progress = status && status.progress ? status.progress : null;
      if (progress && Number(progress.pendingTransactions || 0) > 0) {
        return Math.min(ONCHAIN_RUNNER_INTERVAL_MS, 2500);
      }
      if (progress && Number(progress.pct || 0) < 100) {
        return Math.min(ONCHAIN_RUNNER_INTERVAL_MS, 5000);
      }
      return ONCHAIN_RUNNER_INTERVAL_MS;
    },
  });

  runtimeStatusLoop = createManagedLoop({
    name: "runtime-status",
    logger: console,
    runTask: async () => publishRuntimeStatus(),
    getDelayMs: () => RUNTIME_STATUS_PUBLISH_MS,
  });

  // Build the wallet-tracking component before opening the port so its startup
  // warmups complete before the first user-facing request arrives.
  if (STARTUP_COMPONENT_WARMUP_ENABLED) {
    try {
      if (ENABLE_UI || ENABLE_API) {
        getWalletTrackingComponent();
        getWalletTrackingNextgenComponent();
      }
    } catch (error) {
      console.warn(`[pacifica-flow] wallet tracking warmup failed: ${error.message}`);
    }
  }

  server.listen(PORT, HOST, () => {
    startupState.listeningAt = Date.now();
    if (startupState.phase === "booting") {
      startupState.phase = "listening";
    }
    console.log(`[pacifica-flow] service=${SERVICE} listening on http://${HOST}:${PORT}`);
    console.log(`[pacifica-flow] api=${API_BASE}`);
    console.log(`[pacifica-flow] ws=${WS_URL}`);
    console.log(`[pacifica-flow] account=${pipeline.getAccount() || "(none)"}`);
    console.log(`[pacifica-flow] signer_mode=${signer.mode}`);
    console.log(
      `[pacifica-flow] pacifica_api_rpm_cap=${API_RPM_CAP} rate_limit_capacity=${RATE_LIMIT_CAPACITY}/window_ms=${RATE_LIMIT_WINDOW_MS}`
    );
    const pacificaRateState = rateGuard.getState();
    console.log(
      `[pacifica-flow] pacifica_api_rpm_used=${pacificaRateState.used1m} pacifica_api_rpm_cap=${pacificaRateState.rpmCap}`
    );
    console.log(`[pacifica-flow] pipeline_persist_every_ms=${PIPELINE_PERSIST_EVERY_MS}`);
    console.log(
      `[pacifica-flow] pipeline_skip_replay=${PIPELINE_SKIP_REPLAY ? "true" : "false"}`
    );
    if (API_CONFIG_KEY) {
      console.log("[pacifica-flow] pf_api_key=present");
    }
    if ((ENABLE_UI || ENABLE_API) && STARTUP_COMPONENT_WARMUP_ENABLED) {
      const walletTrackingComponentPrewarmTimer = setTimeout(() => {
        try {
          getWalletTrackingComponent();
        } catch (error) {
          console.warn(`[wallet-tracking] component prewarm failed: ${error.message}`);
        }
      }, 50);
      if (
        walletTrackingComponentPrewarmTimer &&
        typeof walletTrackingComponentPrewarmTimer.unref === "function"
      ) {
        walletTrackingComponentPrewarmTimer.unref();
      }
    }
    if ((ENABLE_UI || ENABLE_API) && STARTUP_ROUTE_WARMUP_ENABLED) {
      const walletExplorerV3WarmupTimer = setTimeout(() => {
        (async () => {
          try {
            walletTrackingReadModel.preload();
            loadWalletExplorerV3Snapshot();
          } catch (error) {
            console.warn(`[wallet-explorer] startup warmup failed: ${error.message}`);
          }
        })();
      }, 500);
      if (walletExplorerV3WarmupTimer && typeof walletExplorerV3WarmupTimer.unref === "function") {
        walletExplorerV3WarmupTimer.unref();
      }
    }
    console.log(
      `[pacifica-flow] indexer=${INDEXER_ENABLED ? "enabled" : "disabled"} source_file=${WALLET_SOURCE_FILE}`
    );
    if (ENABLE_UI && STARTUP_ANALYTICS_WARMUP_ENABLED) {
      setTimeout(() => {
        getExchangeBootstrapPayload().catch(() => null);
      }, 1000);
      const exchangeBootstrapTimer = setInterval(() => {
        getExchangeBootstrapPayload().catch(() => null);
      }, 30000);
      if (typeof exchangeBootstrapTimer.unref === "function") {
        exchangeBootstrapTimer.unref();
      }
    }
    if (INDEXER_ENABLED) {
      console.log(
        `[pacifica-flow] indexer_mode=${INDEXER_DISCOVERY_ONLY ? "discovery_only" : "discover_and_scan"} wallet_scan_concurrency=${effectiveWalletScanConcurrency} rest_shard_strategy=${String(
          process.env.PACIFICA_INDEXER_REST_SHARD_STRATEGY || "wallet_hash"
        ).trim()}`
      );
      console.log(
        `[pacifica-flow] indexer_worker shard=${INDEXER_WORKER_SHARD_INDEX}/${INDEXER_WORKER_SHARD_COUNT} data_dir=${INDEXER_DATA_DIR} external_shards=${INDEXER_EXTERNAL_SHARDS_ENABLED ? "enabled" : "disabled"}`
      );
      console.log(`[pacifica-flow] indexer_batch_size=${effectiveIndexerBatchSize}`);
      console.log(
        `[pacifica-flow] indexer_live_mode wallets_per_scan=${INDEXER_LIVE_WALLETS_PER_SCAN} wallets_per_scan_min=${INDEXER_LIVE_WALLETS_PER_SCAN_MIN} wallets_per_scan_max=${INDEXER_LIVE_WALLETS_PER_SCAN_MAX} live_refresh_target_ms=${INDEXER_LIVE_REFRESH_TARGET_MS} live_refresh_concurrency=${INDEXER_LIVE_REFRESH_CONCURRENCY} live_max_pages_per_wallet=${INDEXER_LIVE_MAX_PAGES_PER_WALLET}`
      );
      console.log(
        `[pacifica-flow] indexer_history_mode=${INDEXER_FULL_HISTORY_PER_WALLET ? "full_history" : "capped"} max_pages_per_wallet=${INDEXER_MAX_PAGES_PER_WALLET} full_history_pages_per_scan=${INDEXER_FULL_HISTORY_PAGES_PER_SCAN} deep_history_pages_per_scan=${INDEXER_DEEP_HISTORY_PAGES_PER_SCAN} deep_history_trades_page_limit=${INDEXER_DEEP_HISTORY_TRADES_PAGE_LIMIT} deep_history_funding_page_limit=${INDEXER_DEEP_HISTORY_FUNDING_PAGE_LIMIT}`
      );
      console.log(
        `[pacifica-flow] indexer_activation trades_page_limit=${INDEXER_ACTIVATION_TRADES_PAGE_LIMIT} funding_page_limit=${INDEXER_ACTIVATION_FUNDING_PAGE_LIMIT} reserve_min=${INDEXER_ACTIVATION_RESERVE_MIN} reserve_max=${INDEXER_ACTIVATION_RESERVE_MAX} continuation_page_budget=${INDEXER_BACKFILL_PAGE_BUDGET_WHEN_LIVE_PRESSURE} deep_backfill_share_cap=${INDEXER_DEEP_BACKFILL_MAX_SHARE_PER_PROXY} deep_backfill_tasks_cap=${INDEXER_DEEP_BACKFILL_MIN_TASKS_PER_PROXY}-${INDEXER_DEEP_BACKFILL_MAX_TASKS_PER_PROXY}`
      );
      console.log(
        `[pacifica-flow] indexer_backlog_mode=${INDEXER_BACKLOG_MODE_ENABLED ? "enabled" : "disabled"} backlog_wallets_threshold=${INDEXER_BACKLOG_WALLETS_THRESHOLD} backlog_avg_wait_ms_threshold=${INDEXER_BACKLOG_AVG_WAIT_MS_THRESHOLD} backlog_discover_every_cycles=${INDEXER_BACKLOG_DISCOVER_EVERY_CYCLES} backlog_refill_batch=${INDEXER_BACKLOG_REFILL_BATCH}`
      );
      console.log(
        `[pacifica-flow] indexer_cache_entries_per_endpoint=${INDEXER_CACHE_ENTRIES_PER_ENDPOINT} state_save_min_interval_ms=${INDEXER_STATE_SAVE_MIN_INTERVAL_MS} scan_ramp_quiet_ms=${INDEXER_SCAN_RAMP_QUIET_MS} scan_ramp_step_ms=${INDEXER_SCAN_RAMP_STEP_MS}`
      );
      console.log(
        `[pacifica-flow] indexer_client_cooldown 429_base_ms=${INDEXER_CLIENT_429_COOLDOWN_BASE_MS} 429_max_ms=${INDEXER_CLIENT_429_COOLDOWN_MAX_MS} err_base_ms=${INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_BASE_MS} err_max_ms=${INDEXER_CLIENT_SERVER_ERROR_COOLDOWN_MAX_MS} timeout_base_ms=${INDEXER_CLIENT_TIMEOUT_COOLDOWN_BASE_MS} timeout_max_ms=${INDEXER_CLIENT_TIMEOUT_COOLDOWN_MAX_MS} default_ms=${INDEXER_CLIENT_DEFAULT_COOLDOWN_MS}`
      );
      console.log(
        `[pacifica-flow] snapshots=${SNAPSHOT_ENABLED ? "enabled" : "disabled"} refresh_ms=${SNAPSHOT_REFRESH_MS}`
      );
      console.log(
        `[pacifica-flow] indexer_runner=${INDEXER_RUNNER_ENABLED ? "enabled" : "disabled"}`
      );
    }
    console.log(
      `[pacifica-flow] global_kpi_worker=${GLOBAL_KPI_ENABLED ? "enabled" : "disabled"} method=${GLOBAL_KPI_VOLUME_METHOD} refresh_ms=${GLOBAL_KPI_REFRESH_MS} path=${GLOBAL_KPI_PATH}`
    );
    if (GLOBAL_KPI_ENABLED && GLOBAL_KPI_VOLUME_METHOD === "defillama_compat") {
      console.log(
        `[pacifica-flow] global_kpi_defillama start_date=${GLOBAL_KPI_DEFILLAMA_START_DATE} backfill_days_per_run=${GLOBAL_KPI_BACKFILL_DAYS_PER_RUN} symbol_concurrency=${GLOBAL_KPI_SYMBOL_CONCURRENCY} strict_daily=${GLOBAL_KPI_STRICT_DAILY} use_multi_egress=${GLOBAL_KPI_USE_MULTI_EGRESS} external_shards_only=${GLOBAL_KPI_EXTERNAL_SHARDS_ONLY} shard_clients=${globalKpiRestClientEntries.length || 1}`
      );
    }
    if (WALLET_SOURCE_URL) {
      console.log(`[pacifica-flow] indexer_source_url=${WALLET_SOURCE_URL}`);
    }
    if (INDEXER_ENABLED) {
      console.log(`[pacifica-flow] deposit_wallets_path=${DEPOSIT_WALLETS_PATH}`);
    }
    console.log(
      `[pacifica-flow] multi_egress=${MULTI_EGRESS_ENABLED ? "enabled" : "disabled"} clients=${indexerRestClientEntries.length} include_direct=${MULTI_EGRESS_INCLUDE_DIRECT} per_ip_rpm_cap=${MULTI_EGRESS_RPM_CAP_PER_IP} transport=${MULTI_EGRESS_TRANSPORT} proxy_file=${MULTI_EGRESS_PROXY_FILE}`
    );
    if (MULTI_EGRESS_ENABLED) {
      const proxyClients = indexerRestClientEntries.filter((row) => row.proxyUrl).length;
      console.log(
        `[pacifica-flow] multi_egress_proxies_loaded=${proxyClients} max_proxies=${MULTI_EGRESS_MAX_PROXIES || "all"} match_workers=${MULTI_EGRESS_MATCH_WORKERS}`
      );
    }
    console.log(
      `[pacifica-flow] onchain=${ONCHAIN_ENABLED ? "enabled" : "disabled"} rpc=${SOLANA_RPC_URL}`
    );
    console.log(
      `[pacifica-flow] helius=${HELIUS_API_KEYS.length > 0 ? "enabled" : "disabled"} keys=${HELIUS_API_KEYS.length} keys_file=${HELIUS_API_KEYS_FILE}`
    );
    console.log(
      `[pacifica-flow] solana_log_trigger=${
        solanaLogTriggerMonitor && solanaLogTriggerMonitor.getStatus().enabled ? "enabled" : "disabled"
      } ws=${solanaLogTriggerMonitor ? solanaLogTriggerMonitor.getStatus().wsUrl || "(none)" : "(none)"} trigger_file=${LIVE_WALLET_TRIGGER_FILE}`
    );
    if (ONCHAIN_ENABLED) {
      console.log(
        `[pacifica-flow] onchain_mode=${ONCHAIN_MODE} programs=${ONCHAIN_PROGRAM_IDS.join(",")} start_ms=${ONCHAIN_START_TIME_MS} end_ms=${ONCHAIN_END_TIME_MS || "(none)"}`
      );
      console.log(
        `[pacifica-flow] onchain_discovery_type=${ONCHAIN_DISCOVERY_TYPE} deposit_vaults=${ONCHAIN_DEPOSIT_VAULTS.join(",")}`
      );
      if (ONCHAIN_EXCLUDE_ADDRESSES.length) {
        console.log(
          `[pacifica-flow] onchain_exclude_addresses=${ONCHAIN_EXCLUDE_ADDRESSES.join(",")}`
        );
      }
      console.log(
        `[pacifica-flow] solana_rate_limit_capacity=${SOLANA_RPC_RATE_LIMIT_CAPACITY}/window_ms=${SOLANA_RPC_RATE_LIMIT_WINDOW_MS}`
      );
      console.log(
        `[pacifica-flow] solana_bucket global_capacity=${SOLANA_RPC_GLOBAL_BUCKET_CAPACITY} global_refill_per_sec=${SOLANA_RPC_GLOBAL_REFILL_PER_SEC} sig_refill_per_sec=${SOLANA_RPC_SIG_REFILL_PER_SEC} tx_refill_per_sec=${SOLANA_RPC_TX_REFILL_PER_SEC} sig_max_concurrency=${SOLANA_RPC_SIG_MAX_CONCURRENCY} tx_max_concurrency=${SOLANA_RPC_TX_MAX_CONCURRENCY}`
      );
      console.log(
        `[pacifica-flow] onchain_batch scan_interval_ms=${ONCHAIN_SCAN_INTERVAL_MS} signature_page_limit=${ONCHAIN_SIGNATURE_PAGE_LIMIT} scan_pages_per_cycle=${ONCHAIN_SCAN_PAGES_PER_CYCLE} scan_pages_max=${ONCHAIN_SCAN_PAGES_MAX} tx_concurrency=${ONCHAIN_TX_CONCURRENCY} max_tx_per_cycle=${ONCHAIN_MAX_TX_PER_CYCLE} pending_max_attempts=${ONCHAIN_PENDING_MAX_ATTEMPTS}`
      );
      console.log(`[pacifica-flow] onchain_log_format=${ONCHAIN_LOG_FORMAT}`);
      console.log(
        `[pacifica-flow] onchain_backoff base_429_delay_ms=${SOLANA_RPC_429_BASE_DELAY_MS} max_429_delay_ms=${SOLANA_RPC_429_MAX_DELAY_MS} ramp_quiet_ms=${SOLANA_RPC_RAMP_QUIET_MS}`
      );
      console.log(
        `[pacifica-flow] onchain_runner=${ONCHAIN_RUNNER_ENABLED ? "enabled" : "disabled"} interval_ms=${ONCHAIN_RUNNER_INTERVAL_MS}`
      );
    }
  });

  async function runOnchainStep() {
    if (!onchainDiscovery || typeof onchainDiscovery.discoverStep !== "function") return;
    const result = await onchainDiscovery.discoverStep({
      pages: ONCHAIN_SCAN_PAGES_PER_CYCLE,
      validateLimit: ONCHAIN_VALIDATE_BATCH,
    });
    if (!result || result.ok !== true) {
      const reason = result && result.error ? String(result.error) : "unknown_error";
      if (reason === "discovery already running") {
        return;
      }
      console.warn(
        `[onchain-runner] step failed: ${reason}`
      );
      return;
    }
    const progress =
      result.progress && result.progress.pct !== undefined && result.progress.pct !== null
        ? result.progress.pct
        : "n/a";
    console.log(
      `[onchain-runner] step ok scanned_programs=${result.scannedPrograms || 0} new_wallets=${(result.newWallets || []).length} pending_tx=${result.pendingTransactions || 0} progress_pct=${progress}`
    );
    if (
      walletTrackingComponent &&
      typeof walletTrackingComponent.ingestWalletTrigger === "function" &&
      Array.isArray(result.newWallets)
    ) {
      const triggerAt = Date.now();
      result.newWallets.forEach((wallet) => {
        if (!wallet) return;
        walletTrackingComponent.ingestWalletTrigger({
          wallet,
          at: triggerAt,
          source: "onchain_discovery",
          reason: "onchain_discovery",
        });
      });
    }
  }

  let startupWarmupPromise = null;
  let backgroundServicesStarted = false;

  async function runStartupWarmupStep(stepName, fn, successPatch = null) {
    const startedAt = Date.now();
    setStartupStep(stepName, {
      status: "loading",
      startedAt,
      finishedAt: null,
      error: null,
    });
    try {
      const result = await fn();
      const patch =
        typeof successPatch === "function"
          ? successPatch(result)
          : successPatch && typeof successPatch === "object"
          ? successPatch
          : {};
      setStartupStep(stepName, {
        status: "ready",
        finishedAt: Date.now(),
        error: null,
        ...patch,
      });
      return result;
    } catch (error) {
      const message = error && error.message ? error.message : String(error || "startup_step_failed");
      startupState.lastError = message;
      setStartupStep(stepName, {
        status: "error",
        finishedAt: Date.now(),
        error: message,
      });
      console.warn(`[startup] ${stepName} failed: ${message}`);
      return null;
    }
  }

  async function runStartupWarmup() {
    if (startupWarmupPromise) return startupWarmupPromise;
    startupState.phase = "warming";
    startupWarmupPromise = (async () => {
      if (!STARTUP_BLOCKING_WARMUP_ENABLED) {
        startupState.phase = "ready";
        startupState.readyAt = Date.now();
        return getStartupStatus();
      }
      const uiReadModelOnly = UI_READ_MODEL_ONLY_MODE;

      await runStartupWarmupStep(
        "pipeline",
        () => (uiReadModelOnly ? Promise.resolve({ skipped: true, mode: "deferred" }) : pipeline.load({ skipReplay: PIPELINE_SKIP_REPLAY })),
        (result) => ({
          mode: result && result.mode ? result.mode : PIPELINE_SKIP_REPLAY ? "snapshot_only" : "replay",
          replayed: Number((result && result.replayed) || 0) || 0,
          skipped: Boolean(result && result.skipped),
          skippedReplay: PIPELINE_SKIP_REPLAY,
          deferred: uiReadModelOnly,
        })
      );

      if (!uiReadModelOnly && ACCOUNT_ENV_PRESENT) {
        pipeline.setAccount(INITIAL_ACCOUNT || null);
      } else if (!uiReadModelOnly && INDEXER_ENABLED) {
        pipeline.setAccount(null);
      }

      await runStartupWarmupStep(
        "collectionStore",
        () => (uiReadModelOnly ? Promise.resolve(null) : collectionStore.load()),
        { deferred: uiReadModelOnly }
      );
      await runStartupWarmupStep(
        "walletStore",
        () => {
          const slimUiMode = !INDEXER_ENABLED && walletStore && walletStore.externalShardsEnabled;
          if (!slimUiMode) {
            return walletStore.load();
          }
          return walletTrackingReadModel.getDefaultWalletPage();
        },
        (result) => {
          const slimUiMode = !INDEXER_ENABLED && walletStore && walletStore.externalShardsEnabled;
          const previewRows = result && Array.isArray(result.rows) ? result.rows.length : null;
          return {
            walletCount: slimUiMode
              ? Number(previewRows || 0)
              : walletStore && walletStore.data && walletStore.data.wallets
              ? Object.keys(walletStore.data.wallets).length
              : 0,
            deferred: slimUiMode,
            source: slimUiMode ? "wallets_preview_snapshot" : "wallet_store",
          };
        }
      );
      await runStartupWarmupStep(
        "minimalUiDataset",
        () => (ENABLE_UI && MINIMAL_UI_MODE ? refreshMinimalUiDataset(true) : Promise.resolve(null)),
        (result) => ({
          walletCount:
            result && result.wallets && Number.isFinite(Number(result.wallets.total))
              ? Number(result.wallets.total)
              : 0,
          generatedAt:
            result && Number.isFinite(Number(result.generatedAt))
              ? Number(result.generatedAt)
              : null,
          deferred: !(ENABLE_UI && MINIMAL_UI_MODE),
        })
      );
      await runStartupWarmupStep(
        "walletMetricsHistoryStore",
        () => (uiReadModelOnly ? Promise.resolve(null) : walletMetricsHistoryStore.load()),
        { deferred: uiReadModelOnly }
      );
      await runStartupWarmupStep(
        "positionLifecycleStore",
        () => (uiReadModelOnly ? Promise.resolve(null) : positionLifecycleStore.load()),
        { deferred: uiReadModelOnly }
      );

      if (!uiReadModelOnly && onchainDiscovery && typeof onchainDiscovery.load === "function") {
        await runStartupWarmupStep("onchainState", () => onchainDiscovery.load());
      }
      if (!uiReadModelOnly && walletIndexer && typeof walletIndexer.load === "function") {
        await runStartupWarmupStep("walletIndexerState", () => walletIndexer.load());
      }

      const errored = Object.values(startupState.steps || {}).some(
        (row) => row && row.status === "error"
      );
      startupState.phase = errored ? "degraded" : "ready";
      startupState.readyAt = Date.now();
      return getStartupStatus();
    })();
    return startupWarmupPromise;
  }

  async function startBackgroundServices() {
    if (backgroundServicesStarted) return;
    backgroundServicesStarted = true;
    const uiReadModelOnly = UI_READ_MODEL_ONLY_MODE;

    if (uiReadModelOnly) {
      const snapshotPrewarmTimer = setTimeout(() => {
        try {
          walletTrackingReadModel.preload();
        } catch (error) {
          console.warn(`[wallet-snapshot] prewarm failed: ${error.message}`);
        }
      }, 250);
      if (snapshotPrewarmTimer && typeof snapshotPrewarmTimer.unref === "function") {
        snapshotPrewarmTimer.unref();
      }
    }

    if (LOCAL_RUNTIME_STATUS_PUBLISH_ENABLED) {
      runtimeStatusLoop.start(RUNTIME_STATUS_PUBLISH_MS);
      try {
        publishRuntimeStatus();
      } catch (error) {
        console.warn(`[runtime-status] initial publish failed: ${error.message}`);
      }
    }

    if (!uiReadModelOnly && ENABLE_LIVE && WS_ENABLED) {
      liveHost.start();
    }

    if (
      !uiReadModelOnly &&
      ENABLE_API &&
      solanaLogTriggerMonitor &&
      solanaLogTriggerMonitor.getStatus().enabled
    ) {
      solanaLogTriggerMonitor.start();
    }

    if (SNAPSHOT_ENABLED) {
      snapshotLoop.start(SNAPSHOT_REFRESH_MS);
      try {
        await refreshSnapshots();
      } catch (error) {
        console.warn(`[bootstrap] initial refresh failed: ${error.message}`);
      }
    } else {
      console.log("[snapshot-refresh] skipped (PACIFICA_SNAPSHOT_ENABLED=false)");
    }

    if (GLOBAL_KPI_ENABLED) {
      globalKpiLoop.start(GLOBAL_KPI_REFRESH_MS);
      try {
        const data = await refreshGlobalKpi();
        const meta = data && data.volumeMeta && typeof data.volumeMeta === "object" ? data.volumeMeta : {};
        console.log(
          `[global-kpi] fetched daily_volume=${Number(data.dailyVolume || 0).toFixed(
            2
          )} cumulative_volume=${Number(data.totalHistoricalVolume || 0).toFixed(
            2
          )} tracked_to=${meta.lastProcessedDate || "n/a"} remaining_days=${Number(
            meta.remainingDaysToToday !== undefined ? meta.remainingDaysToToday : NaN
          ).toString()} oi=${Number(data.openInterestAtEnd || 0).toFixed(2)} fetched_at=${new Date(
            Number(data.fetchedAt || Date.now())
          ).toISOString()}`
        );
      } catch (error) {
        console.warn(`[global-kpi] initial refresh failed: ${error.message}`);
      }
    }

    if (ENABLE_API) {
      if (!uiReadModelOnly && POSITION_LIFECYCLE_LOOP_ENABLED) {
        positionLifecycleLoop.start(
          Math.max(5000, Number(process.env.PACIFICA_POSITION_LIFECYCLE_INGEST_MS || 10000))
        );
      }
      if (!uiReadModelOnly && WALLET_METRICS_HISTORY_LOOP_ENABLED) {
        walletMetricsHistoryLoop.start(
          Math.max(60000, Number(process.env.PACIFICA_WALLET_METRICS_HISTORY_CAPTURE_MS || 300000))
        );
      }

      if (INDEXER_ENABLED && INDEXER_RUNNER_ENABLED) {
        try {
          await walletIndexer.start();
        } catch (error) {
          console.warn(`[wallet-indexer] start failed: ${error.message}`);
        }
      }

      if (ONCHAIN_ENABLED && ONCHAIN_RUNNER_ENABLED) {
        try {
          await runOnchainStep();
          onchainLoop.start(ONCHAIN_RUNNER_INTERVAL_MS);
        } catch (error) {
          console.warn(`[onchain-runner] start failed: ${error.message}`);
        }
      }

      {
        const defaultWalletPreviewPath = path.join(DATA_ROOT, "ui", "wallets_page1_default.json");
        const shouldPrewarmWalletTracking =
          STARTUP_COMPONENT_WARMUP_ENABLED &&
          (!uiReadModelOnly || !fs.existsSync(defaultWalletPreviewPath));
        if (shouldPrewarmWalletTracking) {
          setTimeout(() => {
            try {
              nextgenRuntime.start();
              getWalletTrackingNextgenComponent();
              getWalletTrackingComponent();
            } catch (error) {
              console.warn(`[wallet-tracking] deferred init failed: ${error.message}`);
            }
          }, Math.max(50, ENABLE_UI && !INDEXER_RUNNER_ENABLED ? 100 : 50));
        }
      }

      if (!uiReadModelOnly && STARTUP_ANALYTICS_WARMUP_ENABLED) {
        const initialAnalyticsWarmupDelayMs =
          ENABLE_UI && !INDEXER_RUNNER_ENABLED ? 10000 : 1000;
        setTimeout(() => {
          ingestPositionLifecycle(true).catch((error) => {
            console.warn(`[position-lifecycle] initial ingest failed: ${error.message}`);
          });
        }, initialAnalyticsWarmupDelayMs);
        setTimeout(() => {
          captureWalletMetricsHistory(true).catch((error) => {
            console.warn(`[wallet-metrics-history] initial capture failed: ${error.message}`);
          });
        }, initialAnalyticsWarmupDelayMs + 1000);
      }

      if (copyTradingExecutionLoop) {
        copyTradingExecutionLoop.start(
          Math.max(3000, Number(process.env.PACIFICA_COPY_EXECUTION_POLL_MS || 5000))
        );
        try {
          await copyTradingExecutionWorker.runCycle();
        } catch (error) {
          console.warn(`[copy-trading-execution] initial cycle failed: ${error.message}`);
        }
      }
    }

  }

  (async () => {
    await runStartupWarmup();
    await startBackgroundServices();
  })();

  if (!UI_READ_MODEL_ONLY_MODE) {
    apiUsageTimer = setInterval(() => {
      const rateState = rateGuard.getState();
      console.log(
        `[pacifica-api] pacifica_api_rpm_used=${rateState.used1m} pacifica_api_rpm_cap=${rateState.rpmCap} tokens=${rateState.tokens} pause_ms=${rateState.pauseRemainingMs}`
      );
      if (indexerRestClientEntries.length > 1) {
        const egress = buildEgressUsageSnapshot(indexerRestClientEntries);
        console.log(
          `[pacifica-egress] clients=${egress.clients} active=${egress.active} rpm_used_total=${egress.rpmUsedTotal.toFixed(1)} rpm_cap_total=${egress.rpmCapTotal.toFixed(1)} top=${(egress.top || []).join(",") || "none"}`
        );
      }
    }, API_USAGE_LOG_INTERVAL_MS);
  }

  function shutdown() {
    if (snapshotLoop) snapshotLoop.stop();
    if (apiUsageTimer) clearInterval(apiUsageTimer);
    if (onchainLoop) onchainLoop.stop();
    if (globalKpiLoop) globalKpiLoop.stop();
    if (walletMetricsHistoryLoop) walletMetricsHistoryLoop.stop();
    if (positionLifecycleLoop) positionLifecycleLoop.stop();
    if (runtimeStatusLoop) runtimeStatusLoop.stop();
    if (copyTradingExecutionLoop) copyTradingExecutionLoop.stop();

    try {
      if (ENABLE_LIVE && WS_ENABLED) liveHost.stop();
      if (ENABLE_API && INDEXER_ENABLED && INDEXER_RUNNER_ENABLED) walletIndexer.stop();
      if (solanaLogTriggerMonitor) solanaLogTriggerMonitor.stop();
      if (walletMetricsHistoryStore) walletMetricsHistoryStore.stop();
      if (positionLifecycleStore) positionLifecycleStore.stop();
      nextgenRuntime.stop();
      if (LOCAL_RUNTIME_STATUS_PUBLISH_ENABLED && runtimeStatusStore) publishRuntimeStatus();
      pipeline.flushAll();
    } catch (_error) {
      // ignore
    }

    if (runtimeStatusStore) runtimeStatusStore.remove();

    server.close(() => {
      process.exit(0);
    });

    setTimeout(() => process.exit(0), 1800).unref();
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((error) => {
  console.error(`[pacifica-flow] startup failed: ${error.message}`);
  if (error && error.stack) {
    console.error(error.stack);
  }
  process.exit(1);
});
