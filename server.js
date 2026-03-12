const fs = require("fs");
const http = require("http");
const path = require("path");
const { URL } = require("url");

const { createGeneralDataComponent } = require("./src/components/general-data");
const { createWalletTrackingComponent } = require("./src/components/wallet-tracking");
const { createCreatorStudioComponent } = require("./src/components/creator-studio");
const { createLiveTradesHostComponent } = require("./src/components/live-trades-host");

const { createClockSync } = require("./src/services/transport/clock_sync");
const { createRateLimitGuard } = require("./src/services/transport/rate_limit_guard");
const { createRestClient } = require("./src/services/transport/rest_client");
const { createRetryPolicy } = require("./src/services/transport/retry_policy");
const { createSigner } = require("./src/services/transport/signer");
const { createWsClient } = require("./src/services/transport/ws_client");
const { createSolanaRpcClient } = require("./src/services/indexer/solana_rpc_client");
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
const { LiveWalletTriggerStore } = require("./src/services/analytics/live_wallet_trigger_store");
const { WalletMetricsHistoryStore } = require("./src/services/analytics/wallet_metrics_history_store");
const { PositionLifecycleStore } = require("./src/services/analytics/position_lifecycle_store");

const PORT = Number(process.env.PORT || 3200);
const HOST = process.env.HOST || "0.0.0.0";
const SERVICE_RAW = String(process.env.PACIFICA_SERVICE || "all").toLowerCase();
const VALID_SERVICES = new Set(["all", "api", "ui", "live"]);
const SERVICE = VALID_SERVICES.has(SERVICE_RAW) ? SERVICE_RAW : "all";

const ENABLE_API = SERVICE !== "ui";
const ENABLE_UI = SERVICE === "all" || SERVICE === "ui";
const ENABLE_LIVE = SERVICE === "all" || SERVICE === "api" || SERVICE === "live";

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
const PIPELINE_PERSIST_EVERY_MS = Math.max(
  1000,
  Number(
    process.env.PACIFICA_PIPELINE_PERSIST_EVERY_MS ||
      (!INDEXER_ENABLED && !ONCHAIN_ENABLED ? 15000 : 5000)
  )
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
const RATE_LIMIT_CAPACITY = Math.max(
  0.1,
  Number(((API_RPM_CAP * RATE_LIMIT_WINDOW_MS) / 60000).toFixed(3))
);
const API_USAGE_LOG_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_API_USAGE_LOG_INTERVAL_MS || 15000)
);
const MULTI_EGRESS_ENABLED =
  String(process.env.PACIFICA_MULTI_EGRESS_ENABLED || "false").toLowerCase() === "true";
const MULTI_EGRESS_PROXY_FILE = process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE
  ? path.resolve(process.env.PACIFICA_MULTI_EGRESS_PROXY_FILE)
  : path.join(__dirname, "data", "indexer", "working_proxies.txt");
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
  String(process.env.PACIFICA_MULTI_EGRESS_TRANSPORT || "curl").toLowerCase() === "fetch"
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
const SOLANA_RPC_RATE_LIMIT_CAPACITY = Math.max(
  1,
  Number(process.env.SOLANA_RPC_RATE_LIMIT_CAPACITY || 3)
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
  Math.min(16, Number(process.env.PACIFICA_INDEXER_WALLET_SCAN_CONCURRENCY || 4))
);
const INDEXER_LIVE_WALLETS_PER_SCAN = Math.max(
  0,
  Math.min(256, Number(process.env.PACIFICA_INDEXER_LIVE_WALLETS_PER_SCAN || 10))
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
  Math.min(1024, Number(process.env.PACIFICA_INDEXER_LIVE_WALLETS_PER_SCAN_MAX || 256))
);
const INDEXER_LIVE_REFRESH_TARGET_MS = Math.max(
  5000,
  Number(process.env.PACIFICA_INDEXER_LIVE_REFRESH_TARGET_MS || 90000)
);
const INDEXER_LIVE_MAX_PAGES_PER_WALLET = Math.max(
  1,
  Math.min(50, Number(process.env.PACIFICA_INDEXER_LIVE_MAX_PAGES_PER_WALLET || 1))
);
const INDEXER_FULL_HISTORY_PAGES_PER_SCAN = Math.max(
  1,
  Number(process.env.PACIFICA_INDEXER_FULL_HISTORY_PAGES_PER_SCAN || 12)
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

const INLINE_WALLET_SEEDS = parseWalletSeedList(process.env.PACIFICA_WALLET_SEEDS || "");

const DATA_ROOT = process.env.PACIFICA_DATA_DIR
  ? path.resolve(process.env.PACIFICA_DATA_DIR)
  : path.join(__dirname, "data");
const PIPELINE_DATA_DIR = path.join(DATA_ROOT, "pipeline");
const CREATOR_DATA_DIR = path.join(DATA_ROOT, "creator");
const INDEXER_DATA_DIR = path.join(DATA_ROOT, "indexer");
const LIVE_POSITIONS_DATA_DIR = path.join(DATA_ROOT, "live_positions");
const LIVE_WALLET_TRIGGER_FILE = String(
  process.env.PACIFICA_LIVE_WALLET_TRIGGER_FILE ||
    path.join(LIVE_POSITIONS_DATA_DIR, "wallet_activity_triggers.ndjson")
).trim();

const SNAPSHOT_REFRESH_MS = Math.max(
  10000,
  Number(process.env.PACIFICA_SNAPSHOT_REFRESH_MS || 45000)
);
const SNAPSHOT_ENABLED =
  String(process.env.PACIFICA_SNAPSHOT_ENABLED || "true").toLowerCase() !== "false";
const GLOBAL_KPI_ENABLED =
  String(process.env.PACIFICA_GLOBAL_KPI_ENABLED || "true").toLowerCase() !== "false";
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
  process.env.PACIFICA_GLOBAL_KPI_DEFILLAMA_START_DATE || "2025-09-09"
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
const GLOBAL_KPI_USE_MULTI_EGRESS =
  String(process.env.PACIFICA_GLOBAL_KPI_USE_MULTI_EGRESS || "false").toLowerCase() === "true";
const GLOBAL_KPI_DEFAULT_REFRESH_MS =
  GLOBAL_KPI_VOLUME_METHOD === "defillama_compat" ? 300000 : 15000;
const GLOBAL_KPI_REFRESH_MS = Math.max(
  GLOBAL_KPI_VOLUME_METHOD === "defillama_compat" ? 60000 : 5000,
  Number(process.env.PACIFICA_GLOBAL_KPI_REFRESH_MS || GLOBAL_KPI_DEFAULT_REFRESH_MS)
);
const GLOBAL_KPI_PATH = process.env.PACIFICA_GLOBAL_KPI_PATH
  ? path.resolve(process.env.PACIFICA_GLOBAL_KPI_PATH)
  : path.join(PIPELINE_DATA_DIR, "global_kpi.json");
const GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION = 7;

const PUBLIC_DIR = path.join(__dirname, "public");

function sendJson(res, statusCode, payload) {
  const data = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "Content-Length": Buffer.byteLength(data),
  });
  res.end(data);
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

function computeDefiLlamaV2FromPrices(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce(
    (acc, item) => {
      const volume24h = Number(item && item.volume_24h !== undefined ? item.volume_24h : 0);
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
      const volume24h = Number(item && item.volume_24h !== undefined ? item.volume_24h : 0);
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
    if (!row || !hasSymbolVolumes(row)) return day;
    cursor += ONE_DAY_MS;
  }
  return null;
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

async function fetchPacificaMarkets(restClient) {
  const infoRes = await restClient.get("/info", { cost: 1 });
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
          },
          cost: 3,
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
          // DeFiLlama-compatible historical daily volume (USD) by symbol/day candle.
          const symbolVolume = v * c;
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
    safePath === "app.js" || safePath === "styles.css" || safePath === "theme.css";
  const isLiveTradeRuntimeAsset =
    safePath.startsWith("live-trade/") && (ext === ".js" || ext === ".css");
  const cacheControl = isHtml || isRootRuntimeAsset || isLiveTradeRuntimeAsset
    ? "no-store"
    : "public, max-age=604800, stale-while-revalidate=86400";
  res.writeHead(200, {
    "Content-Type": contentTypeFor(filePath),
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

  const clockSync = createClockSync();
  const rateGuard = createRateLimitGuard({
    capacity: RATE_LIMIT_CAPACITY,
    refillWindowMs: RATE_LIMIT_WINDOW_MS,
  });

  const retryPolicy = createRetryPolicy({
    maxAttempts: 4,
    baseDelayMs: 200,
    maxDelayMs: 3000,
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
  const effectiveWalletScanConcurrency =
    MULTI_EGRESS_MATCH_WORKERS && indexerRestClients.length > 1
      ? Math.max(INDEXER_WALLET_SCAN_CONCURRENCY, indexerRestClients.length)
      : INDEXER_WALLET_SCAN_CONCURRENCY;
  const effectiveIndexerBatchSize =
    MULTI_EGRESS_MATCH_WORKERS && indexerRestClients.length > 1
      ? Math.max(INDEXER_BATCH_SIZE, effectiveWalletScanConcurrency)
      : INDEXER_BATCH_SIZE;
  const globalKpiRestClient =
    GLOBAL_KPI_USE_MULTI_EGRESS && (indexerRoundRobinRestClient || restClient)
      ? indexerRoundRobinRestClient || restClient
      : restClient;

  let globalKpiState = readJson(GLOBAL_KPI_PATH, null) || null;
  let globalKpiRefreshInFlight = null;

  async function doRefreshGlobalKpi() {
    const startedAt = Date.now();
    const nowMs = Date.now();
    const todayStartMs = getUtcStartOfDayMs(nowMs);
    const todayUtcDate = formatUtcDate(todayStartMs);

    const res = await globalKpiRestClient.get("/info/prices", { cost: 1 });
    const payload = res && res.payload ? res.payload : {};
    const rows = Array.isArray(payload.data) ? payload.data : [];
    const totalsFromPrices = computeDefiLlamaV2FromPrices(rows);
    const dailyVolumeFromPrices24h = Number(totalsFromPrices.dailyVolume || 0);
    let dailyVolume = dailyVolumeFromPrices24h;
    let totalHistoricalVolume = dailyVolumeFromPrices24h;
    let dailyVolumeDefillamaCompat = null;
    let volumeSource = "/api/v1/info/prices:sum(volume_24h)";
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
      const canScaleLegacyCompatHistory =
        resetForCompatUpgrade &&
        !resetForStartDateChange &&
        previousCompatVersion === 6 &&
        GLOBAL_KPI_DEFILLAMA_COMPAT_VERSION === 7 &&
        prevHistory.dailyByDate &&
        typeof prevHistory.dailyByDate === "object" &&
        !Array.isArray(prevHistory.dailyByDate);
      if (resetForCompatUpgrade && !canScaleLegacyCompatHistory) {
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
      if (
        !resetForCompatUpgrade &&
        !resetForStartDateChange &&
        prevHistory.dailyByDate &&
        typeof prevHistory.dailyByDate === "object" &&
        !Array.isArray(prevHistory.dailyByDate)
      ) {
        dailyByDate = { ...prevHistory.dailyByDate };
      } else if (canScaleLegacyCompatHistory) {
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
      }

      const markets = await fetchPacificaMarkets(globalKpiRestClient);
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
          volumeSource: "/api/v1/kline?interval=1d&start_time=<utc_day_start>:sum(v*c)",
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
          volumeRank: buildDefiLlamaVolumeRank(rows, Array.isArray(rows) ? rows.length : 0),
          history: nextHistoryState,
          updatedAt: Date.now(),
        };
        ensureDir(path.dirname(GLOBAL_KPI_PATH));
        writeJsonAtomic(GLOBAL_KPI_PATH, globalKpiState);
      };

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

      const processedDates = [];
      for (const dayMs of daysToProcessMs) {
        const dayDate = formatUtcDate(dayMs);
        const dayEndMs = dayMs + ONE_DAY_MS - 1;
        const daySymbols = markets
          .filter((market) => {
            const listedAt = Number(market && market.createdAtMs);
            if (!Number.isFinite(listedAt)) return true;
            return listedAt <= dayEndMs;
          })
          .map((market) => market.symbol)
          .filter(Boolean);
        const compat = await computeDefiLlamaCompatDailyVolume({
          restClient: globalKpiRestClient,
          symbols: daySymbols,
          startOfDayMs: dayMs,
          symbolConcurrency: GLOBAL_KPI_SYMBOL_CONCURRENCY,
          logger: console,
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
        "/api/v1/kline?interval=1d&start_time=<utc_day_start>:sum(v*c)";
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
  const solanaRpcClient = createSolanaRpcClient({
    rpcUrl: SOLANA_RPC_URL,
    retryPolicy: solanaRetryPolicy,
    rateLimitGuard: solanaRateGuard,
    adaptive: {
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
    },
    logger: console,
  });
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
    rpcUrl: deriveRpcWsUrl(process.env.SOLANA_WS_URL || SOLANA_RPC_URL),
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

  const pipeline = new PacificaPipelineService({
    dataDir: PIPELINE_DATA_DIR,
    apiBase: API_BASE,
    wsUrl: WS_URL,
    serviceName: SERVICE,
    persistEveryMs: PIPELINE_PERSIST_EVERY_MS,
  });

  await pipeline.load();

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
  collectionStore.load();

  const walletStore = new WalletExplorerStore({
    dataDir: INDEXER_DATA_DIR,
    filePath: path.join(INDEXER_DATA_DIR, "wallets.json"),
  });
  walletStore.load();
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
  walletMetricsHistoryStore.load();
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
  positionLifecycleStore.load();

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
        })),
        walletStore,
        walletSource,
        onchainDiscovery,
        dataDir: INDEXER_DATA_DIR,
        statePath: path.join(INDEXER_DATA_DIR, "indexer_state.json"),
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
        liveWalletsPerScan: INDEXER_LIVE_WALLETS_PER_SCAN,
        liveWalletsPerScanMin: INDEXER_LIVE_WALLETS_PER_SCAN_MIN,
        liveWalletsPerScanMax: INDEXER_LIVE_WALLETS_PER_SCAN_MAX,
        liveRefreshTargetMs: INDEXER_LIVE_REFRESH_TARGET_MS,
        liveMaxPagesPerWallet: INDEXER_LIVE_MAX_PAGES_PER_WALLET,
        fullHistoryPagesPerScan: INDEXER_FULL_HISTORY_PAGES_PER_SCAN,
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
        discoveryOnly: INDEXER_DISCOVERY_ONLY,
        onchainPagesPerDiscoveryCycle: ONCHAIN_SCAN_PAGES_PER_CYCLE,
        onchainPagesMaxPerCycle: ONCHAIN_SCAN_PAGES_MAX,
        onchainValidatePerCycle: INDEXER_DISCOVERY_ONLY ? 0 : ONCHAIN_VALIDATE_BATCH,
        logger: console,
      })
    : null;
  if (walletIndexer) walletIndexer.load();

  const liveHost = createLiveTradesHostComponent({
    wsClient,
    pipeline,
    account: pipeline.getAccount(),
    logger: console,
  });

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

  const generalDataComponent = createGeneralDataComponent({
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

  const walletTrackingComponent = createWalletTrackingComponent({
    sendJson,
    pipeline,
    walletStore,
    walletIndexer,
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
  });

  const creatorStudioComponent = createCreatorStudioComponent({
    sendJson,
    readJsonBody,
    collectionStore,
    pipeline,
  });

  pipeline.setTransportState({
    apiBase: API_BASE,
    wsUrl: WS_URL,
    wsStatus: WS_ENABLED && ENABLE_LIVE ? "connecting" : "disabled",
  });

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    try {
      if (ENABLE_API) {
        if (await generalDataComponent.handleRequest(req, res, url)) return;
        if (await walletTrackingComponent.handleRequest(req, res, url)) return;
        if (await creatorStudioComponent.handleRequest(req, res, url)) return;
      }

      if (serveStatic(req, res, url)) return;

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

  let snapshotTimer = null;
  let apiUsageTimer = null;
  let onchainTimer = null;
  let globalKpiTimer = null;
  let walletMetricsHistoryTimer = null;
  let positionLifecycleTimer = null;

  async function captureWalletMetricsHistory(force = false) {
    if (
      !ENABLE_API ||
      !walletMetricsHistoryStore ||
      typeof walletTrackingComponent.getLiveWalletSnapshot !== "function"
    ) {
      return;
    }
    const snapshot = walletTrackingComponent.getLiveWalletSnapshot(400);
    walletMetricsHistoryStore.capture({
      walletRows: walletStore ? walletStore.list() : [],
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

  server.listen(PORT, HOST, () => {
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
    if (API_CONFIG_KEY) {
      console.log("[pacifica-flow] pf_api_key=present");
    }
    console.log(
      `[pacifica-flow] indexer=${INDEXER_ENABLED ? "enabled" : "disabled"} source_file=${WALLET_SOURCE_FILE}`
    );
    if (INDEXER_ENABLED) {
      console.log(
        `[pacifica-flow] indexer_mode=${INDEXER_DISCOVERY_ONLY ? "discovery_only" : "discover_and_scan"} wallet_scan_concurrency=${effectiveWalletScanConcurrency}`
      );
      console.log(`[pacifica-flow] indexer_batch_size=${effectiveIndexerBatchSize}`);
      console.log(
        `[pacifica-flow] indexer_live_mode wallets_per_scan=${INDEXER_LIVE_WALLETS_PER_SCAN} wallets_per_scan_min=${INDEXER_LIVE_WALLETS_PER_SCAN_MIN} wallets_per_scan_max=${INDEXER_LIVE_WALLETS_PER_SCAN_MAX} live_refresh_target_ms=${INDEXER_LIVE_REFRESH_TARGET_MS} live_max_pages_per_wallet=${INDEXER_LIVE_MAX_PAGES_PER_WALLET}`
      );
      console.log(
        `[pacifica-flow] indexer_history_mode=${INDEXER_FULL_HISTORY_PER_WALLET ? "full_history" : "capped"} max_pages_per_wallet=${INDEXER_MAX_PAGES_PER_WALLET} full_history_pages_per_scan=${INDEXER_FULL_HISTORY_PAGES_PER_SCAN}`
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
        `[pacifica-flow] global_kpi_defillama start_date=${GLOBAL_KPI_DEFILLAMA_START_DATE} backfill_days_per_run=${GLOBAL_KPI_BACKFILL_DAYS_PER_RUN} symbol_concurrency=${GLOBAL_KPI_SYMBOL_CONCURRENCY} strict_daily=${GLOBAL_KPI_STRICT_DAILY} use_multi_egress=${GLOBAL_KPI_USE_MULTI_EGRESS}`
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
        `[pacifica-flow] onchain_batch signature_page_limit=${ONCHAIN_SIGNATURE_PAGE_LIMIT} scan_pages_per_cycle=${ONCHAIN_SCAN_PAGES_PER_CYCLE} scan_pages_max=${ONCHAIN_SCAN_PAGES_MAX} tx_concurrency=${ONCHAIN_TX_CONCURRENCY} max_tx_per_cycle=${ONCHAIN_MAX_TX_PER_CYCLE} pending_max_attempts=${ONCHAIN_PENDING_MAX_ATTEMPTS}`
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

  (async () => {
    if (ENABLE_LIVE && WS_ENABLED) {
      liveHost.start();
    }

    if (ENABLE_API && solanaLogTriggerMonitor && solanaLogTriggerMonitor.getStatus().enabled) {
      solanaLogTriggerMonitor.start();
    }

    if (ENABLE_API && INDEXER_ENABLED && INDEXER_RUNNER_ENABLED) {
      try {
        await walletIndexer.start();
      } catch (error) {
        console.warn(`[wallet-indexer] start failed: ${error.message}`);
      }
    }

    if (ENABLE_API && ONCHAIN_ENABLED && ONCHAIN_RUNNER_ENABLED) {
      try {
        if (onchainDiscovery && typeof onchainDiscovery.load === "function") {
          onchainDiscovery.load();
        }
        await runOnchainStep();
      } catch (error) {
        console.warn(`[onchain-runner] start failed: ${error.message}`);
      }
    }

    if (SNAPSHOT_ENABLED) {
      try {
        await refreshSnapshots();
      } catch (error) {
        console.warn(`[bootstrap] initial refresh failed: ${error.message}`);
      }
    } else {
      console.log("[snapshot-refresh] skipped (PACIFICA_SNAPSHOT_ENABLED=false)");
    }

    if (GLOBAL_KPI_ENABLED) {
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
      try {
        await ingestPositionLifecycle(true);
      } catch (error) {
        console.warn(`[position-lifecycle] initial ingest failed: ${error.message}`);
      }
      try {
        await captureWalletMetricsHistory(true);
      } catch (error) {
        console.warn(`[wallet-metrics-history] initial capture failed: ${error.message}`);
      }
    }
  })();

  if (SNAPSHOT_ENABLED) {
    snapshotTimer = setInterval(async () => {
      try {
        await refreshSnapshots();
      } catch (error) {
        console.warn(`[snapshot-refresh] ${error.message}`);
      }
    }, SNAPSHOT_REFRESH_MS);
  }

  if (GLOBAL_KPI_ENABLED) {
    globalKpiTimer = setInterval(async () => {
      try {
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
      } catch (error) {
        console.warn(`[global-kpi] ${error.message}`);
      }
    }, GLOBAL_KPI_REFRESH_MS);
  }

  if (ENABLE_API) {
    positionLifecycleTimer = setInterval(async () => {
      try {
        await ingestPositionLifecycle(false);
      } catch (error) {
        console.warn(`[position-lifecycle] ${error.message}`);
      }
    }, Math.max(5000, Number(process.env.PACIFICA_POSITION_LIFECYCLE_INGEST_MS || 10000)));

    walletMetricsHistoryTimer = setInterval(async () => {
      try {
        await captureWalletMetricsHistory(false);
      } catch (error) {
        console.warn(`[wallet-metrics-history] ${error.message}`);
      }
    }, Math.max(60000, Number(process.env.PACIFICA_WALLET_METRICS_HISTORY_CAPTURE_MS || 300000)));
  }

  if (ENABLE_API && ONCHAIN_ENABLED && ONCHAIN_RUNNER_ENABLED) {
    onchainTimer = setInterval(async () => {
      try {
        await runOnchainStep();
      } catch (error) {
        console.warn(`[onchain-runner] ${error.message}`);
      }
    }, ONCHAIN_RUNNER_INTERVAL_MS);
  }

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

  function shutdown() {
    clearInterval(snapshotTimer);
    clearInterval(apiUsageTimer);
    clearInterval(onchainTimer);
    clearInterval(globalKpiTimer);
    clearInterval(walletMetricsHistoryTimer);
    clearInterval(positionLifecycleTimer);

    try {
      if (ENABLE_LIVE && WS_ENABLED) liveHost.stop();
      if (ENABLE_API && INDEXER_ENABLED && INDEXER_RUNNER_ENABLED) walletIndexer.stop();
      if (solanaLogTriggerMonitor) solanaLogTriggerMonitor.stop();
      if (walletMetricsHistoryStore) walletMetricsHistoryStore.stop();
      if (positionLifecycleStore) positionLifecycleStore.stop();
      pipeline.flushAll();
    } catch (_error) {
      // ignore
    }

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
