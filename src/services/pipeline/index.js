const path = require("path");
const { IdentityService } = require("./identity");
const { EventsService } = require("./events");
const { StateService } = require("./state");
const { MetricsService } = require("./metrics");
const {
  buildDashboardPayload,
  buildFundingHistoryPayload,
  buildOrdersHistoryPayload,
  buildTimelinePayload,
  buildTradesHistoryPayload,
} = require("./api");
const { ensureDir, nowMs } = require("./utils");

class PacificaPipelineService {
  constructor(options = {}) {
    this.dataDir = options.dataDir || path.join(process.cwd(), "data", "pipeline");
    ensureDir(this.dataDir);

    this.identity = new IdentityService({
      dataDir: this.dataDir,
      filePath: path.join(this.dataDir, "identity.json"),
    });

    this.events = new EventsService({
      dataDir: this.dataDir,
      eventsPath: path.join(this.dataDir, "events", "events.ndjson"),
      indexPath: path.join(this.dataDir, "events", "index.json"),
      flushEvery: 15,
    });

    this.state = new StateService({
      dataDir: this.dataDir,
      filePath: path.join(this.dataDir, "snapshots", "state.json"),
    });

    this.metrics = new MetricsService({
      dataDir: this.dataDir,
      filePath: path.join(this.dataDir, "snapshots", "metrics.json"),
    });

    this.transportState = {
      apiBase: options.apiBase || null,
      wsUrl: options.wsUrl || null,
      wsStatus: "idle",
    };

    this.serviceName = options.serviceName || "all";
    this.persistEveryMs = Math.max(200, Number(options.persistEveryMs || 1000));
    this.lastPersistAt = 0;
  }

  async load() {
    this.identity.load();
    this.events.load();
    this.state.load();
    this.metrics.load();

    const skipReplay =
      String(process.env.PACIFICA_PIPELINE_SKIP_REPLAY || "false").toLowerCase() === "true";
    if (skipReplay) {
      // Fast-start mode: trust persisted snapshots and avoid replaying large event logs.
      this.metrics.refresh(this.state.getState());
      this.persistMaybe(true);
      return {
        replayed: 0,
        lastEventId:
          this.events && this.events.index && Number.isFinite(Number(this.events.index.lastEventId))
            ? Number(this.events.index.lastEventId)
            : null,
        skipped: true,
        mode: "snapshot_only",
      };
    }

    const replay = await this.rebuildFromEventLog();
    this.flushAll();

    return replay;
  }

  async rebuildFromEventLog() {
    this.state.reset();
    this.metrics.reset();

    const replay = await this.events.replayEvents((entry) => {
      this.state.applyEvent(entry);
    });

    this.metrics.refresh(this.state.getState());

    return replay;
  }

  setAccount(address) {
    this.identity.setAccount(address);
    this.identity.save();
  }

  getAccount() {
    return this.identity.getAccount();
  }

  setTransportState(patch = {}) {
    this.transportState = {
      ...this.transportState,
      ...patch,
    };
  }

  recordEvent(event, options = {}) {
    const withTimestamp = {
      timestamp: nowMs(),
      ...event,
    };

    const appended = this.events.appendEvent(withTimestamp, options);
    if (!appended.inserted) return appended;

    const stateEvent = {
      ...withTimestamp,
      eventId: appended.eventId,
    };

    this.state.applyEvent(stateEvent);
    this.metrics.refresh(this.state.getState());
    this.persistMaybe();

    return appended;
  }

  persistMaybe(force = false) {
    const now = nowMs();
    if (!force && now - this.lastPersistAt < this.persistEveryMs) return;
    this.state.save();
    this.metrics.save();
    this.lastPersistAt = now;
  }

  recordSnapshot(snapshotType, data, meta = {}) {
    return this.recordEvent(
      {
        source: "rest",
        type: `snapshot.${snapshotType}`,
        data,
        ...meta,
      },
      {
        dedupeKey: `snapshot:${snapshotType}:${meta && meta.snapshotKey ? meta.snapshotKey : nowMs()}`,
      }
    );
  }

  updateWsStatus(status) {
    this.setTransportState({ wsStatus: status });
    this.recordEvent(
      {
        source: "system",
        type: "status.ws",
        data: { status },
      },
      {
        dedupeKey: `status.ws:${status}:${Math.floor(nowMs() / 1000)}`,
      }
    );
  }

  markBootstrap() {
    this.recordEvent(
      {
        source: "system",
        type: "status.bootstrap",
        data: { ok: true },
      },
      {
        dedupeKey: `status.bootstrap:${Math.floor(nowMs() / 1000)}`,
      }
    );
  }

  getState() {
    return this.state.getState();
  }

  getMetrics() {
    return this.metrics.getMetrics();
  }

  getTransportState() {
    return this.transportState;
  }

  getDashboardPayload() {
    return buildDashboardPayload({
      state: this.getState(),
      metrics: this.getMetrics(),
      account: this.getAccount(),
      transport: this.transportState,
      service: this.serviceName,
    });
  }

  getOrdersHistoryPayload(query = {}) {
    return buildOrdersHistoryPayload({
      state: this.getState(),
      query,
    });
  }

  getTradesHistoryPayload(query = {}) {
    return buildTradesHistoryPayload({
      state: this.getState(),
      query,
    });
  }

  getFundingHistoryPayload(query = {}) {
    return buildFundingHistoryPayload({
      state: this.getState(),
      query,
    });
  }

  getTimelinePayload(query = {}) {
    return buildTimelinePayload({
      state: this.getState(),
      query,
    });
  }

  flushAll() {
    this.events.flush();
    this.identity.save();
    this.persistMaybe(true);
  }
}

module.exports = {
  PacificaPipelineService,
};
