const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const {
  appendLine,
  ensureDir,
  nowMs,
  readJson,
  splitLines,
  writeJsonAtomic,
} = require("./utils");

const INDEX_VERSION = 1;
const MAX_DEDUPE_KEYS = 50000;

function emptyIndex() {
  return {
    version: INDEX_VERSION,
    lastEventId: 0,
    eventCount: 0,
    updatedAt: null,
    dedupeKeys: {},
  };
}

function hashKey(value) {
  return crypto.createHash("sha1").update(String(value)).digest("hex");
}

class EventsService {
  constructor(options = {}) {
    this.dataDir = options.dataDir;
    this.eventsPath =
      options.eventsPath || path.join(this.dataDir || ".", "events", "events.ndjson");
    this.indexPath =
      options.indexPath || path.join(this.dataDir || ".", "events", "index.json");
    this.index = emptyIndex();
    this.flushEvery = Math.max(1, Number(options.flushEvery || 20));
    this.unflushed = 0;
  }

  load() {
    ensureDir(path.dirname(this.eventsPath));
    ensureDir(path.dirname(this.indexPath));
    if (!fs.existsSync(this.eventsPath)) fs.writeFileSync(this.eventsPath, "");
    const loaded = readJson(this.indexPath, null);
    if (loaded && loaded.version === INDEX_VERSION) {
      this.index = {
        ...emptyIndex(),
        ...loaded,
        dedupeKeys: loaded.dedupeKeys || {},
      };
    } else {
      this.index = emptyIndex();
    }
    return this.index;
  }

  makeDedupeKey(event = {}, explicit = null) {
    if (explicit) return String(explicit);
    if (event && event.channel && event.li !== undefined && event.li !== null) {
      return `ws:${event.channel}:${event.li}`;
    }
    if (event && event.type && event.timestamp) {
      return `event:${event.type}:${event.timestamp}:${hashKey(JSON.stringify(event.data || {}))}`;
    }
    return `event:${hashKey(JSON.stringify(event || {}))}`;
  }

  appendEvent(event = {}, options = {}) {
    const dedupeKey = this.makeDedupeKey(event, options.dedupeKey);
    if (this.index.dedupeKeys[dedupeKey]) {
      return {
        inserted: false,
        reason: "duplicate",
        eventId: this.index.dedupeKeys[dedupeKey],
      };
    }

    const eventId = this.index.lastEventId + 1;
    const entry = {
      eventId,
      insertedAt: nowMs(),
      ...event,
    };

    appendLine(this.eventsPath, JSON.stringify(entry));

    this.index.lastEventId = eventId;
    this.index.eventCount += 1;
    this.index.updatedAt = nowMs();
    this.index.dedupeKeys[dedupeKey] = eventId;

    const keys = Object.keys(this.index.dedupeKeys);
    if (keys.length > MAX_DEDUPE_KEYS) {
      const overflow = keys.length - MAX_DEDUPE_KEYS;
      keys.slice(0, overflow).forEach((key) => {
        delete this.index.dedupeKeys[key];
      });
    }

    this.unflushed += 1;
    if (this.unflushed >= this.flushEvery) this.flush();

    return {
      inserted: true,
      eventId,
      entry,
    };
  }

  readAllEvents() {
    if (!fs.existsSync(this.eventsPath)) return [];
    const content = fs.readFileSync(this.eventsPath, "utf8");
    const lines = splitLines(content);
    const out = [];
    lines.forEach((line) => {
      try {
        out.push(JSON.parse(line));
      } catch (_error) {
        // ignore malformed line
      }
    });
    return out;
  }

  async replayEvents(onEvent) {
    if (!fs.existsSync(this.eventsPath)) {
      return {
        replayed: 0,
        lastEventId: null,
      };
    }

    const stream = fs.createReadStream(this.eventsPath, "utf8");
    const rl = readline.createInterface({
      input: stream,
      crlfDelay: Infinity,
    });

    let replayed = 0;
    let lastEventId = null;

    try {
      for await (const line of rl) {
        if (!line || !line.trim()) continue;
        let entry;
        try {
          entry = JSON.parse(line);
        } catch (_error) {
          continue;
        }

        if (!entry || typeof entry !== "object") continue;
        if (typeof onEvent === "function") {
          await onEvent(entry);
        }

        replayed += 1;
        if (entry.eventId !== undefined && entry.eventId !== null) {
          lastEventId = entry.eventId;
        }
      }
    } finally {
      rl.close();
      stream.close();
    }

    return {
      replayed,
      lastEventId,
    };
  }

  flush() {
    writeJsonAtomic(this.indexPath, this.index);
    this.unflushed = 0;
  }
}

module.exports = {
  EventsService,
};
