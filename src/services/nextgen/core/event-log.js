"use strict";

const fs = require("fs");
const path = require("path");
const config = require("../common/config");
const { ensureParent, readJson, writeJsonAtomic } = require("./json-file");

const EVENTS_PATH = path.join(config.eventsDir, "events.ndjson");
const CURSORS_PATH = path.join(config.eventsDir, "cursors.json");

function ensureLog() {
  ensureParent(EVENTS_PATH);
  if (!fs.existsSync(EVENTS_PATH)) {
    fs.writeFileSync(EVENTS_PATH, "");
  }
}

function appendEvents(events) {
  ensureLog();
  const rows = []
    .concat(events || [])
    .filter(Boolean)
    .map((event) => JSON.stringify(event))
    .join("\n");
  if (!rows) return 0;
  fs.appendFileSync(EVENTS_PATH, rows + "\n");
  return rows.length;
}

function readAllEvents() {
  ensureLog();
  const text = fs.readFileSync(EVENTS_PATH, "utf8");
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function readCursor(name) {
  const payload = readJson(CURSORS_PATH, {});
  return Math.max(0, Number((payload || {})[name] || 0));
}

function writeCursor(name, offset) {
  const payload = readJson(CURSORS_PATH, {});
  payload[name] = Math.max(0, Number(offset || 0));
  writeJsonAtomic(CURSORS_PATH, payload);
}

function readEventsFromOffset(startOffset) {
  ensureLog();
  const fd = fs.openSync(EVENTS_PATH, "r");
  try {
    const stat = fs.fstatSync(fd);
    const size = stat.size;
    const offset = Math.max(0, Math.min(Number(startOffset || 0), size));
    const buffer = Buffer.alloc(size - offset);
    fs.readSync(fd, buffer, 0, size - offset, offset);
    const text = buffer.toString("utf8");
    const lines = text
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
    return {
      nextOffset: size,
      events: lines.map((line) => JSON.parse(line)),
    };
  } finally {
    fs.closeSync(fd);
  }
}

module.exports = {
  EVENTS_PATH,
  CURSORS_PATH,
  appendEvents,
  readAllEvents,
  readCursor,
  writeCursor,
  readEventsFromOffset,
};
