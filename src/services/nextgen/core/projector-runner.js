"use strict";

const { loadState, saveState } = require("./state-store");
const { readCursor, readEventsFromOffset, writeCursor } = require("./event-log");
const { applyEvent } = require("../projections/apply-event");

function runProjection({ cursorName = "projector" } = {}) {
  const startOffset = readCursor(cursorName);
  const { nextOffset, events } = readEventsFromOffset(startOffset);
  const state = loadState();
  for (const event of events) {
    applyEvent(state, event);
  }
  saveState(state);
  writeCursor(cursorName, nextOffset);
  return {
    eventsApplied: events.length,
    nextOffset,
    generatedAt: state.meta.generatedAt || Date.now(),
  };
}

module.exports = { runProjection };
