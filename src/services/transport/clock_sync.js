function createClockSync() {
  const state = {
    offsetMs: 0,
    samples: 0,
    lastSyncAt: null,
  };

  function observeServerDate(dateHeader) {
    if (!dateHeader) return;
    const serverTs = Date.parse(dateHeader);
    if (!Number.isFinite(serverTs)) return;

    const localTs = Date.now();
    const sampleOffset = serverTs - localTs;

    // Smooth offset updates to avoid abrupt jumps.
    const alpha = state.samples < 5 ? 0.45 : 0.2;
    state.offsetMs = state.offsetMs * (1 - alpha) + sampleOffset * alpha;
    state.samples += 1;
    state.lastSyncAt = localTs;
  }

  function nowMs() {
    return Date.now() + state.offsetMs;
  }

  function getState() {
    return {
      offsetMs: Math.round(state.offsetMs),
      samples: state.samples,
      lastSyncAt: state.lastSyncAt,
    };
  }

  return {
    getState,
    nowMs,
    observeServerDate,
  };
}

module.exports = {
  createClockSync,
};
