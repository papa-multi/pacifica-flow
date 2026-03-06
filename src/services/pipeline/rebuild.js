async function rebuildStateFromEvents(events, stateService, metricsService) {
  stateService.reset();
  metricsService.reset();

  const sorted = [...(Array.isArray(events) ? events : [])].sort(
    (a, b) => Number(a.eventId || 0) - Number(b.eventId || 0)
  );

  sorted.forEach((entry) => {
    stateService.applyEvent(entry);
  });

  metricsService.refresh(stateService.getState());

  return {
    replayed: sorted.length,
    lastEventId: sorted.length ? sorted[sorted.length - 1].eventId : null,
  };
}

module.exports = {
  rebuildStateFromEvents,
};
