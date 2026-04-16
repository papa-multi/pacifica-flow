"use strict";

function buildEventEnvelope({
  eventId,
  source,
  channel,
  entityType,
  entityKey,
  wallet = null,
  market = null,
  sequence,
  eventTime,
  payload,
}) {
  return {
    event_id: String(eventId),
    source: String(source),
    channel: String(channel),
    entity_type: String(entityType),
    entity_key: String(entityKey),
    wallet: wallet ? String(wallet) : null,
    market: market ? String(market) : null,
    sequence,
    event_time: Number(eventTime || Date.now()),
    ingested_at: Date.now(),
    payload: payload && typeof payload === "object" ? payload : {},
  };
}

module.exports = { buildEventEnvelope };
