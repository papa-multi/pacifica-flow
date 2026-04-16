"use strict";

function createLogger(service) {
  return {
    info(message, extra) {
      console.log(JSON.stringify({ level: "info", service, message, ...(extra || {}) }));
    },
    warn(message, extra) {
      console.warn(JSON.stringify({ level: "warn", service, message, ...(extra || {}) }));
    },
    error(message, extra) {
      console.error(JSON.stringify({ level: "error", service, message, ...(extra || {}) }));
    },
  };
}

module.exports = { createLogger };
