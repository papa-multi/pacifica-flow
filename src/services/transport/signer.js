function createSigner() {
  async function sign(_input) {
    throw new Error(
      "Signer is disabled in Phase 1 read-only mode. Enable trading mode to sign payloads."
    );
  }

  return {
    mode: "read_only",
    sign,
  };
}

module.exports = {
  createSigner,
};
