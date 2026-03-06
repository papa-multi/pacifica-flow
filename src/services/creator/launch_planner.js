function validateMintSetup(input = {}) {
  const errors = [];
  const stages = Array.isArray(input.stages) ? input.stages : [];

  if (!stages.length) {
    errors.push("at least one mint stage is required");
  }

  stages.forEach((stage, idx) => {
    const name = String(stage.name || "").trim();
    const mode = String(stage.mode || "public").toLowerCase();
    const price = String(stage.price || "0").trim();
    const perWalletLimit = Number(stage.perWalletLimit || 0);
    const startTime = Number(stage.startTime || 0);
    const endTime = Number(stage.endTime || 0);

    if (!name) errors.push(`stage[${idx}] name is required`);
    if (!["allowlist", "public"].includes(mode))
      errors.push(`stage[${idx}] mode must be allowlist/public`);
    if (!price) errors.push(`stage[${idx}] price is required`);
    if (!Number.isFinite(perWalletLimit) || perWalletLimit <= 0)
      errors.push(`stage[${idx}] perWalletLimit must be > 0`);
    if (!Number.isFinite(startTime) || !Number.isFinite(endTime) || endTime <= startTime)
      errors.push(`stage[${idx}] time window is invalid`);
  });

  const reveal = {
    enabled: Boolean(input.reveal && input.reveal.enabled),
    revealTime: input.reveal ? Number(input.reveal.revealTime || 0) : null,
    placeholderUri: input.reveal ? String(input.reveal.placeholderUri || "").trim() : null,
  };

  if (reveal.enabled) {
    if (!Number.isFinite(reveal.revealTime) || reveal.revealTime <= 0) {
      errors.push("reveal.revealTime is required when reveal is enabled");
    }
    if (!reveal.placeholderUri) {
      errors.push("reveal.placeholderUri is required when reveal is enabled");
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    normalized: {
      stages,
      reveal,
      metadataMode: String(input.metadataMode || "upload").toLowerCase(),
      perMintTxLimit: Number(input.perMintTxLimit || 1),
    },
  };
}

function buildLaunchPlan({ collection, mintConfig }) {
  const steps = [
    {
      key: "create_collection",
      title: "Create Collection Contract/Account",
      signer: "user_wallet",
      execution: "client_signed_tx",
    },
    {
      key: "upload_metadata",
      title: "Pin Metadata + Asset References",
      signer: "service_auto",
      execution: "backend_orchestrated",
    },
    {
      key: "configure_mint",
      title: "Configure Mint Stages + Rules",
      signer: "user_wallet",
      execution: "client_signed_tx",
    },
    {
      key: "activate_sale",
      title: "Activate Mint",
      signer: "user_wallet",
      execution: "client_signed_tx",
    },
  ];

  const preview = {
    collection: {
      id: collection.id,
      name: collection.name,
      symbol: collection.symbol,
      network: collection.network,
      supplyType: collection.supplyType,
      supplyCap: collection.supplyCap,
      mintPrice: collection.mintPrice,
      royaltyBps: collection.royaltyBps,
      mintStart: collection.mintStart,
      mintEnd: collection.mintEnd,
    },
    mint: mintConfig,
    warnings: [],
  };

  if (mintConfig.reveal && mintConfig.reveal.enabled && mintConfig.metadataMode === "generate") {
    preview.warnings.push(
      "Reveal is enabled with generated metadata. Ensure deterministic trait generation seed is finalized before launch."
    );
  }

  return {
    generatedAt: Date.now(),
    mode: "self_serve",
    requiresAdminAction: false,
    requiresUserSignatures: true,
    steps,
    preview,
  };
}

module.exports = {
  buildLaunchPlan,
  validateMintSetup,
};
