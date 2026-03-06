const { buildLaunchPlan, validateMintSetup } = require("../../services/creator/launch_planner");

function parseCollectionId(pathname) {
  const parts = pathname.split("/").filter(Boolean);
  // /api/creator/collections/:id OR /api/creator/collections/:id/mint-plan
  if (parts.length >= 4 && parts[0] === "api" && parts[1] === "creator" && parts[2] === "collections") {
    return parts[3];
  }
  return null;
}

function createCreatorStudioComponent({ sendJson, readJsonBody, collectionStore, pipeline }) {
  async function handleRequest(req, res, url) {
    if (url.pathname === "/api/creator/schema") {
      sendJson(res, 200, {
        flow: [
          "Connect Wallet",
          "Create Collection",
          "Upload Assets",
          "Configure Mint",
          "Launch",
        ],
        networks: ["testnet", "mainnet"],
        supplyTypes: ["fixed", "unlimited"],
        stageModes: ["allowlist", "public"],
        selfServe: true,
        requiresAdminAction: false,
      });
      return true;
    }

    if (url.pathname === "/api/creator/collections" && req.method === "GET") {
      const owner = url.searchParams.get("owner") || pipeline.getAccount() || null;
      sendJson(res, 200, {
        generatedAt: Date.now(),
        rows: collectionStore.list(owner),
      });
      return true;
    }

    if (url.pathname === "/api/creator/collections" && req.method === "POST") {
      const body = await readJsonBody(req);
      const result = collectionStore.createDraft(body.owner, body);
      if (!result.ok) {
        sendJson(res, 400, { ok: false, errors: result.errors });
        return true;
      }

      const total = collectionStore.list().length;
      pipeline.recordEvent(
        {
          source: "creator",
          type: "creator.collection_count",
          data: { count: total },
        },
        {
          dedupeKey: `creator.collection_count:${total}`,
        }
      );

      sendJson(res, 201, {
        ok: true,
        collection: result.collection,
        message: "Collection draft created. Next: configure mint and preview launch.",
      });
      return true;
    }

    if (url.pathname.startsWith("/api/creator/collections/") && req.method === "GET") {
      const collectionId = parseCollectionId(url.pathname);
      if (!collectionId) {
        sendJson(res, 404, { error: "Collection route not found" });
        return true;
      }
      const found = collectionStore.getById(collectionId);
      if (!found) {
        sendJson(res, 404, { error: "Collection not found" });
        return true;
      }
      sendJson(res, 200, {
        ok: true,
        collection: found,
      });
      return true;
    }

    if (url.pathname.endsWith("/mint-plan") && req.method === "POST") {
      const collectionId = parseCollectionId(url.pathname);
      const found = collectionStore.getById(collectionId);
      if (!found) {
        sendJson(res, 404, { error: "Collection not found" });
        return true;
      }

      const body = await readJsonBody(req);
      const validation = validateMintSetup(body);
      if (!validation.ok) {
        sendJson(res, 400, { ok: false, errors: validation.errors });
        return true;
      }

      collectionStore.updateMintConfig(collectionId, validation.normalized);
      const plan = buildLaunchPlan({
        collection: found,
        mintConfig: validation.normalized,
      });
      collectionStore.setLaunchPlan(collectionId, plan);

      sendJson(res, 200, {
        ok: true,
        launchPlan: plan,
      });
      return true;
    }

    if (url.pathname.endsWith("/launch") && req.method === "POST") {
      const collectionId = parseCollectionId(url.pathname);
      const found = collectionStore.getById(collectionId);
      if (!found) {
        sendJson(res, 404, { error: "Collection not found" });
        return true;
      }

      if (!found.launchPlan) {
        sendJson(res, 409, {
          error: "Launch plan missing. Generate mint plan before launch.",
        });
        return true;
      }

      const launchIntent = {
        collectionId: found.id,
        createdAt: Date.now(),
        mode: "self_serve",
        requiresUserSignatures: true,
        status: "pending_user_signatures",
        unsignedActions: found.launchPlan.steps
          .filter((step) => step.signer === "user_wallet")
          .map((step, idx) => ({
            sequence: idx + 1,
            action: step.key,
            network: found.network,
            tx: {
              type: "unsigned",
              payload: {
                collectionId: found.id,
                action: step.key,
              },
            },
          })),
      };

      sendJson(res, 200, {
        ok: true,
        launchIntent,
        note:
          "Backend orchestration is automatic. User must sign client-side actions to complete launch.",
      });
      return true;
    }

    return false;
  }

  return {
    handleRequest,
  };
}

module.exports = {
  createCreatorStudioComponent,
};
