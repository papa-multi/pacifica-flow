(function () {
  const state = {
    authenticated: false,
    user: null,
    copyTradingEligible: false,
    mfaSetup: null,
    backupCodes: [],
    backupCodesRevealed: false,
    apiKeys: [],
    loading: false,
  };
  let backupStatusRestoreTimer = null;

  const dom = {
    runtimeError: document.getElementById("settings-runtime-error"),
    authPill: document.getElementById("settings-auth-pill"),
    copyPill: document.getElementById("settings-copy-pill"),
    avatar: document.getElementById("settings-avatar"),
    displayName: document.getElementById("settings-display-name"),
    providerBadge: document.getElementById("settings-provider-badge"),
    email: document.getElementById("settings-email"),
    memberSince: document.getElementById("settings-member-since"),
    linkedLogin: document.getElementById("settings-linked-login"),
    accountStatus: document.getElementById("settings-account-status"),
    accountId: document.getElementById("settings-account-id"),
    displayInput: document.getElementById("settings-display-input"),
    profileForm: document.getElementById("settings-profile-form"),
    mfaStatus: document.getElementById("settings-mfa-status"),
    copyStatus: document.getElementById("settings-copy-status"),
    mfaCopy: document.getElementById("settings-mfa-copy"),
    mfaSetupBtn: document.getElementById("settings-mfa-setup"),
    mfaManageBtn: document.getElementById("settings-mfa-manage"),
    qrImage: document.getElementById("settings-qr-image"),
    qrPlaceholder: document.getElementById("settings-qr-placeholder"),
    setupCopy: document.getElementById("settings-setup-copy"),
    secret: document.getElementById("settings-secret"),
    secretCopyBtn: document.getElementById("settings-secret-copy-btn"),
    mfaCode: document.getElementById("settings-mfa-code"),
    mfaVerifyBtn: document.getElementById("settings-mfa-verify"),
    accessNote: document.getElementById("settings-access-note"),
    backupStatus: document.getElementById("settings-backup-status"),
    backupCopy: document.getElementById("settings-backup-copy"),
    backupCodeInput: document.getElementById("settings-backup-code"),
    backupGenerateBtn: document.getElementById("settings-backup-generate"),
    backupToggleBtn: document.getElementById("settings-backup-toggle"),
    backupList: document.getElementById("settings-backup-list"),
    backupCopyBtn: document.getElementById("settings-backup-copy-btn"),
    backupDownloadBtn: document.getElementById("settings-backup-download"),
    backupHint: document.getElementById("settings-backup-hint"),
    securityResetBtn: document.getElementById("settings-security-reset"),
    apiForm: document.getElementById("settings-api-form"),
    apiAccountWallet: document.getElementById("settings-api-account-wallet"),
    apiAgentWallet: document.getElementById("settings-api-agent-wallet"),
    apiAgentPrivateKey: document.getElementById("settings-api-agent-private-key"),
    apiStatus: document.getElementById("settings-api-status"),
    apiList: document.getElementById("settings-api-list"),
    apiCount: document.getElementById("settings-api-count"),
  };

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function shortName(name = "") {
    const value = String(name || "").trim();
    return value ? value[0].toUpperCase() : "P";
  }

  function fmtDateTime(value) {
    const ts = Number(value);
    if (!Number.isFinite(ts) || ts <= 0) return "-";
    return new Intl.DateTimeFormat("en-GB", {
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "UTC",
    }).format(new Date(ts)) + " UTC";
  }

  function showRuntimeError(message) {
    if (!dom.runtimeError) return;
    dom.runtimeError.hidden = false;
    dom.runtimeError.textContent = `Settings runtime error: ${message}`;
  }

  async function fetchJson(url, options = {}) {
    const response = await fetch(url, {
      method: options.method || "GET",
      headers: {
        Accept: "application/json",
        ...(options.json ? { "Content-Type": "application/json" } : {}),
      },
      credentials: "same-origin",
      cache: "no-store",
      body: options.json ? JSON.stringify(options.json) : undefined,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload && payload.error ? String(payload.error) : `HTTP ${response.status}`);
    }
    return payload;
  }

  function setButtonLoading(button, text, loading) {
    if (!button) return;
    if (!button.dataset.originalText) button.dataset.originalText = button.textContent || "";
    button.disabled = Boolean(loading);
    button.textContent = loading ? text : button.dataset.originalText;
  }

  function escapeAttr(value) {
    return esc(value).replace(/`/g, "&#96;");
  }

  function formatBackupCode(raw) {
    const value = String(raw || "")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, "");
    if (!value) return "";
    const groups = value.match(/.{1,4}/g) || [value];
    return groups.join("-");
  }

  function setBackupCodes(codes = [], meta = {}) {
    state.backupCodes = Array.isArray(codes) ? codes.filter(Boolean).map((code) => formatBackupCode(code)) : [];
    renderBackupCodes(meta);
  }

  function formatApiKeyPreview(value) {
    const key = String(value || "").trim();
    if (!key) return "—";
    if (key.length <= 8) return `${key.slice(0, 4)}…`;
    return `${key.slice(0, 4)}…${key.slice(-4)}`;
  }

  function formatWalletPreview(value) {
    const key = String(value || "").trim();
    if (!key) return "—";
    if (key.length <= 12) return key;
    return `${key.slice(0, 6)}…${key.slice(-4)}`;
  }

  function setApiKeys(apiKeys = []) {
    state.apiKeys = Array.isArray(apiKeys) ? apiKeys.filter(Boolean) : [];
    renderApiKeys();
  }

  function notifyApiKeyChange() {
    const stamp = String(Date.now());
    try {
      window.localStorage.setItem("pf-api-key-last-updated", stamp);
    } catch (_error) {
      // Ignore storage failures; the custom event still updates the current tab.
    }
    window.dispatchEvent(
      new CustomEvent("pf-api-key-change", {
        detail: { at: Number(stamp) },
      })
    );
  }

  function renderUser() {
    const user = state.user || null;
    const displayName = String(user && user.displayName ? user.displayName : "Trader");
    const isTwitter = Boolean(user && user.authProvider === "twitter");
    const accountId = user && (user.id || user.userId || user.accountId || user.walletId || "");
    const email = isTwitter
      ? String(
          user && user.twitterUsername
            ? `Connected with Twitter${user.twitterName ? ` (@${user.twitterName})` : ` (@${user.twitterUsername})`}`
            : "Connected with Twitter"
        )
      : String(user && user.email ? user.email : "Sign in to view your profile");
    if (dom.authPill) dom.authPill.textContent = state.authenticated ? "Signed in" : "Guest";
    if (dom.copyPill) dom.copyPill.textContent = state.copyTradingEligible ? "Copy Trading unlocked" : "Copy Trading locked";
    if (dom.avatar) {
      dom.avatar.textContent = shortName(displayName);
      dom.avatar.dataset.tone = state.copyTradingEligible ? "ready" : "locked";
    }
    if (dom.displayName) dom.displayName.textContent = displayName;
    if (dom.providerBadge) {
      dom.providerBadge.textContent = state.authenticated
        ? isTwitter
          ? "Twitter"
          : "Connected"
        : "Guest";
      dom.providerBadge.dataset.tone = state.authenticated ? "ready" : "muted";
    }
    if (dom.email) dom.email.textContent = email;
    if (dom.memberSince) {
      dom.memberSince.textContent = user && user.createdAt ? `Member since ${fmtDateTime(user.createdAt)}` : "Member since -";
    }
    if (dom.linkedLogin) {
      dom.linkedLogin.textContent = isTwitter ? "Twitter" : state.authenticated ? "Unknown" : "-";
    }
    if (dom.accountStatus) {
      dom.accountStatus.textContent = state.authenticated ? "Active" : "Signed out";
    }
    if (dom.accountId) {
      dom.accountId.textContent = accountId ? String(accountId) : "-";
    }
    if (dom.displayInput) dom.displayInput.value = user && user.displayName ? user.displayName : "";
    if (dom.mfaStatus) dom.mfaStatus.textContent = state.copyTradingEligible ? "Enabled" : "Disabled";
    if (dom.copyStatus) dom.copyStatus.textContent = state.copyTradingEligible ? "Unlocked" : "Locked";
    if (dom.mfaCopy) {
      dom.mfaCopy.textContent = state.copyTradingEligible
        ? "Google Authenticator is active. Copy Trading is unlocked."
        : "Enable Google Authenticator to unlock Copy Trading.";
    }
    if (dom.accessNote) {
      dom.accessNote.textContent = state.copyTradingEligible
        ? "Copy Trading is unlocked for this account."
        : "Copy Trading is only available after Google Authenticator is enabled.";
    }
    if (dom.mfaSetupBtn) dom.mfaSetupBtn.textContent = state.copyTradingEligible ? "Refresh Authenticator Setup" : "Set up Authenticator";
    if (dom.mfaManageBtn) dom.mfaManageBtn.textContent = state.copyTradingEligible ? "Refresh authenticator" : "Manage authenticator";
    if (dom.mfaManageBtn) dom.mfaManageBtn.disabled = !state.authenticated;
    if (dom.displayInput) dom.displayInput.disabled = !state.authenticated;
    if (dom.profileForm) {
      const saveButton = dom.profileForm.querySelector("button[type='submit']");
      if (saveButton) saveButton.disabled = !state.authenticated;
    }
    if (dom.mfaVerifyBtn) dom.mfaVerifyBtn.disabled = !state.authenticated;
    if (dom.backupGenerateBtn) dom.backupGenerateBtn.disabled = !state.authenticated || !state.copyTradingEligible;
    if (dom.backupCopyBtn) dom.backupCopyBtn.disabled = !state.backupCodes.length;
    if (dom.backupDownloadBtn) dom.backupDownloadBtn.disabled = !state.backupCodes.length;
    if (dom.backupToggleBtn) dom.backupToggleBtn.disabled = !state.backupCodes.length;
    if (dom.backupToggleBtn) dom.backupToggleBtn.textContent = state.backupCodesRevealed ? "Hide codes" : "Reveal codes";
    if (dom.apiCount) {
      const count = state.apiKeys.length;
      dom.apiCount.textContent = count ? `${count} saved access set${count === 1 ? "" : "s"}` : "No access sets";
    }
    if (dom.apiStatus) {
      dom.apiStatus.textContent = state.apiKeys.length
        ? "Validate a saved Pacifica access set to confirm live account access."
        : "Save a Pacifica access set first, then validate it here.";
    }
    if (dom.apiForm) {
      const submitButton = dom.apiForm.querySelector("button[type='submit']");
      if (submitButton) submitButton.disabled = !state.authenticated;
    }
    if (dom.apiAccountWallet) dom.apiAccountWallet.disabled = !state.authenticated;
    if (dom.apiAgentWallet) dom.apiAgentWallet.disabled = !state.authenticated;
    if (dom.apiAgentPrivateKey) dom.apiAgentPrivateKey.disabled = !state.authenticated;
  }

  function renderApiKeys() {
    if (!dom.apiList) return;
    dom.apiList.innerHTML = "";
    if (!state.apiKeys.length) {
      const empty = document.createElement("p");
      empty.className = "settings-api-empty";
      empty.textContent = state.authenticated
        ? "No Pacifica access sets saved yet."
        : "Sign in to add Pacifica access.";
      dom.apiList.appendChild(empty);
      return;
    }
    state.apiKeys.forEach((row) => {
      const item = document.createElement("div");
      item.className = "settings-api-item";
      item.innerHTML = `
        <div class="settings-api-item-copy">
          <strong>${escapeAttr(row.label || "Pacifica access")}</strong>
          <span>${escapeAttr(row.accountWalletPreview || formatWalletPreview(row.accountWallet || row.apiKey || ""))}</span>
          <small>Agent: ${escapeAttr(row.agentWalletPreview || formatWalletPreview(row.agentWallet || row.apiKey || ""))}</small>
          <small>${row.createdAt ? `Added ${fmtDateTime(row.createdAt)}` : "Added recently"}</small>
        </div>
        <div class="settings-api-item-actions">
          <button class="wp-btn wp-btn-ghost" type="button" data-action="validate" data-id="${escapeAttr(row.id)}">Validate</button>
          <button class="wp-btn wp-btn-ghost" type="button" data-action="delete" data-id="${escapeAttr(row.id)}">Remove</button>
        </div>
      `;
      dom.apiList.appendChild(item);
    });
  }

  function renderSetup(payload) {
    state.mfaSetup = payload || null;
    const qrDataUrl = String(payload && payload.qrDataUrl ? payload.qrDataUrl : "").trim();
    if (dom.qrImage) {
      if (qrDataUrl) {
        dom.qrImage.src = qrDataUrl;
        dom.qrImage.hidden = false;
      } else {
        dom.qrImage.hidden = true;
        dom.qrImage.removeAttribute("src");
      }
    }
    if (dom.qrPlaceholder) {
      dom.qrPlaceholder.hidden = Boolean(qrDataUrl);
    }
    if (dom.secret) {
      dom.secret.textContent = payload && payload.secret ? payload.secret : "-";
    }
    if (dom.secretCopyBtn) {
      dom.secretCopyBtn.disabled = !(payload && payload.secret);
    }
    if (dom.setupCopy) {
      dom.setupCopy.textContent = payload && payload.otpauthUrl
        ? "Scan the QR code with Google Authenticator, or copy the manual key below if you prefer to type it in."
        : "Click Set up Authenticator to generate a QR code and manual key.";
    }
  }

  function renderBackupCodes(meta = {}) {
    const configured = Boolean(meta && meta.configured);
    const remaining = Number(meta && meta.remaining != null ? meta.remaining : state.backupCodes.length) || 0;
    const generatedAt = Number(meta && meta.generatedAt ? meta.generatedAt : 0) || 0;
    if (dom.backupStatus) {
      dom.backupStatus.textContent = configured
        ? `${remaining} code${remaining === 1 ? "" : "s"} remaining`
        : "Not generated";
    }
    if (dom.backupCopy) {
      dom.backupCopy.textContent = configured
        ? "These recovery codes let you sign in once each if Google Authenticator is unavailable. Copy or download them now."
        : "Generate one-time backup codes and keep them offline. Each code can only be used once.";
    }
    if (dom.backupHint) {
      dom.backupHint.textContent = state.backupCodesRevealed
        ? "Visible now. Hide them again once you have copied or downloaded them."
        : "Hidden by default. Reveal them only when you need to copy or download.";
    }
    if (!dom.backupList) return;
    dom.backupList.innerHTML = "";
    dom.backupList.dataset.revealed = state.backupCodesRevealed ? "true" : "false";
    if (!state.backupCodes.length) {
      const empty = document.createElement("p");
      empty.className = "settings-backup-empty";
      empty.textContent = configured
        ? "Backup codes are configured, but this view does not currently have a fresh generated set to display."
        : "No backup codes generated yet.";
      dom.backupList.appendChild(empty);
      return;
    }
    state.backupCodes.forEach((code, index) => {
      const row = document.createElement("div");
      row.className = "settings-backup-code";
      row.innerHTML = `<span>${escapeAttr(code)}</span><small>#${index + 1}${generatedAt ? ` • ${fmtDateTime(generatedAt)}` : ""}</small>`;
      dom.backupList.appendChild(row);
    });
  }

  async function loadSettings() {
    try {
      const payload = await fetchJson("/api/auth/settings");
      state.authenticated = true;
      state.user = payload && payload.user ? payload.user : null;
      state.copyTradingEligible = Boolean(payload && payload.copyTradingEligible);
      renderUser();
      renderBackupCodes({
        configured: Boolean(state.user && state.user.mfaBackupCodesConfigured),
        remaining: Number(state.user && state.user.mfaBackupCodesRemaining != null ? state.user.mfaBackupCodesRemaining : 0),
        generatedAt: Number(state.user && state.user.mfaBackupCodesGeneratedAt != null ? state.user.mfaBackupCodesGeneratedAt : 0),
      });
      setApiKeys(Array.isArray(payload && payload.apiKeys) ? payload.apiKeys : []);
      if (state.copyTradingEligible) {
        const setup = await fetchJson("/api/auth/mfa/setup", { method: "POST", json: {} });
        renderSetup(setup);
      }
      const welcome = new URL(window.location.href).searchParams.get("welcome") === "1";
      if (welcome && !state.copyTradingEligible && dom.mfaSetupBtn) {
        dom.mfaSetupBtn.focus();
      }
    } catch (_error) {
      state.authenticated = false;
      state.user = null;
      state.copyTradingEligible = false;
      setApiKeys([]);
      renderUser();
      renderSetup(null);
    }
  }

  function flashBackupStatus(text) {
    if (!dom.backupStatus) return;
    if (backupStatusRestoreTimer) window.clearTimeout(backupStatusRestoreTimer);
    dom.backupStatus.textContent = text;
    backupStatusRestoreTimer = window.setTimeout(() => {
      renderBackupCodes({
        configured: Boolean(state.user && state.user.mfaBackupCodesConfigured),
        remaining: Number(state.user && state.user.mfaBackupCodesRemaining != null ? state.user.mfaBackupCodesRemaining : state.backupCodes.length),
        generatedAt: Number(state.user && state.user.mfaBackupCodesGeneratedAt != null ? state.user.mfaBackupCodesGeneratedAt : 0),
      });
      backupStatusRestoreTimer = null;
    }, 2000);
  }

  async function saveProfile(event) {
    event.preventDefault();
    if (!state.authenticated) return;
    const displayName = String(dom.displayInput && dom.displayInput.value ? dom.displayInput.value : "").trim();
    const submitButton = event.submitter || (dom.profileForm && dom.profileForm.querySelector("button[type='submit']"));
    setButtonLoading(submitButton, "Saving…", true);
    try {
      const payload = await fetchJson("/api/auth/profile", {
        method: "POST",
        json: { displayName },
      });
      state.user = payload && payload.user ? payload.user : state.user;
      renderUser();
      if (payload && Array.isArray(payload.backupCodes)) {
        state.backupCodesRevealed = false;
        setBackupCodes(payload.backupCodes, {
          configured: true,
          remaining: Number(payload.backupCodesRemaining != null ? payload.backupCodesRemaining : payload.backupCodes.length),
          generatedAt: Number(payload.user && payload.user.mfaBackupCodesGeneratedAt != null ? payload.user.mfaBackupCodesGeneratedAt : 0),
        });
      }
      if (window.__PF_AUTH_UI_REFRESH) {
        window.__PF_AUTH_UI_REFRESH().catch(() => null);
      }
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to save profile.");
    } finally {
      setButtonLoading(submitButton, "Save profile", false);
    }
  }

  async function setupMfa() {
    if (!state.authenticated) return;
    setButtonLoading(dom.mfaSetupBtn, "Loading…", true);
    try {
      const payload = await fetchJson("/api/auth/mfa/setup", { method: "POST", json: {} });
      renderSetup(payload);
      if (payload && payload.secret && dom.mfaCode) {
        dom.mfaCode.focus();
      }
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to set up Google Authenticator.");
    } finally {
      setButtonLoading(dom.mfaSetupBtn, "Set up Authenticator", false);
    }
  }

  async function verifyMfa() {
    if (!state.authenticated) return;
    const code = String(dom.mfaCode && dom.mfaCode.value ? dom.mfaCode.value : "").trim();
    if (!/^\d{6}$/.test(code)) {
      showRuntimeError("Enter the 6-digit Google Authenticator code.");
      return;
    }
    setButtonLoading(dom.mfaVerifyBtn, "Verifying…", true);
    try {
      const payload = await fetchJson("/api/auth/mfa/verify", {
        method: "POST",
        json: { code },
      });
      state.user = payload && payload.user ? payload.user : state.user;
      state.copyTradingEligible = Boolean(payload && payload.mfaEnabled);
      renderUser();
      const setup = await fetchJson("/api/auth/mfa/setup", { method: "POST", json: {} });
      renderSetup(setup);
      if (payload && Array.isArray(payload.backupCodes)) {
        state.backupCodesRevealed = false;
        setBackupCodes(payload.backupCodes, {
          configured: true,
          remaining: Number(payload.backupCodesRemaining != null ? payload.backupCodesRemaining : payload.backupCodes.length),
          generatedAt: Number(payload.user && payload.user.mfaBackupCodesGeneratedAt != null ? payload.user.mfaBackupCodesGeneratedAt : 0),
        });
      }
      if (window.__PF_AUTH_UI_REFRESH) {
        window.__PF_AUTH_UI_REFRESH().catch(() => null);
      }
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to verify authenticator code.");
    } finally {
      setButtonLoading(dom.mfaVerifyBtn, "Verify & Enable", false);
    }
  }

  async function disableMfa() {
    if (!state.authenticated || !state.copyTradingEligible) return;
    const code = String(dom.mfaCode && dom.mfaCode.value ? dom.mfaCode.value : "").trim();
    if (!/^\d{6}$/.test(code)) {
      showRuntimeError("Enter the current Google Authenticator code before disabling MFA.");
      return;
    }
    setButtonLoading(dom.securityResetBtn, "Disabling…", true);
    try {
      const payload = await fetchJson("/api/auth/mfa/disable", {
        method: "POST",
        json: { code },
      });
      state.user = payload && payload.user ? payload.user : state.user;
      state.copyTradingEligible = false;
      state.backupCodesRevealed = false;
      renderUser();
      renderSetup(null);
      setBackupCodes([], { configured: false, remaining: 0, generatedAt: 0 });
      if (window.__PF_AUTH_UI_REFRESH) {
        window.__PF_AUTH_UI_REFRESH().catch(() => null);
      }
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to disable MFA.");
    } finally {
      setButtonLoading(dom.securityResetBtn, "Reset 2FA", false);
    }
  }

  async function manageMfa() {
    if (!state.authenticated) return;
    await setupMfa();
  }

  async function generateBackupCodes() {
    if (!state.authenticated || !state.copyTradingEligible) return;
    const code = String(
      dom.backupCodeInput && dom.backupCodeInput.value
        ? dom.backupCodeInput.value
        : dom.mfaCode && dom.mfaCode.value
          ? dom.mfaCode.value
          : ""
    ).trim();
    if (code.length < 6) {
      showRuntimeError("Enter your current authenticator code before generating backup codes.");
      return;
    }
    setButtonLoading(dom.backupGenerateBtn, "Generating…", true);
    try {
      const payload = await fetchJson("/api/auth/mfa/backup-codes", {
        method: "POST",
        json: { code },
      });
      state.user = payload && payload.user ? payload.user : state.user;
      state.copyTradingEligible = Boolean(state.user && state.user.mfaEnabled);
      state.backupCodesRevealed = false;
      renderUser();
      setBackupCodes(payload && Array.isArray(payload.backupCodes) ? payload.backupCodes : [], {
        configured: true,
        remaining: Number(payload && payload.backupCodesRemaining != null ? payload.backupCodesRemaining : 0),
        generatedAt: Number(payload && payload.backupCodesGeneratedAt != null ? payload.backupCodesGeneratedAt : 0),
      });
      if (window.__PF_AUTH_UI_REFRESH) {
        window.__PF_AUTH_UI_REFRESH().catch(() => null);
      }
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to generate backup codes.");
    } finally {
      setButtonLoading(dom.backupGenerateBtn, "Generate backup codes", false);
    }
  }

  async function copyBackupCodes() {
    if (!state.backupCodes.length) return;
    const text = state.backupCodes.join("\n");
    try {
      await navigator.clipboard.writeText(text);
      flashBackupStatus("Copied");
    } catch (_error) {
      flashBackupStatus("Copy unavailable");
    }
  }

  async function copySecretKey() {
    const secret = String(state.mfaSetup && state.mfaSetup.secret ? state.mfaSetup.secret : dom.secret && dom.secret.textContent ? dom.secret.textContent : "").trim();
    if (!secret || secret === "-") return;
    try {
      await navigator.clipboard.writeText(secret);
      flashBackupStatus("Key copied");
    } catch (_error) {
      flashBackupStatus("Copy unavailable");
    }
  }

  async function saveApiKey(event) {
    event.preventDefault();
    if (!state.authenticated) return;
    const accountWallet = String(dom.apiAccountWallet && dom.apiAccountWallet.value ? dom.apiAccountWallet.value : "").trim();
    const agentWallet = String(dom.apiAgentWallet && dom.apiAgentWallet.value ? dom.apiAgentWallet.value : "").trim();
    const agentPrivateKey = String(
      dom.apiAgentPrivateKey && dom.apiAgentPrivateKey.value ? dom.apiAgentPrivateKey.value : ""
    ).trim();
    const submitButton = event.submitter || (dom.apiForm && dom.apiForm.querySelector("button[type='submit']"));
    setButtonLoading(submitButton, "Saving…", true);
    try {
      const payload = await fetchJson("/api/auth/api-keys", {
        method: "POST",
        json: { accountWallet, agentWallet, agentPrivateKey },
      });
      setApiKeys(Array.isArray(payload && payload.apiKeys) ? payload.apiKeys : []);
      notifyApiKeyChange();
      if (dom.apiAccountWallet) dom.apiAccountWallet.value = "";
      if (dom.apiAgentWallet) dom.apiAgentWallet.value = "";
      if (dom.apiAgentPrivateKey) dom.apiAgentPrivateKey.value = "";
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to save Pacifica access.");
    } finally {
      setButtonLoading(submitButton, "Save Pacifica access", false);
    }
  }

  async function deleteApiKey(keyId) {
    if (!state.authenticated || !keyId) return;
    try {
      const payload = await fetchJson(`/api/auth/api-keys/${encodeURIComponent(keyId)}`, {
        method: "DELETE",
      });
      setApiKeys(Array.isArray(payload && payload.apiKeys) ? payload.apiKeys : []);
      notifyApiKeyChange();
    } catch (error) {
      showRuntimeError(error && error.message ? error.message : "Failed to remove Pacifica access.");
    }
  }

  async function validateApiKey(keyId) {
    if (!state.authenticated || !keyId) return;
    if (dom.apiStatus) dom.apiStatus.textContent = "Validating live Pacifica access...";
    try {
      const payload = await fetchJson(`/api/auth/api-keys/${encodeURIComponent(keyId)}/validate`, {
        method: "POST",
      });
      const accountWallet = payload && payload.accountWallet ? formatWalletPreview(payload.accountWallet) : "-";
      const agentWallet = payload && payload.agentWallet ? formatWalletPreview(payload.agentWallet) : "-";
      const count = Array.isArray(payload && payload.apiKeys) ? payload.apiKeys.length : 0;
      if (dom.apiStatus) {
        dom.apiStatus.textContent = `Validated Pacifica access for ${accountWallet} via ${agentWallet}. Returned ${count} API key${count === 1 ? "" : "s"}.`;
      }
    } catch (error) {
      const message = error && error.message ? error.message : "Validation failed.";
      if (dom.apiStatus) dom.apiStatus.textContent = message;
      showRuntimeError(message);
    }
  }

  function downloadBackupCodes() {
    if (!state.backupCodes.length) return;
    const text = [
      "PacificaFlow backup codes",
      "",
      ...state.backupCodes.map((code, index) => `${index + 1}. ${code}`),
      "",
      "Keep these codes offline. Each code can be used once.",
    ].join("\n");
    const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "pacificaflow-backup-codes.txt";
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function bindEvents() {
    if (dom.profileForm) dom.profileForm.addEventListener("submit", (event) => saveProfile(event).catch(() => null));
    if (dom.apiForm) dom.apiForm.addEventListener("submit", (event) => saveApiKey(event).catch(() => null));
    if (dom.mfaSetupBtn) dom.mfaSetupBtn.addEventListener("click", () => setupMfa().catch(() => null));
    if (dom.mfaManageBtn) dom.mfaManageBtn.addEventListener("click", () => manageMfa().catch(() => null));
    if (dom.mfaVerifyBtn) dom.mfaVerifyBtn.addEventListener("click", () => verifyMfa().catch(() => null));
    if (dom.backupGenerateBtn) dom.backupGenerateBtn.addEventListener("click", () => generateBackupCodes().catch(() => null));
    if (dom.backupCopyBtn) dom.backupCopyBtn.addEventListener("click", () => copyBackupCodes().catch(() => null));
    if (dom.backupDownloadBtn) dom.backupDownloadBtn.addEventListener("click", () => downloadBackupCodes());
    if (dom.secretCopyBtn) dom.secretCopyBtn.addEventListener("click", () => copySecretKey().catch(() => null));
    if (dom.securityResetBtn) dom.securityResetBtn.addEventListener("click", () => disableMfa().catch(() => null));
    if (dom.apiList) {
      dom.apiList.addEventListener("click", (event) => {
        const target = event.target instanceof HTMLElement ? event.target.closest("button[data-action][data-id]") : null;
        if (!target) return;
        const keyId = String(target.dataset.id || "");
        const action = String(target.dataset.action || "");
        if (action === "validate") {
          validateApiKey(keyId).catch(() => null);
          return;
        }
        deleteApiKey(keyId).catch(() => null);
      });
    }
    if (dom.backupToggleBtn) {
      dom.backupToggleBtn.addEventListener("click", () => {
        state.backupCodesRevealed = !state.backupCodesRevealed;
        renderUser();
        renderBackupCodes({
          configured: Boolean(state.user && state.user.mfaBackupCodesConfigured),
          remaining: Number(state.user && state.user.mfaBackupCodesRemaining != null ? state.user.mfaBackupCodesRemaining : state.backupCodes.length),
          generatedAt: Number(state.user && state.user.mfaBackupCodesGeneratedAt != null ? state.user.mfaBackupCodesGeneratedAt : 0),
        });
      });
    }
    window.addEventListener("pf-auth-change", () => loadSettings().catch(() => null));
  }

  function init() {
    bindEvents();
    renderUser();
    renderSetup(null);
    loadSettings().catch((error) => showRuntimeError(error && error.message ? error.message : "Failed to load settings."));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
