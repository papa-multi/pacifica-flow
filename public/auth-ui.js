(function () {
  const AUTH_STATE = {
    loading: true,
    authenticated: false,
    user: null,
    signinChallenge: null,
    twitterConfigured: null,
    authAccessAllowed: false,
    authAccessLoaded: false,
    authAccessReason: null,
    mode: "twitter",
    step: "twitter",
  };
  let accountMenuOpen = false;

  function qs(selector, root = document) {
    return root.querySelector(selector);
  }

  function qsa(selector, root = document) {
    return Array.from(root.querySelectorAll(selector));
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function setMessage(text, tone = "") {
    const node = qs("[data-auth-message]");
    if (!node) return;
    node.textContent = text || "";
    node.dataset.tone = tone || "";
  }

  function ensureModal() {
    let modal = qs("[data-auth-modal]");
    if (modal) return modal;
    modal = document.createElement("div");
    modal.className = "pf-auth-modal";
    modal.hidden = true;
    modal.dataset.authModal = "true";
    modal.innerHTML = `
      <div class="pf-auth-modal-card" role="dialog" aria-modal="true" aria-labelledby="pf-auth-title">
        <header class="pf-auth-modal-head">
          <div>
            <span class="pf-auth-kicker">PacificaFlow</span>
            <h2 id="pf-auth-title">Twitter login</h2>
            <p id="pf-auth-copy">Use Twitter to sign in. Google Authenticator remains mandatory for Copy Trading.</p>
          </div>
          <button class="pf-auth-close" type="button" data-auth-close aria-label="Close auth dialog">×</button>
        </header>
        <div class="pf-auth-panel" data-auth-step="twitter">
          <button class="pf-auth-submit pf-auth-twitter-start" type="button" data-auth-twitter-start>Continue with Twitter</button>
          <p class="pf-auth-verify-copy">
            Twitter is your account login. After that, you will still need Google Authenticator before Copy Trading is unlocked.
          </p>
        </div>
        <div class="pf-auth-panel" data-auth-step="mfa" hidden>
          <p class="pf-auth-verify-copy">
            Open Google Authenticator or use a backup code for <strong data-auth-mfa-email></strong>.
          </p>
          <form class="pf-auth-form" data-auth-mfa-form>
            <label class="pf-auth-field">
              <span>Authenticator or backup code</span>
              <input data-auth-mfa-code inputmode="text" autocapitalize="characters" autocomplete="one-time-code" spellcheck="false" placeholder="123456 or BACKUP-CODE" maxlength="32" required />
            </label>
            <button class="pf-auth-submit" type="submit" data-auth-mfa-submit>Verify code</button>
          </form>
        </div>
        <p class="pf-auth-message" data-auth-message></p>
      </div>
    `;
    document.body.appendChild(modal);
    return modal;
  }

  function ensureHeaderDock() {
    let dock = qs("[data-auth-header-dock]");
    if (dock) return dock;
    dock = document.createElement("div");
    dock.className = "pf-auth-header-dock";
    dock.dataset.authShell = "true";
    dock.dataset.authHeaderDock = "true";
    dock.innerHTML = `
      <button class="pf-auth-entry" type="button" data-auth-action="account" aria-haspopup="menu" aria-expanded="false">
        <span class="pf-auth-entry-label">Account</span>
      </button>
      <div class="pf-auth-account-popover" data-auth-account-popover hidden>
        <div class="pf-auth-account-popover-head">
          <span class="pf-auth-kicker">PacificaFlow</span>
          <strong data-auth-popover-title>Account</strong>
          <p data-auth-popover-copy>Sign in or create an account to unlock Copy Trading.</p>
          <span class="pf-auth-account-name" data-auth-user-name hidden>Trader</span>
        </div>
        <div class="pf-auth-account-popover-actions">
          <a class="pf-auth-popover-link" href="/settings/" data-auth-when="authed">Settings</a>
          <button class="pf-auth-popover-link" type="button" data-auth-action="signout" data-auth-when="authed">Sign out</button>
        </div>
      </div>
    `;
    document.body.appendChild(dock);
    return dock;
  }

  function getHeaderDock() {
    return qs("[data-auth-header-dock]");
  }

  function closeAccountMenu() {
    const dock = getHeaderDock();
    if (!dock) return;
    const menu = qs("[data-auth-account-popover]", dock);
    const entry = qs("[data-auth-action='account']", dock);
    if (menu) menu.hidden = true;
    if (entry) entry.setAttribute("aria-expanded", "false");
    accountMenuOpen = false;
  }

  function toggleAccountMenu(forceOpen) {
    const dock = getHeaderDock();
    if (!dock) return;
    const menu = qs("[data-auth-account-popover]", dock);
    const entry = qs("[data-auth-action='account']", dock);
    if (!menu || !entry) return;
    const shouldOpen = typeof forceOpen === "boolean" ? forceOpen : menu.hidden;
    menu.hidden = !shouldOpen;
    entry.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
    accountMenuOpen = shouldOpen;
  }

  function setStep(modal, step) {
    const nextStep = step === "mfa" ? "mfa" : "twitter";
    const twitterPanel = qs('[data-auth-step="twitter"]', modal);
    const mfaPanel = qs('[data-auth-step="mfa"]', modal);
    if (twitterPanel) twitterPanel.hidden = nextStep !== "twitter";
    if (mfaPanel) mfaPanel.hidden = nextStep !== "mfa";
    modal.dataset.step = nextStep;
    AUTH_STATE.step = nextStep;
  }

  function openModal(mode = "twitter") {
    const modal = ensureModal();
    const nextMode = mode === "mfa" ? "mfa" : "twitter";
    closeAccountMenu();
    modal.hidden = false;
    modal.dataset.mode = nextMode;
    AUTH_STATE.mode = nextMode;
    document.body.classList.add("pf-auth-modal-open");
    const title = qs("#pf-auth-title", modal);
    const copy = qs("#pf-auth-copy", modal);
    if (title) title.textContent = nextMode === "mfa" ? "Google Authenticator" : "Twitter login";
    if (copy) {
      copy.textContent =
        nextMode === "mfa"
          ? "Enter your Google Authenticator code or a backup code to finish signing in."
          : "Use Twitter to sign in. Google Authenticator is still mandatory for Copy Trading.";
    }
    if (nextMode === "mfa" && AUTH_STATE.signinChallenge) {
      setStep(modal, "mfa");
    } else {
      if (nextMode !== "mfa") {
        AUTH_STATE.signinChallenge = null;
      }
      setStep(modal, "twitter");
    }
    setMessage("", "");
    const twitterStart = qs("[data-auth-twitter-start]", modal);
    const codeInput = qs("[data-auth-mfa-code]", modal);
    if (nextMode === "mfa" && codeInput) {
      codeInput.focus();
    } else if (twitterStart) {
      twitterStart.focus();
    }
  }

  function closeModal() {
    const modal = qs("[data-auth-modal]");
    if (!modal) return;
    modal.hidden = true;
    document.body.classList.remove("pf-auth-modal-open");
    setMessage("", "");
    closeAccountMenu();
  }

  function buildReturnPath() {
    const url = new URL(window.location.href);
    url.searchParams.delete("auth");
    url.searchParams.delete("mfaToken");
    url.searchParams.delete("label");
    url.searchParams.delete("message");
    return `${url.pathname}${url.search}${url.hash}` || "/";
  }

  function startTwitterLogin() {
    if (!AUTH_STATE.authAccessLoaded) {
      setMessage("Checking account access…", "");
      return;
    }
    if (AUTH_STATE.authAccessAllowed === false) {
      setMessage(AUTH_STATE.authAccessReason || "Account access is restricted to an approved IP.", "error");
      return;
    }
    if (AUTH_STATE.twitterConfigured === false) {
      setMessage("Twitter login is not configured on this server.", "error");
      return;
    }
    const returnTo = encodeURIComponent(buildReturnPath());
    window.location.assign(`/api/auth/twitter/start?return=${returnTo}`);
  }

  function setMfaState(payload) {
    const modal = ensureModal();
    const label = String(
      payload && payload.label
        ? payload.label
        : AUTH_STATE.user?.displayName || AUTH_STATE.user?.twitterUsername || AUTH_STATE.user?.email || ""
    ).trim();
    AUTH_STATE.signinChallenge = {
      mfaToken: String(payload && payload.mfaToken ? payload.mfaToken : "").trim(),
      label,
    };
    modal.dataset.mode = "mfa";
    AUTH_STATE.mode = "mfa";
    setStep(modal, "mfa");
    const emailLabel = qs("[data-auth-mfa-email]", modal);
    const codeInput = qs("[data-auth-mfa-code]", modal);
    const title = qs("#pf-auth-title", modal);
    const copy = qs("#pf-auth-copy", modal);
    if (title) title.textContent = "Google Authenticator";
    if (copy) {
      copy.textContent = "Enter the current code from Google Authenticator or a backup code from Settings to finish signing in.";
    }
    if (emailLabel) emailLabel.textContent = label || "your account";
    if (codeInput) {
      codeInput.value = "";
      codeInput.focus();
    }
    setMessage("Two-factor authentication is required to continue.", "");
  }

  async function submitMfaVerification(event) {
    event.preventDefault();
    const modal = ensureModal();
    const mfaToken = String(AUTH_STATE.signinChallenge && AUTH_STATE.signinChallenge.mfaToken ? AUTH_STATE.signinChallenge.mfaToken : "").trim();
    const code = String(qs("[data-auth-mfa-code]", modal)?.value || "").trim();
    if (!mfaToken) {
      setMessage("Please sign in again to request a new authentication challenge.", "error");
      return;
    }
    if (code.length < 6) {
      setMessage("Enter a Google Authenticator code or backup code.", "error");
      return;
    }
    setMessage("Verifying authenticator code...", "");
    const response = await fetch("/api/auth/signin/verify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      credentials: "same-origin",
      body: JSON.stringify({ mfaToken, code }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload && payload.error ? String(payload.error) : `Request failed (${response.status})`);
    }
    AUTH_STATE.loading = false;
    AUTH_STATE.authenticated = Boolean(payload && payload.authenticated);
    AUTH_STATE.user = payload && payload.user ? payload.user : null;
    AUTH_STATE.signinChallenge = null;
    updateAuthUi();
    window.history.replaceState({}, document.title, buildReturnPath());
    closeModal();
    window.dispatchEvent(
      new CustomEvent("pf-auth-change", {
        detail: {
          authenticated: AUTH_STATE.authenticated,
          user: AUTH_STATE.user,
        },
      })
    );
  }

  async function fetchAuthState() {
    try {
      const response = await fetch("/api/auth/me", {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload && payload.error ? String(payload.error) : "auth_fetch_failed");
      AUTH_STATE.loading = false;
      AUTH_STATE.authenticated = Boolean(payload && payload.authenticated);
      AUTH_STATE.user = payload && payload.user ? payload.user : null;
      if (!AUTH_STATE.authenticated && !AUTH_STATE.signinChallenge) {
        AUTH_STATE.signinChallenge = null;
      }
      updateAuthUi();
      return AUTH_STATE;
    } catch (_error) {
      AUTH_STATE.loading = false;
      AUTH_STATE.authenticated = false;
      AUTH_STATE.user = null;
      if (!AUTH_STATE.signinChallenge) {
        AUTH_STATE.signinChallenge = null;
      }
      updateAuthUi();
      return AUTH_STATE;
    }
  }

  async function fetchTwitterStatus() {
    try {
    const response = await fetch("/api/auth/twitter/status", {
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    AUTH_STATE.twitterConfigured = Boolean(response.ok && payload && payload.configured);
    } catch (_error) {
      AUTH_STATE.twitterConfigured = false;
    }
    updateAuthUi();
  }

  async function fetchAuthAccess() {
    try {
      const response = await fetch("/api/auth/access", {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await response.json().catch(() => ({}));
      AUTH_STATE.authAccessLoaded = true;
      AUTH_STATE.authAccessAllowed = Boolean(response.ok && payload && payload.allowed);
      AUTH_STATE.authAccessReason = payload && payload.reason ? String(payload.reason) : null;
    } catch (_error) {
      AUTH_STATE.authAccessLoaded = false;
      AUTH_STATE.authAccessAllowed = false;
      AUTH_STATE.authAccessReason = null;
    }
    updateAuthUi();
  }

  function updateAuthUi() {
    const isAuthed = Boolean(AUTH_STATE.authenticated);
    const isMfaEnabled = Boolean(AUTH_STATE.user?.mfaEnabled);
    const isRestricted = Boolean(!AUTH_STATE.authAccessLoaded || AUTH_STATE.authAccessAllowed === false);
    document.body.dataset.authState = isAuthed ? "authed" : "guest";
    qsa("[data-auth-when]").forEach((node) => {
      const when = String(node.getAttribute("data-auth-when") || "").trim().toLowerCase();
      const shouldShow =
        when === "authed"
          ? isAuthed
          : when === "guest"
            ? !isAuthed
            : when === "copy-trading"
              ? isAuthed && isMfaEnabled
              : true;
      node.hidden = !shouldShow;
    });
    qsa("[data-auth-user-name]").forEach((node) => {
      node.textContent = isAuthed
        ? String(AUTH_STATE.user?.displayName || AUTH_STATE.user?.email || "Trader")
        : "Trader";
    });
    qsa("[data-auth-action='account']").forEach((node) => {
      if (!(node instanceof HTMLElement)) return;
      node.disabled = isRestricted;
      node.setAttribute("aria-disabled", isRestricted ? "true" : "false");
      node.title = isRestricted
        ? AUTH_STATE.authAccessLoaded
          ? AUTH_STATE.authAccessReason || "Account access is restricted to an approved IP."
          : "Checking account access…"
        : "Open the account menu";
      node.classList.toggle("is-disabled", isRestricted);
      const label = qs(".pf-auth-entry-label", node);
      if (label) label.textContent = !AUTH_STATE.authAccessLoaded ? "Checking…" : isRestricted ? "Restricted" : "Account";
    });
    qsa("[data-auth-shell]").forEach((node) => {
      node.dataset.state = isAuthed ? "authed" : "guest";
    });
    const dock = getHeaderDock();
    if (dock) {
      const title = qs("[data-auth-popover-title]", dock);
      const copy = qs("[data-auth-popover-copy]", dock);
      const userName = qs("[data-auth-user-name]", dock);
      if (title) title.textContent = isAuthed ? "Account" : "Account";
      if (copy) {
        copy.textContent = isAuthed
          ? "Your account is active. Use Settings to manage security and Copy Trading access."
          : !AUTH_STATE.authAccessLoaded
            ? "Checking whether this IP is allowed to access account sign-in."
            : isRestricted
            ? AUTH_STATE.authAccessReason || "Account access is restricted to an approved IP."
            : "Sign in or create an account to unlock Copy Trading.";
      }
      if (userName) {
        userName.hidden = !isAuthed;
        userName.textContent = isAuthed
          ? String(AUTH_STATE.user?.displayName || AUTH_STATE.user?.email || "Trader")
          : "Trader";
      }
      qsa("[data-auth-when]", dock).forEach((node) => {
        const when = String(node.getAttribute("data-auth-when") || "").trim().toLowerCase();
        node.hidden =
          when === "authed"
            ? !isAuthed
            : when === "guest"
              ? isAuthed
              : false;
      });
    }
    const modal = qs("[data-auth-modal]");
    if (modal && !modal.hidden) {
      const twitterStart = qs("[data-auth-twitter-start]", modal);
      if (twitterStart) {
        twitterStart.disabled = AUTH_STATE.twitterConfigured === false;
        twitterStart.textContent = AUTH_STATE.twitterConfigured === false
          ? "Twitter login unavailable"
          : !AUTH_STATE.authAccessLoaded
            ? "Checking access…"
            : isRestricted
            ? "Access restricted"
            : "Continue with Twitter";
        twitterStart.disabled = twitterStart.disabled || isRestricted;
      }
    }
  }

  async function signOut() {
    try {
      await fetch("/api/auth/signout", {
        method: "POST",
        headers: { Accept: "application/json" },
        credentials: "same-origin",
      });
    } catch (_error) {}
    AUTH_STATE.loading = false;
    AUTH_STATE.authenticated = false;
    AUTH_STATE.user = null;
    AUTH_STATE.signinChallenge = null;
    updateAuthUi();
    window.dispatchEvent(new CustomEvent("pf-auth-change", { detail: { authenticated: false, user: null } }));
  }

  function bindEvents() {
    document.addEventListener("click", (event) => {
      const target = event.target instanceof HTMLElement ? event.target : null;
      if (!target) return;
      const twitterStart = target.closest("[data-auth-twitter-start]");
      if (twitterStart instanceof HTMLElement) {
        event.preventDefault();
        startTwitterLogin();
        return;
      }
      const dock = getHeaderDock();
      if (dock && target.closest("[data-auth-header-dock]")) {
        const accountButton = target.closest("[data-auth-action='account']");
        if (accountButton instanceof HTMLElement) {
          event.preventDefault();
          if (AUTH_STATE.authAccessLoaded && AUTH_STATE.authAccessAllowed === false) {
            setMessage(AUTH_STATE.authAccessReason || "Account access is restricted to an approved IP.", "error");
            return;
          }
          if (Boolean(AUTH_STATE.authenticated)) {
            toggleAccountMenu();
          } else {
            closeAccountMenu();
            openModal("twitter");
          }
          return;
        }
      }
      const actionButton = target.closest("[data-auth-action]");
      if (actionButton instanceof HTMLElement) {
        const action = String(actionButton.getAttribute("data-auth-action") || "").trim();
        if (action === "signout") {
          event.preventDefault();
          closeAccountMenu();
          signOut().catch(() => null);
        }
      }
      const closeButton = target.closest("[data-auth-close]");
      if (closeButton instanceof HTMLElement) {
        event.preventDefault();
        closeModal();
      }
    });

    document.addEventListener("submit", (event) => {
      const form = event.target;
      if (form instanceof HTMLFormElement && form.matches("[data-auth-mfa-form]")) {
        event.preventDefault();
        submitMfaVerification(event).catch((error) => {
          setMessage(error && error.message ? String(error.message) : "Authenticator verification failed.", "error");
        });
      }
    });

    document.addEventListener("click", (event) => {
      const target = event.target instanceof HTMLElement ? event.target : null;
      if (!target) return;
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      const modal = qs("[data-auth-modal]");
      if (modal && !modal.hidden) closeModal();
      closeAccountMenu();
    });

    document.addEventListener("click", (event) => {
      const modal = qs("[data-auth-modal]");
      if (!modal || modal.hidden) return;
      if (event.target === modal) closeModal();
    });

    document.addEventListener("click", (event) => {
      const target = event.target instanceof HTMLElement ? event.target : null;
      if (!target || !accountMenuOpen) return;
      const dock = getHeaderDock();
      if (!dock) return;
      if (!target.closest("[data-auth-header-dock]")) {
        closeAccountMenu();
      }
    });
  }

  function init() {
    ensureModal();
    ensureHeaderDock();
    bindEvents();
    updateAuthUi();
    const params = new URLSearchParams(window.location.search);
    if (params.get("auth") === "twitter-mfa" && params.get("mfaToken")) {
      const label = String(params.get("label") || "").trim();
      setMfaState({
        mfaToken: params.get("mfaToken"),
        label: label || "your Twitter account",
      });
      openModal("mfa");
    }
    fetchAuthAccess().catch(() => null);
    fetchTwitterStatus().catch(() => null);
    fetchAuthState().catch(() => null);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }

  window.__PF_AUTH_UI_REFRESH = fetchAuthState;
  window.__PF_AUTH_UI_STATE = AUTH_STATE;
})();
