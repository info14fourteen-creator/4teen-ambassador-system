import { registerAmbassador } from "./register";

export interface MountAmbassadorWidgetOptions {
  target: HTMLElement;
  backendBaseUrl: string;
  defaultSlug?: string;
  title?: string;
  description?: string;
}

interface WidgetState {
  isSubmitting: boolean;
  error: string | null;
  success: {
    slug: string;
    referralLink: string;
    txid: string;
  } | null;
}

function assertElement(target: HTMLElement | null | undefined): HTMLElement {
  if (!target) {
    throw new Error("target element is required");
  }

  return target;
}

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function createEl<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className?: string,
  text?: string
): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);

  if (className) {
    el.className = className;
  }

  if (text) {
    el.textContent = text;
  }

  return el;
}

function setButtonBusy(button: HTMLButtonElement, busy: boolean): void {
  button.disabled = busy;
  button.textContent = busy ? "Registering..." : "Register Ambassador";
}

function normalizeReferralLink(link: string): string {
  return assertNonEmpty(link, "referralLink");
}

function buildAbsoluteReferralLink(relativeOrAbsoluteLink: string): string {
  const normalized = normalizeReferralLink(relativeOrAbsoluteLink);

  if (/^https?:\/\//i.test(normalized)) {
    return normalized;
  }

  return `${window.location.origin}${normalized.startsWith("/") ? normalized : `/${normalized}`}`;
}

export function mountAmbassadorWidget(
  options: MountAmbassadorWidgetOptions
): void {
  const target = assertElement(options.target);

  const state: WidgetState = {
    isSubmitting: false,
    error: null,
    success: null
  };

  const root = createEl(
    "div",
    "fourteen-ambassador-widget rounded-2xl border border-white/10 bg-white/5 p-5"
  );

  const header = createEl("div", "fourteen-ambassador-widget__header");
  const title = createEl(
    "h2",
    "fourteen-ambassador-widget__title text-xl font-semibold",
    options.title || "Become an ambassador"
  );
  const description = createEl(
    "p",
    "fourteen-ambassador-widget__description mt-2 text-sm opacity-80",
    options.description ||
      "Choose your public referral slug, register on-chain, and receive your referral link."
  );

  header.appendChild(title);
  header.appendChild(description);

  const form = createEl("form", "fourteen-ambassador-widget__form mt-4") as HTMLFormElement;

  const slugLabel = createEl("label", "fourteen-ambassador-widget__label block text-sm");
  slugLabel.textContent = "Referral slug";

  const slugInput = createEl(
    "input",
    "fourteen-ambassador-widget__input mt-2 w-full rounded-xl border border-white/10 bg-transparent px-3 py-3 outline-none"
  ) as HTMLInputElement;
  slugInput.type = "text";
  slugInput.name = "slug";
  slugInput.placeholder = "stan";
  slugInput.autocomplete = "off";
  slugInput.value = options.defaultSlug || "";

  const submitButton = createEl(
    "button",
    "fourteen-ambassador-widget__submit mt-4 rounded-2xl px-4 py-3 font-semibold"
  ) as HTMLButtonElement;
  submitButton.type = "submit";
  submitButton.textContent = "Register Ambassador";

  const messageBox = createEl("div", "fourteen-ambassador-widget__message mt-4 text-sm");
  const successBox = createEl("div", "fourteen-ambassador-widget__success mt-4");
  successBox.style.display = "none";

  const successTitle = createEl(
    "div",
    "fourteen-ambassador-widget__success-title text-sm font-semibold",
    "Registration completed"
  );

  const successSlug = createEl("div", "fourteen-ambassador-widget__success-slug mt-2 text-sm");
  const successLinkWrap = createEl("div", "fourteen-ambassador-widget__success-link mt-2 text-sm");
  const successLink = createEl("a", "underline") as HTMLAnchorElement;
  successLink.target = "_blank";
  successLink.rel = "noreferrer";

  const successTx = createEl("div", "fourteen-ambassador-widget__success-tx mt-2 text-xs opacity-80");

  successLinkWrap.appendChild(successLink);
  successBox.appendChild(successTitle);
  successBox.appendChild(successSlug);
  successBox.appendChild(successLinkWrap);
  successBox.appendChild(successTx);

  function render(): void {
    setButtonBusy(submitButton, state.isSubmitting);

    if (state.error) {
      messageBox.textContent = state.error;
      messageBox.style.display = "block";
    } else {
      messageBox.textContent = "";
      messageBox.style.display = "none";
    }

    if (state.success) {
      const absoluteLink = buildAbsoluteReferralLink(state.success.referralLink);

      successSlug.textContent = `Slug: ${state.success.slug}`;
      successLink.href = absoluteLink;
      successLink.textContent = absoluteLink;
      successTx.textContent = `Tx: ${state.success.txid}`;
      successBox.style.display = "block";
    } else {
      successBox.style.display = "none";
    }
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (state.isSubmitting) {
      return;
    }

    state.isSubmitting = true;
    state.error = null;
    state.success = null;
    render();

    try {
      const result = await registerAmbassador({
        slug: slugInput.value,
        backendBaseUrl: options.backendBaseUrl
      });

      state.success = {
        slug: result.slug,
        referralLink: result.referralLink,
        txid: result.txid
      };
    } catch (error) {
      state.error =
        error && typeof error === "object" && "message" in error
          ? String((error as { message?: unknown }).message || "").trim() || "Registration failed"
          : typeof error === "string" && error.trim()
            ? error.trim()
            : "Registration failed";
    } finally {
      state.isSubmitting = false;
      render();
    }
  });

  slugLabel.appendChild(slugInput);

  form.appendChild(slugLabel);
  form.appendChild(submitButton);

  root.appendChild(header);
  root.appendChild(form);
  root.appendChild(messageBox);
  root.appendChild(successBox);

  target.innerHTML = "";
  target.appendChild(root);

  render();
}
