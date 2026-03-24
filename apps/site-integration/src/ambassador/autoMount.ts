import { mountAmbassadorWidget } from "./widget";

export interface AutoMountAmbassadorWidgetsOptions {
  backendBaseUrl: string;
  selector?: string;
}

const DEFAULT_SELECTOR = '[data-fourteen-ambassador-widget="register"]';

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

function isHTMLElement(value: Element | null): value is HTMLElement {
  return value instanceof HTMLElement;
}

function readOptionalAttr(
  element: HTMLElement,
  name: string
): string | undefined {
  const value = element.getAttribute(name);

  if (value == null) {
    return undefined;
  }

  const normalized = value.trim();
  return normalized || undefined;
}

export function autoMountAmbassadorWidgets(
  options: AutoMountAmbassadorWidgetsOptions
): void {
  const backendBaseUrl = assertNonEmpty(options.backendBaseUrl, "backendBaseUrl");
  const selector = options.selector || DEFAULT_SELECTOR;

  const elements = Array.from(document.querySelectorAll(selector));

  for (const element of elements) {
    if (!isHTMLElement(element)) {
      continue;
    }

    if (element.dataset.fourteenAmbassadorMounted === "true") {
      continue;
    }

    mountAmbassadorWidget({
      target: element,
      backendBaseUrl,
      defaultSlug: readOptionalAttr(element, "data-default-slug"),
      defaultMeta: readOptionalAttr(element, "data-default-meta"),
      title: readOptionalAttr(element, "data-title"),
      description: readOptionalAttr(element, "data-description")
    });

    element.dataset.fourteenAmbassadorMounted = "true";
  }
}

export function autoMountAmbassadorWidgetsOnReady(
  options: AutoMountAmbassadorWidgetsOptions
): void {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => {
      autoMountAmbassadorWidgets(options);
    });
    return;
  }

  autoMountAmbassadorWidgets(options);
}
