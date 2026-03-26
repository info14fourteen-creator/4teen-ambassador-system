# 4teen-ambassador-system — CORE OVERVIEW

Generated: 2026-03-26T10:58:25.267Z
Repository: info14fourteen-creator/4teen-ambassador-system
Branch: main

## Curated project tree

```txt
- .github/
  - workflows/
    - allocation-worker-daily.yml
    - build-ai-bundles.yml
- ai/
  - MASTER_PLAN.md
  - WORKING_RULES.md
- apps/
  - cabinet/
    - src/
      - app/
        - ambassador/
          - page.tsx
      - hooks/
        - useAmbassadorDashboard.ts
      - lib/
        - blockchain/
          - controller.ts
        - referral/
          - storage.ts
        - telegram/
          - link.ts
  - site-integration/
    - src/
      - ambassador/
        - autoMount.ts
        - hash.ts
        - register.ts
        - widget.ts
      - purchase/
        - afterBuy.ts
        - submitAttribution.ts
      - referral/
        - capture.ts
        - firstTouch.ts
        - storage.ts
- scripts/
  - build-ai-bundles.mjs
- services/
  - allocation-worker/
    - src/
      - app/
        - processAttribution.ts
      - db/
        - ambassadors.ts
        - postgres.ts
        - purchases.ts
      - domain/
        - allocation.ts
        - attribution.ts
      - jobs/
        - allocatePurchase.ts
        - dailyMaintenance.ts
        - prepareAmbassadorWithdrawal.ts
        - processAmbassadorPendingQueue.ts
        - rentEnergy.ts
        - replayDeferredPurchases.ts
      - services/
        - gasStation.ts
      - tron/
        - controller.ts
        - hashing.ts
        - resources.ts
      - index.ts
      - run-scan.ts
      - server.ts
    - package.json
    - tsconfig.json
- shared/
  - config/
    - contracts.ts
    - referral.ts
  - utils/
    - slug.ts
- package.json
- README.md
```

## Included files

- 4teen-ambassador-system :: ai/WORKING_RULES.md
- 4teen-ambassador-system :: package.json
- 4teen-ambassador-system :: README.md
- 4teen-ambassador-system :: shared/config/contracts.ts
- 4teen-ambassador-system :: shared/config/referral.ts
- 4teen-ambassador-system :: shared/utils/slug.ts

---

## FILE: 4teen-ambassador-system :: ai/WORKING_RULES.md

```md
# 4TEEN AMBASSADOR SYSTEM — WORKING RULES (GOLD STANDARD)

## Purpose

Define strict rules for how we work during the build of the 4TEEN ambassador, referral, dashboard, worker, and notification system.

This document is the operating contract for all further steps.

---

## CORE PRINCIPLES

### 1. FULL FILE REWRITE ONLY

- No partial patches
- No diff-style edits
- No “change this line” instructions

Every time:

- you send a file path or current file content
- I return a FULL rewritten version
- ready for copy-paste

### 2. ENGLISH ONLY

- All code comments MUST be in English
- No Russian comments
- No mixed language inside code or technical files

### 3. SOURCE OF TRUTH = REPO SNAPSHOT + YOUR FILE

- The project snapshot is the shared context
- The current repo file is always the final source of truth
- I must not assume missing code
- I must not reuse outdated versions when a newer file exists

### 4. NO GUESSING

If something is unclear:

- I ask OR design the safest production-ready version
- never invent hidden behavior
- never invent missing contract logic
- never invent API payloads without a clear basis

### 5. CLEAN ARCHITECTURE FIRST

We are not stacking hacks.
We are building a stable system.

If a file is bad:

- we rewrite

If a file is useless:

- we delete

If a module boundary is wrong:

- we refactor cleanly

---

## FILE DECISION RULE

Every file falls into ONE category:

1. KEEP
2. REWRITE
3. DELETE
4. CREATE

I should always explicitly state which one it is when relevant.

---

## REWRITE STANDARD

Every rewritten file must be:

- self-contained
- clean
- readable
- no dead code
- no legacy hacks
- no hidden fallbacks
- no silent attribution changes
- strict single responsibility
- production-oriented

---

## PROJECT-SPECIFIC RULES

### Referral rules

- Attribution model = first-touch before first successful purchase
- Referral source may be stored before purchase
- Buyer is bound on-chain only after the first successful verified purchase
- Disabled ambassadors must not receive new buyer bindings
- Existing bound buyers must still continue to attribute to the same ambassador unless owner explicitly rebinds

### Controller rules

- The controller must never mix:
  - ownerAvailableBalance
  - totalReservedRewards
  - unallocatedPurchaseFunds
- Owner must never be able to withdraw reserved ambassador rewards
- Direct random TRX transfers must not silently corrupt accounting
- Contract reads for dashboard should be convenient but safe for compilation

### Cabinet rules

- Cabinet must not contain hidden business logic
- Cabinet reads from blockchain and backend
- Cabinet must expose:
  - ambassador profile
  - referral link
  - stats
  - rewards
  - Telegram connection status
  - withdraw action

### Worker rules

- Worker verifies purchases before allocation
- One purchase must never be processed twice
- Allocation must be traceable
- Failed allocations must be replayable
- Worker logic must be deterministic and logged

### Telegram rules

- Telegram must be linked only through cabinet
- No manual wallet claiming through bot chat
- Bot is a notification and status layer, not a source of truth
- Link tokens must be one-time or short-lived

---

## SNAPSHOT RULES

The repository snapshot exists to keep long-term context stable.

### Snapshot goals

- preserve current repo structure
- preserve current file contents
- preserve architectural rules
- allow me to stay aligned with the latest repo state

### Snapshot format

The AI snapshot must be split into multiple files, not one giant file.

Required outputs:

- `ai/latest/ai-project-map.txt`
- `ai/latest/ai-bundle-core.txt`
- `ai/latest/ai-bundle-cabinet.txt`
- `ai/latest/ai-bundle-site.txt`
- `ai/latest/ai-bundle-worker.txt`
- `ai/latest/ai-bundle-telegram.txt`

### Snapshot priority

When both snapshot and current user-provided file exist:

- the newest current file wins

---

## WORKFLOW

You send file or task →  
I respond with:

1. Decision (keep / rewrite / delete / create)
2. Full file content (if rewrite/create)
3. Short explanation when needed

Then next file.

---

## STRICT RULE

No skipping steps.  
No jumping ahead.  
No “let’s fix later”.

Everything must work step-by-step.

---

## FINAL GOAL

- Stable referral system
- Stable ambassador cabinet
- Stable allocation worker
- Stable Telegram notification flow
- Full control over attribution and rewards
- No accounting corruption
- Ready for production operations

---

## END
```

---

## FILE: 4teen-ambassador-system :: package.json

```json
{
  "name": "fourteen-heroku-worker-root",
  "private": true,
  "scripts": {
    "build": "npm run build:worker",
    "build:worker": "npm --prefix services/allocation-worker install --include=dev && npm --prefix services/allocation-worker run build",
    "start": "npm run start:worker",
    "start:worker": "npm --prefix services/allocation-worker run start",
    "build:ai": "node scripts/build-ai-bundles.mjs",
    "heroku-postbuild": "npm run build:worker"
  }
}
```

---

## FILE: 4teen-ambassador-system :: README.md

```md
# 4teen-ambassador-system
Full-stack ambassador and referral system for the 4TEEN token:  first-touch attribution, on-chain reward distribution, dashboard, and real-time Telegram notifications.
```

---

## FILE: 4teen-ambassador-system :: shared/config/contracts.ts

```ts
export const TRON_NETWORK = "mainnet";

export const FOURTEEN_CONTROLLER_CONTRACT = "TF8yhohRfMxsdVRr7fFrYLh5fxK8sAFkeZ";
export const FOURTEEN_TOKEN_CONTRACT = "TMLXiCW2ZAkvjmn79ZXa4vdHX5BE3n9x4A";

export const TRONSCAN_BASE_URL = "https://tronscan.org/#";
export const TRONSCAN_ADDRESS_URL = `${TRONSCAN_BASE_URL}/address`;
export const TRONSCAN_TRANSACTION_URL = `${TRONSCAN_BASE_URL}/transaction`;

function assertNonEmpty(value: string, fieldName: string): string {
  const normalized = String(value || "").trim();

  if (!normalized) {
    throw new Error(`${fieldName} is required`);
  }

  return normalized;
}

export function buildTronscanAddressUrl(address: string): string {
  return `${TRONSCAN_ADDRESS_URL}/${assertNonEmpty(address, "address")}`;
}

export function buildTronscanTransactionUrl(txid: string): string {
  return `${TRONSCAN_TRANSACTION_URL}/${assertNonEmpty(txid, "txid")}`;
}
```

---

## FILE: 4teen-ambassador-system :: shared/config/referral.ts

```ts
export const REFERRAL_QUERY_PARAM = "r";
export const REFERRAL_STORAGE_KEY = "fourteen_referral_first_touch_v1";

export const REFERRAL_TTL_DAYS = 30;
export const REFERRAL_TTL_MS = REFERRAL_TTL_DAYS * 24 * 60 * 60 * 1000;

export const REFERRAL_SLUG_MIN_LENGTH = 3;
export const REFERRAL_SLUG_MAX_LENGTH = 64;

export const REFERRAL_SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{1,62}[a-z0-9])?$/;
```

---

## FILE: 4teen-ambassador-system :: shared/utils/slug.ts

```ts
import {
  REFERRAL_SLUG_MAX_LENGTH,
  REFERRAL_SLUG_MIN_LENGTH,
  REFERRAL_SLUG_PATTERN
} from "../config/referral";

export function normalizeSlug(input: string): string {
  return String(input || "").trim().toLowerCase();
}

export function isValidSlug(slug: string): boolean {
  const normalized = normalizeSlug(slug);

  if (normalized.length < REFERRAL_SLUG_MIN_LENGTH) return false;
  if (normalized.length > REFERRAL_SLUG_MAX_LENGTH) return false;

  return REFERRAL_SLUG_PATTERN.test(normalized);
}

export function assertValidSlug(slug: string): string {
  const normalized = normalizeSlug(slug);

  if (!isValidSlug(normalized)) {
    throw new Error("Invalid referral slug");
  }

  return normalized;
}
```
