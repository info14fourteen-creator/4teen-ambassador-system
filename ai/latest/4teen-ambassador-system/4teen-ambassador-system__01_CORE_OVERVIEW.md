# 4teen-ambassador-system — CORE OVERVIEW

Generated: 2026-04-01T17:36:31.875Z
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
        - dashboardSnapshots.ts
        - postgres.ts
        - purchases.ts
      - domain/
        - allocation.ts
        - attribution.ts
      - jobs/
        - allocatePurchase.ts
        - dailyMaintenance.ts
        - finalizeAmbassadorWithdrawal.ts
        - prepareAmbassadorWithdrawal.ts
        - processAmbassadorPendingQueue.ts
        - rentEnergy.ts
        - replayDeferredPurchases.ts
      - services/
        - cabinet.ts
        - dashboardRefresh.ts
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

## FILE PATH

`ai/WORKING_RULES.md`

## FILE CONTENT

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

## FILE PATH

`package.json`

## FILE CONTENT

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

## FILE PATH

`README.md`

## FILE CONTENT

```md
# 4TEEN Ambassador System

Full-stack ambassador and referral system for the 4TEEN ecosystem.

This repository is not just a small referral helper. Based on the current snapshot, it is a multi-part system that combines:

- first-touch referral capture on the frontend
- ambassador registration flow
- purchase attribution submission after buy
- ambassador cabinet/dashboard
- allocation worker for verification and reward processing
- off-chain queue and accounting
- on-chain reward settlement on TRON
- daily maintenance and replay workflows

The current architecture is built around a claim-first ledger strategy: verified purchases can be accepted and stored off-chain first, then finalized on-chain later when resources are available or when withdrawal preparation runs.

## Honest project positioning

The most accurate description of the repository today is:

4TEEN Ambassador System is a full-stack referral and reward distribution platform for TRON that includes frontend referral capture, ambassador registration, a dashboard/cabinet, and a backend allocation worker with deferred on-chain settlement.

## Core business model

The project follows these rules in the current snapshot:

- attribution model is first-touch before the buyer's first successful purchase
- referral source can be stored before purchase
- verified purchase data is preserved off-chain
- blockchain acts as the final reward settlement layer
- if Energy or Bandwidth is insufficient, allocation can be deferred safely
- withdrawal preparation can process pending ambassador-owned purchases later

This means the system is designed for operational safety, not just immediate on-chain writes.

## Repository structure

```text
.github/
  workflows/
    allocation-worker-daily.yml
    build-ai-bundles.yml
ai/
  MASTER_PLAN.md
  WORKING_RULES.md
apps/
  cabinet/
    src/
      app/ambassador/page.tsx
      hooks/useAmbassadorDashboard.ts
      lib/blockchain/controller.ts
      lib/referral/storage.ts
      lib/telegram/link.ts
  site-integration/
    src/
      ambassador/
        autoMount.ts
        hash.ts
        register.ts
        widget.ts
      purchase/
        afterBuy.ts
        submitAttribution.ts
      referral/
        capture.ts
        firstTouch.ts
        storage.ts
services/
  allocation-worker/
    src/
      app/processAttribution.ts
      db/
        ambassadors.ts
        postgres.ts
        purchases.ts
      domain/
        allocation.ts
        attribution.ts
      jobs/
        allocatePurchase.ts
        dailyMaintenance.ts
        prepareAmbassadorWithdrawal.ts
        processAmbassadorPendingQueue.ts
        rentEnergy.ts
        replayDeferredPurchases.ts
      services/gasStation.ts
      tron/
        controller.ts
        hashing.ts
        resources.ts
      index.ts
      run-scan.ts
      server.ts
shared/
  config/
    contracts.ts
    referral.ts
  utils/
    slug.ts
package.json
README.md
```

## Main parts of the system

### 1. Shared configuration

Located in `shared/`

This layer contains shared constants and helpers used across frontend and backend.

Important files:

- `shared/config/contracts.ts`
- `shared/config/referral.ts`
- `shared/utils/slug.ts`

Current shared config includes:

- TRON mainnet configuration
- controller contract address
- token contract address
- Tronscan URL builders
- referral query param and storage key
- referral slug validation rules
- referral TTL of 30 days

## 2. Site integration

Located in `apps/site-integration/`

This layer is responsible for integrating referral and ambassador flows into the frontend site.

It includes:

- referral capture from URL params
- first-touch persistence logic
- attribution storage helpers
- ambassador registration helpers
- auto-mount widget logic
- purchase post-processing after buy
- attribution submission to backend

Important files:

- `apps/site-integration/src/referral/capture.ts`
- `apps/site-integration/src/referral/firstTouch.ts`
- `apps/site-integration/src/referral/storage.ts`
- `apps/site-integration/src/ambassador/hash.ts`
- `apps/site-integration/src/ambassador/register.ts`
- `apps/site-integration/src/ambassador/widget.ts`
- `apps/site-integration/src/ambassador/autoMount.ts`
- `apps/site-integration/src/purchase/afterBuy.ts`
- `apps/site-integration/src/purchase/submitAttribution.ts`

### What the site integration does

The frontend can:

- capture `?r=slug`
- validate and normalize the slug
- store the first-touch referral record
- keep it for the configured TTL window
- submit attribution after a successful purchase
- mount an ambassador registration widget into a page
- register an ambassador on-chain through Tron wallet connection

### Ambassador registration flow

The current registration flow includes:

- slug normalization
- keccak-based hash generation for slug data
- contract-based registration call through Tron wallet
- return of txid and referral link on success

### Purchase attribution flow

After buy, the frontend:

- reads the stored referral
- skips submission if no referral exists
- sends `txHash`, `buyerWallet`, and `slug` to the backend attribution endpoint

## 3. Ambassador cabinet

Located in `apps/cabinet/`

This is the ambassador-facing dashboard layer.

It includes:

- ambassador page UI
- dashboard loading hook
- blockchain reads for ambassador state
- local referral storage helpers
- wallet explorer / telegram-related link helper

Important files:

- `apps/cabinet/src/app/ambassador/page.tsx`
- `apps/cabinet/src/hooks/useAmbassadorDashboard.ts`
- `apps/cabinet/src/lib/blockchain/controller.ts`
- `apps/cabinet/src/lib/referral/storage.ts`
- `apps/cabinet/src/lib/telegram/link.ts`

### Cabinet responsibilities

Based on the current code shape, the cabinet is designed to:

- detect connected wallet state
- load ambassador dashboard data
- show available on-chain rewards
- show rewards pending backend sync
- show requested-for-processing amounts
- show purchase counts by status group
- trigger withdraw flow
- show the latest withdrawal transaction

The cabinet is presentation and orchestration oriented. It reads blockchain/backend state rather than embedding hidden business rules.

## 4. Allocation worker

Located in `services/allocation-worker/`

This is the backend runtime that verifies purchases, decides allocation strategy, updates database state, and coordinates on-chain settlement.

Important files:

- `services/allocation-worker/src/app/processAttribution.ts`
- `services/allocation-worker/src/domain/attribution.ts`
- `services/allocation-worker/src/domain/allocation.ts`
- `services/allocation-worker/src/db/ambassadors.ts`
- `services/allocation-worker/src/db/purchases.ts`
- `services/allocation-worker/src/db/postgres.ts`
- `services/allocation-worker/src/jobs/allocatePurchase.ts`
- `services/allocation-worker/src/jobs/dailyMaintenance.ts`
- `services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts`
- `services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts`
- `services/allocation-worker/src/jobs/rentEnergy.ts`
- `services/allocation-worker/src/jobs/replayDeferredPurchases.ts`
- `services/allocation-worker/src/tron/controller.ts`
- `services/allocation-worker/src/tron/hashing.ts`
- `services/allocation-worker/src/tron/resources.ts`
- `services/allocation-worker/src/server.ts`
- `services/allocation-worker/src/run-scan.ts`

### Worker responsibilities

The worker handles:

- frontend attribution intake
- verified purchase preparation
- attribution decisions
- allocation decisions
- DB persistence for ambassadors and purchases
- replay of deferred purchases
- withdrawal preparation queues
- daily maintenance jobs
- resource-aware TRON operations
- contract/controller interaction

### Claim-first strategy

The current master plan explicitly supports two operational modes:

1. Eager mode
   - if resources are available, verified purchases may be written on-chain immediately

2. Claim-first mode
   - if resources are insufficient, verified purchases stay safely recorded off-chain
   - on-chain allocation can happen later during maintenance or withdrawal preparation

This is one of the most important architectural characteristics of the project.

## 5. Workflows and operations

The repository includes GitHub workflows for:

- daily maintenance execution against the allocation worker
- AI bundle generation and publishing

Relevant files:

- `.github/workflows/allocation-worker-daily.yml`
- `.github/workflows/build-ai-bundles.yml`
- `scripts/build-ai-bundles.mjs`

The daily maintenance workflow performs:

- worker health check
- POST call to the daily maintenance job endpoint
- failure inspection after the maintenance run

## Current technical stack

Based on the snapshot, the worker uses:

- TypeScript
- Node.js runtime
- `tronweb`
- `pg`
- `@noble/hashes`

The worker package also uses:

- `tsx` for local development

## Build and run

### Root scripts

The root package currently exposes:

- `npm run build`
- `npm run build:worker`
- `npm run start`
- `npm run start:worker`
- `npm run build:ai`

### Worker scripts

Inside `services/allocation-worker/` the package exposes:

- `npm run build`
- `npm run start`
- `npm run dev`
- `npm run check`

### Typical commands

From the repository root:

```bash
npm install
npm run build
npm run start
```

For worker-only local development:

```bash
cd services/allocation-worker
npm install
npm run dev
```

## Current flows in plain language

### Referral capture

- visitor lands with `?r=slug`
- slug is validated and normalized
- first-touch referral is stored
- record stays valid for the configured TTL

### Ambassador registration

- ambassador chooses a public slug
- frontend computes slug hash
- Tron wallet signs/sends the registration transaction
- frontend returns txid and referral link

### Verified purchase handling

- site submits attribution after purchase
- worker verifies purchase data
- system decides whether to allocate now or defer
- verified purchase is preserved safely in DB

### Withdrawal processing

- ambassador opens cabinet
- pending ambassador purchases can be prepared for allocation
- available rewards can be withdrawn when contract state allows it

## Important notes about current scope

A few things are important to say honestly:

- the snapshot clearly includes referral, cabinet, worker, and site integration layers
- the snapshot includes a telegram-related cabinet helper file, but the dedicated TELEGRAM snapshot section is empty
- because of that, it is safer to position Telegram as partial/integration-adjacent in this README, not as a fully documented standalone module in this snapshot bundle

## Best short description for the repo

You can use this short version at the top of GitHub if you want something cleaner:

4TEEN Ambassador System is a TRON-based full-stack referral and ambassador platform with first-touch attribution, ambassador registration, dashboard/cabinet flows, and a resource-aware backend worker for deferred reward allocation.

## Summary

This repository is best understood as four connected layers working together:

- frontend referral and ambassador integration
- ambassador dashboard/cabinet
- backend allocation and accounting worker
- shared TRON/referral configuration

It is not only a referral link helper.
It is not only a dashboard.
It is not only a worker.

It is a full ambassador operations system designed for safe attribution, deferred settlement, and on-chain reward distribution in the 4TEEN ecosystem.
```

---

## FILE PATH

`shared/config/contracts.ts`

## FILE CONTENT

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

## FILE PATH

`shared/config/referral.ts`

## FILE CONTENT

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

## FILE PATH

`shared/utils/slug.ts`

## FILE CONTENT

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
