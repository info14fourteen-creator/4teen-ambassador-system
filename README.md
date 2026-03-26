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
