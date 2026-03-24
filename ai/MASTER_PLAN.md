# 4TEEN AMBASSADOR SYSTEM — DEVELOPMENT MASTER PLAN

## PURPOSE

This document defines the full development plan for the 4TEEN Ambassador System.

It includes:
- architecture layers
- development phases
- required features
- strict execution order
- testing checkpoints

This is the single source of truth for project execution.

---

# SYSTEM OVERVIEW

The system consists of 5 core layers:

1. SITE (referral capture)
2. CABINET (ambassador dashboard)
3. CONTROLLER (smart contract)
4. WORKER (allocation engine)
5. TELEGRAM (notification layer)

Flow:

Referral → Purchase → Verification → Allocation → Rewards → Dashboard → Notifications

---

# CORE BUSINESS LOGIC

## Ambassador Levels

- Bronze → 10%
- Silver → 25% (10 buyers)
- Gold → 50% (100 buyers)
- Platinum → 75% (1000 buyers)

Level can be:
- auto-calculated
- manually overridden by owner

---

## Referral Model

FIRST-TOUCH attribution

Rules:

- Referral is captured on visit (?r=slug)
- Stored locally
- NOT bound immediately
- Binding happens ONLY after first successful purchase
- Buyer is permanently assigned to ambassador

---

## Purchase Flow

1. User opens site with ?r=slug
2. Slug stored locally (first-touch)
3. User makes purchase (buyTokens)
4. TX hash received
5. Frontend sends:
   - txHash
   - buyer wallet
   - stored slug

6. Worker:
   - verifies TX
   - extracts amount
   - calls contract

7. Contract:
   recordVerifiedPurchase()

---

## Money Flow (CRITICAL)

From each purchase:

- 7% → goes to controller
- inside controller:
  - part → ambassador reward
  - part → owner profit

STRICT RULES:

- ownerAvailableBalance ≠ totalReservedRewards
- reserved rewards must NEVER be withdrawable by owner
- unallocatedPurchaseFunds must be processed only once

---

# DEVELOPMENT PHASES

---

## PHASE 1 — REFERRAL CORE (CRITICAL)

FILES:

apps/site-integration/src/referral/
- capture.ts
- firstTouch.ts
- storage.ts

apps/site-integration/src/purchase/
- afterBuy.ts
- submitAttribution.ts

shared/utils/
- slug.ts

---

### IMPLEMENTATION

- parse ?r=slug
- validate slug format
- store in localStorage
- apply first-touch logic
- set expiration (TTL)
- prevent overwrite

---

### RESULT

- referral persists across sessions
- referral survives reloads
- referral sent after purchase

---

### TEST

1. open site with ?r=abc
2. reload page
3. remove param
4. confirm slug still exists
5. simulate purchase
6. confirm attribution sent

---

## PHASE 2 — CONTROLLER INTEGRATION

FILES:

apps/cabinet/src/lib/blockchain/
- controller.ts

shared/config/
- contracts.ts

---

### IMPLEMENTATION

- connect TronWeb
- connect wallet
- read contract:
  - ambassador stats
  - rewards
  - level
- implement withdrawRewards()

---

### RESULT

- cabinet can read blockchain
- user sees real rewards

---

### TEST

1. connect wallet
2. read stats
3. read rewards
4. verify correctness

---

## PHASE 3 — CABINET (UI + LOGIC)

FILES:

apps/cabinet/src/app/ambassador/page.tsx
apps/cabinet/src/hooks/useAmbassadorDashboard.ts

---

### IMPLEMENTATION

- wallet connect
- display:
  - level
  - referral link
  - total earnings
  - claimable rewards
- withdraw button
- telegram connect button

---

### RESULT

- working ambassador dashboard

---

### TEST

1. open cabinet
2. connect wallet
3. verify UI data
4. click withdraw

---

## PHASE 4 — WORKER (CORE ENGINE)

FILES:

services/allocation-worker/src/
- run-scan.ts
- jobs/scanBuyEvents.ts
- jobs/allocatePurchase.ts
- tron/controller.ts
- db/purchases.ts

---

### IMPLEMENTATION

- scan BuyTokens events
- extract:
  - buyer
  - amount
- match referral
- build purchaseId
- call:
  recordVerifiedPurchase()

- store processed TX
- prevent duplicates

---

### RESULT

- automatic reward allocation

---

### TEST

1. execute real purchase
2. worker detects TX
3. allocation happens
4. contract updates rewards

---

## PHASE 5 — TELEGRAM

FILES:

services/telegram-bot/src/
- server.ts
- commands/start.ts
- services/auth.ts
- services/notifications.ts

---

### IMPLEMENTATION

- connect via cabinet only
- generate link token
- link wallet ↔ telegram
- send notifications:
  - new purchase
  - reward
  - level up

---

### RESULT

- real-time ambassador feedback

---

### TEST

1. connect Telegram
2. trigger purchase
3. receive notification

---

## PHASE 6 — AUTOMATION

FILES:

.github/workflows/

---

### IMPLEMENTATION

- run worker periodically
- retry failed allocations
- generate AI bundles

---

### RESULT

- system runs without manual intervention

---

# DATA SAFETY RULES

AI and developer must always ensure:

1. No double allocation
2. No reward leakage
3. No incorrect ambassador binding
4. No mixing balances
5. No silent failure

---

# FINAL ACCEPTANCE CRITERIA

System is complete when:

- referral tracking is stable
- purchases are correctly attributed
- rewards are correctly allocated
- dashboard reflects real data
- withdrawals work
- telegram notifications work
- no accounting inconsistencies exist

---

# EXECUTION RULE

Work strictly phase-by-phase.

DO NOT:
- skip steps
- mix layers
- jump ahead

Each phase must be:
- implemented
- tested
- validated

before moving forward.

---

# END
