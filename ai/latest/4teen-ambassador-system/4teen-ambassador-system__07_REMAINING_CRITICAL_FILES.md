# 4teen-ambassador-system — REMAINING CRITICAL FILES

Generated: 2026-03-30T20:32:36.005Z
Repository: info14fourteen-creator/4teen-ambassador-system
Branch: main

## Included files

- 4teen-ambassador-system :: ai/MASTER_PLAN.md

---

## FILE: 4teen-ambassador-system :: ai/MASTER_PLAN.md

```md
# 4TEEN AMBASSADOR SYSTEM — MASTER PLAN
# VERSION: CLAIM-FIRST LEDGER STRATEGY
# STATUS: ACTIVE
# OWNER: STAN
# MODE: STRICT PHASE EXECUTION

---

## CORE IDEA

System must support TWO valid allocation modes:

1. **EAGER MODE**
   - if worker has enough Energy and Bandwidth now,
   - verified purchase may be written on-chain immediately.

2. **CLAIM-FIRST MODE (PRIMARY SAFETY MODE)**
   - if resources are insufficient,
   - purchase must remain safely recorded off-chain,
   - and be written on-chain later,
   - mainly when ambassador requests withdrawal.

This means:

- purchase attribution is accepted immediately
- verification is accepted immediately
- accounting is preserved immediately
- on-chain reward finalization is NOT mandatory immediately
- on-chain write may be postponed safely

---

## SYSTEM PRINCIPLE

Blockchain is the final reward settlement layer.

Off-chain database is the operational queue and accounting layer.

So:

- frontend captures attribution
- worker verifies purchase
- DB stores verified purchase safely
- contract write happens:
  - immediately if resources are available
  - or later during withdrawal preparation
  - or later during maintenance replay

---

## NON-NEGOTIABLE RULES

1. One purchase = one unique purchaseId
2. One purchase must NEVER be allocated twice
3. Off-chain verified purchase must NEVER be lost
4. Failed on-chain allocation must NEVER erase the verified record
5. Withdrawal preparation must process only ambassador-owned pending purchases
6. Owner balance must NEVER mix with reserved ambassador rewards
7. No silent state transitions
8. Any deferred purchase must remain replayable
9. Contract write must always be idempotent from system perspective
10. Claim flow must tolerate partial progress and resume safely

---

## TARGET BUSINESS FLOW

### FLOW A — PURCHASE

1. user opens site with `?r=slug`
2. referral is captured by first-touch logic
3. user buys token
4. frontend sends attribution:
   - txHash
   - buyerWallet
   - slug
5. worker verifies:
   - tx exists
   - event is BuyTokens
   - values match
6. worker stores purchase in DB as verified
7. system decides:
   - if enough Energy + Bandwidth -> try immediate on-chain allocation
   - else keep purchase deferred

### FLOW B — WITHDRAW REQUEST

1. ambassador opens cabinet
2. ambassador clicks withdraw request
3. backend loads ambassador pending verified purchases
4. backend builds queue ordered by creation time
5. backend attempts on-chain allocation one by one:
   - oldest first
   - only for this ambassador
6. for every successful contract write:
   - purchase becomes allocated
7. if resources end:
   - stop safely
   - keep remaining purchases pending
8. after queue pass:
   - read real contract reward state
   - if withdrawable amount > 0 -> allow reward withdrawal
9. execute reward withdrawal
10. persist result

### FLOW C — DAILY / MANUAL MAINTENANCE

1. check current Energy and Bandwidth
2. optionally rent/buy resources
3. replay deferred / failed purchases
4. stop safely when resource budget is exhausted
5. preserve full audit log

---

## ARCHITECTURE DECISION

### SOURCE OF TRUTH BY LAYER

**Referral capture**
- frontend storage + backend attribution record

**Verified purchase existence**
- off-chain DB

**Allocated reward / withdrawable reward**
- blockchain contract

**Operational queue**
- off-chain DB

This split is intentional.

---

## PURCHASE LIFECYCLE

Each purchase must move through explicit statuses.

### REQUIRED STATUSES

- `received`
- `verified`
- `deferred`
- `allocation_in_progress`
- `allocated`
- `allocation_failed_retryable`
- `allocation_failed_final`
- `withdraw_included`
- `withdraw_completed`

### STATUS RULES

#### `received`
Attribution was submitted but not yet fully verified.

#### `verified`
Purchase is valid and safe to allocate.
No on-chain write guaranteed yet.

#### `deferred`
Purchase is verified but intentionally postponed due to:
- insufficient Energy
- insufficient Bandwidth
- policy decision
- claim-first batching logic

#### `allocation_in_progress`
Temporary operational lock during active contract write attempt.

#### `allocated`
`recordVerifiedPurchase(...)` already succeeded on-chain.

#### `allocation_failed_retryable`
Write attempt failed, but may be retried later.

#### `allocation_failed_final`
Used only for truly unrecoverable cases:
- invalid purchase data
- mismatched buyer
- invalid ambassador mapping
- contract-level permanent reject

#### `withdraw_included`
Purchase has been included in a specific ambassador withdrawal preparation run.

#### `withdraw_completed`
Final withdrawal flow related to prepared rewards completed.

---

## MAIN STRATEGY CHANGE

### OLD MODEL
- worker tries to allocate every verified purchase as soon as possible

### NEW MODEL
- worker MAY still allocate immediately
- but system is optimized for:
  - safe deferred queue
  - claim-time on-chain finalization
  - resource-aware replay

### WHY
Because on-chain write cost is per purchase.
`recordVerifiedPurchase(...)` is not batch settlement.
Therefore blind eager allocation is not optimal when resources are constrained.

---

## CLAIM-FIRST WITHDRAWAL STRATEGY

When ambassador requests withdrawal:

1. lock ambassador withdrawal session
2. select all purchases where:
   - ambassadorWallet = current ambassador
   - status in (`verified`, `deferred`, `allocation_failed_retryable`)
3. sort by oldest first
4. for each purchase:
   - check resources
   - if enough -> call `recordVerifiedPurchase(...)`
   - if success -> mark `allocated`
   - if insufficient resources -> stop queue safely
   - if retryable error -> mark retryable and stop or continue based on policy
5. read contract reward state
6. if reward available:
   - proceed to withdraw
7. if no reward available:
   - return clear status to UI

### IMPORTANT
Withdrawal request does NOT mean:
- "write everything no matter what"

It means:
- "process as much verified queue as safely possible for this ambassador"

---

## RESOURCE POLICY

System must check BOTH:

- Energy
- Bandwidth

before each on-chain write.

### PRE-CHECK RULE

Do not send allocation transaction if:
- available Energy < configured minimum
- available Bandwidth < configured minimum

### POSTPONE RULE

If resources are insufficient:
- do not burn blindly
- do not send transaction
- mark purchase as deferred
- keep purchase replayable

### OPTIONAL RENT MODE

System may support a separate resource provider module:
- buy/rent Energy
- optionally top up before maintenance
- optionally top up before withdrawal preparation

But business logic must NOT depend on provider availability.

Resource provider is helper infrastructure, not core truth.

---

## PHASES

---

# PHASE 1 — REFERRAL CORE

## FILES
- apps/site-integration/src/referral/capture.ts
- apps/site-integration/src/referral/firstTouch.ts
- apps/site-integration/src/referral/storage.ts
- apps/site-integration/src/purchase/afterBuy.ts
- apps/site-integration/src/purchase/submitAttribution.ts
- shared/utils/slug.ts

## IMPLEMENTATION
- parse `?r=slug`
- validate slug
- store first-touch referral
- preserve TTL logic
- send attribution after buy

## RESULT
- stable referral capture
- no overwrite abuse
- attribution survives reload/session flow

---

# PHASE 2 — PURCHASE LEDGER / DATA MODEL

## FILES
- services/allocation-worker/src/db/purchases.ts
- services/allocation-worker/src/db/ambassadors.ts
- services/allocation-worker/src/types.ts

## IMPLEMENTATION
- extend purchase schema for new lifecycle statuses
- add fields:
  - allocationMode
  - allocationAttempts
  - lastAllocationAttemptAt
  - lastAllocationErrorCode
  - lastAllocationErrorMessage
  - deferredReason
  - withdrawSessionId
- add indexed queries for:
  - pending by ambassador
  - replayable failures
  - verified/deferred queue
- ensure idempotent updates

## RESULT
- DB becomes real operational ledger
- pending queue is safe and queryable

---

# PHASE 3 — RESOURCE GATE

## FILES
- services/allocation-worker/src/tron/resources.ts
- services/allocation-worker/src/config.ts
- services/allocation-worker/src/index.ts

## IMPLEMENTATION
- unified resource read:
  - Energy
  - Bandwidth
- configurable thresholds
- single decision helper:
  - `canSendAllocationNow()`
- classify failures:
  - insufficient_energy
  - insufficient_bandwidth
  - retryable_network
  - permanent_validation

## RESULT
- no blind allocation attempts
- deterministic defer decision

---

# PHASE 4 — ALLOCATION ENGINE REWRITE

## FILES
- services/allocation-worker/src/index.ts
- services/allocation-worker/src/run-scan.ts
- services/allocation-worker/src/tron/controller.ts
- services/allocation-worker/src/server.ts

## IMPLEMENTATION
- rewrite allocation flow around explicit modes:
  - eager
  - deferred
  - claim-first
- immediate scan path:
  - verify purchase
  - if resources enough -> allocate
  - else -> defer
- replay path:
  - only retry replayable purchases
- make `recordVerifiedPurchase(...)` strictly single-purchase unit
- add per-purchase lock/idempotency

## RESULT
- worker no longer assumes immediate allocation is mandatory
- worker becomes queue-aware

---

# PHASE 5 — WITHDRAWAL PREPARATION ENGINE

## FILES
- services/allocation-worker/src/jobs/prepareAmbassadorWithdrawal.ts
- services/allocation-worker/src/jobs/processAmbassadorPendingQueue.ts
- services/allocation-worker/src/server.ts
- services/allocation-worker/src/db/purchases.ts
- services/allocation-worker/src/tron/controller.ts

## IMPLEMENTATION
- new withdraw-preparation job
- load ambassador queue
- process purchases one by one
- stop on resource exhaustion
- create withdraw session log
- expose API endpoint for cabinet:
  - prepare withdrawal
  - get session status
- after preparation:
  - read on-chain reward
  - decide whether withdraw can continue

## RESULT
- ambassador withdrawal becomes the main settlement trigger
- queue is monetized only when needed

---

# PHASE 6 — CABINET WITHDRAW UX

## FILES
- apps/cabinet/src/app/ambassador/page.tsx
- apps/cabinet/src/hooks/useAmbassadorDashboard.ts
- apps/cabinet/src/lib/api/withdraw.ts

## IMPLEMENTATION
- withdraw button becomes two-step operationally:
  1. prepare pending purchases
  2. withdraw rewards
- show statuses:
  - pending purchases count
  - prepared count
  - deferred count
  - on-chain ready amount
- show clear messages:
  - "processing pending purchases"
  - "resource shortage, try again later"
  - "rewards ready for withdrawal"

## RESULT
- ambassador understands why reward may not be instantly withdrawable
- UX matches real accounting model

---

# PHASE 7 — DAILY MAINTENANCE

## FILES
- services/allocation-worker/src/jobs/dailyMaintenance.ts
- services/allocation-worker/src/jobs/replayDeferredPurchases.ts
- .github/workflows/allocation-worker-daily.yml

## IMPLEMENTATION
- daily maintenance sequence:
  1. resource check
  2. optional rent call
  3. replay deferred queue
  4. scan new events
- stop when resource budget is exhausted
- never spam chain blindly
- maintain logs and counters

## RESULT
- backlog shrinks automatically
- system still works even before ambassador presses withdraw

---

# PHASE 8 — OPTIONAL RESOURCE RENT MODULE

## FILES
- services/allocation-worker/src/providers/resources/gasstation.ts
- services/allocation-worker/src/jobs/rentResources.ts
- services/allocation-worker/src/config.ts

## IMPLEMENTATION
- isolated provider client
- no direct coupling with core accounting
- support:
  - check balance/reserve
  - request resource top-up
- provider failure must never break core queue logic

## RESULT
- resource rental becomes optional optimization layer

---

# PHASE 9 — TELEGRAM / NOTIFICATIONS

## FILES
- services/telegram-bot/src/server.ts
- services/telegram-bot/src/commands/start.ts
- services/telegram-bot/src/services/auth.ts
- services/telegram-bot/src/services/notifications.ts

## IMPLEMENTATION
- notify ambassador:
  - purchase verified
  - purchase deferred
  - purchase allocated
  - withdrawal ready
- never promise on-chain allocation before it really happened

## RESULT
- real-time feedback without accounting lies

---

# PHASE 10 — AUDIT / OBSERVABILITY

## FILES
- services/allocation-worker/src/logging/
- services/allocation-worker/src/server.ts
- services/allocation-worker/src/db/purchases.ts

## IMPLEMENTATION
- structured logs for every purchase state transition
- withdraw session logs
- counters:
  - verified
  - deferred
  - allocated
  - retryable_failed
  - final_failed
- admin endpoints for diagnostics

## RESULT
- we can always explain where any purchase currently is

---

## REQUIRED ENDPOINTS

### EXISTING / KEEP
- `GET /health`
- `POST /scan`
- `GET /failures`
- `POST /replay-failed`

### NEW / REQUIRED
- `POST /withdraw/prepare`
- `GET /withdraw/session`
- `GET /purchases/pending-by-ambassador`
- `POST /maintenance/replay-deferred`
- `GET /resources`
- `POST /resources/rent` (optional provider-backed)

---

## FAILURE POLICY

### RETRYABLE
- insufficient Energy
- insufficient Bandwidth
- temporary RPC/provider issue
- timeout before confirmed permanent reject

### FINAL
- invalid txHash
- purchase does not match BuyTokens event
- buyer mismatch
- ambassador mapping invalid
- purchase already permanently invalidated

Retryable failures must stay in replay queue.

---

## ORDERING POLICY

For ambassador withdrawal preparation:
- oldest purchase first

Reason:
- deterministic accounting
- simpler debugging
- fair queue progression

---

## SECURITY POLICY

1. only cabinet-authenticated ambassador may request own withdrawal preparation
2. ambassador may never trigger processing for another ambassador queue
3. purchase must always remain bound to original ambassador once verified
4. replay must remain idempotent
5. server must reject duplicate active withdraw sessions for same ambassador

---

## FINAL ACCEPTANCE CRITERIA

System is complete when:

- referral capture is stable
- verified purchases are always persisted
- no verified purchase is lost due to resource shortage
- immediate allocation works when resources exist
- deferred allocation works when resources are missing
- ambassador withdrawal preparation processes pending purchases one by one
- reward withdrawal reflects real on-chain state
- replay queue works
- no double allocation is possible
- logs explain every purchase state

---

## EXECUTION RULE

Work strictly phase-by-phase.

DO NOT:
- mix unrelated layers
- do partial hidden rewrites
- change contract assumptions silently
- remove safety statuses
- build UI before backend state machine is stable

Each phase must be:
- implemented
- tested
- validated
- frozen

before moving to the next one.

---

# END
```
