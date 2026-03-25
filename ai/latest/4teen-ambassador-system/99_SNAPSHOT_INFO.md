# SNAPSHOT INFO — 4teen-ambassador-system

- Generated: 2026-03-25T17:11:08.873Z
- Repository: info14fourteen-creator/4teen-ambassador-system
- Branch: main
- Files captured: 46
- Snapshot documents: 11
- Zip archive: ai/latest/4teen-ambassador-system.zip

## Notes

- Every snapshot file contains real file contents.
- Files are grouped for easier AI reading.
- Repository name is embedded in every snapshot file.
- Working rules remain in ai/WORKING_RULES.md.

## WORKING RULES

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
