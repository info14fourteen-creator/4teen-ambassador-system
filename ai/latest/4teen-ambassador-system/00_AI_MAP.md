# AI MAP — 4teen-ambassador-system

- Generated: 2026-03-25T17:11:08.862Z
- Repository: info14fourteen-creator/4teen-ambassador-system
- Branch: main
- Total source files included: 46
- Output folder: ai/latest/4teen-ambassador-system
- Zip archive: ai/latest/4teen-ambassador-system.zip

## Snapshot files

- 01_PROJECT_OVERVIEW.md — PROJECT OVERVIEW (3 files)
- 02_CORE_SHARED_AND_TOOLING.md — CORE SHARED AND TOOLING (6 files)
- 03_APPS_CABINET.md — APPS CABINET (5 files)
- 04_APPS_SITE_INTEGRATION.md — APPS SITE INTEGRATION (9 files)
- 05_WORKER_DOMAIN_DB_SERVER.md — WORKER DOMAIN DB SERVER (9 files)
- 06_WORKER_TRON_AND_JOBS.md — WORKER TRON AND JOBS (9 files)
- 07_TELEGRAM_BOT.md — TELEGRAM BOT (0 files)
- 08_INFRA_AND_WORKFLOWS.md — INFRA AND WORKFLOWS (2 files)
- 09_REMAINING_FILES.md — REMAINING FILES (5 files)

## Project tree

```text
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
    - Procfile
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

## Raw links

- Folder base: https://raw.githubusercontent.com/info14fourteen-creator/4teen-ambassador-system/main/ai/latest/4teen-ambassador-system
- Working rules: https://raw.githubusercontent.com/info14fourteen-creator/4teen-ambassador-system/main/ai/WORKING_RULES.md
