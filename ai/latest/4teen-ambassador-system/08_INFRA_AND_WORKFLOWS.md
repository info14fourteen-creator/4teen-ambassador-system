# REPOSITORY: 4teen-ambassador-system
# SECTION: INFRA AND WORKFLOWS
# GENERATED_AT: 2026-03-25T17:12:48.372Z

## INCLUDED FILES

- .github/workflows/allocation-worker-daily.yml
- .github/workflows/build-ai-bundles.yml

## REPOSITORY LINK BASE

- https://raw.githubusercontent.com/info14fourteen-creator/4teen-ambassador-system/main/ai/latest/4teen-ambassador-system

---

## FILE: .github/workflows/allocation-worker-daily.yml

```yml
name: allocation-worker-daily

on:
  schedule:
    - cron: "10 0 * * *"
  workflow_dispatch:

jobs:
  daily-maintenance:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Health check
        env:
          ALLOCATION_WORKER_BASE_URL: ${{ secrets.ALLOCATION_WORKER_BASE_URL }}
        run: |
          set -euo pipefail

          if [ -z "${ALLOCATION_WORKER_BASE_URL:-}" ]; then
            echo "ALLOCATION_WORKER_BASE_URL secret is required"
            exit 1
          fi

          echo "Checking health: ${ALLOCATION_WORKER_BASE_URL}/health"
          curl --fail --silent --show-error \
            "${ALLOCATION_WORKER_BASE_URL}/health"

      - name: Run daily maintenance
        env:
          ALLOCATION_WORKER_BASE_URL: ${{ secrets.ALLOCATION_WORKER_BASE_URL }}
          ALLOCATION_WORKER_CRON_SECRET: ${{ secrets.ALLOCATION_WORKER_CRON_SECRET }}
        run: |
          set -euo pipefail

          if [ -z "${ALLOCATION_WORKER_BASE_URL:-}" ]; then
            echo "ALLOCATION_WORKER_BASE_URL secret is required"
            exit 1
          fi

          if [ -z "${ALLOCATION_WORKER_CRON_SECRET:-}" ]; then
            echo "ALLOCATION_WORKER_CRON_SECRET secret is required"
            exit 1
          fi

          echo "Running daily maintenance: ${ALLOCATION_WORKER_BASE_URL}/jobs/daily-maintenance"

          HTTP_CODE=$(
            curl --silent --show-error \
              --output response.json \
              --write-out "%{http_code}" \
              --request POST \
              "${ALLOCATION_WORKER_BASE_URL}/jobs/daily-maintenance" \
              --header "Content-Type: application/json" \
              --header "x-cron-secret: ${ALLOCATION_WORKER_CRON_SECRET}" \
              --data '{}'
          )

          echo "HTTP status: ${HTTP_CODE}"
          cat response.json

          if [ "${HTTP_CODE}" -lt 200 ] || [ "${HTTP_CODE}" -ge 300 ]; then
            echo "Daily maintenance request failed"
            exit 1
          fi

      - name: Check failures after maintenance
        env:
          ALLOCATION_WORKER_BASE_URL: ${{ secrets.ALLOCATION_WORKER_BASE_URL }}
        run: |
          set -euo pipefail

          echo "Checking failures: ${ALLOCATION_WORKER_BASE_URL}/failures"
          curl --fail --silent --show-error \
            "${ALLOCATION_WORKER_BASE_URL}/failures"
```

---

## FILE: .github/workflows/build-ai-bundles.yml

```yml
name: Build and Publish AI Bundles

on:
  workflow_dispatch:
  push:
    branches:
      - main
    paths-ignore:
      - 'ai/latest/**'

permissions:
  contents: write

jobs:
  build-and-publish:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Install root dependencies
        run: npm install

      - name: Build AI bundles
        run: npm run build:ai

      - name: Commit and push generated AI files
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

          git add ai/latest

          if git diff --cached --quiet; then
            echo "No AI bundle changes to commit."
          else
            git commit -m "chore: update AI bundles [skip ci]"
            git push
          fi

      - name: Print links
        run: |
          echo "AI map:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-project-map.txt"
          echo
          echo "AI core bundle:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-core.txt"
          echo
          echo "AI cabinet bundle:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-cabinet.txt"
          echo
          echo "AI site bundle:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-site.txt"
          echo
          echo "AI worker bundle:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-worker.txt"
          echo
          echo "AI telegram bundle:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-telegram.txt"
          echo
          echo "Working rules:"
          echo "https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/WORKING_RULES.md"

      - name: Add workflow summary
        run: |
          {
            echo "## AI bundle links"
            echo
            echo "- AI map: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-project-map.txt"
            echo "- AI core bundle: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-core.txt"
            echo "- AI cabinet bundle: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-cabinet.txt"
            echo "- AI site bundle: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-site.txt"
            echo "- AI worker bundle: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-worker.txt"
            echo "- AI telegram bundle: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/latest/ai-bundle-telegram.txt"
            echo "- Working rules: https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${GITHUB_REF_NAME}/ai/WORKING_RULES.md"
          } >> "$GITHUB_STEP_SUMMARY"
```
