# 4teen-ambassador-system — BUILD AND WORKFLOWS

Generated: 2026-03-30T23:52:06.545Z
Repository: info14fourteen-creator/4teen-ambassador-system
Branch: main

## Included files

- 4teen-ambassador-system :: .github/workflows/allocation-worker-daily.yml
- 4teen-ambassador-system :: .github/workflows/build-ai-bundles.yml
- 4teen-ambassador-system :: scripts/build-ai-bundles.mjs

---

## FILE: 4teen-ambassador-system :: .github/workflows/allocation-worker-daily.yml

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

## FILE: 4teen-ambassador-system :: .github/workflows/build-ai-bundles.yml

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

---

## FILE: 4teen-ambassador-system :: scripts/build-ai-bundles.mjs

```js
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";

const ROOT = process.cwd();
const AI_DIR = path.join(ROOT, "ai");
const LATEST_DIR = path.join(AI_DIR, "latest");

const REPOS = [
  {
    repoName: "4teen-ambassador-system",
    groups: [
      {
        key: "01_CORE_OVERVIEW",
        title: "CORE OVERVIEW",
        matchers: [
          "README.md",
          "package.json",
          "tsconfig.base.json",
          "turbo.json",
          "docs/",
          "shared/",
          "ai/WORKING_RULES.md"
        ]
      },
      {
        key: "02_BUILD_AND_WORKFLOWS",
        title: "BUILD AND WORKFLOWS",
        matchers: [
          ".github/workflows/",
          "scripts/"
        ]
      },
      {
        key: "03_WORKER",
        title: "ALLOCATION WORKER",
        matchers: ["services/allocation-worker/"]
      },
      {
        key: "04_CABINET",
        title: "CABINET",
        matchers: ["apps/cabinet/"]
      },
      {
        key: "05_SITE_INTEGRATION",
        title: "SITE INTEGRATION",
        matchers: ["apps/site-integration/"]
      },
      {
        key: "06_TELEGRAM",
        title: "TELEGRAM",
        matchers: ["services/telegram-bot/"]
      },
      {
        key: "07_REMAINING_CRITICAL_FILES",
        title: "REMAINING CRITICAL FILES",
        matchers: []
      }
    ]
  }
];

const IGNORE_DIRS = new Set([
  ".git",
  "node_modules",
  ".next",
  ".turbo",
  "dist",
  "build",
  "coverage",
  ".vercel",
  ".idea",
  ".vscode"
]);

const IGNORE_PREFIXES = [
  "ai/latest/"
];

const IGNORE_FILES = new Set([
  ".env",
  ".env.local",
  ".env.production",
  ".env.development",
  "package-lock.json",
  "pnpm-lock.yaml",
  "yarn.lock"
]);

const ALLOWED_EXTENSIONS = new Set([
  ".cjs",
  ".css",
  ".html",
  ".js",
  ".json",
  ".jsx",
  ".md",
  ".mjs",
  ".svg",
  ".ts",
  ".tsx",
  ".txt",
  ".yml",
  ".yaml"
]);

const MAX_OUTPUT_FILES_PER_REPO = 10;
const MAX_SOURCE_FILE_BYTES = 240 * 1024;
const MAX_SECTION_BYTES = 1_600_000;
const MAX_TOTAL_SELECTED_FILES = 180;

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function toPosix(p) {
  return p.split(path.sep).join("/");
}

function shouldIgnore(relPath) {
  const posix = toPosix(relPath);

  if (IGNORE_PREFIXES.some((prefix) => posix.startsWith(prefix))) {
    return true;
  }

  if (IGNORE_FILES.has(path.basename(posix))) {
    return true;
  }

  return false;
}

function walk(dir, out = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    if (entry.name.startsWith(".") && ![".github"].includes(entry.name)) {
      continue;
    }

    const abs = path.join(dir, entry.name);
    const rel = path.relative(ROOT, abs);
    const posix = toPosix(rel);

    if (shouldIgnore(posix)) continue;

    if (entry.isDirectory()) {
      if (IGNORE_DIRS.has(entry.name)) continue;
      walk(abs, out);
      continue;
    }

    const ext = path.extname(entry.name).toLowerCase();
    if (!ALLOWED_EXTENSIONS.has(ext)) continue;

    out.push(posix);
  }

  return out;
}

function fileSize(relPath) {
  try {
    return fs.statSync(path.join(ROOT, relPath)).size;
  } catch {
    return 0;
  }
}

function readText(relPath) {
  try {
    return fs.readFileSync(path.join(ROOT, relPath), "utf8");
  } catch {
    return "";
  }
}

function detectLang(file) {
  const ext = path.extname(file).toLowerCase();
  if (ext === ".ts") return "ts";
  if (ext === ".tsx") return "tsx";
  if (ext === ".js") return "js";
  if (ext === ".mjs") return "js";
  if (ext === ".cjs") return "js";
  if (ext === ".jsx") return "jsx";
  if (ext === ".json") return "json";
  if (ext === ".md") return "md";
  if (ext === ".css") return "css";
  if (ext === ".html") return "html";
  if (ext === ".svg") return "svg";
  if (ext === ".yml" || ext === ".yaml") return "yml";
  return "txt";
}

function matchesRule(file, rule) {
  const normalizedFile = toPosix(file);
  const normalizedRule = toPosix(rule);

  if (!normalizedRule) return false;

  if (normalizedRule.endsWith("/")) {
    return normalizedFile.startsWith(normalizedRule);
  }

  return (
    normalizedFile === normalizedRule ||
    normalizedFile.startsWith(`${normalizedRule}/`) ||
    normalizedFile.startsWith(`${normalizedRule}.`)
  );
}

function getRepoInfo(repoName) {
  const repository =
    process.env.GITHUB_REPOSITORY || `info14fourteen-creator/${repoName}`;
  const branch = process.env.GITHUB_REF_NAME || "main";

  return {
    repository,
    branch,
    repoPrefixUrl: `https://raw.githubusercontent.com/${repository}/${branch}/ai/latest/${repoName}`,
    zipUrl: `https://raw.githubusercontent.com/${repository}/${branch}/ai/latest/${repoName}.zip`,
    rulesUrl: `https://raw.githubusercontent.com/${repository}/${branch}/ai/WORKING_RULES.md`
  };
}

function buildTree(files) {
  const rootNode = {};

  for (const file of files) {
    const parts = toPosix(file).split("/");
    let current = rootNode;

    for (let i = 0; i < parts.length; i += 1) {
      const part = parts[i];
      const isLast = i === parts.length - 1;

      if (!current[part]) {
        current[part] = isLast ? null : {};
      }

      current = current[part];
    }
  }

  function render(node, indent = "") {
    const keys = Object.keys(node).sort((a, b) => {
      const aDir = node[a] !== null;
      const bDir = node[b] !== null;
      if (aDir !== bDir) return aDir ? -1 : 1;
      return a.localeCompare(b);
    });

    let out = "";
    for (const key of keys) {
      if (node[key] === null) {
        out += `${indent}- ${key}\n`;
      } else {
        out += `${indent}- ${key}/\n`;
        out += render(node[key], `${indent}  `);
      }
    }
    return out;
  }

  return render(rootNode);
}

function buildSelectedFiles(groupDefs) {
  const allFiles = walk(ROOT).sort((a, b) => a.localeCompare(b));
  const filtered = allFiles.filter((file) => fileSize(file) <= MAX_SOURCE_FILE_BYTES);

  const selected = [];
  const seen = new Set();

  for (const group of groupDefs) {
    for (const file of filtered) {
      const match = group.matchers.some((rule) => matchesRule(file, rule));
      if (!match) continue;
      if (!seen.has(file)) {
        selected.push(file);
        seen.add(file);
      }
    }
  }

  for (const file of filtered) {
    if (selected.length >= MAX_TOTAL_SELECTED_FILES) break;
    if (!seen.has(file)) {
      selected.push(file);
      seen.add(file);
    }
  }

  return selected.slice(0, MAX_TOTAL_SELECTED_FILES);
}

function buildGroups(files, groupDefs) {
  const assigned = new Set();
  const groups = [];

  for (const groupDef of groupDefs) {
    if (groupDef.key === "07_REMAINING_CRITICAL_FILES") continue;

    const matched = files.filter((file) =>
      groupDef.matchers.some((rule) => matchesRule(file, rule))
    );

    const bounded = [];
    let bytes = 0;

    for (const file of matched) {
      const size = fileSize(file);
      if (bytes + size > MAX_SECTION_BYTES) continue;

      bounded.push(file);
      assigned.add(file);
      bytes += size;
    }

    groups.push({
      key: groupDef.key,
      title: groupDef.title,
      files: bounded
    });
  }

  const remaining = [];
  let bytes = 0;

  for (const file of files) {
    if (assigned.has(file)) continue;
    const size = fileSize(file);
    if (bytes + size > MAX_SECTION_BYTES) continue;

    remaining.push(file);
    bytes += size;
  }

  groups.push({
    key: "07_REMAINING_CRITICAL_FILES",
    title: "REMAINING CRITICAL FILES",
    files: remaining
  });

  return groups.slice(0, MAX_OUTPUT_FILES_PER_REPO - 1);
}

function buildSectionDoc(repoName, group, allFiles, info) {
  const lines = [];

  lines.push(`# ${repoName} — ${group.title}`);
  lines.push("");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push(`Repository: ${info.repository}`);
  lines.push(`Branch: ${info.branch}`);
  lines.push("");

  if (group.key === "01_CORE_OVERVIEW") {
    lines.push("## Curated project tree");
    lines.push("");
    lines.push("```txt");
    lines.push(buildTree(allFiles).trimEnd());
    lines.push("```");
    lines.push("");
  }

  lines.push("## Included files");
  lines.push("");
  if (group.files.length === 0) {
    lines.push("- none");
    lines.push("");
    return lines.join("\n");
  }

  for (const file of group.files) {
    lines.push(`- ${repoName} :: ${file}`);
  }

  lines.push("");

  for (const file of group.files) {
    const lang = detectLang(file);
    const content = readText(file);

    lines.push("---");
    lines.push("");
    lines.push(`## FILE: ${repoName} :: ${file}`);
    lines.push("");
    lines.push("```" + lang);
    lines.push(content.trimEnd());
    lines.push("```");
    lines.push("");
  }

  return lines.join("\n");
}

function writeRepoOutputs(repoName, allFiles, groups, info) {
  const repoDir = path.join(LATEST_DIR, repoName);
  ensureDir(repoDir);

  for (const group of groups) {
    const outFile = path.join(repoDir, `${repoName}__${group.key}.md`);
    fs.writeFileSync(outFile, buildSectionDoc(repoName, group, allFiles, info), "utf8");
  }

  const mapLines = [];
  mapLines.push(`# ${repoName} — AI PROJECT MAP`);
  mapLines.push("");
  mapLines.push(`Generated: ${new Date().toISOString()}`);
  mapLines.push(`Repository: ${info.repository}`);
  mapLines.push(`Branch: ${info.branch}`);
  mapLines.push("");
  mapLines.push("## Links");
  mapLines.push("");
  mapLines.push(`- Snapshot dir: ${info.repoPrefixUrl}/`);
  mapLines.push(`- Zip archive: ${info.zipUrl}`);
  mapLines.push(`- Working rules: ${info.rulesUrl}`);
  mapLines.push("");
  mapLines.push("## Snapshot files");
  mapLines.push("");
  for (const group of groups) {
    mapLines.push(`- ${repoName}__${group.key}.md`);
  }
  mapLines.push("");
  mapLines.push("## Curated project tree");
  mapLines.push("");
  mapLines.push("```txt");
  mapLines.push(buildTree(allFiles).trimEnd());
  mapLines.push("```");
  mapLines.push("");

  fs.writeFileSync(
    path.join(repoDir, `${repoName}__ai-project-map.txt`),
    mapLines.join("\n"),
    "utf8"
  );

  const linksLines = [];
  linksLines.push(`${repoName} AI LINKS`);
  linksLines.push("");
  linksLines.push(`Snapshot dir: ${info.repoPrefixUrl}/`);
  linksLines.push(`Zip archive: ${info.zipUrl}`);
  linksLines.push(`Working rules: ${info.rulesUrl}`);
  linksLines.push("");
  for (const group of groups) {
    linksLines.push(`- ${info.repoPrefixUrl}/${repoName}__${group.key}.md`);
  }
  linksLines.push("");

  fs.writeFileSync(
    path.join(repoDir, `${repoName}__links.txt`),
    linksLines.join("\n"),
    "utf8"
  );

  const manifest = {
    generatedAt: new Date().toISOString(),
    repository: info.repository,
    branch: info.branch,
    repoName,
    outputDir: `${info.repoPrefixUrl}/`,
    zipUrl: info.zipUrl,
    sourceFilesIncluded: allFiles,
    snapshotFiles: groups.map((group) => ({
      name: `${repoName}__${group.key}.md`,
      title: group.title,
      files: group.files
    }))
  };

  fs.writeFileSync(
    path.join(repoDir, `${repoName}__manifest.json`),
    JSON.stringify(manifest, null, 2),
    "utf8"
  );

  const zipFile = path.join(LATEST_DIR, `${repoName}.zip`);
  if (fs.existsSync(zipFile)) {
    fs.rmSync(zipFile, { force: true });
  }

  const result = spawnSync(
    process.platform === "win32" ? "powershell" : "zip",
    process.platform === "win32"
      ? [
          "-NoProfile",
          "-Command",
          `Compress-Archive -Path "${repoDir}\\*" -DestinationPath "${zipFile}" -Force`
        ]
      : ["-r", zipFile, repoName],
    process.platform === "win32"
      ? { stdio: "inherit" }
      : { cwd: LATEST_DIR, stdio: "inherit" }
  );

  if (result.status !== 0) {
    throw new Error(`Failed to create zip for ${repoName}`);
  }
}

function main() {
  ensureDir(LATEST_DIR);

  for (const repo of REPOS) {
    const info = getRepoInfo(repo.repoName);
    const allFiles = buildSelectedFiles(repo.groups);
    const groups = buildGroups(allFiles, repo.groups);

    writeRepoOutputs(repo.repoName, allFiles, groups, info);
    console.log(`Built AI bundle for ${repo.repoName}`);
  }

  console.log("All AI bundles generated successfully.");
}

main();
```
