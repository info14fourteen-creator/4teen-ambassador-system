import fs from "fs";
import path from "path";

const ROOT = process.cwd();
const AI_DIR = path.join(ROOT, "ai");
const LATEST_DIR = path.join(AI_DIR, "latest");

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
  ".vscode",
  "ai/latest",
]);

const IGNORE_FILES = [
  ".env",
  ".env.local",
  ".env.production",
  ".env.development",
  "package-lock.json",
  "pnpm-lock.yaml",
  "yarn.lock",
];

const ALLOWED_ROOTS = [
  ".github",
  "apps",
  "services",
  "shared",
  "scripts",
  "docs",
  "ai",
  "package.json",
  "tsconfig.base.json",
  "turbo.json",
  "README.md",
];

const GROUPS = {
  core: [
    ".github/",
    "shared/",
    "scripts/",
    "docs/",
    "package.json",
    "tsconfig.base.json",
    "turbo.json",
    "README.md",
    "ai/WORKING_RULES.md",
  ],
  cabinet: ["apps/cabinet/"],
  site: ["apps/site-integration/"],
  worker: ["services/allocation-worker/"],
  telegram: ["services/telegram-bot/"],
};

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function toPosix(p) {
  return p.split(path.sep).join("/");
}

function shouldIgnore(relPath) {
  const posix = toPosix(relPath);

  if (IGNORE_FILES.some((f) => posix === f || posix.endsWith(`/${f}`))) return true;
  if (posix.includes("/node_modules/")) return true;
  if (posix.includes("/.next/")) return true;
  if (posix.includes("/dist/")) return true;
  if (posix.includes("/build/")) return true;
  if (posix.includes("/coverage/")) return true;
  if (posix.includes("/.turbo/")) return true;
  if (posix.startsWith("ai/latest/")) return true;

  return false;
}

function isAllowedRootEntry(name) {
  return ALLOWED_ROOTS.includes(name);
}

function walk(dir, out = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const abs = path.join(dir, entry.name);
    const rel = path.relative(ROOT, abs);
    const posix = toPosix(rel);

    if (entry.isDirectory()) {
      if (IGNORE_DIRS.has(entry.name)) continue;
      if (shouldIgnore(rel)) continue;
      walk(abs, out);
      continue;
    }

    if (shouldIgnore(rel)) continue;
    out.push(posix);
  }

  return out;
}

function readTextFile(relPath) {
  const abs = path.join(ROOT, relPath);
  try {
    return fs.readFileSync(abs, "utf8");
  } catch {
    return "";
  }
}

function classify(file) {
  for (const [group, prefixes] of Object.entries(GROUPS)) {
    for (const prefix of prefixes) {
      if (file === prefix || file.startsWith(prefix)) return group;
    }
  }
  return "core";
}

function buildMap(files) {
  const lines = [];
  lines.push("# AI PROJECT MAP");
  lines.push("");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push("");

  const grouped = {
    core: [],
    cabinet: [],
    site: [],
    worker: [],
    telegram: [],
  };

  for (const file of files) {
    const group = classify(file);
    grouped[group].push(file);
  }

  for (const key of Object.keys(grouped)) {
    lines.push(`## ${key.toUpperCase()}`);
    lines.push("");
    for (const file of grouped[key].sort()) {
      lines.push(`- ${file}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

function buildBundle(title, files) {
  const lines = [];
  lines.push(`# ${title}`);
  lines.push("");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push("");

  for (const file of files.sort()) {
    const content = readTextFile(file);
    lines.push(`===== FILE: ${file} =====`);
    lines.push(content || "[EMPTY OR UNREADABLE]");
    lines.push(`===== END FILE: ${file} =====`);
    lines.push("");
  }

  return lines.join("\n");
}

function main() {
  ensureDir(LATEST_DIR);

  const rootEntries = fs.readdirSync(ROOT, { withFileTypes: true });
  const allowedTopLevel = rootEntries
    .map((e) => e.name)
    .filter((name) => isAllowedRootEntry(name));

  const files = [];
  for (const name of allowedTopLevel) {
    const abs = path.join(ROOT, name);
    const stat = fs.statSync(abs);

    if (stat.isDirectory()) {
      walk(abs, files);
    } else {
      if (!shouldIgnore(name)) files.push(toPosix(name));
    }
  }

  files.sort();

  const coreFiles = files.filter((f) => classify(f) === "core");
  const cabinetFiles = files.filter((f) => classify(f) === "cabinet");
  const siteFiles = files.filter((f) => classify(f) === "site");
  const workerFiles = files.filter((f) => classify(f) === "worker");
  const telegramFiles = files.filter((f) => classify(f) === "telegram");

  fs.writeFileSync(path.join(LATEST_DIR, "ai-project-map.txt"), buildMap(files), "utf8");
  fs.writeFileSync(path.join(LATEST_DIR, "ai-bundle-core.txt"), buildBundle("AI BUNDLE — CORE", coreFiles), "utf8");
  fs.writeFileSync(path.join(LATEST_DIR, "ai-bundle-cabinet.txt"), buildBundle("AI BUNDLE — CABINET", cabinetFiles), "utf8");
  fs.writeFileSync(path.join(LATEST_DIR, "ai-bundle-site.txt"), buildBundle("AI BUNDLE — SITE INTEGRATION", siteFiles), "utf8");
  fs.writeFileSync(path.join(LATEST_DIR, "ai-bundle-worker.txt"), buildBundle("AI BUNDLE — WORKER", workerFiles), "utf8");
  fs.writeFileSync(path.join(LATEST_DIR, "ai-bundle-telegram.txt"), buildBundle("AI BUNDLE — TELEGRAM", telegramFiles), "utf8");

  console.log("AI bundles generated in ai/latest/");
}

main();
