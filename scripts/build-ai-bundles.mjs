import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";

const ROOT = process.cwd();
const REPO_NAME = "4teen-ambassador-system";

const AI_DIR = path.join(ROOT, "ai");
const LATEST_DIR = path.join(AI_DIR, "latest");
const REPO_OUTPUT_DIR = path.join(LATEST_DIR, REPO_NAME);

const RULES_FILE = path.join(ROOT, "ai", "WORKING_RULES.md");

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
  "ai/latest"
]);

const IGNORE_FILES = new Set([
  ".env",
  ".env.local",
  ".env.production",
  ".env.development",
  "package-lock.json",
  "pnpm-lock.yaml",
  "yarn.lock"
]);

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
  "README.md"
];

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
  ".sql",
  ".ts",
  ".tsx",
  ".txt",
  ".yml",
  ".yaml"
]);

const SECTION_CONFIG = [
  {
    file: "01_PROJECT_OVERVIEW.md",
    title: "PROJECT OVERVIEW",
    includes: [
      "package.json",
      "README.md",
      "ai/WORKING_RULES.md",
      "docs/"
    ]
  },
  {
    file: "02_CORE_SHARED_AND_TOOLING.md",
    title: "CORE SHARED AND TOOLING",
    includes: [
      ".github/",
      "shared/",
      "scripts/",
      "tsconfig.base.json",
      "turbo.json"
    ]
  },
  {
    file: "03_APPS_CABINET.md",
    title: "APPS CABINET",
    includes: [
      "apps/cabinet/"
    ]
  },
  {
    file: "04_APPS_SITE_INTEGRATION.md",
    title: "APPS SITE INTEGRATION",
    includes: [
      "apps/site-integration/"
    ]
  },
  {
    file: "05_WORKER_DOMAIN_DB_SERVER.md",
    title: "WORKER DOMAIN DB SERVER",
    includes: [
      "services/allocation-worker/src/domain/",
      "services/allocation-worker/src/db/",
      "services/allocation-worker/src/server",
      "services/allocation-worker/src/index",
      "services/allocation-worker/package.json",
      "services/allocation-worker/tsconfig",
      "services/allocation-worker/README"
    ]
  },
  {
    file: "06_WORKER_TRON_AND_JOBS.md",
    title: "WORKER TRON AND JOBS",
    includes: [
      "services/allocation-worker/src/tron/",
      "services/allocation-worker/src/jobs/",
      "services/allocation-worker/src/utils/",
      "services/allocation-worker/src/config/"
    ]
  },
  {
    file: "07_TELEGRAM_BOT.md",
    title: "TELEGRAM BOT",
    includes: [
      "services/telegram-bot/"
    ]
  },
  {
    file: "08_INFRA_AND_WORKFLOWS.md",
    title: "INFRA AND WORKFLOWS",
    includes: [
      ".github/workflows/",
      "Procfile",
      "app.json",
      "heroku.yml"
    ]
  },
  {
    file: "09_REMAINING_FILES.md",
    title: "REMAINING FILES",
    includes: []
  }
];

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function toPosix(value) {
  return value.split(path.sep).join("/");
}

function shouldIgnore(relPath) {
  const rel = toPosix(relPath);

  if (IGNORE_FILES.has(path.basename(rel))) {
    return true;
  }

  if (rel.startsWith("ai/latest/")) {
    return true;
  }

  for (const name of IGNORE_DIRS) {
    if (rel === name || rel.startsWith(`${name}/`) || rel.includes(`/${name}/`)) {
      return true;
    }
  }

  return false;
}

function isAllowedRootEntry(name) {
  return ALLOWED_ROOTS.includes(name);
}

function isAllowedExtension(fileName) {
  const ext = path.extname(fileName).toLowerCase();
  return ALLOWED_EXTENSIONS.has(ext) || fileName === "Procfile";
}

function walk(absDir, out = []) {
  const entries = fs.readdirSync(absDir, { withFileTypes: true });

  for (const entry of entries) {
    const abs = path.join(absDir, entry.name);
    const rel = toPosix(path.relative(ROOT, abs));

    if (shouldIgnore(rel)) {
      continue;
    }

    if (entry.isDirectory()) {
      walk(abs, out);
      continue;
    }

    if (!isAllowedExtension(entry.name)) {
      continue;
    }

    out.push(rel);
  }

  return out;
}

function readUtf8(relPath) {
  try {
    return fs.readFileSync(path.join(ROOT, relPath), "utf8");
  } catch {
    return "";
  }
}

function getRepositoryInfo() {
  const repository =
    process.env.GITHUB_REPOSITORY || `info14fourteen-creator/${REPO_NAME}`;
  const branch = process.env.GITHUB_REF_NAME || "main";

  return {
    repository,
    branch,
    outputBaseUrl: `https://raw.githubusercontent.com/${repository}/${branch}/ai/latest/${REPO_NAME}`,
    rulesUrl: `https://raw.githubusercontent.com/${repository}/${branch}/ai/WORKING_RULES.md`
  };
}

function matchesInclude(relPath, includeRule) {
  const rel = toPosix(relPath);
  const rule = toPosix(includeRule);

  if (!rule) {
    return false;
  }

  if (rule.endsWith("/")) {
    return rel.startsWith(rule);
  }

  return rel === rule || rel.startsWith(`${rule}/`) || rel.startsWith(`${rule}.`);
}

function groupFiles(files) {
  const assigned = new Set();
  const groups = [];

  for (const section of SECTION_CONFIG) {
    if (section.file === "09_REMAINING_FILES.md") {
      continue;
    }

    const sectionFiles = files.filter((file) =>
      section.includes.some((rule) => matchesInclude(file, rule))
    );

    for (const file of sectionFiles) {
      assigned.add(file);
    }

    groups.push({
      file: section.file,
      title: section.title,
      files: Array.from(new Set(sectionFiles)).sort()
    });
  }

  const remaining = files.filter((file) => !assigned.has(file)).sort();

  groups.push({
    file: "09_REMAINING_FILES.md",
    title: "REMAINING FILES",
    files: remaining
  });

  return groups;
}

function buildTree(files) {
  const rootNode = {};

  for (const file of files) {
    const parts = toPosix(file).split("/");
    let node = rootNode;

    for (let i = 0; i < parts.length; i += 1) {
      const part = parts[i];
      const isLast = i === parts.length - 1;

      if (!node[part]) {
        node[part] = isLast ? null : {};
      }

      node = node[part];
    }
  }

  function render(node, indent = "") {
    const keys = Object.keys(node).sort((a, b) => {
      const aDir = node[a] !== null;
      const bDir = node[b] !== null;

      if (aDir !== bDir) {
        return aDir ? -1 : 1;
      }

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

function detectLanguage(relPath) {
  const ext = path.extname(relPath).toLowerCase();

  const map = {
    ".js": "js",
    ".mjs": "js",
    ".cjs": "js",
    ".ts": "ts",
    ".tsx": "tsx",
    ".jsx": "jsx",
    ".json": "json",
    ".css": "css",
    ".html": "html",
    ".md": "md",
    ".svg": "svg",
    ".sql": "sql",
    ".yml": "yml",
    ".yaml": "yaml",
    ".txt": "text"
  };

  return map[ext] || "text";
}

function withRepoFileName(fileName) {
  return `${REPO_NAME}__${fileName}`;
}

function buildMapFile(files, repoInfo, groups) {
  const lines = [];

  lines.push(`# AI MAP — ${REPO_NAME}`);
  lines.push("");
  lines.push(`- Generated: ${new Date().toISOString()}`);
  lines.push(`- Repository: ${repoInfo.repository}`);
  lines.push(`- Branch: ${repoInfo.branch}`);
  lines.push(`- Total source files included: ${files.length}`);
  lines.push(`- Output folder: ai/latest/${REPO_NAME}`);
  lines.push(`- Zip archive: ai/latest/${REPO_NAME}.zip`);
  lines.push("");

  lines.push("## Snapshot files");
  lines.push("");

  for (const group of groups) {
    lines.push(
      `- ${withRepoFileName(group.file)} — ${group.title} (${group.files.length} files)`
    );
  }

  lines.push("");
  lines.push("## Project tree");
  lines.push("");
  lines.push("```text");
  lines.push(buildTree(files).trimEnd());
  lines.push("```");
  lines.push("");
  lines.push("## Raw links");
  lines.push("");
  lines.push(`- Folder base: ${repoInfo.outputBaseUrl}`);
  lines.push(`- Working rules: ${repoInfo.rulesUrl}`);
  lines.push("");

  return lines.join("\n");
}

function buildSectionFile(sectionTitle, files, repoInfo) {
  const lines = [];

  lines.push(`# REPOSITORY: ${REPO_NAME}`);
  lines.push(`# SECTION: ${sectionTitle}`);
  lines.push(`# GENERATED_AT: ${new Date().toISOString()}`);
  lines.push("");

  lines.push("## INCLUDED FILES");
  lines.push("");

  for (const file of files) {
    lines.push(`- ${file}`);
  }

  lines.push("");
  lines.push("## REPOSITORY LINK BASE");
  lines.push("");
  lines.push(`- ${repoInfo.outputBaseUrl}`);
  lines.push("");

  for (const file of files) {
    const content = readUtf8(file);
    const lang = detectLanguage(file);

    lines.push("---");
    lines.push("");
    lines.push(`## FILE: ${file}`);
    lines.push("");
    lines.push(`\`\`\`${lang}`);
    lines.push(content.trimEnd());
    lines.push("```");
    lines.push("");
  }

  return lines.join("\n");
}

function buildSnapshotInfo(files, groups, repoInfo) {
  const lines = [];

  lines.push(`# SNAPSHOT INFO — ${REPO_NAME}`);
  lines.push("");
  lines.push(`- Generated: ${new Date().toISOString()}`);
  lines.push(`- Repository: ${repoInfo.repository}`);
  lines.push(`- Branch: ${repoInfo.branch}`);
  lines.push(`- Files captured: ${files.length}`);
  lines.push(`- Snapshot documents: ${groups.length + 2}`);
  lines.push(`- Zip archive: ai/latest/${REPO_NAME}.zip`);
  lines.push("");

  lines.push("## Notes");
  lines.push("");
  lines.push("- Every snapshot file contains real file contents.");
  lines.push("- Files are grouped for easier AI reading.");
  lines.push("- Repository name is embedded in every snapshot file.");
  lines.push("- Snapshot file names are prefixed with repository name.");
  lines.push("- Working rules remain in ai/WORKING_RULES.md.");
  lines.push("");

  if (fs.existsSync(RULES_FILE)) {
    lines.push("## WORKING RULES");
    lines.push("");
    lines.push(readUtf8("ai/WORKING_RULES.md").trimEnd());
    lines.push("");
  }

  return lines.join("\n");
}

function buildLinksFile(repoInfo) {
  return [
    "AI SNAPSHOT LINKS",
    "",
    `Folder base: ${repoInfo.outputBaseUrl}`,
    `Map: ${repoInfo.outputBaseUrl}/00_AI_MAP.md`,
    `Info: ${repoInfo.outputBaseUrl}/99_SNAPSHOT_INFO.md`,
    `Working rules: ${repoInfo.rulesUrl}`,
    ""
  ].join("\n");
}

function buildManifest(files, groups, repoInfo) {
  return {
    generatedAt: new Date().toISOString(),
    repoName: REPO_NAME,
    repository: repoInfo.repository,
    branch: repoInfo.branch,
    totalFiles: files.length,
    outputDir: `ai/latest/${REPO_NAME}`,
    zipPath: `ai/latest/${REPO_NAME}.zip`,
    groups: groups.map((group) => ({
      file: withRepoFileName(group.file),
      title: group.title,
      totalFiles: group.files.length,
      files: group.files
    }))
  };
}

function writeCompatibilityFiles(repoInfo, groups) {
  const pointerPath = path.join(LATEST_DIR, "ai-project-bundle.txt");
  const mapPointerPath = path.join(LATEST_DIR, "ai-project-map.txt");

  const bundlePointer = [
    `AI snapshot moved to ai/latest/${REPO_NAME}/`,
    "",
    `Repository: ${repoInfo.repository}`,
    `Branch: ${repoInfo.branch}`,
    "",
    `Map file: ai/latest/${REPO_NAME}/00_AI_MAP.md`,
    `Info file: ai/latest/${REPO_NAME}/99_SNAPSHOT_INFO.md`,
    "",
    "Snapshot files:",
    ...groups.map(
      (group) => `- ai/latest/${REPO_NAME}/${withRepoFileName(group.file)}`
    ),
    "",
    `Zip archive: ai/latest/${REPO_NAME}.zip`,
    ""
  ].join("\n");

  const mapPointer = [
    `AI map moved to ai/latest/${REPO_NAME}/00_AI_MAP.md`,
    "",
    `Repository folder: ai/latest/${REPO_NAME}/`,
    `Zip archive: ai/latest/${REPO_NAME}.zip`,
    ""
  ].join("\n");

  fs.writeFileSync(pointerPath, bundlePointer, "utf8");
  fs.writeFileSync(mapPointerPath, mapPointer, "utf8");
}

function createZipArchive(sourceDir, zipFilePath) {
  const parentDir = path.dirname(sourceDir);
  const dirName = path.basename(sourceDir);

  try {
    fs.rmSync(zipFilePath, { force: true });
  } catch {}

  const result = spawnSync("zip", ["-r", zipFilePath, dirName], {
    cwd: parentDir,
    stdio: "inherit"
  });

  if (result.status !== 0) {
    throw new Error(`Failed to create zip archive: ${zipFilePath}`);
  }
}

function main() {
  ensureDir(LATEST_DIR);
  ensureDir(REPO_OUTPUT_DIR);

  const rootEntries = fs.readdirSync(ROOT, { withFileTypes: true });
  const allowedTopLevel = rootEntries
    .map((entry) => entry.name)
    .filter((name) => isAllowedRootEntry(name));

  const files = [];

  for (const name of allowedTopLevel) {
    const abs = path.join(ROOT, name);
    const stat = fs.statSync(abs);

    if (stat.isDirectory()) {
      walk(abs, files);
    } else if (!shouldIgnore(name) && isAllowedExtension(name)) {
      files.push(toPosix(name));
    }
  }

  files.sort();

  const repoInfo = getRepositoryInfo();
  const groups = groupFiles(files);

  fs.writeFileSync(
    path.join(REPO_OUTPUT_DIR, "00_AI_MAP.md"),
    buildMapFile(files, repoInfo, groups),
    "utf8"
  );

  for (const group of groups) {
    fs.writeFileSync(
      path.join(REPO_OUTPUT_DIR, withRepoFileName(group.file)),
      buildSectionFile(group.title, group.files, repoInfo),
      "utf8"
    );
  }

  fs.writeFileSync(
    path.join(REPO_OUTPUT_DIR, "99_SNAPSHOT_INFO.md"),
    buildSnapshotInfo(files, groups, repoInfo),
    "utf8"
  );

  fs.writeFileSync(
    path.join(REPO_OUTPUT_DIR, "links.txt"),
    buildLinksFile(repoInfo),
    "utf8"
  );

  fs.writeFileSync(
    path.join(REPO_OUTPUT_DIR, "manifest.json"),
    JSON.stringify(buildManifest(files, groups, repoInfo), null, 2),
    "utf8"
  );

  writeCompatibilityFiles(repoInfo, groups);

  const zipFilePath = path.join(LATEST_DIR, `${REPO_NAME}.zip`);
  createZipArchive(REPO_OUTPUT_DIR, zipFilePath);

  console.log(`AI snapshot generated in: ai/latest/${REPO_NAME}/`);
  console.log(`AI zip archive created: ai/latest/${REPO_NAME}.zip`);
}

main();
