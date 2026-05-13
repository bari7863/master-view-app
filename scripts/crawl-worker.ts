import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { crawlCompanyWebsite, type CrawlExtractedFields, type CrawlSelectableFieldKey } from "../lib/master-data-crawler";

process.env.PLAYWRIGHT_BROWSERS_PATH = process.env.PLAYWRIGHT_BROWSERS_PATH || "0";

type WorkerConfig = {
  apiBaseUrl: string;
  workerToken: string;
  workerName?: string;
  localPort?: number;
  pollIntervalMs?: number;
};

type WorkerStatus = {
  ok: boolean;
  workerId: string;
  workerName: string;
  apiBaseUrl: string;
  running: boolean;
  currentJobId: string | null;
  currentCompany: string | null;
  lastMessage: string;
  lastError: string | null;
  startedAt: string;
};

type WorkerClaimJobResponse = {
  ok: boolean;
  jobId?: string | null;
  message?: string;
  error?: string;
};

type WorkerClaimTargetResponse = {
  ok: boolean;
  completed?: boolean;
  paused?: boolean;
  target?: {
    targetIndex: number;
    rowId: string;
    company: string | null;
    address: string | null;
    websiteUrl: string | null;
    selectedFields: CrawlSelectableFieldKey[];
  } | null;
  error?: string;
};

const appDir =
  typeof (process as any).pkg !== "undefined"
    ? path.dirname(process.execPath)
    : process.cwd();

const configPath = path.join(appDir, "worker-config.json");
const workerIdPath = path.join(appDir, "worker-id.txt");

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function loadConfig(): WorkerConfig {
  if (!fs.existsSync(configPath)) {
    const sample: WorkerConfig = {
      apiBaseUrl: "https://master-view-app-ruby.vercel.app",
      workerToken: "ここにVercelと同じMASTER_CRAWL_WORKER_TOKENを入れてください",
      workerName: os.hostname(),
      localPort: 39281,
      pollIntervalMs: 3000,
    };

    fs.writeFileSync(configPath, JSON.stringify(sample, null, 2), "utf8");

    console.error("worker-config.json を作成しました。workerToken を設定してから再起動してください。");
    process.exit(1);
  }

  const raw = fs.readFileSync(configPath, "utf8");
  const config = JSON.parse(raw) as WorkerConfig;

  if (!config.apiBaseUrl || !config.workerToken) {
    throw new Error("worker-config.json の apiBaseUrl または workerToken が空です。");
  }

  return {
    ...config,
    apiBaseUrl: config.apiBaseUrl.replace(/\/+$/, ""),
    workerName: config.workerName || os.hostname(),
    localPort: config.localPort || 39281,
    pollIntervalMs: config.pollIntervalMs || 3000,
  };
}

function loadOrCreateWorkerId() {
  if (fs.existsSync(workerIdPath)) {
    const existing = fs.readFileSync(workerIdPath, "utf8").trim();
    if (existing) return existing;
  }

  const id = `worker-${os.hostname()}-${crypto.randomUUID()}`;
  fs.writeFileSync(workerIdPath, id, "utf8");
  return id;
}

const config = loadConfig();
const workerId = loadOrCreateWorkerId();

const status: WorkerStatus = {
  ok: true,
  workerId,
  workerName: config.workerName || os.hostname(),
  apiBaseUrl: config.apiBaseUrl,
  running: false,
  currentJobId: null,
  currentCompany: null,
  lastMessage: "worker起動中",
  lastError: null,
  startedAt: new Date().toISOString(),
};

async function callApi<T>(body: Record<string, unknown>): Promise<T> {
  const res = await fetch(`${config.apiBaseUrl}/api/master_data/crawl`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-master-crawl-worker-token": config.workerToken,
    },
    body: JSON.stringify({
      ...body,
      workerId,
      workerName: config.workerName,
    }),
  });

  const data = (await res.json()) as T & { error?: string; ok?: boolean };

  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `APIエラー: ${res.status}`);
  }

  return data;
}

function startLocalStatusServer() {
  const port = config.localPort || 39281;

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "https://master-view-app-ruby.vercel.app");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.url === "/status") {
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      res.end(JSON.stringify(status));
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ ok: false, error: "not found" }));
  });

  server.listen(port, "127.0.0.1", () => {
    console.log(`Local worker status server: http://127.0.0.1:${port}/status`);
  });
}

async function runWithTimeout<T>(task: Promise<T>, timeoutMs: number): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  try {
    return await Promise.race([
      task,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`1社あたりの最大処理時間（${Math.round(timeoutMs / 1000)}秒）を超えました`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

async function registerWorker() {
  await callApi({
    action: "worker_register",
  });

  status.lastMessage = "worker登録完了";
}

async function sendHeartbeat() {
  await callApi({
    action: "worker_heartbeat",
  });
}

async function claimJob() {
  return await callApi<WorkerClaimJobResponse>({
    action: "worker_claim_job",
  });
}

async function claimTarget(jobId: string) {
  return await callApi<WorkerClaimTargetResponse>({
    action: "worker_claim_target",
    jobId,
  });
}

async function reportTarget(params: {
  jobId: string;
  targetIndex: number;
  targetStatus: "done" | "skipped" | "failed";
  statusReason?: string | null;
  extracted?: CrawlExtractedFields | null;
}) {
  await callApi({
    action: "worker_report_target",
    jobId: params.jobId,
    targetIndex: params.targetIndex,
    targetStatus: params.targetStatus,
    statusReason: params.statusReason ?? null,
    extracted: params.extracted ?? null,
  });
}

async function processJob(jobId: string) {
  status.currentJobId = jobId;
  status.running = true;
  status.lastMessage = `ジョブ処理開始: ${jobId}`;

  while (true) {
    await sendHeartbeat();

    const targetRes = await claimTarget(jobId);

    if (targetRes.completed) {
      status.lastMessage = `ジョブ完了: ${jobId}`;
      break;
    }

    if (targetRes.paused) {
      status.lastMessage = `ジョブ中断: ${jobId}`;
      break;
    }

    const target = targetRes.target;

    if (!target) {
      status.lastMessage = "処理対象なし";
      break;
    }

    status.currentCompany = target.company;
    status.lastMessage = `取得中: ${target.company || target.websiteUrl || target.rowId}`;

    if (!target.websiteUrl) {
      await reportTarget({
        jobId,
        targetIndex: target.targetIndex,
        targetStatus: "skipped",
        statusReason: "企業サイトURLが空です",
      });
      continue;
    }

    try {
      const extracted = await runWithTimeout(
        crawlCompanyWebsite(
          target.websiteUrl,
          target.selectedFields,
          {
            company: target.company,
            address: target.address,
          },
          {
            shouldStop: () => false,
          }
        ),
        90 * 1000
      );

      await reportTarget({
        jobId,
        targetIndex: target.targetIndex,
        targetStatus: "done",
        extracted,
      });
    } catch (error) {
      await reportTarget({
        jobId,
        targetIndex: target.targetIndex,
        targetStatus: "failed",
        statusReason: error instanceof Error ? error.message : "不明なエラー",
      });
    }
  }

  status.running = false;
  status.currentJobId = null;
  status.currentCompany = null;
}

async function mainLoop() {
  startLocalStatusServer();

  await registerWorker();

  console.log(`Crawl worker started: ${workerId}`);
  console.log(`Worker name: ${config.workerName}`);
  console.log(`API: ${config.apiBaseUrl}`);

  while (true) {
    try {
      await sendHeartbeat();

      const job = await claimJob();

      if (!job.jobId) {
        status.lastMessage = "待機中";
        await sleep(config.pollIntervalMs || 3000);
        continue;
      }

      await processJob(job.jobId);
    } catch (error) {
      status.running = false;
      status.currentJobId = null;
      status.currentCompany = null;
      status.lastError = error instanceof Error ? error.message : "不明なエラー";
      status.lastMessage = "エラー発生。数秒後に再試行します。";

      console.error(status.lastError);
      await sleep(5000);
    }
  }
}

mainLoop().catch((error) => {
  console.error(error);
  process.exit(1);
});