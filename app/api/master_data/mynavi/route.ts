import { NextRequest, NextResponse } from "next/server";
export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";

import fs from "fs/promises";
import path from "path";
import os from "os";
import { spawn, type ChildProcessByStdio } from "child_process";
import type { Readable } from "stream";

type MynaviRequestBody = {
  action?:
    | "get_total_pages"
    | "start_count_job"
    | "start_job"
    | "get_job_status"
    | "pause_job"
    | "cancel_job"
    | "resume_job";
  gradYear?: string | number;
  pageCount?: "all" | string | number | null;
  jobId?: string | null;
};

type MynaviJobStatus = "idle" | "running" | "paused" | "completed" | "error";

type MynaviJobMode = "count_pages" | "scrape";

type MynaviPhase =
  | "idle"
  | "count_pages"
  | "collect_urls"
  | "scrape_details"
  | "completed"
  | "error";

type MynaviJobState = {
  jobId: string;
  mode: MynaviJobMode;
  gradYear: string;
  status: MynaviJobStatus;
  phase: MynaviPhase;
  totalPages: number;
  detectedTotalPages: number;
  selectedPageCount: number;
  totalUrls: number;
  processedCount: number;
  successCount: number;
  failedCount: number;
  currentPageNumber: number;
  currentUrl: string | null;
  currentField: string | null;
  currentCompany: string | null;
  currentCompanyIndex: number;
  csvPath: string | null;
  csvFileName: string | null;
  statePath: string | null;
  child: ChildProcessByStdio<null, Readable, Readable> | null;
  stopReason: "pause" | "cancel" | null;
  error: string | null;
};

const globalForMynaviJobs = globalThis as typeof globalThis & {
  __masterDataMynaviJobs?: Map<string, MynaviJobState>;
};

const mynaviJobs =
  globalForMynaviJobs.__masterDataMynaviJobs ?? new Map<string, MynaviJobState>();

if (!globalForMynaviJobs.__masterDataMynaviJobs) {
  globalForMynaviJobs.__masterDataMynaviJobs = mynaviJobs;
}

function normalizeGradYear(value: unknown) {
  const num = Number(value);

  if (!Number.isFinite(num) || num < 1 || num > 99) {
    throw new Error("年卒は1～99で指定してください");
  }

  return String(Math.floor(num)).padStart(2, "0");
}

function normalizePageCount(value: unknown) {
  if (value === "all") {
    return "all" as const;
  }

  const num = Number(value);

  if (!Number.isFinite(num) || num < 1 || num > 999) {
    throw new Error("ページ数は1～999またはallで指定してください");
  }

  return Math.floor(num);
}

function getPythonBin() {
  return process.env.MYNAVI_PYTHON_BIN ||
    (process.platform === "win32" ? "python" : "python3");
}

function getScriptPath() {
  return path.join(process.cwd(), "scripts", "mynavi_shinsotsu_unified.py");
}

async function ensureScriptExists() {
  const scriptPath = getScriptPath();
  await fs.access(scriptPath);
  return scriptPath;
}

function parseLastJsonLine(text: string) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line !== "");

  for (let index = lines.length - 1; index >= 0; index -= 1) {
    try {
      return JSON.parse(lines[index]) as Record<string, unknown>;
    } catch {
      continue;
    }
  }

  throw new Error("Pythonの返却形式がJSONではありません");
}

async function runPythonJson(args: string[]) {
  const scriptPath = await ensureScriptExists();
  const pythonBin = getPythonBin();

  return await new Promise<Record<string, unknown>>((resolve, reject) => {
    const child = spawn(pythonBin, ["-X", "utf8", "-u", scriptPath, ...args], {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        PYTHONIOENCODING: "utf-8",
        PYTHONUTF8: "1",
      },
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            stderr.trim() || `Python実行が終了コード ${code} で失敗しました`
          )
        );
        return;
      }

      try {
        resolve(parseLastJsonLine(stdout));
      } catch (error) {
        reject(error);
      }
    });
  });
}

function buildJobResponse(job: MynaviJobState) {
  return {
    ok: true,
    jobId: job.jobId,
    jobStatus: job.status,
    mynaviJobMode: job.mode,
    mynaviGradYear: job.gradYear,
    mynaviPhase: job.phase,
    mynaviTotalPages: job.totalPages,
    mynaviDetectedTotalPages: job.detectedTotalPages,
    mynaviSelectedPageCount: job.selectedPageCount,
    mynaviTotalUrls: job.totalUrls,
    mynaviProcessedCount: job.processedCount,
    mynaviSuccessCount: job.successCount,
    mynaviFailedCount: job.failedCount,
    mynaviCurrentPageNumber: job.currentPageNumber,
    mynaviCurrentField: job.currentField,
    mynaviCurrentCompany: job.currentCompany,
    mynaviCurrentCompanyIndex: job.currentCompanyIndex,
    mynaviCsvFileName: job.csvFileName,
    error: job.error,
  };
}

function handlePythonEvent(jobId: string, line: string) {
  const trimmed = line.trim();

  if (!trimmed.startsWith("{")) {
    return;
  }

  let payload: Record<string, unknown>;

  try {
    payload = JSON.parse(trimmed) as Record<string, unknown>;
  } catch {
    return;
  }

  const job = mynaviJobs.get(jobId);

  if (!job) {
    return;
  }

  const type = String(payload.type ?? "");

  if (type === "meta") {
    job.phase = "collect_urls";
    job.totalPages = Number(payload.total_pages ?? 0);
    job.detectedTotalPages = Number(payload.total_pages ?? 0);
    job.selectedPageCount = Number(payload.selected_page_count ?? 0);
    return;
  }

  if (type === "progress") {
    const phase = String(payload.phase ?? "");

    if (phase === "count_pages") {
      job.phase = "count_pages";
      job.currentField =
        typeof payload.current_field === "string"
          ? payload.current_field
          : "count_open";
      job.currentPageNumber = Number(payload.current_page ?? 1);
      job.detectedTotalPages = Number(
        payload.detected_total_pages ?? job.detectedTotalPages
      );
      if (job.detectedTotalPages > 0) {
        job.totalPages = job.detectedTotalPages;
      }
      return;
    }

    if (phase === "collect_urls") {
      job.phase = "collect_urls";
      job.currentPageNumber = Number(payload.current_page ?? 0);
      job.detectedTotalPages = Number(
        payload.total_pages ?? job.detectedTotalPages
      );
      if (job.detectedTotalPages > 0) {
        job.totalPages = job.detectedTotalPages;
      }
      job.totalUrls = Number(payload.collected_urls ?? 0);
      job.currentField =
        typeof payload.current_field === "string"
          ? payload.current_field
          : "collect_urls";
      return;
    }

    if (phase === "scrape_details") {
      job.phase = "scrape_details";
      job.processedCount = Number(payload.processed ?? 0);
      job.totalUrls = Number(payload.total ?? 0);
      job.successCount = Number(payload.success_count ?? 0);
      job.failedCount = Number(payload.failed_count ?? 0);
      job.currentUrl =
        typeof payload.current_url === "string" ? payload.current_url : null;
      job.currentPageNumber = Number(
        payload.current_page ?? job.currentPageNumber
      );
      job.currentField =
        typeof payload.current_field === "string"
          ? payload.current_field
          : null;
      job.currentCompany =
        typeof payload.current_company === "string"
          ? payload.current_company
          : null;
      job.currentCompanyIndex = Number(
        payload.current_company_index ?? job.currentCompanyIndex
      );
      return;
    }

    return;
  }

  if (type === "result") {
    const phase = String(payload.phase ?? "");

    if (phase === "count_pages") {
      job.status = "completed";
      job.phase = "completed";
      job.totalPages = Number(payload.total_pages ?? job.totalPages);
      job.detectedTotalPages = job.totalPages;
      job.error = null;
      return;
    }

    job.status = "completed";
    job.phase = "completed";
    job.totalPages = Number(payload.total_pages ?? job.totalPages);
    job.detectedTotalPages = job.totalPages;
    job.selectedPageCount = Number(
      payload.selected_page_count ?? job.selectedPageCount
    );
    job.totalUrls = Number(payload.total_urls ?? job.totalUrls);
    job.processedCount = Number(payload.total_urls ?? job.processedCount);
    job.successCount = Number(payload.success_count ?? job.successCount);
    job.failedCount = Number(payload.failed_count ?? job.failedCount);
    job.csvPath =
      typeof payload.csv_path === "string" ? payload.csv_path : job.csvPath;
    job.csvFileName =
      typeof payload.csv_file_name === "string"
        ? payload.csv_file_name
        : job.csvFileName;
    job.error = null;
    return;
  }

  if (type === "error") {
    job.status = "error";
    job.phase = "error";
    job.error =
      typeof payload.message === "string"
        ? payload.message
        : "マイナビ新卒の取得でエラーが発生しました";
  }
}

async function spawnMynaviJob(
  job: MynaviJobState,
  args: string[]
): Promise<MynaviJobState> {
  await ensureScriptExists();

  mynaviJobs.set(job.jobId, job);

  const pythonBin = getPythonBin();
  const scriptPath = getScriptPath();

  const child = spawn(pythonBin, ["-X", "utf8", "-u", scriptPath, ...args], {
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      PYTHONIOENCODING: "utf-8",
      PYTHONUTF8: "1",
    },
  });

  job.child = child;

  let stdoutBuffer = "";
  let stderrBuffer = "";

  child.stdout.on("data", (chunk) => {
    stdoutBuffer += chunk.toString("utf8");

    const lines = stdoutBuffer.split(/\r?\n/);
    stdoutBuffer = lines.pop() ?? "";

    for (const line of lines) {
      handlePythonEvent(job.jobId, line);
    }
  });

  child.stderr.on("data", (chunk) => {
    stderrBuffer += chunk.toString("utf8");
  });

  child.on("error", (error) => {
    const currentJob = mynaviJobs.get(job.jobId);

    if (!currentJob) return;

    currentJob.status = "error";
    currentJob.phase = "error";
    currentJob.error = error.message;
    currentJob.child = null;
  });

  child.on("close", () => {
    const currentJob = mynaviJobs.get(job.jobId);

    if (!currentJob) return;

    currentJob.child = null;

    if (stdoutBuffer.trim()) {
      handlePythonEvent(job.jobId, stdoutBuffer.trim());
    }

    if (currentJob.stopReason === "pause") {
      currentJob.status = "paused";
      currentJob.stopReason = null;
      currentJob.error = null;
      return;
    }

    if (currentJob.stopReason === "cancel") {
      currentJob.stopReason = null;
      return;
    }

    if (currentJob.status !== "completed" && currentJob.status !== "error") {
      currentJob.status = "error";
      currentJob.phase = "error";
      currentJob.error =
        stderrBuffer.trim() || "マイナビ新卒の取得が正常終了しませんでした";
    }
  });

  return job;
}

async function startMynaviCountJob(
  gradYear: string
): Promise<MynaviJobState> {
  const jobId = crypto.randomUUID();

  const job: MynaviJobState = {
    jobId,
    mode: "count_pages",
    gradYear,
    status: "running",
    phase: "count_pages",
    totalPages: 0,
    detectedTotalPages: 0,
    selectedPageCount: 0,
    totalUrls: 0,
    processedCount: 0,
    successCount: 0,
    failedCount: 0,
    currentPageNumber: 1,
    currentUrl: null,
    currentField: "count_open",
    currentCompany: null,
    currentCompanyIndex: 0,
    csvPath: null,
    csvFileName: null,
    statePath: null,
    child: null,
    stopReason: null,
    error: null,
  };

  return await spawnMynaviJob(job, [
    "--mode",
    "count_pages",
    "--grad-year",
    gradYear,
  ]);
}

async function startMynaviJob(
  gradYear: string,
  pageCount: "all" | number
): Promise<MynaviJobState> {
  await ensureScriptExists();

  const jobId = crypto.randomUUID();
  const tempDir = path.join(os.tmpdir(), "master-data-mynavi");
  await fs.mkdir(tempDir, { recursive: true });

  const csvFileName = `mynavi_${gradYear}_${jobId}.csv`;
  const csvPath = path.join(tempDir, csvFileName);
  const statePath = path.join(tempDir, `mynavi_${gradYear}_${jobId}.state.json`);

  const job: MynaviJobState = {
    jobId,
    mode: "scrape",
    gradYear,
    status: "running",
    phase: "idle",
    totalPages: 0,
    detectedTotalPages: 0,
    selectedPageCount: 0,
    totalUrls: 0,
    processedCount: 0,
    successCount: 0,
    failedCount: 0,
    currentPageNumber: 0,
    currentUrl: null,
    currentField: "prepare",
    currentCompany: null,
    currentCompanyIndex: 0,
    csvPath,
    csvFileName,
    statePath,
    child: null,
    stopReason: null,
    error: null,
  };

  return await spawnMynaviJob(job, [
    "--mode",
    "scrape",
    "--grad-year",
    gradYear,
    "--page-count",
    pageCount === "all" ? "all" : String(pageCount),
    "--output-csv",
    csvPath,
    "--state-file",
    statePath,
  ]);
}

async function resumeMynaviJob(jobId: string): Promise<MynaviJobState> {
  const job = mynaviJobs.get(jobId);

  if (!job) {
    throw new Error("マイナビ新卒ジョブが見つかりません");
  }

  if (!job.statePath || !job.csvPath) {
    throw new Error("再開用の状態ファイルが見つかりません");
  }

  job.status = "running";
  job.error = null;
  job.stopReason = null;
  job.currentField = "prepare";

  return await spawnMynaviJob(job, [
    "--mode",
    "scrape_resume",
    "--grad-year",
    job.gradYear,
    "--output-csv",
    job.csvPath,
    "--state-file",
    job.statePath,
  ]);
}

export async function POST(req: NextRequest) {
  try {
    const body = (await req.json()) as MynaviRequestBody;
    const action = body.action;

    if (action === "start_count_job") {
      const gradYear = normalizeGradYear(body.gradYear);
      const job = await startMynaviCountJob(gradYear);

      return NextResponse.json(buildJobResponse(job));
    }

    if (action === "get_total_pages") {
      const gradYear = normalizeGradYear(body.gradYear);

      const result = await runPythonJson([
        "--mode",
        "count_pages",
        "--grad-year",
        gradYear,
      ]);

      return NextResponse.json({
        ok: true,
        mynaviGradYear: gradYear,
        mynaviTotalPages: Number(result.total_pages ?? 0),
      });
    }

    if (action === "start_job") {
      const gradYear = normalizeGradYear(body.gradYear);
      const pageCount = normalizePageCount(body.pageCount ?? "all");

      const job = await startMynaviJob(gradYear, pageCount);

      return NextResponse.json(buildJobResponse(job));
    }

    if (action === "pause_job") {
      const job = body.jobId ? mynaviJobs.get(body.jobId) : null;

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "マイナビ新卒ジョブが見つかりません" },
          { status: 404 }
        );
      }

      if (job.mode !== "scrape" || !job.child) {
        return NextResponse.json(
          { ok: false, error: "中断できるジョブが見つかりません" },
          { status: 400 }
        );
      }

      job.stopReason = "pause";
      job.child.kill();

      return NextResponse.json({
        ...buildJobResponse(job),
        message: "中断指示を受け付けました",
      });
    }

    if (action === "cancel_job") {
      const job = body.jobId ? mynaviJobs.get(body.jobId) : null;

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "マイナビ新卒ジョブが見つかりません" },
          { status: 404 }
        );
      }

      if (job.child) {
        job.stopReason = "cancel";
        job.child.kill();
      }

      mynaviJobs.delete(job.jobId);

      return NextResponse.json({
        ok: true,
        message: "マイナビ新卒を中止しました",
      });
    }

    if (action === "resume_job") {
      const jobId = body.jobId;

      if (!jobId) {
        return NextResponse.json(
          { ok: false, error: "マイナビ新卒ジョブが見つかりません" },
          { status: 404 }
        );
      }

      const resumedJob = await resumeMynaviJob(jobId);
      return NextResponse.json(buildJobResponse(resumedJob));
    }

    if (action === "get_job_status") {
      const job = body.jobId ? mynaviJobs.get(body.jobId) : null;

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "マイナビ新卒ジョブが見つかりません" },
          { status: 404 }
        );
      }

      return NextResponse.json(buildJobResponse(job));
    }

    return NextResponse.json(
      { ok: false, error: "不正なリクエストです" },
      { status: 400 }
    );
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "マイナビ新卒処理でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const action = searchParams.get("action");

    if (action !== "download_csv") {
      return NextResponse.json(
        { ok: false, error: "不正なリクエストです" },
        { status: 400 }
      );
    }

    const jobId = searchParams.get("jobId");
    const job = jobId ? mynaviJobs.get(jobId) : null;

    if (!job) {
      return NextResponse.json(
        { ok: false, error: "マイナビ新卒ジョブが見つかりません" },
        { status: 404 }
      );
    }

    if (
      (job.status !== "completed" && job.status !== "paused") ||
      !job.csvPath ||
      !job.csvFileName
    ) {
      return NextResponse.json(
        { ok: false, error: "CSVはまだ作成されていません" },
        { status: 400 }
      );
    }

    const fileBuffer = await fs.readFile(job.csvPath);

    return new NextResponse(fileBuffer, {
      status: 200,
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(
          job.csvFileName
        )}`,
        "Cache-Control": "no-store",
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "CSVダウンロードでエラーが発生しました",
      },
      { status: 500 }
    );
  }
}