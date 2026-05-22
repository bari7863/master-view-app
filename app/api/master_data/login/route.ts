import { NextRequest, NextResponse } from "next/server";
import {
  clearMasterDataAuthCookie,
  requireMasterDataAuth,
  setMasterDataAuthCookie,
} from "@/lib/master-data-auth";
import { dbReady, pool } from "@/lib/db";

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";

type MasterDataLoginRole = "スーパー管理者" | "管理者" | "従業員";

type MasterDataLoginUser = {
  id: string;
  password: string;
  name: string;
  organization: string;
  role: MasterDataLoginRole;
};

type MasterDataLoginHistoryEvent = {
  loggedAt: string;
  ipAddress: string;
  browser: string;
};

type MasterDataLoginHistoryRow = {
  logged_at: Date | string;
  ip_address: string | null;
  browser: string | null;
};

function isMasterDataLoginRole(value: unknown): value is MasterDataLoginRole {
  return value === "スーパー管理者" || value === "管理者" || value === "従業員";
}

function parseMasterDataLoginUsersByRole(
  usersText: string | undefined,
  role: MasterDataLoginRole
): MasterDataLoginUser[] {
  if (!usersText || usersText.trim() === "") {
    return [];
  }

  const parsed = JSON.parse(usersText) as unknown;

  if (!Array.isArray(parsed)) {
    return [];
  }

  return parsed
    .map((user) => {
      if (!user || typeof user !== "object") {
        return null;
      }

      const item = user as Record<string, unknown>;

      const id = typeof item.id === "string" ? item.id.trim() : "";
      const password = typeof item.password === "string" ? item.password : "";
      const name = typeof item.name === "string" ? item.name.trim() : "";
      const organization =
        typeof item.organization === "string" ? item.organization.trim() : "";

      const rawRole = item.role;
      const resolvedRole =
        role === "管理者" && rawRole === "スーパー管理者"
          ? "スーパー管理者"
          : role;

      if (id === "" || password === "" || name === "") {
        return null;
      }

      return {
        id,
        password,
        name,
        organization,
        role: resolvedRole,
      };
    })
    .filter((user): user is MasterDataLoginUser => user !== null);
}

function getMasterDataLoginUsers(): MasterDataLoginUser[] {
  const adminUsers = parseMasterDataLoginUsersByRole(
    process.env.MASTER_DATA_LOGIN_ADMINS,
    "管理者"
  );

  const employeeUsers = parseMasterDataLoginUsersByRole(
    process.env.MASTER_DATA_LOGIN_EMPLOYEES,
    "従業員"
  );

  if (adminUsers.length > 0 || employeeUsers.length > 0) {
    return [...adminUsers, ...employeeUsers];
  }

  const usersText = process.env.MASTER_DATA_LOGIN_USERS;

  if (usersText && usersText.trim() !== "") {
    const parsed = JSON.parse(usersText) as unknown;

    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .map((user) => {
        if (!user || typeof user !== "object") {
          return null;
        }

        const item = user as Record<string, unknown>;

        const id = typeof item.id === "string" ? item.id.trim() : "";
        const password =
          typeof item.password === "string" ? item.password : "";
        const name = typeof item.name === "string" ? item.name.trim() : "";
        const role = item.role;

        const organization =
          typeof item.organization === "string" ? item.organization.trim() : "";

        if (
          id === "" ||
          password === "" ||
          name === "" ||
          !isMasterDataLoginRole(role)
        ) {
          return null;
        }

        return {
          id,
          password,
          name,
          organization,
          role,
        };
      })
      .filter((user): user is MasterDataLoginUser => user !== null);
  }

  const legacyId = process.env.MASTER_DATA_LOGIN_ID?.trim() ?? "";
  const legacyPassword = process.env.MASTER_DATA_LOGIN_PASSWORD ?? "";

  if (legacyId !== "" && legacyPassword !== "") {
    return [
      {
        id: legacyId,
        password: legacyPassword,
        name: legacyId,
        role: "管理者",
        organization: "",
      },
    ];
  }

  return [];
}

function normalizeRequestIpAddress(value: string | undefined | null) {
  const ipAddress = value?.trim();

  if (!ipAddress) {
    return "";
  }

  if (
    ipAddress === "::1" ||
    ipAddress === "127.0.0.1" ||
    ipAddress === "::ffff:127.0.0.1"
  ) {
    return "ローカル環境";
  }

  return ipAddress;
}

function getRequestIpAddress(req: NextRequest) {
  const forwardedFor = req.headers.get("x-forwarded-for");
  const forwardedIp = forwardedFor?.split(",")[0]?.trim();

  return (
    normalizeRequestIpAddress(forwardedIp) ||
    normalizeRequestIpAddress(req.headers.get("x-real-ip")) ||
    normalizeRequestIpAddress(req.headers.get("cf-connecting-ip")) ||
    "取得できません"
  );
}

function getBrowserLabel(userAgent: string) {
  if (/Edg\//i.test(userAgent)) return "Edge";
  if (/Chrome\//i.test(userAgent) || /CriOS\//i.test(userAgent)) return "Chrome";
  if (/Firefox\//i.test(userAgent) || /FxiOS\//i.test(userAgent)) return "Firefox";
  if (/Safari\//i.test(userAgent)) return "Safari";

  return "不明";
}

function createMasterDataLoginHistoryEvent(
  req: NextRequest
): MasterDataLoginHistoryEvent {
  return {
    loggedAt: new Date().toISOString(),
    ipAddress: getRequestIpAddress(req),
    browser: getBrowserLabel(req.headers.get("user-agent") ?? ""),
  };
}

function toLoginHistoryIsoString(value: Date | string) {
  if (value instanceof Date) {
    return value.toISOString();
  }

  const date = new Date(value);

  if (!Number.isNaN(date.getTime())) {
    return date.toISOString();
  }

  return String(value);
}

async function saveMasterDataLoginHistoryToDb(
  loginUser: MasterDataLoginUser,
  event: MasterDataLoginHistoryEvent
) {
  await dbReady;

  await pool.query(
    `
      INSERT INTO master_data_login_history (
        user_id,
        user_name,
        user_role,
        ip_address,
        browser,
        logged_at
      )
      VALUES ($1, $2, $3, $4, $5, $6)
    `,
    [
      loginUser.id,
      loginUser.name,
      loginUser.role,
      event.ipAddress,
      event.browser,
      event.loggedAt,
    ]
  );
}

async function fetchMasterDataLoginHistoryFromDb(userId: string) {
  await dbReady;

  const result = await pool.query<MasterDataLoginHistoryRow>(
    `
      SELECT
        logged_at,
        ip_address,
        browser
      FROM master_data_login_history
      WHERE user_id = $1
      ORDER BY logged_at DESC
      LIMIT 50
    `,
    [userId]
  );

  return result.rows.map((row) => ({
    loggedAt: toLoginHistoryIsoString(row.logged_at),
    ipAddress: row.ip_address || "-",
    browser: row.browser || "-",
  }));
}

function findMasterDataLoginUser(id: string, password: string) {
  return getMasterDataLoginUsers().find(
    (user) => user.id === id && user.password === password
  );
}

export async function POST(req: NextRequest) {
  try {
    const body = (await req.json()) as Record<string, unknown>;

    const id = typeof body.id === "string" ? body.id.trim() : "";
    const password = typeof body.password === "string" ? body.password : "";

    const loginUser = findMasterDataLoginUser(id, password);

    if (!loginUser) {
      return NextResponse.json(
        { ok: false, error: "IDまたはパスワードが違います" },
        { status: 401 }
      );
    }

    const loginHistoryEvent = createMasterDataLoginHistoryEvent(req);

    await saveMasterDataLoginHistoryToDb(loginUser, loginHistoryEvent);

    const loginHistory = await fetchMasterDataLoginHistoryFromDb(loginUser.id);

    const response = NextResponse.json({
      ok: true,
      loginUser: {
        id: loginUser.id,
        password: loginUser.password,
        name: loginUser.name,
        role: loginUser.role,
        organization: loginUser.organization,
      },
      loginHistoryEvent,
      loginHistory,
    });

    setMasterDataAuthCookie(response, {
      id: loginUser.id,
      name: loginUser.name,
      role: loginUser.role,
      organization: loginUser.organization,
    });

    return response;
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error ? error.message : "ログイン処理でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}

export async function GET(req: NextRequest) {
  const authError = requireMasterDataAuth(req);

  if (authError) {
    return authError;
  }

  try {
    const userId = new URL(req.url).searchParams.get("userId")?.trim() ?? "";

    if (userId === "") {
      return NextResponse.json(
        { ok: false, error: "ユーザーIDが指定されていません" },
        { status: 400 }
      );
    }

    const loginHistory = await fetchMasterDataLoginHistoryFromDb(userId);

    return NextResponse.json({
      ok: true,
      loginHistory,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "ログイン履歴の取得でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}

export async function DELETE() {
  const response = NextResponse.json({ ok: true });
  clearMasterDataAuthCookie(response);

  return response;
}