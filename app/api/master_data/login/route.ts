import { NextRequest, NextResponse } from "next/server";
import {
  clearMasterDataAuthCookie,
  setMasterDataAuthCookie,
} from "@/lib/master-data-auth";

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";

type MasterDataLoginRole = "管理者" | "従業員";

type MasterDataLoginUser = {
  id: string;
  password: string;
  name: string;
  role: MasterDataLoginRole;
};

function isMasterDataLoginRole(value: unknown): value is MasterDataLoginRole {
  return value === "管理者" || value === "従業員";
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

      if (id === "" || password === "" || name === "") {
        return null;
      }

      return {
        id,
        password,
        name,
        role,
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
      },
    ];
  }

  return [];
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

    const response = NextResponse.json({
      ok: true,
      loginUser: {
        id: loginUser.id,
        name: loginUser.name,
        role: loginUser.role,
      },
    });
    setMasterDataAuthCookie(response);

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

export async function DELETE() {
  const response = NextResponse.json({ ok: true });
  clearMasterDataAuthCookie(response);

  return response;
}