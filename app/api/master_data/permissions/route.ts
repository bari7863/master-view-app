import { NextRequest, NextResponse } from "next/server";
import { requireMasterDataAdmin } from "@/lib/master-data-auth";
import {
  getMasterDataUserPermissionSettings,
  upsertMasterDataUserPermissionSettings,
} from "@/lib/master-data-permissions";

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
        typeof item.organization === "string"
          ? item.organization.trim()
          : "";

      const rawRole = item.role;
      const resolvedRole =
        role === "管理者" && rawRole === "スーパー管理者"
          ? "スーパー管理者"
          : role;

      if (
        id === "" ||
        password === "" ||
        name === "" ||
        organization === ""
      ) {
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
        typeof item.organization === "string"
          ? item.organization.trim()
          : "";

      const role = item.role;

      if (
        id === "" ||
        password === "" ||
        name === "" ||
        organization === "" ||
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

function findEditablePermissionUser(
  currentUser: { role: MasterDataLoginRole; organization: string },
  targetUserId: string
) {
  const targetUser = getMasterDataLoginUsers().find(
    (user) => user.id === targetUserId
  );

  if (!targetUser) {
    return undefined;
  }

  if (currentUser.role === "スーパー管理者") {
    return targetUser.role === "管理者" || targetUser.role === "従業員"
      ? targetUser
      : undefined;
  }

  if (currentUser.role === "管理者") {
    return targetUser.role === "従業員" &&
      targetUser.organization === currentUser.organization
      ? targetUser
      : undefined;
  }

  return undefined;
}

export async function GET(req: NextRequest) {
  const { user: adminUser, errorResponse } = requireMasterDataAdmin(req);

  if (errorResponse) {
    return errorResponse;
  }

  if (!adminUser) {
    return NextResponse.json(
      { ok: false, error: "管理者情報を確認できません" },
      { status: 401 }
    );
  }

  try {
    const employees = getMasterDataLoginUsers().filter((user) => {
      if (adminUser.role === "スーパー管理者") {
        return user.role === "管理者" || user.role === "従業員";
      }

      return (
        user.role === "従業員" &&
        user.organization === adminUser.organization
      );
    });

    const items = await Promise.all(
      employees.map(async (employee) => {
        const settings = await getMasterDataUserPermissionSettings(
          employee.id,
          employee.organization
        );

        return {
          id: employee.id,
          name: employee.name,
          role: employee.role,
          organization: employee.organization,
          permissions: settings.permissions,
          allowedFilters: settings.allowedFilters,
        };
      })
    );

    return NextResponse.json({
      ok: true,
      employees: items,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "権限情報の取得でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}

export async function PATCH(req: NextRequest) {
  const { user: adminUser, errorResponse } = requireMasterDataAdmin(req);

  if (errorResponse) {
    return errorResponse;
  }

  if (!adminUser) {
    return NextResponse.json(
      { ok: false, error: "管理者情報を確認できません" },
      { status: 401 }
    );
  }

  try {
    const body = (await req.json()) as Record<string, unknown>;

    const userId = typeof body.userId === "string" ? body.userId.trim() : "";
    const permissions = body.permissions;
    const allowedFilters = body.allowedFilters;

    if (userId === "") {
      return NextResponse.json(
        { ok: false, error: "アカウントIDが指定されていません" },
        { status: 400 }
      );
    }

    const targetEmployee = findEditablePermissionUser(adminUser, userId);

    if (!targetEmployee) {
      return NextResponse.json(
        { ok: false, error: "編集できるアカウントが見つかりません" },
        { status: 404 }
      );
    }

    const settings = await upsertMasterDataUserPermissionSettings({
      userId: targetEmployee.id,
      organization: targetEmployee.organization,
      permissions,
      allowedFilters,
    });

    return NextResponse.json({
      ok: true,
      employee: {
        id: targetEmployee.id,
        name: targetEmployee.name,
        role: targetEmployee.role,
        organization: targetEmployee.organization,
        permissions: settings.permissions,
        allowedFilters: settings.allowedFilters,
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "権限情報の更新でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}

export async function POST(req: NextRequest) {
  return PATCH(req);
}