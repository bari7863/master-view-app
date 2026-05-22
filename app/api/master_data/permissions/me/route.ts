import { NextRequest, NextResponse } from "next/server";
import { requireMasterDataUser } from "@/lib/master-data-auth";
import { getMasterDataUserPermissionSettings } from "@/lib/master-data-permissions";

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  const { user, errorResponse } = requireMasterDataUser(req);

  if (errorResponse) {
    return errorResponse;
  }

  if (!user) {
    return NextResponse.json(
      { ok: false, error: "ログインユーザー情報を確認できません" },
      { status: 401 }
    );
  }

  if (user.role === "スーパー管理者") {
    return NextResponse.json({
      ok: true,
      permissions: {},
      allowedFilters: {},
    });
  }

  try {
    const settings = await getMasterDataUserPermissionSettings(
      user.id,
      user.organization
    );

    return NextResponse.json({
      ok: true,
      permissions: settings.permissions,
      allowedFilters: settings.allowedFilters,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "ログイン中ユーザーの権限取得でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}