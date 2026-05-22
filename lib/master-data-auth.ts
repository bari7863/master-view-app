import { createHmac, timingSafeEqual } from "crypto";
import { NextRequest, NextResponse } from "next/server";

const MASTER_DATA_AUTH_COOKIE_NAME = "master-data-auth";
const MASTER_DATA_AUTH_USER_COOKIE_NAME = "master-data-auth-user";
const MASTER_DATA_AUTH_USER_SIGNATURE_COOKIE_NAME =
  "master-data-auth-user-signature";
const MASTER_DATA_AUTH_COOKIE_MAX_AGE_SECONDS = 60 * 30;

export type MasterDataLoginRole = "スーパー管理者" | "管理者" | "従業員";

export type MasterDataAuthUser = {
  id: string;
  name: string;
  role: MasterDataLoginRole;
  organization: string;
};

type MasterDataAuthResult = {
  user: MasterDataAuthUser | null;
  errorResponse: NextResponse | null;
};

function safeEqualText(a: string, b: string) {
  const aBuffer = Buffer.from(a);
  const bBuffer = Buffer.from(b);

  if (aBuffer.length !== bBuffer.length) {
    return false;
  }

  return timingSafeEqual(aBuffer, bBuffer);
}

function getRequiredAuthToken() {
  const token = process.env.MASTER_DATA_AUTH_TOKEN;

  if (!token || token.trim() === "") {
    throw new Error("MASTER_DATA_AUTH_TOKEN が .env.local に設定されていません");
  }

  return token;
}

function createMasterDataCookieOptions(maxAge: number) {
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge,
  };
}

function createMasterDataAuthUserPayload(loginUser: MasterDataAuthUser) {
  return Buffer.from(
    JSON.stringify({
      id: loginUser.id,
      name: loginUser.name,
      role: loginUser.role,
      organization: loginUser.organization,
    }),
    "utf8"
  ).toString("base64url");
}

function createMasterDataAuthUserSignature(payload: string) {
  return createHmac("sha256", getRequiredAuthToken())
    .update(payload)
    .digest("hex");
}

function parseMasterDataAuthUserPayload(
  payload: string
): MasterDataAuthUser | null {
  try {
    const decoded = Buffer.from(payload, "base64url").toString("utf8");
    const parsed = JSON.parse(decoded) as Record<string, unknown>;

    const id = typeof parsed.id === "string" ? parsed.id.trim() : "";
    const name = typeof parsed.name === "string" ? parsed.name.trim() : "";
    const rawRole = parsed.role;
    const organization =
      typeof parsed.organization === "string"
        ? parsed.organization.trim()
        : "";

    if (
      id === "" ||
      name === "" ||
      (rawRole !== "スーパー管理者" && rawRole !== "管理者" && rawRole !== "従業員")
    ) {
      return null;
    }

    const role: MasterDataLoginRole = rawRole;

    return {
      id,
      name,
      role,
      organization,
    };
  } catch {
    return null;
  }
}

function hasValidMasterDataAuth(req: NextRequest) {
  const expectedToken = process.env.MASTER_DATA_AUTH_TOKEN ?? "";
  const actualToken = req.cookies.get(MASTER_DATA_AUTH_COOKIE_NAME)?.value ?? "";

  return (
    expectedToken.trim() !== "" &&
    actualToken.trim() !== "" &&
    safeEqualText(actualToken, expectedToken)
  );
}

export function isValidMasterDataLogin(id: string, password: string) {
  const expectedId = process.env.MASTER_DATA_LOGIN_ID ?? "";
  const expectedPassword = process.env.MASTER_DATA_LOGIN_PASSWORD ?? "";

  if (expectedId.trim() === "" || expectedPassword.trim() === "") {
    return false;
  }

  return safeEqualText(id, expectedId) && safeEqualText(password, expectedPassword);
}

export function setMasterDataAuthCookie(
  response: NextResponse,
  loginUser?: MasterDataAuthUser
) {
  const token = getRequiredAuthToken();

  response.cookies.set(
    MASTER_DATA_AUTH_COOKIE_NAME,
    token,
    createMasterDataCookieOptions(MASTER_DATA_AUTH_COOKIE_MAX_AGE_SECONDS)
  );

  if (!loginUser) {
    return;
  }

  const payload = createMasterDataAuthUserPayload(loginUser);
  const signature = createMasterDataAuthUserSignature(payload);

  response.cookies.set(
    MASTER_DATA_AUTH_USER_COOKIE_NAME,
    payload,
    createMasterDataCookieOptions(MASTER_DATA_AUTH_COOKIE_MAX_AGE_SECONDS)
  );

  response.cookies.set(
    MASTER_DATA_AUTH_USER_SIGNATURE_COOKIE_NAME,
    signature,
    createMasterDataCookieOptions(MASTER_DATA_AUTH_COOKIE_MAX_AGE_SECONDS)
  );
}

export function clearMasterDataAuthCookie(response: NextResponse) {
  response.cookies.set(
    MASTER_DATA_AUTH_COOKIE_NAME,
    "",
    createMasterDataCookieOptions(0)
  );

  response.cookies.set(
    MASTER_DATA_AUTH_USER_COOKIE_NAME,
    "",
    createMasterDataCookieOptions(0)
  );

  response.cookies.set(
    MASTER_DATA_AUTH_USER_SIGNATURE_COOKIE_NAME,
    "",
    createMasterDataCookieOptions(0)
  );
}

export function requireMasterDataAuth(req: NextRequest) {
  if (hasValidMasterDataAuth(req)) {
    return null;
  }

  return NextResponse.json(
    { ok: false, error: "ログインしてください" },
    { status: 401 }
  );
}

export function getCurrentMasterDataUser(req: NextRequest) {
  if (!hasValidMasterDataAuth(req)) {
    return null;
  }

  const payload =
    req.cookies.get(MASTER_DATA_AUTH_USER_COOKIE_NAME)?.value ?? "";
  const signature =
    req.cookies.get(MASTER_DATA_AUTH_USER_SIGNATURE_COOKIE_NAME)?.value ?? "";

  if (payload.trim() === "" || signature.trim() === "") {
    return null;
  }

  const expectedSignature = createMasterDataAuthUserSignature(payload);

  if (!safeEqualText(signature, expectedSignature)) {
    return null;
  }

  return parseMasterDataAuthUserPayload(payload);
}

export function refreshMasterDataAuthCookie(
  response: NextResponse,
  req: NextRequest
) {
  const currentUser = getCurrentMasterDataUser(req);
  setMasterDataAuthCookie(response, currentUser ?? undefined);
}

export function requireMasterDataUser(req: NextRequest): MasterDataAuthResult {
  const authError = requireMasterDataAuth(req);

  if (authError) {
    return {
      user: null,
      errorResponse: authError,
    };
  }

  const user = getCurrentMasterDataUser(req);

  if (!user) {
    return {
      user: null,
      errorResponse: NextResponse.json(
        { ok: false, error: "ログインユーザー情報を確認できません" },
        { status: 401 }
      ),
    };
  }

  return {
    user,
    errorResponse: null,
  };
}

export function requireMasterDataAdmin(req: NextRequest): MasterDataAuthResult {
  const result = requireMasterDataUser(req);

  if (result.errorResponse || !result.user) {
    return result;
  }

  if (result.user.role !== "スーパー管理者" && result.user.role !== "管理者") {
    return {
      user: null,
      errorResponse: NextResponse.json(
        { ok: false, error: "管理者またはスーパー管理者のみ実行できます" },
        { status: 403 }
      ),
    };
  }

  return result;
}