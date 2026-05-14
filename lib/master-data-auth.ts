import { timingSafeEqual } from "crypto";
import { NextRequest, NextResponse } from "next/server";

const MASTER_DATA_AUTH_COOKIE_NAME = "master-data-auth";
const MASTER_DATA_AUTH_COOKIE_MAX_AGE_SECONDS = 60 * 30;

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

export function isValidMasterDataLogin(id: string, password: string) {
  const expectedId = process.env.MASTER_DATA_LOGIN_ID ?? "";
  const expectedPassword = process.env.MASTER_DATA_LOGIN_PASSWORD ?? "";

  if (expectedId.trim() === "" || expectedPassword.trim() === "") {
    return false;
  }

  return safeEqualText(id, expectedId) && safeEqualText(password, expectedPassword);
}

export function setMasterDataAuthCookie(response: NextResponse) {
  const token = getRequiredAuthToken();

  response.cookies.set(MASTER_DATA_AUTH_COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: MASTER_DATA_AUTH_COOKIE_MAX_AGE_SECONDS,
  });
}

export function clearMasterDataAuthCookie(response: NextResponse) {
  response.cookies.set(MASTER_DATA_AUTH_COOKIE_NAME, "", {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: 0,
  });
}

export function requireMasterDataAuth(req: NextRequest) {
  const expectedToken = process.env.MASTER_DATA_AUTH_TOKEN ?? "";
  const actualToken = req.cookies.get(MASTER_DATA_AUTH_COOKIE_NAME)?.value ?? "";

  if (
    expectedToken.trim() !== "" &&
    actualToken.trim() !== "" &&
    safeEqualText(actualToken, expectedToken)
  ) {
    return null;
  }

  return NextResponse.json(
    { ok: false, error: "ログインしてください" },
    { status: 401 }
  );
}