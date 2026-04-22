from fastapi import APIRouter, Cookie, Depends, HTTPException, Response
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..auth import (
    create_access_token,
    create_refresh_token,
    create_2fa_token,
    decode_token,
    generate_totp_secret,
    get_totp_uri,
    hash_password,
    verify_password,
    verify_totp,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    REFRESH_TOKEN_EXPIRE_DAYS,
)
from ..database import get_db, get_current_user

router = APIRouter(prefix="/api/auth", tags=["auth"])

_SECURE_COOKIE = False   # set True when serving over HTTPS


def _set_auth_cookies(response: Response, user_id: int, role: str):
    access  = create_access_token(user_id, role)
    refresh = create_refresh_token(user_id)
    response.set_cookie(
        "access_token", access,
        httponly=True, secure=_SECURE_COOKIE, samesite="lax",
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )
    response.set_cookie(
        "refresh_token", refresh,
        httponly=True, secure=_SECURE_COOKIE, samesite="lax",
        max_age=REFRESH_TOKEN_EXPIRE_DAYS * 86400,
    )


# ── Register ──────────────────────────────────────────────────────────────────

class RegisterBody(BaseModel):
    email:    EmailStr
    username: str
    password: str


@router.post("/register", status_code=201)
def register(body: RegisterBody, response: Response, db: Session = Depends(get_db)):
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if len(body.username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")

    existing = db.execute(
        text("SELECT id FROM users WHERE email = :e OR username = :u"),
        {"e": body.email, "u": body.username},
    ).fetchone()
    if existing:
        raise HTTPException(409, "Email or username already taken")

    row = db.execute(
        text("""
            INSERT INTO users (email, username, password_hash)
            VALUES (:e, :u, :pw)
            RETURNING id, role
        """),
        {"e": body.email, "u": body.username, "pw": hash_password(body.password)},
    ).fetchone()
    db.commit()

    _set_auth_cookies(response, row[0], row[1])
    return {"id": row[0], "email": body.email, "username": body.username, "role": row[1]}


# ── Login ─────────────────────────────────────────────────────────────────────

class LoginBody(BaseModel):
    email:    str
    password: str


@router.post("/login")
def login(body: LoginBody, response: Response, db: Session = Depends(get_db)):
    row = db.execute(
        text("SELECT id, password_hash, role, is_active, totp_enabled FROM users WHERE email = :e"),
        {"e": body.email},
    ).fetchone()

    if not row or not verify_password(body.password, row[1]):
        raise HTTPException(401, "Invalid email or password")
    if not row[3]:
        raise HTTPException(403, "Account is inactive")

    db.execute(
        text("UPDATE users SET last_login = NOW() WHERE id = :id"),
        {"id": row[0]},
    )
    db.commit()

    if row[4]:  # totp_enabled
        temp_token = create_2fa_token(row[0])
        return {"requires_2fa": True, "temp_token": temp_token}

    _set_auth_cookies(response, row[0], row[2])
    return {"requires_2fa": False, "id": row[0], "role": row[2]}


# ── 2FA verification (second step of login) ───────────────────────────────────

class TwoFAVerifyBody(BaseModel):
    temp_token: str
    code:       str


@router.post("/login/2fa")
def login_2fa(body: TwoFAVerifyBody, response: Response, db: Session = Depends(get_db)):
    from jose import JWTError
    try:
        payload = decode_token(body.temp_token)
        if payload.get("type") != "2fa_pending":
            raise HTTPException(401, "Invalid 2FA token")
        user_id = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise HTTPException(401, "Invalid or expired 2FA token")

    row = db.execute(
        text("SELECT role, totp_secret, is_active FROM users WHERE id = :id"),
        {"id": user_id},
    ).fetchone()
    if not row or not row[2]:
        raise HTTPException(401, "User not found or inactive")

    if not verify_totp(row[1], body.code):
        raise HTTPException(401, "Invalid 2FA code")

    _set_auth_cookies(response, user_id, row[0])
    return {"requires_2fa": False, "id": user_id, "role": row[0]}


# ── Logout ────────────────────────────────────────────────────────────────────

@router.post("/logout")
def logout(response: Response):
    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")
    return {"status": "logged_out"}


# ── Token refresh ─────────────────────────────────────────────────────────────

@router.post("/refresh")
def refresh(response: Response, refresh_token: str | None = Cookie(None), db: Session = Depends(get_db)):
    if not refresh_token:
        raise HTTPException(401, "No refresh token")
    from jose import JWTError
    try:
        payload = decode_token(refresh_token)
        if payload.get("type") != "refresh":
            raise HTTPException(401, "Invalid token type")
        user_id = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise HTTPException(401, "Invalid or expired refresh token")

    row = db.execute(
        text("SELECT role, is_active FROM users WHERE id = :id"),
        {"id": user_id},
    ).fetchone()
    if not row or not row[1]:
        raise HTTPException(401, "User not found or inactive")

    _set_auth_cookies(response, user_id, row[0])
    return {"status": "refreshed"}


# ── Current user ─────────────────────────────────────────────────────────────

@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return current_user


# ── Change password ───────────────────────────────────────────────────────────

class ChangePasswordBody(BaseModel):
    current_password: str
    new_password:     str


@router.patch("/password")
def change_password(
    body: ChangePasswordBody,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if len(body.new_password) < 8:
        raise HTTPException(400, "New password must be at least 8 characters")

    row = db.execute(
        text("SELECT password_hash FROM users WHERE id = :id"),
        {"id": current_user["id"]},
    ).fetchone()
    if not verify_password(body.current_password, row[0]):
        raise HTTPException(401, "Current password is incorrect")

    db.execute(
        text("UPDATE users SET password_hash = :pw WHERE id = :id"),
        {"pw": hash_password(body.new_password), "id": current_user["id"]},
    )
    db.commit()
    return {"status": "password_changed"}


# ── 2FA setup ─────────────────────────────────────────────────────────────────

@router.post("/2fa/setup")
def setup_2fa(current_user: dict = Depends(get_current_user), db: Session = Depends(get_db)):
    """Generate a TOTP secret and return the otpauth URI for QR code rendering.
    Refuses to overwrite a secret that is already active — disable first.
    """
    row = db.execute(
        text("SELECT totp_enabled FROM users WHERE id = :id"),
        {"id": current_user["id"]},
    ).fetchone()
    if row and row[0]:
        raise HTTPException(400, "2FA is already enabled — disable it before setting up again")

    secret = generate_totp_secret()
    db.execute(
        text("UPDATE users SET totp_secret = :s WHERE id = :id"),
        {"s": secret, "id": current_user["id"]},
    )
    db.commit()
    uri = get_totp_uri(secret, current_user["email"])
    return {"secret": secret, "uri": uri}


class TwoFAEnableBody(BaseModel):
    code: str


@router.post("/2fa/enable")
def enable_2fa(
    body: TwoFAEnableBody,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.execute(
        text("SELECT totp_secret FROM users WHERE id = :id"),
        {"id": current_user["id"]},
    ).fetchone()
    if not row or not row[0]:
        raise HTTPException(400, "Run /2fa/setup first to generate a secret")

    if not verify_totp(row[0], body.code):
        raise HTTPException(400, "Invalid code — check your authenticator app")

    db.execute(
        text("UPDATE users SET totp_enabled = true WHERE id = :id"),
        {"id": current_user["id"]},
    )
    db.commit()
    return {"status": "2fa_enabled"}


class TwoFADisableBody(BaseModel):
    password: str


@router.post("/2fa/disable")
def disable_2fa(
    body: TwoFADisableBody,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    row = db.execute(
        text("SELECT password_hash FROM users WHERE id = :id"),
        {"id": current_user["id"]},
    ).fetchone()
    if not verify_password(body.password, row[0]):
        raise HTTPException(401, "Incorrect password")

    db.execute(
        text("UPDATE users SET totp_enabled = false, totp_secret = NULL WHERE id = :id"),
        {"id": current_user["id"]},
    )
    db.commit()
    return {"status": "2fa_disabled"}
