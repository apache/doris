#!/usr/bin/env python3
"""Persist and select the rotating credentials used by code review jobs."""

from __future__ import annotations

import argparse
import base64
import copy
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


STATE_VERSION = 1
AVAILABLE = "available"
QUOTA_EXHAUSTED = "quota_exhausted"
RATE_LIMITED = "rate_limited"
AUTHENTICATION_FAILED = "authentication_failed"
TRANSIENT_FAILURE = "transient_failure"
STATUS_VALUES = {AVAILABLE, QUOTA_EXHAUSTED, RATE_LIMITED, AUTHENTICATION_FAILED}
QUOTA_RETRY_DELAY = timedelta(hours=12)
RATE_LIMIT_RETRY_DELAY = timedelta(hours=1)
PERMANENT_AUTH_FAILURE_PATTERN = re.compile(
    r"refresh[\s_-]*token.*(?:revoked|expired|already\s+used)|"
    r"(?:revoked|expired).*refresh[\s_-]*token",
    re.IGNORECASE,
)
USAGE_LIMIT_MESSAGE_PATTERN = re.compile(
    r"you(?:'|\u2019)ve hit your usage limit|usage limit",
    re.IGNORECASE,
)
USAGE_LIMIT_RETRY_PATTERN = re.compile(
    r"try again at\s+(?:(?P<date>[A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4}\s+"
    r"\d{1,2}:\d{2}\s+(?:AM|PM))|(?P<time>\d{1,2}:\d{2}\s+(?:AM|PM)))",
    re.IGNORECASE | re.DOTALL,
)
CONTENT_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class Selection:
    auth_object: str | None
    all_quota_exhausted: bool
    next_retry_at: str | None


@dataclass(frozen=True)
class FailureClassification:
    kind: str
    http_status: int | None
    retry_after: str | None


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_account_state() -> dict[str, Any]:
    return {
        "content_digest": None,
        "status": AVAILABLE,
        "last_failure_at": None,
        "last_http_status": None,
        "last_success_at": None,
        "last_recovered_at": None,
        "retry_after": None,
    }


def default_state() -> dict[str, Any]:
    return {"version": STATE_VERSION, "next_account": 0, "accounts": {}}


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_state()

    state = json.loads(path.read_text())
    if not isinstance(state, dict):
        raise ValueError("Authentication state must be a JSON object")
    if state.get("version") != STATE_VERSION:
        raise ValueError(f"Unsupported authentication state version: {state.get('version')!r}")
    if not isinstance(state.get("next_account"), int):
        raise ValueError("Authentication state next_account must be an integer")
    if not isinstance(state.get("accounts"), dict):
        raise ValueError("Authentication state accounts must be an object")
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as temporary:
        json.dump(state, temporary, indent=2, sort_keys=True)
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def parse_id_token(id_token: str) -> dict[str, Any]:
    parts = id_token.split(".")
    if len(parts) < 3 or any(not part for part in parts[:3]):
        raise ValueError("Codex id_token must be a parseable JWT")
    payload = parts[1]
    try:
        decoded = base64.b64decode(payload + "=" * (-len(payload) % 4), altchars=b"-_", validate=True)
        claims = json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Codex id_token must be a parseable JWT") from exc
    if not isinstance(claims, dict):
        raise ValueError("Codex id_token JWT payload must be an object")
    return claims


def string_claim(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Codex id_token {name} claim must be a string")
    return value


def auth_identity(credentials: dict[str, Any]) -> dict[str, Any]:
    if credentials.get("auth_mode") != "chatgpt":
        raise ValueError("Codex credentials must use ChatGPT authentication")

    tokens = credentials.get("tokens")
    if not isinstance(tokens, dict):
        raise ValueError("Codex credentials must contain a tokens object")
    for token_name in ("access_token", "refresh_token", "id_token"):
        if not isinstance(tokens.get(token_name), str) or not tokens[token_name]:
            raise ValueError(f"Codex {token_name} must be a non-empty string")

    claims = parse_id_token(tokens["id_token"])
    profile = claims.get("https://api.openai.com/profile")
    if profile is not None and not isinstance(profile, dict):
        raise ValueError("Codex id_token profile claim must be an object")
    auth_claims = claims.get("https://api.openai.com/auth")
    if auth_claims is not None and not isinstance(auth_claims, dict):
        raise ValueError("Codex id_token auth claim must be an object")
    profile = profile or {}
    auth_claims = auth_claims or {}
    fedramp = auth_claims.get("chatgpt_account_is_fedramp", False)
    if not isinstance(fedramp, bool):
        raise ValueError("Codex id_token chatgpt_account_is_fedramp claim must be a boolean")

    email = claims.get("email")
    if email is None:
        email = profile.get("email")
    return {
        "email": string_claim(email, "email"),
        "chatgpt_user_id": string_claim(
            auth_claims.get("chatgpt_user_id", auth_claims.get("user_id")), "chatgpt_user_id"
        ),
        "chatgpt_account_id": string_claim(auth_claims.get("chatgpt_account_id"), "chatgpt_account_id"),
        "chatgpt_account_is_fedramp": fedramp,
        "account_id": string_claim(tokens.get("account_id"), "account_id"),
    }


def validate_auth(credentials: dict[str, Any]) -> None:
    auth_identity(credentials)
    last_refresh = credentials.get("last_refresh")
    if last_refresh is not None:
        if not isinstance(last_refresh, str) or not last_refresh:
            raise ValueError("Codex last_refresh must be a non-empty timestamp")
        parse_timestamp(last_refresh)


def promote_auth(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    validate_auth(baseline)
    validate_auth(candidate)
    if auth_identity(baseline) != auth_identity(candidate):
        raise ValueError("Refreshed credentials changed an immutable account or workspace claim")
    if not isinstance(candidate.get("last_refresh"), str) or not candidate["last_refresh"]:
        raise ValueError("Refreshed credentials must include last_refresh")

    promoted = copy.deepcopy(baseline)
    for token_name in ("access_token", "refresh_token", "id_token"):
        promoted["tokens"][token_name] = candidate["tokens"][token_name]
    promoted["last_refresh"] = candidate["last_refresh"]
    return promoted


def promote_auth_file(baseline_path: Path, candidate_path: Path, output_path: Path) -> None:
    baseline = json.loads(baseline_path.read_text())
    candidate = json.loads(candidate_path.read_text())
    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        raise ValueError("Codex credentials must be JSON objects")
    save_state(output_path, promote_auth(baseline, candidate))


def account_state(state: dict[str, Any], auth_object: str) -> dict[str, Any]:
    accounts = state["accounts"]
    entry = accounts.setdefault(auth_object, default_account_state())
    if not isinstance(entry, dict):
        raise ValueError(f"Authentication state for {auth_object!r} must be an object")
    status = entry.get("status", AVAILABLE)
    if status not in STATUS_VALUES:
        raise ValueError(f"Unknown authentication status for {auth_object!r}: {status!r}")
    entry.setdefault("status", AVAILABLE)
    entry.setdefault("content_digest", None)
    entry.setdefault("last_failure_at", None)
    entry.setdefault("last_http_status", None)
    entry.setdefault("last_success_at", None)
    entry.setdefault("last_recovered_at", None)
    entry.setdefault("retry_after", None)
    return entry


def validate_content_digest(digest: str) -> None:
    if CONTENT_DIGEST_PATTERN.fullmatch(digest) is None:
        raise ValueError("Codex credential content digest must be a SHA-256 hex string")


def reconcile_auth_content(
    state: dict[str, Any], auth_object: str, content_digest: str, now: datetime
) -> None:
    validate_content_digest(content_digest)
    legacy_key = f"{auth_object}#{content_digest}"
    if auth_object not in state["accounts"] and legacy_key in state["accounts"]:
        state["accounts"][auth_object] = state["accounts"].pop(legacy_key)
    entry = account_state(state, auth_object)
    known_digest = entry["content_digest"]
    if known_digest is None:
        entry["content_digest"] = content_digest
    elif known_digest != content_digest:
        entry.update(default_account_state())
        entry["content_digest"] = content_digest
        entry["last_recovered_at"] = format_timestamp(now)


def set_auth_content_digest(state: dict[str, Any], auth_object: str, content_digest: str) -> None:
    validate_content_digest(content_digest)
    account_state(state, auth_object)["content_digest"] = content_digest


def recover_expired_accounts(state: dict[str, Any], auth_objects: list[str], now: datetime) -> None:
    for auth_object in auth_objects:
        entry = account_state(state, auth_object)
        retry_after = entry["retry_after"]
        if retry_after is None:
            continue
        if parse_timestamp(retry_after) <= now:
            entry["status"] = AVAILABLE
            entry["retry_after"] = None
            entry["last_recovered_at"] = format_timestamp(now)


def next_retry_at(state: dict[str, Any], auth_objects: list[str]) -> str | None:
    retry_at = [
        entry["retry_after"]
        for auth_object in auth_objects
        for entry in [account_state(state, auth_object)]
        if entry["retry_after"] is not None
    ]
    return min(retry_at, key=parse_timestamp, default=None)


def quota_retry_at(now: datetime, retry_after: datetime | None) -> datetime:
    latest_retry_at = now + QUOTA_RETRY_DELAY
    if retry_after is None or retry_after > latest_retry_at:
        return latest_retry_at
    return retry_after


def all_quota_exhausted(state: dict[str, Any], auth_objects: list[str]) -> bool:
    return bool(auth_objects) and all(
        account_state(state, auth_object)["status"] == QUOTA_EXHAUSTED for auth_object in auth_objects
    )


def select_auth_object(state: dict[str, Any], auth_objects: list[str], now: datetime) -> Selection:
    if not auth_objects:
        raise ValueError("At least one auth object is required")

    auth_objects = sorted(set(auth_objects))
    recover_expired_accounts(state, auth_objects, now)
    starting_index = state["next_account"] % len(auth_objects)
    for offset in range(len(auth_objects)):
        index = (starting_index + offset) % len(auth_objects)
        auth_object = auth_objects[index]
        if account_state(state, auth_object)["status"] == AVAILABLE:
            state["next_account"] = (index + 1) % len(auth_objects)
            return Selection(auth_object, False, None)

    return Selection(None, all_quota_exhausted(state, auth_objects), next_retry_at(state, auth_objects))


def record_result(
    state: dict[str, Any],
    auth_object: str,
    result: str,
    now: datetime,
    http_status: int | None = None,
    retry_after: datetime | None = None,
) -> None:
    entry = account_state(state, auth_object)
    timestamp = format_timestamp(now)
    if result == AVAILABLE:
        entry["status"] = AVAILABLE
        entry["last_success_at"] = timestamp
        entry["retry_after"] = None
        return

    if result == QUOTA_EXHAUSTED:
        http_status = http_status or 403
        retry_at = quota_retry_at(now, retry_after)
    elif result == RATE_LIMITED:
        http_status = http_status or 429
    elif result == AUTHENTICATION_FAILED:
        http_status = http_status or 401
        retry_at = None
    elif result == TRANSIENT_FAILURE:
        entry["status"] = AVAILABLE
        entry["last_failure_at"] = timestamp
        entry["last_http_status"] = http_status
        entry["retry_after"] = None
        return
    else:
        raise ValueError(f"Unsupported result: {result!r}")

    if result == RATE_LIMITED:
        retry_at = now + RATE_LIMIT_RETRY_DELAY

    entry["status"] = result
    entry["last_failure_at"] = timestamp
    entry["last_http_status"] = http_status
    entry["retry_after"] = format_timestamp(retry_at) if retry_at is not None else None


def extract_http_status(text: str) -> int | None:
    matches = list(
        re.finditer(
            r"(?:http(?:\s+status)?|status(?:\s+code)?|error\s+code)\s*[:=]?\s*([1-5]\d{2})"
            r"|failed\s+to\s+refresh\s+token\s*:\s*([1-5]\d{2})\b",
            text,
            re.IGNORECASE,
        )
    )
    if matches:
        return int(next(group for group in reversed(matches[-1].groups()) if group is not None))
    return None


def usage_limit_retry_after(text: str, now: datetime | None = None) -> datetime | None:
    match = USAGE_LIMIT_RETRY_PATTERN.search(text)
    if match is None:
        return None

    full_timestamp = match.group("date")
    if full_timestamp is not None:
        timestamp = re.sub(r"(\d)(?:st|nd|rd|th)\b", r"\1", full_timestamp, flags=re.IGNORECASE)
        try:
            return datetime.strptime(timestamp, "%b %d, %Y %I:%M %p").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    current_time = now or utc_now()
    try:
        retry_time = datetime.strptime(match.group("time"), "%I:%M %p").time()
    except ValueError:
        return None
    retry_at = datetime.combine(current_time.date(), retry_time, tzinfo=timezone.utc)
    return retry_at if retry_at > current_time else current_time + QUOTA_RETRY_DELAY


def classify_failure(text: str) -> str:
    if PERMANENT_AUTH_FAILURE_PATTERN.search(text):
        return AUTHENTICATION_FAILED
    if USAGE_LIMIT_MESSAGE_PATTERN.search(text):
        return QUOTA_EXHAUSTED
    http_status = extract_http_status(text)
    if http_status == 429:
        return RATE_LIMITED
    if http_status in {402, 403}:
        return QUOTA_EXHAUSTED
    if http_status == 401:
        return AUTHENTICATION_FAILED
    if http_status in {408, 409, 425, 500, 502, 503, 504}:
        return TRANSIENT_FAILURE
    if re.search(
        r"connection reset|econnreset|eai_again|network is unreachable|etimedout|timeout|timed? out",
        text,
        re.IGNORECASE,
    ):
        return TRANSIENT_FAILURE
    return "fatal"


def event_error_message(event: object) -> str | None:
    if not isinstance(event, dict):
        return None

    event_type = event.get("type")
    if event_type == "turn.failed":
        error = event.get("error")
        message = error.get("message") if isinstance(error, dict) else None
    elif event_type == "error":
        error = event.get("error")
        message = event.get("message") or (error.get("message") if isinstance(error, dict) else None)
    else:
        return None

    return message if isinstance(message, str) and message.strip() else None


def terminal_event_error(events: str) -> str | None:
    message = None
    for line in events.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        event_message = event_error_message(event)
        if event_message is not None:
            message = event_message
    return message


def terminal_stderr_error(stderr: str) -> str | None:
    return next((line for line in reversed(stderr.splitlines()) if line.strip()), None)


def classify_terminal_failure(
    events: str, stderr: str, now: datetime | None = None
) -> FailureClassification:
    message = terminal_event_error(events) or terminal_stderr_error(stderr) or ""
    current_time = now or utc_now()
    retry_after = usage_limit_retry_after(message, current_time)
    if classify_failure(message) == QUOTA_EXHAUSTED:
        retry_after = quota_retry_at(current_time, retry_after)
    return FailureClassification(
        kind=classify_failure(message),
        http_status=extract_http_status(message),
        retry_after=format_timestamp(retry_after) if retry_after is not None else None,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    commands_with_state = (
        subcommands.add_parser("initialize"),
        subcommands.add_parser("select"),
        subcommands.add_parser("record"),
        subcommands.add_parser("reconcile"),
        subcommands.add_parser("set-digest"),
    )
    for command in commands_with_state:
        command.add_argument("--state", type=Path, required=True)
        command.add_argument("--now", type=parse_timestamp)

    initialize = subcommands.choices["initialize"]
    initialize.add_argument("--auth-object", action="append", required=True)

    select = subcommands.choices["select"]
    select.add_argument("--auth-object", action="append", required=True)

    record = subcommands.choices["record"]
    record.add_argument("--auth-object", required=True)
    record.add_argument(
        "--result",
        choices=[AVAILABLE, QUOTA_EXHAUSTED, RATE_LIMITED, AUTHENTICATION_FAILED, TRANSIENT_FAILURE],
        required=True,
    )
    record.add_argument("--http-status", type=int)
    record.add_argument("--retry-after", type=parse_timestamp)
    record.add_argument("--known-auth-object", action="append", required=True)

    reconcile = subcommands.choices["reconcile"]
    reconcile.add_argument("--auth-object", action="append", required=True)
    reconcile.add_argument("--content-digest", action="append", required=True)

    set_digest = subcommands.choices["set-digest"]
    set_digest.add_argument("--auth-object", required=True)
    set_digest.add_argument("--content-digest", required=True)

    classify = subcommands.add_parser("classify")
    classify.add_argument("--events", type=Path, required=True)
    classify.add_argument("--stderr", type=Path, required=True)

    validate = subcommands.add_parser("validate")
    validate.add_argument("--auth", type=Path, required=True)

    promote = subcommands.add_parser("promote")
    promote.add_argument("--baseline", type=Path, required=True)
    promote.add_argument("--candidate", type=Path, required=True)
    promote.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "promote":
        promote_auth_file(args.baseline, args.candidate, args.output)
        return
    if args.command == "classify":
        classification = classify_terminal_failure(
            args.events.read_text(errors="replace"), args.stderr.read_text(errors="replace")
        )
        print(
            json.dumps(
                {
                    "kind": classification.kind,
                    "http_status": classification.http_status,
                    "retry_after": classification.retry_after,
                }
            )
        )
        return
    if args.command == "validate":
        credentials = json.loads(args.auth.read_text())
        if not isinstance(credentials, dict):
            raise ValueError("Codex credentials must be a JSON object")
        validate_auth(credentials)
        return

    now = args.now or utc_now()
    state = load_state(args.state)
    if args.command == "initialize":
        for auth_object in args.auth_object:
            account_state(state, auth_object)
        save_state(args.state, state)
        return

    if args.command == "reconcile":
        if len(args.auth_object) != len(args.content_digest):
            raise ValueError("Each auth object must have exactly one content digest")
        for auth_object, content_digest in zip(args.auth_object, args.content_digest):
            reconcile_auth_content(state, auth_object, content_digest, now)
        save_state(args.state, state)
        return

    if args.command == "set-digest":
        set_auth_content_digest(state, args.auth_object, args.content_digest)
        save_state(args.state, state)
        return

    if args.command == "select":
        selection = select_auth_object(state, args.auth_object, now)
        save_state(args.state, state)
        print(
            json.dumps(
                {
                    "auth_object": selection.auth_object,
                    "all_quota_exhausted": selection.all_quota_exhausted,
                    "next_retry_at": selection.next_retry_at,
                }
            )
        )
        return

    record_result(state, args.auth_object, args.result, now, args.http_status, args.retry_after)
    recover_expired_accounts(state, args.known_auth_object, now)
    save_state(args.state, state)
    print(
        json.dumps(
            {
                "all_quota_exhausted": all_quota_exhausted(state, args.known_auth_object),
                "next_retry_at": next_retry_at(state, args.known_auth_object),
            }
        )
    )


if __name__ == "__main__":
    main()
