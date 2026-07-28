#!/usr/bin/env python3
"""Persist and select the rotating credentials used by code review jobs."""

from __future__ import annotations

import argparse
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
QUOTA_RETRY_DELAY = timedelta(days=1)
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


def promote_auth(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    if baseline.get("auth_mode") != "chatgpt" or candidate.get("auth_mode") != "chatgpt":
        raise ValueError("Codex credentials must use ChatGPT authentication")

    baseline_tokens = baseline.get("tokens")
    candidate_tokens = candidate.get("tokens")
    if not isinstance(baseline_tokens, dict) or not isinstance(candidate_tokens, dict):
        raise ValueError("Codex credentials must contain a tokens object")
    for token_name in ("access_token", "refresh_token"):
        if not isinstance(baseline_tokens.get(token_name), str) or not baseline_tokens[token_name]:
            raise ValueError(f"Baseline {token_name} must be a non-empty string")
        if not isinstance(candidate_tokens.get(token_name), str) or not candidate_tokens[token_name]:
            raise ValueError(f"Candidate {token_name} must be a non-empty string")

    immutable_baseline = copy.deepcopy(baseline)
    immutable_candidate = copy.deepcopy(candidate)
    for credentials in (immutable_baseline, immutable_candidate):
        del credentials["tokens"]["access_token"]
        del credentials["tokens"]["refresh_token"]
        credentials.pop("last_refresh", None)
    if immutable_candidate != immutable_baseline:
        raise ValueError("Refreshed credentials changed an immutable identity field")

    promoted = copy.deepcopy(baseline)
    promoted["tokens"]["access_token"] = candidate_tokens["access_token"]
    promoted["tokens"]["refresh_token"] = candidate_tokens["refresh_token"]
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
    entry.setdefault("last_failure_at", None)
    entry.setdefault("last_http_status", None)
    entry.setdefault("last_success_at", None)
    entry.setdefault("last_recovered_at", None)
    entry.setdefault("retry_after", None)
    return entry


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
        retry_at = retry_after or now + QUOTA_RETRY_DELAY
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
    labelled_statuses = re.findall(
        r"(?:http(?:\s+status)?|status(?:\s+code)?|error\s+code)\s*[:=]?\s*([1-5]\d{2})",
        text,
        re.IGNORECASE,
    )
    if labelled_statuses:
        return int(labelled_statuses[-1])
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
    return retry_at if retry_at > current_time else retry_at + timedelta(days=1)


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
    if retry_after is None and classify_failure(message) == QUOTA_EXHAUSTED:
        retry_after = current_time + QUOTA_RETRY_DELAY
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

    classify = subcommands.add_parser("classify")
    classify.add_argument("--events", type=Path, required=True)
    classify.add_argument("--stderr", type=Path, required=True)

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

    now = args.now or utc_now()
    state = load_state(args.state)
    if args.command == "initialize":
        for auth_object in args.auth_object:
            account_state(state, auth_object)
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
