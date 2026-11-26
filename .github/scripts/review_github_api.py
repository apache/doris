#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Capture gh HTTP failures and retry transient failures of read-only REST GETs."""

import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

LIMIT = 65536
HEADERS = {
    "content-type", "content-length", "date", "x-github-request-id",
    "retry-after", "x-ratelimit-limit", "x-ratelimit-remaining",
    "x-ratelimit-reset", "x-ratelimit-resource",
}


def redact(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    value = value or ""
    # Artifacts do not inherit Actions' secret masker. Never store the request
    # headers or body; also redact credentials echoed by an error response.
    for name, secret in os.environ.items():
        if re.search(r"(?:TOKEN|SECRET|PASSWORD|(?:^|_)KEY|(?:^|_)AK|(?:^|_)SK)$", name) and secret:
            for encoded in (secret, json.dumps(secret)[1:-1]):
                value = value.replace(encoded, "[REDACTED]")
    value = re.sub(r"\b(?:gh[pousr]_[A-Za-z0-9]+|github_pat_[A-Za-z0-9_]+)\b", "[REDACTED]", value)
    value = re.sub(
        r'(?i)(["\']?(?:access_token|refresh_token|id_token|token|api_key|secret|'
        r'client_secret|password|authorization|cookie|set-cookie)["\']?\s*[:=]\s*)'
        r'("(?:\\.|[^"\\])*"|[^\r\n,}]+)', r'\1"[REDACTED]"', value,
    )
    value = re.sub(r"(https?://)[^/\s:@]+:[^/@\s]+@", r"\1[REDACTED]@", value)
    value = re.sub(r"(?i)([?&](?:token|access_token|signature|sig|key)=)[^&\s\"']+", r"\1[REDACTED]", value)
    return value


def excerpt(value):
    value = redact(value)
    return value[:LIMIT] + ("\n[truncated]" if len(value) > LIMIT else "")


def split_trace(stderr):
    """Keep only the final HTTP exchange; discard every outgoing header/body.

    GH_DEBUG=api writes HTTP diagnostics to stderr, independently of gh api's
    response parser. Its body is a formatted, size-limited debug excerpt, not
    a byte-for-byte copy. Unknown debug formatting is discarded, never dumped.
    """
    text = stderr.decode("utf-8", errors="replace") if isinstance(stderr, bytes) else (stderr or "")
    response = {"method": None, "path": None, "status": None, "headers": {},
                "body_excerpt": "", "body_source": "gh_debug", "request_count": 0}
    ordinary = []
    mode = "ordinary"
    body = []
    body_chars = 0
    for line in text.splitlines():
        if line.startswith("* Request at "):
            count = response["request_count"] + 1
            response = {**response, "method": None, "path": None, "status": None,
                        "headers": {}, "body_excerpt": "", "request_count": count}
            body = []
            body_chars = 0
            response.pop("body_truncated", None)
            mode = "request"
        elif line.startswith("> "):
            match = re.match(r"> ([A-Z]+) (\S+) HTTP/", line)
            if match:
                response["method"] = match[1]
                response["path"] = redact(urlsplit(match[2]).path)
            mode = "request"
        elif re.match(r"< HTTP/\S+ \d{3}", line):
            response["status"] = int(line.split()[2])
            response["headers"] = {}
            body = []
            body_chars = 0
            mode = "headers"
        elif mode == "headers" and line.startswith("< "):
            name, sep, value = line[2:].partition(":")
            if sep and name.lower() in HEADERS:
                response["headers"][name.lower()] = excerpt(value.strip())
        elif line.startswith("* Request took "):
            mode = "ordinary"
        elif mode == "headers" and not line:
            mode = "body"
        elif mode == "body":
            # Bound retained response content even if debug output grows.
            line = redact(line)  # Redact before truncation can split a secret.
            if body_chars < LIMIT:
                response["body_truncated"] = len(line) + body_chars > LIMIT
                body.append(line[:LIMIT - body_chars])
                body_chars += len(line) + 1
            else:
                response["body_truncated"] = True
        elif mode == "ordinary" and not line.startswith("* "):
            ordinary.append(line)
    response["body_excerpt"] = excerpt("\n".join(body).strip())
    if mode == "request":
        # A transport failure has no response; do not mistake request data for
        # its body. subprocess timeout/type still identifies the failure.
        response["body_excerpt"] = ""
    return response, excerpt("\n".join(ordinary).strip())


def record_failure(command, error, stdout="", stderr="", *, diagnostic=None):
    response, ordinary = split_trace(stderr) if diagnostic is None else (diagnostic, excerpt(stderr))
    record = {
        "time": datetime.now(timezone.utc).isoformat(),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "job": os.environ.get("GITHUB_JOB"),
        "gh_version": os.environ.get("REVIEW_GH_VERSION"),
        "error_type": type(error).__name__,
        "returncode": getattr(error, "returncode", None),
        "timeout_seconds": getattr(error, "timeout", None),
        "http": response,
        "stderr": ordinary,
        "stdout_excerpt": excerpt(stdout),
    }
    # Do not record argv: -f/--body/--input can contain credentials or entire
    # reviews. Endpoint metadata comes only from the outgoing request line.
    summary = (
        f"GitHub request failed: {response['method'] or 'unknown'} {response['path'] or 'unknown'}; "
        f"HTTP={response['status']}; request_id={response['headers'].get('x-github-request-id', 'unknown')}; "
        f"{record['error_type']}; exit={record['returncode']}; timeout={record['timeout_seconds']}"
    )
    record["summary"] = redact(summary)
    directory = os.environ.get("REVIEW_GITHUB_DIAGNOSTICS_DIR")
    if directory:
        try:
            path = Path(directory)
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            data = json.dumps(record, ensure_ascii=True, indent=2) + "\n"
            # Unique files support concurrent helper calls without append races.
            filename = path / f"{time.time_ns()}-{uuid.uuid4().hex}.json"
            with open(filename, "x", opener=lambda p, f: os.open(p, f, 0o600)) as handle:
                handle.write(data)
        except OSError as exc:
            print(f"Could not persist GitHub diagnostic ({type(exc).__name__})", file=sys.stderr)
    print(record["summary"], file=sys.stderr, flush=True)
    return ordinary, record


def is_read_only_get(command):
    """Recognize only the body-free gh api forms used by the review pipeline.

    Unknown options remain single-attempt. In particular, fields/input imply
    POST in gh, and GraphQL uses POST even without an explicit method. Never
    infer safety from the last HTTP response: a write can redirect to GET.
    """
    if len(command) < 3 or Path(command[0]).name != "gh" or command[1] != "api":
        return False
    endpoint = None
    args = iter(command[2:])
    for arg in args:
        if arg in ("--paginate", "--slurp", "-XGET", "--method=GET"):
            continue
        if arg in ("-X", "--method"):
            if next(args, None) != "GET":
                return False
        elif arg in ("-H", "--header"):
            if next(args, None) is None:
                return False
        elif arg.startswith("-") or endpoint is not None:
            return False
        else:
            endpoint = arg
    return endpoint is not None and urlsplit(endpoint).path.rstrip("/").split("/")[-1] != "graphql"


def retry_delay(response, attempt):
    """Do not shorten a server-requested wait, or guess if it is malformed."""
    delay = 2 ** attempt
    value = response["headers"].get("retry-after")
    if value is None:
        return delay
    try:
        if value.isdigit():
            return max(delay, int(value))
        when = parsedate_to_datetime(value)
        return max(delay, (when - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def run_gh(command, **kwargs):
    """Buffer each attempt; a caller timeout bounds the entire GET operation."""
    env = dict(kwargs.pop("env", os.environ))
    env.update(GH_DEBUG="api", NO_COLOR="1")
    retry_get = (is_read_only_get(command) and kwargs.get("check") is True
                 and kwargs.get("capture_output") is True and kwargs.get("text") is True
                 and kwargs.get("input") is None and kwargs.get("stdin") is None)
    if retry_get:
        # Fit within the runner's two-minute input-resolution step. Existing
        # shorter submission/watchdog budgets include all attempts and sleeps.
        budget = min(90, kwargs.get("timeout") if kwargs.get("timeout") is not None else 90)
        deadline = time.monotonic() + budget
    for attempt in range(1, 4):
        options = dict(kwargs)
        if retry_get:
            options["timeout"] = min(30, max(0, deadline - time.monotonic()))
        try:
            result = subprocess.run(command, env=env, **options)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            ordinary, record = record_failure(command, error, error.stdout, error.stderr)
            error.stderr = ordinary + "\n" + record["summary"]
            if retry_get:
                # gh --paginate may emit incomplete JSON before a later page
                # fails. Retain it only in diagnostics, never in caller output.
                error.stdout = ""
                response = record["http"]
                if (isinstance(error, subprocess.CalledProcessError)
                        and error.returncode > 0 and response["method"] == "GET"
                        and response["status"] in (502, 503, 504) and attempt < 3):
                    delay = retry_delay(response, attempt)
                    if delay is not None and delay < deadline - time.monotonic():
                        print(f"Retrying GitHub GET after HTTP {response['status']} in {delay:g}s "
                              f"(attempt {attempt + 1}/3)", file=sys.stderr, flush=True)
                        time.sleep(delay)
                        if time.monotonic() < deadline:
                            continue
            raise
        except OSError as error:
            record_failure(command, error)
            raise
        response, ordinary = split_trace(result.stderr)
        result.stderr = ordinary
        result.github_response = response
        return result


def decode_json(result, command):
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        record_failure(command, error, result.stdout, result.stderr,
                       diagnostic=result.github_response)
        raise


def main():
    command = ["gh", *sys.argv[1:]]
    try:
        result = run_gh(command, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as error:
        sys.stdout.write(error.stdout or "")
        print(error.stderr, file=sys.stderr)
        return error.returncode if error.returncode >= 0 else 128 - error.returncode
    except subprocess.TimeoutExpired as error:
        print(error.stderr, file=sys.stderr)
        return 124
    except OSError as error:
        print(f"Could not execute gh ({type(error).__name__})", file=sys.stderr)
        return 1
    sys.stdout.write(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
