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

"""Render one terminal Doris CI result. Pass --upload on a publisher agent to copy it to OSS."""
import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
from urllib.request import urlopen

PIPELINES = {
    "compile": "Compile", "p0": "P0", "cloud_p0": "Cloud P0", "vault_p0": "Vault P0",
    "feut": "FE UT", "beut": "BE UT", "cloudut": "Cloud UT", "external": "External",
    "nonconcurrent": "NonConcurrent", "performance": "Performance",
}
RESULTS = {"success": "Passed", "failure": "Failed", "error": "Build error", "canceled": "Canceled"}
STEPS = {
    "precheck": "Environment check", "checkout": "Checkout and merge", "binary": "Binary compilation",
    "cases": "Test case packaging", "setup": "Test setup", "tests": "Tests",
    "benchmark": "Benchmarks", "upload": "Artifact upload", "cleanup": "Cleanup",
}
TC_STAGE = re.compile(r"^\[[\d:]+\][ EiW]:\s*Step (\d+)/(\d+): (.*?) \(Command Line\)(?: \(([0-9hms: ]+)\))?\s*$")
BUCKET = "doris-pipelines"
REGION = "cn-hongkong"
ENDPOINT = f"oss-{REGION}-internal.aliyuncs.com"


def count(value):
    if not re.fullmatch(r"[0-9]{1,18}", value):
        raise argparse.ArgumentTypeError("expected a non-negative integer of up to 18 digits")
    return int(value)


def sha(value):
    if not re.fullmatch(r"[a-f0-9]{40}", value):
        raise argparse.ArgumentTypeError("expected a full lowercase Git SHA")
    return value


def target_branch(value):
    if not re.fullmatch(r"master|branch-\d{1,2}\.\d{1,2}", value):
        raise argparse.ArgumentTypeError("expected a public Doris target branch")
    return value


def time_with_offset(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
    if not parsed or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("expected an ISO timestamp with a UTC offset")
    return parsed.astimezone(timezone(timedelta(hours=8)))


# Escape at the HTML boundary. Do not reinterpret failure text as HTML or rewrite its diagnosis.
KEY = r"(?:[\w.-]*[._-])?(?:access[_-]?key(?:[_-]?id)?|api[_-]?key|secret(?:[_-]?access)?[_-]?key|accesskeysecret|client[_-]?secret|secret|passw(?:or)?d|pwd|(?:[a-z]+[_-]?)?token|signature|credential)"
SECRET_VALUE = re.compile(
    rf"""(?ix)(?P<prefix>(?<![\w])["']?{KEY}["']?\s*[:=]\s*)
    (?:"(?P<double>(?:\\.|[^"\\])*)"|'(?P<single>(?:\\.|[^'\\])*)'|(?P<bare>\[redacted\]|[^\s,;&)\]}}]+))""")
PEM = re.compile(r"-----BEGIN ([A-Z ]*PRIVATE KEY)-----.*?(?:-----END \1-----|\Z)", re.S)
TOKEN = re.compile(r"\b(?:LTAI[A-Za-z0-9]{12,}|(?:AKIA|ASIA)[A-Z0-9]{16}|(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]{12,}|eyJ[\w-]+\.eyJ[\w-]+\.[\w-]+)\b")
SIGNED_URL = re.compile(r"""(?i)https?://[^\s<>"']*[?&](?:x-(?:oss|amz)-(?:credential|signature)|signature|ossaccesskeyid)=[^\s<>"']*""")
PRIVATE_IP = re.compile(r"(?<![\w.])(?:(?:10|127)\.(?:\d{1,3}\.){2}|(?:192\.168|172\.(?:1[6-9]|2\d|3[01])|169\.254)\.\d{1,3}\.)\d{1,3}(?![\w.])")
INTERNAL_HOST = re.compile(r"(?i)(?<=://)(?:[a-z0-9-]+\.)+(?:internal|local|svc\.cluster\.local)(?=[:/\s]|$)|(?<=://)(?:[a-z0-9-]+\.)*oss-[a-z0-9-]+-internal\.aliyuncs\.com\b")


def redact(value):
    if not isinstance(value, str):
        raise ValueError("Expected a text field")
    value = PEM.sub("[redacted]", value)
    value = SIGNED_URL.sub("[redacted URL]", value)
    value = re.sub(r"(?im)(\b(?:Authorization\s*[:=]\s*(?:(?:Bearer|Basic)\s+)?|Bearer\s+))[^\s,;\"']+",
                   r"\1[redacted]", value)
    value = re.sub(r"(?im)^([ \t]*(?:Set-Cookie|Cookie)\s*:)[^\r\n]*", r"\1 [redacted]", value)
    value = re.sub(r"(?i)(\b[\w+.-]+://)[^/\s@]+:[^/\s@]+@", r"\1[redacted]@", value)
    def mask(m):
        delimiter = '"' if m["double"] is not None else "'" if m["single"] is not None else ""
        return m["prefix"] + delimiter + "[redacted]" + delimiter
    value = SECRET_VALUE.sub(mask, value)
    return PRIVATE_IP.sub("[private address]", INTERNAL_HOST.sub("[private host]", TOKEN.sub("[redacted]", value)))


def display(value):
    return escape(redact(value))


BUILD_TYPES = {
    "compile": "Doris_Compile", "p0": "Doris_P0", "cloud_p0": "Doris_CloudP0", "vault_p0": "Doris_VaultP0",
    "feut": "Doris_FEUT", "beut": "Doris_BEUT", "cloudut": "Doris_CloudUT", "external": "Doris_External",
    "nonconcurrent": "Doris_NonConcurrent", "performance": "Doris_Performance",
}
TC_OUTPUT = re.compile(r"^\[[\d:]+\]([ EiW]):\s*\[Step (\d+)/\d+\] (.*)$")


def read_teamcity(args):
    if not args.teamcity:
        return {}
    build = json.loads(args.teamcity.read_text(encoding="utf-8"))
    if (not isinstance(build, dict) or type(build.get("id")) is not int or build["id"] != args.build_id
            or build.get("buildTypeId") != args.build_type_id or build.get("state") != "finished"):
        raise ValueError("Expected a terminal TeamCity Build for this build ID and pipeline")
    status = "canceled" if build.get("canceledInfo") is not None else str(build.get("status", "")).lower()
    if status.replace("cancelled", "canceled") != args.status:
        raise ValueError("TeamCity result does not match --status")
    for field, singular in (("testOccurrences", "testOccurrence"), ("problemOccurrences", "problemOccurrence")):
        collection = build.get(field, {})
        records = collection.get(singular, [])
        if (collection.get("nextHref") or collection.get("count", len(records)) != len(records)):
            raise ValueError(f"Supply all pages of {field} before rendering")
        if any("build" in row and str(row["build"].get("id")) != str(args.build_id) for row in records):
            raise ValueError(f"{field} contains a record from another build")
    return build


def log_lines(stream):
    """Skip pathological lines whole so partial secret values never pass through."""
    while raw := stream.readline(65537):
        if len(raw) > 65536:
            while raw and not raw.endswith("\n"):
                raw = stream.readline(65537)
            yield "[oversized line omitted]\n"
        else:
            yield raw


def log_sections(path):
    """Show TeamCity's own step labels/errors and the last runtime lines of failed steps."""
    stages, errors, tails = {}, {}, {}
    with path.open(encoding="utf-8-sig", errors="replace") as stream:
        if not stream.readline(128).startswith("Build '"):
            stream.seek(0)
            excerpt = stream.read(262145)
            if len(excerpt) > 262144:
                excerpt = excerpt[:262144].rpartition("\n")[0] + "\n[excerpt truncated]"
            return stages, [("Failure log", excerpt)]
        stream.seek(0)
        for number, raw in enumerate(log_lines(stream), 1):
            line = raw.rstrip("\r\n")
            header = TC_STAGE.match(line)
            if header:
                step, _, label, duration = header.groups()
                stages[step] = [label, "Not recorded", duration]
                continue
            match = TC_OUTPUT.match(line)
            if not match or match[2] not in stages:
                continue
            level, step, message = match.groups()
            if level == "i":  # TeamCity can log the generated shell script as an information block.
                continue
            tail = tails.setdefault(step, deque(maxlen=16))
            tail.append((number, line))
            if level == "E":
                first_errors = errors.setdefault(step, {})
                if len(first_errors) < 100:
                    first_errors[number] = line
            exit_code = re.match(r"Process exited with code (-?\d+)\b", message)
            if exit_code:
                stages[step][1] = "Passed" if exit_code[1] == "0" else "Failed"
            elif re.match(r"Build step .+ is skipped\b", message):
                stages[step][1] = "Skipped"
            elif level == "E" and re.search(r"\bfailed\s*$", message):
                stages[step][1] = "Failed"
    excerpts = []
    for step, (label, state, _) in stages.items():
        if state == "Failed":
            lines = dict(tails.get(step, ()))
            lines.update(errors.get(step, {}))
            if lines:
                ordered = sorted(lines)
                excerpts.append((f"{label} · build log L{ordered[0]:,}–L{ordered[-1]:,}",
                                 "\n".join(lines[n] for n in ordered)))
    return stages, excerpts


def junit_results(path, counts):
    with path.open("rb") as stream:
        raw = stream.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024 or b"<!DOCTYPE" in raw.upper() or b"\0" in raw:
        raise ValueError("Expected a UTF-8 JUnit file up to 16 MiB without a DTD")
    root = ET.fromstring(raw.decode("utf-8-sig"))
    for node in root.iter():
        node.tag = node.tag.rsplit("}", 1)[-1]
    if root.tag not in ("testsuites", "testsuite"):
        raise ValueError("Expected a JUnit testsuite or testsuites")
    # Reconcile nested suite totals with visible cases. Some producers retain
    # only failing rows or omit case details for a whole shard.
    totals = {}
    for suite in reversed(list(root.iter())):
        if suite.tag not in ("testsuites", "testsuite"):
            continue
        result = Counter()
        for child in suite:
            if child in totals:
                result.update(totals.pop(child))
            elif child.tag == "testcase":
                failed = child.find("failure") is not None or child.find("error") is not None
                state = "failed" if failed else ("ignored" if child.find("skipped") is not None else "passed")
                result[state] += 1
                result["total"] += 1
        if "tests" in suite.attrib:
            total = count(suite.get("tests"))
            failed = (count(suite.get("failures")) + count(suite.get("errors", "0"))
                      if "failures" in suite.attrib else result["failed"])
            skipped = count(suite.get("skipped", str(result["ignored"])))
            if (total < result["total"] or failed < result["failed"] or skipped < result["ignored"]
                    or total - failed - skipped < result["passed"]):
                raise ValueError("Inconsistent JUnit counters")
            remaining = total - failed - skipped - result["passed"]
            # Without aggregate failure/skip counts, omitted cases have no known outcome.
            if "failures" in suite.attrib and ("skipped" in suite.attrib or result["total"] == total):
                result["passed"] += remaining
                remaining = 0
            result["total"], result["failed"], result["ignored"], result["unknown"] = total, failed, skipped, remaining
        totals[suite] = result
    counts.update(totals[root])
    for case in root.iter("testcase"):
        failures = [child for child in case if child.tag in ("failure", "error")]
        if not failures:
            continue
        name = case.get("name", "")
        fields = [value for failure in failures for value in
                  (failure.get("type"), failure.get("message"), "".join(failure.itertext()))]
        yield name, case.get("classname", ""), "\n".join(v for v in fields if v is not None), False


DOWNLOAD_NAMES = {"build": "Build log", "cluster": "Doris logs", "core": "Coredump"}
# A broker URL is a scoped grant for one artifact. Never accept an arbitrary
# URL from a build, or copy the redirect destination's cloud credentials.
BROKER_PATH = re.compile(r"/_ci/diagnostics/d/[0-9a-f]{32}")
CORE_NAME = re.compile(r"(?:core(?:[._-](?:\d+|doris[_-](?:be|fe)|be|gz|zst|lz4|tar|dump|pair)(?:[._-][\w.-]+)*)?|"
                       r"(?:[\w.-]+[._-])?coredump(?:[._-][\w.-]+)?|[\w.-]+\.core(?:\.(?:gz|zst|lz4))?)", re.I)
CORE_EXCLUDE = re.compile(r"(?:binary|executable|symbols?|backtrace|stacktrace|\.profraw|\.json|\.txt|\.log|\.sha256)", re.I)
FRAME = re.compile(r"^\s*(?:#\s*\d+\s|(?:\*\s*)?frame #\d+:|Program terminated with signal\b|"
                   r"Thread \d+ .* received signal\b|\[Current thread is |\* thread #\d+)")
# Static OSS pages must continue to reflect expiry without changing their bytes on a publisher retry.
EXPIRY_JS = """function expireLinks() {
  for (const link of document.querySelectorAll('a[data-expires]')) {
    if (Number(link.dataset.expires) <= Date.now() / 1000) {
      link.replaceWith(document.createTextNode(link.textContent + ' (expired)'));
    }
  }
}
expireLinks();
window.addEventListener('pageshow', expireLinks);
document.addEventListener('click', event => {
  const link = event.target.closest('a[data-expires]');
  if (link && Number(link.dataset.expires) <= Date.now() / 1000) {
    event.preventDefault();
    expireLinks();
  }
}, true);"""
EXPIRY_HASH = base64.b64encode(hashlib.sha256(EXPIRY_JS.encode()).digest()).decode()


def is_core(name):
    return not CORE_EXCLUDE.search(name) and CORE_NAME.fullmatch(name) is not None


def download_kinds(obj):
    name, group = obj["name"], obj["group"]
    if not isinstance(name, str) or not re.fullmatch(r"[\w.-]+", name):
        return []
    kinds = []
    if group in ("build-log", "build") and re.fullmatch(r"build[-.]log(?:\.safe-v1)?\.(?:gz|zip)", name):
        kinds.append("build")
    if group in ("cluster", "doris-logs", "ut") and name.endswith((".tar.gz", ".tgz", ".zip", ".tar.zst")):
        kinds.append("cluster")
    if ((group in ("core", "coredump", "core-file", "core-pair", "large-file") and is_core(name))
            or (obj.get("archive") is True and any(is_core(Path(src).name) for src in obj.get("sources", [])
                                                   if isinstance(src, str)))):
        kinds.append("core")
    return kinds


def broker_link(obj, origin):
    if not origin:
        raise ValueError("Set TEAMCITY_CURRENT_URL to the trusted HTTPS origin for diagnostic links")
    url, expires = obj.get("url"), obj.get("expires")
    if not isinstance(url, str) or not url.isascii() or re.search(r"[\s\\#]", url):
        raise ValueError("Invalid diagnostics download URL")
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != urlsplit(origin).netloc or not BROKER_PATH.fullmatch(parsed.path):
        raise ValueError("Unexpected diagnostics download host or path")
    params = dict(parse_qsl(parsed.query, strict_parsing=True))
    if (type(expires) is not int or not 0 < expires < 2**34 or parsed.query.count("&") != 1 or
            set(params) != {"expires", "signature"} or params["expires"] != str(expires) or
            not re.fullmatch(r"[0-9a-fA-F]{64}", params["signature"])):
        raise ValueError("Invalid or inconsistent diagnostics download grant")
    return url, expires


def stack_preview(text):
    if "\x00" in text or text.startswith("\x7fELF"):
        raise ValueError("Supply a text backtrace instead of a binary core")
    frames = []
    for line in text.splitlines():
        if FRAME.match(line):
            frames.append(line[:2048])
            if len(frames) == 80:
                break
    return "\n".join(frames)


def download_section(args, build):
    links = {kind: [] for kind in DOWNLOAD_NAMES}
    expected = args.build_type_id
    core_state = "No core archive supplied"
    if args.artifact_index:
        index = json.loads(args.artifact_index.read_text(encoding="utf-8"))
        if str(index.get("buildId")) != str(args.build_id) or index.get("pipeline") != expected:
            raise ValueError("Artifact receipt must match this build ID and configuration")
        if index.get("state") == "EXPORTED":
            core_state = "No core archive listed"
            for obj in index["objects"]:
                kinds = download_kinds(obj)
                if kinds:
                    url, expires = broker_link(obj, args.teamcity_origin)
                    for kind in kinds:
                        links[kind].append((obj["name"], url, expires))
    for kind, filename in args.local_downloads.items():
        links[kind] = [(f"Download {DOWNLOAD_NAMES[kind].lower()}", filename, None)]
    if build and args.teamcity_origin and not links["build"]:
        links["build"].append(("Download from TeamCity (requires access)",
                               f"{args.teamcity_origin}/downloadBuildLog.html?buildId={args.build_id}&archived=true", None))
    stacks = []
    if args.core_diagnostics:
        receipt = json.loads(args.core_diagnostics.read_text(encoding="utf-8"))
        if (str(receipt.get("build", receipt.get("buildId"))) != str(args.build_id)
                or receipt.get("pipeline", expected) != expected):
            raise ValueError("Core diagnostics must match this build ID and configuration")
        pairs = receipt.get("core_pairs", receipt.get("pairs"))
        if isinstance(pairs, list):
            if not receipt.get("errors"):
                core_state = "Core recorded, no downloadable archive listed" if pairs else "No core recorded"
            else:
                core_state = "Core collection reported errors"
            for pair in pairs:
                trace = pair.get("backtrace") if isinstance(pair, dict) else None
                if isinstance(trace, str) and (stack := stack_preview(trace[:1048576])):
                    stacks.append(stack)
    if args.core_backtrace:
        with args.core_backtrace.open("rb") as stream:
            stack = stack_preview(stream.read(1048576).decode("utf-8-sig", errors="replace"))
        if stack and stack not in stacks:
            stacks.append(stack)
        if stack and core_state == "No core recorded" and not links["core"]:
            core_state = "Backtrace supplied, no downloadable archive listed"
    items = []
    for kind, name in DOWNLOAD_NAMES.items():
        anchors = []
        for label, url, expires in links[kind]:
            expiration, attr = "", ""
            if expires:
                dt = datetime.fromtimestamp(expires, timezone.utc)
                expiration = f' <small>Valid until {dt:%Y-%m-%d %H:%M UTC}</small>'
                attr = f' data-expires="{expires}"'
            anchors.append(f'<a{attr} href="{escape(url, quote=True)}" rel="noreferrer">{escape(label)}</a>{expiration}')
        unavailable = core_state if kind == "core" else "No archive supplied"
        items.append(f'<div><dt>{name}</dt><dd>{"<br>".join(anchors) or unavailable}</dd></div>')
    section = '<h2>Downloads</h2><dl>' + "".join(items) + '</dl>'
    if stacks:
        section += '<h2>Core backtrace</h2>' + "".join(f'<pre><code>{display(stack)}</code></pre>' for stack in stacks)
    elif links["core"] or args.core_backtrace or core_state.startswith("Core recorded"):
        section += '<p>No debugger backtrace available for this build.</p>'
    if any(expires for entries in links.values() for _, _, expires in entries):
        section += f'<script type="text/javascript">{EXPIRY_JS}</script>'
    return section


def report(args):
    build = read_teamcity(args)
    tests = build.get("testOccurrences", {})
    counters = tests.get("testCounters", {})
    cases = []
    for row in tests.get("testOccurrence", []):
        if row.get("status") in ("FAILURE", "ERROR"):
            cases.append((row["name"], "", row.get("details", ""), row.get("muted", False)))
    junit = Counter()
    for path in args.junit:
        cases.extend(junit_results(path, junit))
    stages, excerpts = log_sections(args.failure_log) if args.failure_log else ({}, [])
    problems = build.get("problemOccurrences", {}).get("problemOccurrence", [])
    title, result = PIPELINES[args.pipeline], RESULTS[args.status]
    def facts(items):
        return "".join(f"<div><dt>{key}</dt><dd>{display(str(v))}</dd></div>" for key,v in items if v is not None)
    rows = []
    for label, key, override in (("Total", "all", None), ("Passed", "success", args.passed),
                                 ("Failed", "failed", args.failed), ("Muted", "muted", None),
                                 ("Ignored", "ignored", args.ignored)):
        value = counters.get(key, override)
        if value is not None and (type(value) is not int or value < 0 or override not in (None, value)):
            raise ValueError(f"Invalid or conflicting {key} counter")
        rows.append((label, value))
    if args.unmuted_failed is not None:
        rows.append(("Unmuted failures", args.unmuted_failed))
    sections = [download_section(args, build)]
    if any(v is not None for _,v in rows):
        sections.append(f'<h2>Test results</h2><dl>{facts(rows)}</dl>')
    elif args.pipeline != "compile" and not args.junit and not build:
        sections.append('<h2>Test results</h2><p>Case counts were not recorded in this snapshot.</p>')
    if args.junit:
        sections.append(f'<p>Supplied JUnit files: {junit["total"]} test executions · {junit["failed"]} failed · '
                        f'{junit["passed"]} passed · {junit["ignored"]} skipped'
                        + (f' · {junit["unknown"]} without recorded outcome' if junit["unknown"] else '') + '.</p>')
    if problems:
        sections.append('<h2>Build problems</h2>')
        for item in problems:
            sections.append(f'<pre><code>{display(item.get("details", ""))}</code></pre>')
    if cases:
        sections.append('<h2>Failed tests</h2>')
        if args.junit:
            sections.append(f'<p>{len(cases)} of {junit["failed"]} failures in supplied JUnit files have case details.</p>')
        for idx,(name,classname,details,muted) in enumerate(cases):
            badge = ' <small>Muted</small>' if muted else ''
            scope = f'<p>{display(classname)}</p>' if classname else ''
            sections.append(f'<details{" open" if idx == 0 else ""}><summary>{display(name)}{badge}</summary>'
                            f'{scope}<pre><code>{display(details)}</code></pre></details>')
    elif (args.junit and junit["failed"]) or (not args.junit and (counters.get("failed") or args.failed or args.unmuted_failed)):
        sections.append('<p>The supplied results contain failure counts without individual case details.</p>')
    if stages:
        rows_html = "".join(f'<tr><th scope="row">{display(name)}</th><td>{state}</td><td>{elapsed or "—"}</td></tr>'
                            for name,state,elapsed in stages.values())
        sections.append('<h2>Build steps</h2><table><thead><tr><th>Step</th><th>Result</th><th>Duration</th></tr>'
                        f'</thead><tbody>{rows_html}</tbody></table>')
    for label, text in excerpts:
        sections.append(f'<h2>{display(label)}</h2><pre><code>{display(text)}</code></pre>')
    def tc_time(field):
        value = build.get(field)
        return datetime.strptime(value, "%Y%m%dT%H%M%S%z").astimezone(timezone(timedelta(hours=8))) if value else None
    start, finish = args.started_at or tc_time("startDate"), args.finished_at or tc_time("finishDate")
    info = [("Build ID", f"#{args.build_id}"), ("Build number", build.get("number")),
            ("Target branch", args.target_branch), ("PR head SHA", args.sha), ("Tested SHA", args.tested_sha)]
    if args.failed_step and not problems and not stages:
        info.append(("Failed step", STEPS[args.failed_step]))
    for key,value in (("Started (UTC+08:00)", start), ("Finished (UTC+08:00)", finish)):
        if value:
            info.append((key,value.strftime("%Y-%m-%d %H:%M:%S")))
    if start and finish:
        seconds = int((finish - start).total_seconds())
        if seconds < 0:
            raise ValueError("Finish time precedes start time")
        info.append(("Duration", f"{seconds // 60}m {seconds % 60:02d}s"))
    pr = f'<div><dt>Pull request</dt><dd><a href="https://github.com/apache/doris/pull/{args.pr}">#{args.pr}</a></dd></div>' if args.pr else ""
    sections.append(f'<h2>Build details</h2><dl>{facts(info)}{pr}</dl>')
    status_text = f'<pre class="status">{display(build["statusText"])}</pre>' if build.get("statusText") else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'sha256-{EXPIRY_HASH}'; base-uri 'none'; form-action 'none'">
<title>{title} · {result} · #{args.build_id}</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#f6f8f7;color:#162b32;font:1rem/1.5 system-ui,sans-serif}}
main{{max-width:64rem;margin:auto;padding:clamp(1rem,4vw,3rem)}}h1{{margin:.3rem 0;font-size:2.6rem}}
header{{border-bottom:1px solid #bdcfca;padding-bottom:1rem}}h2{{font-size:1.15rem;margin:1.7rem 0 .5rem}}
strong{{color:#913827}}strong.success{{color:#236044}}strong.canceled,small,dt{{color:#536962}}
dl,table,details,pre{{background:#fff;border:1px solid #d2dfdb;border-radius:.4rem}}
dl{{display:grid;grid-template-columns:repeat(auto-fit,minmax(11rem,1fr));gap:1rem;padding:1rem}}
dd{{margin:0;overflow-wrap:anywhere}}pre{{padding:.9rem;white-space:pre-wrap;overflow-wrap:anywhere;font: .86rem/1.55 ui-monospace,monospace}}
details{{padding:.6rem .9rem;margin:.4rem 0}}summary{{cursor:pointer;overflow-wrap:anywhere;white-space:pre-wrap}}details pre,.status{{border:0;margin:.4rem 0 0}}
table{{width:100%;border-collapse:collapse}}td,th{{text-align:left;padding:.5rem;border-bottom:1px solid #d2dfdb}}a{{color:#165b64}}
</style></head><body><main><header><div>Apache Doris · Community CI</div><h1>{title}</h1>
<strong class="{args.status}">{result}</strong> · Build #{args.build_id}{status_text}</header>{"".join(sections)}</main></body></html>
""".encode("utf-8")


def prepare_public_log(source, folder):
    output = folder / "build-log.gz"
    private_key = False
    with source.open(encoding="utf-8-sig") as src, output.open("wb") as dst:
        with gzip.GzipFile(filename="", fileobj=dst, mode="wb", mtime=0) as compressed:
            while line := src.readline(65537):
                if "\x00" in line or len(line) > 65536:
                    raise ValueError("--public-build-log expects UTF-8 text with lines up to 64 Ki characters")
                if re.search(r"-----BEGIN [A-Z ]*PRIVATE KEY-----", line):
                    private_key = True
                    compressed.write(b"[redacted]\n")
                if private_key:
                    if re.search(r"-----END [A-Z ]*PRIVATE KEY-----", line):
                        private_key = False
                    continue
                compressed.write(redact(line).encode("utf-8"))
    digest = hashlib.sha256()
    with output.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1048576), b""):
            digest.update(chunk)
    return output.rename(folder / f"build-log-{digest.hexdigest()}.txt.gz")


def stage_diagnostic(kind, source, folder):
    name, extension = ("coredump", ".core") if kind == "core" else ("doris-logs", ".tar.gz")
    staged = folder / name
    digest = hashlib.sha256()
    with source.open("rb") as src, staged.open("wb") as dst:
        header = src.read(20)
        if kind == "core":
            if (len(header) < 20 or header[:4] != b"\x7fELF" or header[4] not in (1, 2) or
                    header[5] not in (1, 2) or int.from_bytes(header[16:18], "little" if header[5] == 1 else "big") != 4):
                raise ValueError("--public-core expects an ELF core file")
        elif not header.startswith(b"\x1f\x8b\x08"):
            raise ValueError("--public-doris-log expects a gzip-compressed log archive")
        src.seek(0)
        for chunk in iter(lambda: src.read(1048576), b""):
            digest.update(chunk)
            dst.write(chunk)
    return staged.rename(folder / f"{name}-{digest.hexdigest()}{extension}")


def upload(file, key):
    # Always use the agent role and the Hong Kong private endpoint.
    attachment = file.suffix != ".html"
    mime = {".gz": "application/gzip", ".core": "application/octet-stream", ".html": "text/html"}[file.suffix]
    result = subprocess.run([
        "ossutil", "api", "put-object", "--mode", "EcsRamRole", "--region", REGION,
        "--endpoint", f"https://{ENDPOINT}", "--bucket", BUCKET, "--key", key,
        "--body", f"file://{file}", "--content-type", mime if attachment else mime + "; charset=utf-8",
        "--content-disposition", "attachment" if attachment else "inline", "--forbid-overwrite",
    ], capture_output=True, timeout=60)
    if result.returncode:
        if b"FileAlreadyExists" not in result.stderr + result.stdout:
            raise ValueError("OSS upload failed; the same report can be retried")
        url = f"https://{BUCKET}.{ENDPOINT}/{key}"
        with urlopen(url, timeout=15) as existing, file.open("rb") as staged:
            if existing.geturl() != url or existing.status != 200 or existing.headers.get_content_type() != mime:
                raise ValueError("Unexpected object at the existing OSS key")
            while chunk := staged.read(1048576):
                if existing.read(len(chunk)) != chunk:
                    raise ValueError("The OSS key already contains different content")
            if existing.read(1):
                raise ValueError("The OSS key already contains different content")
    print(f"oss://{BUCKET}/{key}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pipeline", choices=PIPELINES)
    parser.add_argument("--build-id", required=True, type=count)
    parser.add_argument("--status", required=True, type=str.lower, choices=RESULTS)
    parser.add_argument("--pr", type=count)
    parser.add_argument("--sha", type=sha, help="PR head")
    parser.add_argument("--tested-sha", type=sha, help="commit actually tested, when known")
    parser.add_argument("--failed-step", choices=STEPS)
    parser.add_argument("--target-branch", type=target_branch)
    parser.add_argument("--started-at", type=time_with_offset)
    parser.add_argument("--finished-at", type=time_with_offset)
    for name in ("passed", "failed", "unmuted-failed", "ignored"):
        parser.add_argument(f"--{name}", type=count)
    parser.add_argument("--public-build-log", type=Path, help="trusted, publication-ready full build log (UTF-8 text), stored beside the report as gzip")
    parser.add_argument("--public-doris-log", type=Path, help="publication-ready Doris log archive (.tar.gz) from this build")
    parser.add_argument("--public-core", type=Path, help="publication-approved raw ELF core from this build")
    parser.add_argument("--artifact-index", type=Path, help="this build's native oss-artifacts.json download receipt")
    parser.add_argument("--core-diagnostics", type=Path, help="this build's native core-diagnostics.json or core-pairs.json")
    parser.add_argument("--core-backtrace", type=Path, help="text output of a debugger backtrace for this build")
    parser.add_argument("--failure-log", type=Path, help="plain failure excerpt or full TeamCity plain-text build log")
    parser.add_argument("--junit", type=Path, action="append", default=[], help="local JUnit XML (repeat for separate shards)")
    parser.add_argument("--public-source", type=Path, help=argparse.SUPPRESS)  # compatibility with existing callers
    parser.add_argument("--build-type-id", help="exact expected TeamCity configuration ID for a pipeline copy")
    parser.add_argument("--teamcity", "--diagnostics", type=Path, help="TeamCity REST Build JSON including testOccurrences and problemOccurrences")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--upload", action="store_true", help="also publish to the fixed OSS agent-test prefix")
    args = parser.parse_args(argv)
    args.build_type_id = args.build_type_id or BUILD_TYPES[args.pipeline]
    args.teamcity_origin = os.environ.get("TEAMCITY_CURRENT_URL", "").rstrip("/")
    if args.teamcity_origin and not re.fullmatch(r"https://[A-Za-z0-9.-]+(?::[0-9]+)?", args.teamcity_origin):
        parser.error("TEAMCITY_CURRENT_URL must be a trusted HTTPS origin")
    if args.teamcity and args.junit:
        parser.error("Choose TeamCity or JUnit as the source of test results")
    if len({p.resolve() for p in args.junit}) != len(args.junit):
        parser.error("Each JUnit input must be supplied only once")
    if not args.build_id or args.pr == 0:
        parser.error("Build and PR IDs must be positive")
    if args.failed_step and args.status not in ("failure", "error"):
        parser.error("--failed-step requires a failed build")
    key = f"builds/agent-test/{args.build_id}/index.html"
    output = args.output_dir / key
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=output.parent) as staging:
            folder = Path(staging).resolve()
            files = []
            args.local_downloads = {}
            for kind, source in (("build", args.public_build_log), ("cluster", args.public_doris_log), ("core", args.public_core)):
                if source:
                    attachment = prepare_public_log(source, folder) if kind == "build" else stage_diagnostic(kind, source, folder)
                    args.local_downloads[kind] = attachment.name
                    files.append(attachment)
            staged = folder / "index.html"
            staged.write_bytes(report(args))
            for file in [*files, staged]:
                if args.upload:
                    upload(file, str(Path(key).with_name(file.name)))
                os.replace(file, output.with_name(file.name))
        print(output)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError, argparse.ArgumentTypeError, ET.ParseError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
