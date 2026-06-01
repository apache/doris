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

"""Extract Doris profile operator/counter names from the current source tree."""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path


PROFILE_CONST_RE = re.compile(r'inline constexpr char\s+([A-Z0-9_]+)\[\]\s*=\s*"([^"]+)"')
MACRO_RE = re.compile(
    r"\b(ADD_(?:CHILD_)?(?:TIMER|COUNTER)(?:_WITH_LEVEL)?|add_nonzero_counter|add_info_string|add_derived_counter)\s*\("
)
STRING_RE = re.compile(r'"([^"]+)"')
NAME_ASSIGN_RE = re.compile(r'\b(?:_name|_op_name)\s*=\s*"([^"]+)"')
CLASS_RE = re.compile(r"\bclass\s+([A-Za-z0-9_]*OperatorX|[A-Za-z0-9_]*LocalState)\b")
FACTORY_RE = re.compile(r"std::make_shared<([A-Za-z0-9_]+OperatorX)>")
CASE_RE = re.compile(r"case\s+(T(?:PlanNode|DataSink)Type::[A-Z0-9_]+)")


def rel(path: Path, root: Path) -> str:
    return str(path.relative_to(root))


def load_profile_constants(root: Path) -> dict[str, str]:
    header = root / "be/src/runtime/runtime_profile_counter_names.h"
    constants = {}
    for match in PROFILE_CONST_RE.finditer(header.read_text(encoding="utf-8")):
        constants[f"profile::{match.group(1)}"] = match.group(2)
    return constants


def extract_macro_args(line: str, start: int) -> str:
    depth = 0
    out = []
    for char in line[start:]:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                out.append(char)
                break
        out.append(char)
    return "".join(out)


def counter_name_from_args(args: str, constants: dict[str, str]) -> str | None:
    strings = STRING_RE.findall(args)
    if strings:
        # For ADD_CHILD_TIMER(profile, child, parent), the first string is the counter name.
        return strings[0]
    for const_name, value in constants.items():
        if const_name in args:
            return value
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="/mnt/disk3/zhaochangle/doris")
    parser.add_argument("--output", default="/tmp/doris-profile-reader/source-profile-inventory.md")
    args = parser.parse_args()

    root = Path(args.repo)
    constants = load_profile_constants(root)
    source_roots = [
        root / "be/src/exec/operator",
        root / "be/src/exec/pipeline",
        root / "be/src/exec/scan",
        root / "be/src/exec/exchange",
        root / "be/src/exec/sink",
        root / "be/src/storage",
        root / "be/src/runtime",
        root / "fe/fe-core/src/main/java/org/apache/doris/common/profile",
        root / "fe/fe-core/src/main/java/org/apache/doris/nereids/processor/post",
        root / "fe/fe-core/src/main/java/org/apache/doris/planner",
    ]
    files = []
    for source_root in source_roots:
        if source_root.exists():
            files.extend(p for p in source_root.rglob("*") if p.suffix in {".cpp", ".h", ".java"})

    counters: dict[str, list[str]] = defaultdict(list)
    names: dict[str, list[str]] = defaultdict(list)
    classes: dict[str, list[str]] = defaultdict(list)
    factories: dict[str, list[str]] = defaultdict(list)
    cases: dict[str, list[str]] = defaultdict(list)

    for path in sorted(files):
        text = path.read_text(encoding="utf-8", errors="ignore")
        relative = rel(path, root)
        for line_no, line in enumerate(text.splitlines(), 1):
            for match in MACRO_RE.finditer(line):
                args_text = extract_macro_args(line, match.end() - 1)
                counter_name = counter_name_from_args(args_text, constants)
                if counter_name:
                    counters[counter_name].append(f"{relative}:{line_no}")
            for match in NAME_ASSIGN_RE.finditer(line):
                names[match.group(1)].append(f"{relative}:{line_no}")
            for match in CLASS_RE.finditer(line):
                classes[match.group(1)].append(f"{relative}:{line_no}")
            for match in FACTORY_RE.finditer(line):
                factories[match.group(1)].append(f"{relative}:{line_no}")
            for match in CASE_RE.finditer(line):
                cases[match.group(1)].append(f"{relative}:{line_no}")

    lines = ["# Doris Source Profile Inventory", ""]
    lines.append("## Factory Cases")
    for key in sorted(cases):
        lines.append(f"- `{key}`: {', '.join(cases[key][:6])}")
    lines.append("")

    lines.append("## Operator Name Assignments")
    for key in sorted(names):
        lines.append(f"- `{key}`: {', '.join(names[key][:6])}")
    lines.append("")

    lines.append("## Operator Classes In Factory")
    for key in sorted(factories):
        lines.append(f"- `{key}`: {', '.join(factories[key][:6])}")
    lines.append("")

    lines.append("## Counter And Info Names")
    for key in sorted(counters):
        refs = counters[key]
        lines.append(f"- `{key}` ({len(refs)} registrations): {', '.join(refs[:8])}")
    lines.append("")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
