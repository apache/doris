#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Classify PR paths while preserving production, unit-test and suite evidence."""
import re


def normalized_path(path):
    normalized = path[2:] if path.startswith("./") else path
    kind = "production"
    if normalized.startswith("be/test/"):
        kind = "unit_test"
        normalized = normalized.replace("be/test/", "be/src/", 1)
        normalized = re.sub(r"^be/src/(?:expected_result|data)/", "be/src/", normalized)
        normalized = re.sub(r"_test(?=\.[^/]+$)", "", normalized)
    elif normalized.startswith("cloud/test/"):
        # Cloud tests are frequently flat and cannot be safely mapped to the
        # production tree. Keep cloud as a broad unit-test hint instead.
        return normalized, "unit_test"
    elif "/src/test/" in normalized:
        kind = "unit_test"
        normalized = normalized.replace("/src/test/", "/src/main/", 1)
        normalized = re.sub(r"Test(?=\.java$)", "", normalized)
    elif normalized.startswith(("regression-test/suites/", "regression-test/data/")):
        kind = "regression_test"
    elif normalized.startswith(("be/benchmark/", "regression-test/realdata/")):
        kind = "benchmark"
    return normalized, kind


def classify(path, config):
    normalized, kind = normalized_path(path)
    if kind in {"regression_test", "unit_test"}:
        for route in config["test_routes"]:
            if re.search(route["pattern"], normalized):
                return dict(path=path, normalized_path=normalized, kind=kind,
                            module=route["module"], matched_pattern=route["pattern"])
    if kind == "regression_test":
        return dict(path=path, normalized_path=normalized, kind=kind,
                    module="test.other", matched_pattern=None)
    for mid, module in sorted(config["modules"].items(), key=lambda item: -item[1]["priority"]):
        for pattern in module["patterns"]:
            if re.search(pattern, normalized):
                return dict(path=path, normalized_path=normalized, kind=kind,
                            module=mid, matched_pattern=pattern)
    return dict(path=path, normalized_path=normalized, kind=kind, module="other", matched_pattern=None)


def classify_pr(paths, config):
    """Classify affected modules; tests supplement source paths."""
    evidence = [classify(p, config) for p in paths]
    primary = {e["module"] for e in evidence if e["kind"] == "production"}
    secondary = {e["module"] for e in evidence if e["kind"] != "production"}
    # Unit tests can provide specific module evidence when production files only
    # touch a broad common/fallback area. The caller can inspect this separately.
    return dict(primary_modules=sorted(primary), test_modules=sorted(secondary),
                routing_modules=sorted(primary if primary else secondary), evidence=evidence)
