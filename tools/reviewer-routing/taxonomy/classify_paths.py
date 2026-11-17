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
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPEC = json.loads((HERE / "module-spec.json").read_text())
MODULES = sorted(SPEC["modules"], key=lambda m: -m["priority"])
ALIASES = json.loads((HERE / "historical-path-aliases.json").read_text())["aliases"]

# These suite roots express a subsystem. General query, data type and correctness
# roots are intentionally absent because their contents span many specialties.
SUITE_HINTS = [
    (r"^(?:inverted_index|search|mysql_fulltext)", "search"),
    (r"^ann_index", "ann"),
    (r"^(?:variant|jsonb|json)_", "variant"),
    (r"^(?:mtmv|mv|rollup)(?:_|$)", "mtmv"),
    (r"^(?:statistics|stats_up_down)", "statistics"),
    (r"^spill_", "execution.spill"),
    (r"^(?:pipeline|nereids_broadcast_shuffle_join)_", "execution"),
    (r"^workload_manager_", "runtime.workload"),
    (r"^(?:load|insert|large_txn_load|table_stream)(?:_|$)", "load"),
    (r"^compaction$|^segcompaction_", "storage.compaction"),
    (r"^(?:schema_change|alter|rollup)(?:_|$)", "schema-change"),
    (r"^unique_with_mow|^unique_seq_map", "storage.mow"),
    (r"^(?:clone|tablet_scheduler)_", "replication"),
    (r"^(?:backup_restore|restore)(?:_|$)", "backup"),
    (r"^(?:cloud|vault|s3_vault|hdfs_vault|inject_hdfs_vault)(?:_|$)", "cloud"),
    (r"^(?:auth|account|ranger|mysql_ssl|http_auth|sql_block_rule)(?:_|$)", "auth"),
    (r"^(?:javaudf|pythonudf|pythonudaf|pythonudtf|pythonudf_complex)", "udf"),
    (r"^nereids_", "optimizer"),
    (r"^(?:arrow_flight_sql|prepared_stmt|coordinator)_", "query"),
    (r"^(?:external_table|connector|trino|flink_connector)(?:_|$)", "connector"),
    (r"^(?:function|bitmap_functions|cast)(?:_|$)", "functions"),
    (r"^(?:http_rest_api|http|information_schema|metrics)(?:_|$)", "api"),
    (r"^(?:node|manager)(?:_|$)", "cluster"),
    (r"^(?:partition|table|database|ddl|catalog_recycle_bin)(?:_|$)", "metadata"),
]


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
    normalized = ALIASES.get(normalized, normalized)
    return normalized, kind


def classify(path):
    normalized, kind = normalized_path(path)
    if kind == "regression_test":
        root = normalized.split("/")[2]
        for pattern, module_id in SUITE_HINTS:
            if re.search(pattern, root):
                # A directory named external_table_p0 is insufficient to select
                # a particular connector, but a nested iceberg/ directory is.
                if module_id == "connector":
                    for connector in ("iceberg", "paimon", "hive", "jdbc", "trino", "maxcompute", "fluss", "es", "mongodb"):
                        if re.search(r"(?:^|/|_)" + connector + r"(?:/|_|\.)", normalized):
                            module_id += "." + connector
                            break
                return dict(path=path, normalized_path=normalized, kind=kind,
                            module=module_id, matched_pattern="suite:" + pattern)
        return dict(path=path, normalized_path=normalized, kind=kind,
                    module="test.other", matched_pattern=None)
    if path.startswith("cloud/test/"):
        return dict(path=path, normalized_path=normalized, kind=kind,
                    module="cloud", matched_pattern="cloud unit-test fallback")
    for module in MODULES:
        for pattern in module["patterns"]:
            if re.search(pattern, normalized):
                return dict(path=path, normalized_path=normalized, kind=kind,
                            module=module["id"], matched_pattern=pattern)
    raise AssertionError("The 'other' module must match every nonempty path")


def classify_pr(paths):
    """Return routing and research evidence; tests only supplement source paths."""
    evidence = [classify(p) for p in paths]
    primary = {e["module"] for e in evidence if e["kind"] == "production"}
    secondary = {e["module"] for e in evidence if e["kind"] != "production"}
    # Unit tests can provide specific module evidence when production files only
    # touch a broad common/fallback area. The caller can inspect this separately.
    return dict(primary_modules=sorted(primary), test_modules=sorted(secondary),
                routing_modules=sorted(primary if primary else secondary), evidence=evidence)


if __name__ == "__main__":
    paths = sys.argv[1:] or [p.strip() for p in sys.stdin if p.strip()]
    print(json.dumps(classify_pr(paths), ensure_ascii=False, indent=2))
