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

"""Generate the Lance time-travel regression fixture.

time_travel.lance is a root table of the preinstalled Directory catalog next to
all_types.lance. Every other fixture is compacted to a single version, so this is the one
dataset whose history survives: three commits, none of them cleaned up.

    version 1  create  row_id 1..3   tag column "v1"   Lance tag v1
    version 2  append  row_id 4..6   tag column "v2"   Lance tag v2
    version 3  append  row_id 7..9   tag column "v3"   Lance tag v3

Each version also carries a Lance tag of the same name (stored under _refs/tags/), which the
suites select with tbl@tag(v2). A branch "dev" (stored under tree/dev/, metadata in
_refs/branches/) is forked from version 2 and carries one extra commit:

    dev version 3  append  row_id 100  tag column "dev"

so the branch's version 3 has rows 1..6 and 100, while main's version 3 has rows 1..9; the
suites select it with tbl@branch(dev). The main chain is not touched by the branch. A tag "rel"
points at the branch's version 3, so tbl@tag(rel) must read the branch, not main's version 3.

A Lance branch is a shallow clone: its manifests record the parent's location as an absolute
URI (Manifest.base_paths), so a branch only reads where it was created. The committed
branch was therefore created against the fixture's final location, s3://warehouse/lance/
time_travel.lance, and synced back into this directory; build() writes the main chain and
the tags locally, and --create-branch <uri> creates the branch at the uploaded location:

  python3 lance_build_time_travel.py --create-branch s3://warehouse/lance/time_travel.lance \
      --storage-option endpoint=http://127.0.0.1:19000 --storage-option access_key_id=admin \
      --storage-option secret_access_key=password --storage-option region=us-east-1 \
      --storage-option allow_http=true
  # then sync tree/ and _refs/branches/ from that location into preinstalled_data/lance/time_travel.lance/

check() verifies the branch files and that they point at that URI without opening the branch,
which is not possible offline.

The commits are spaced apart so that FOR TIME AS OF can land between two of them. Lance
stores the commit time of each version in its manifest, and that time is whatever wall
clock this script ran at. Doris has no SQL to read those times back, so the regression
suites hard-code them (test_lance_time_travel and test_lance_rest_time_travel, together
with their .out files). Regenerating this dataset therefore means updating those suites
from the times this script prints; that is why lance_build_preinstalled_catalog.py carries
the committed directory over as-is, like all_types.lance, instead of rebuilding it, and only
runs check() on it.

The same directory serves three catalogs in the regression suites: the filesystem catalog,
the REST catalog with storage-native versions, and the REST catalog with namespace-managed
versions, where lance_rest_server.py answers the version endpoints from a static list
matching the versions written here.

Run it with the writer pinned in lance_fixture_requirements.txt; the main script's
check_pinned_writer() enforces the pin for the whole catalog, this script alone does not.

Usage:
  python3 lance_build_time_travel.py preinstalled_data/lance/time_travel.lance
  python3 lance_build_time_travel.py --check preinstalled_data/lance/time_travel.lance
"""
import argparse
import shutil
import time
from pathlib import Path

import lance
import pyarrow as pa

COMMITS = (("create", 1, 3, "v1"), ("append", 4, 6, "v2"), ("append", 7, 9, "v3"))
BRANCH = "dev"
BRANCH_ROW_ID = 100
BRANCH_TAG = "rel"
BRANCH_PARENT_URI = "s3://warehouse/lance/time_travel.lance"
COMMIT_GAP_SECONDS = 1.5


def rows_of(low: int, high: int, tag: str) -> pa.Table:
    return pa.table({
        "row_id": pa.array(range(low, high + 1), pa.int32()),
        "tag": pa.array([tag] * (high - low + 1), pa.string()),
    })


def build(output: Path) -> None:
    if output.exists():
        shutil.rmtree(output)
    for index, (mode, low, high, tag) in enumerate(COMMITS):
        if index > 0:
            time.sleep(COMMIT_GAP_SECONDS)
        # Match all_types.lance (data storage version 2.2) so every committed Lance data file
        # shares one on-disk format with the rest of the fixture.
        lance.write_dataset(rows_of(low, high, tag), str(output), mode=mode,
                            data_storage_version="2.2")
    dataset = lance.dataset(str(output))
    for version, (_, _, _, tag) in zip((1, 2, 3), COMMITS):
        dataset.tags.create(tag, version)
    print(f"main chain and tags written; create the branch with --create-branch {BRANCH_PARENT_URI}")


def create_branch(uri: str, storage_options: dict) -> None:
    """Forks the branch at the dataset's final location and appends its extra row there."""
    dataset = lance.dataset(uri, storage_options=storage_options)
    assert [v["version"] for v in dataset.versions()] == [1, 2, 3], "upload the main chain first"
    dataset.create_branch(BRANCH, 2)
    lance.write_dataset(rows_of(BRANCH_ROW_ID, BRANCH_ROW_ID, BRANCH), f"{uri}/tree/{BRANCH}",
                        mode="append", data_storage_version="2.2", storage_options=storage_options)
    branch = lance.dataset(f"{uri}/tree/{BRANCH}", storage_options=storage_options)
    assert branch.version == 3 and sorted(branch.to_table()["row_id"].to_pylist()) == [1, 2, 3, 4, 5, 6, BRANCH_ROW_ID]
    dataset.tags.create(BRANCH_TAG, (BRANCH, 3))
    print(f"branch {BRANCH} and tag {BRANCH_TAG} created at {uri}; sync tree/, _refs/branches/ and _refs/tags/ back")


def check(output: Path) -> None:
    dataset = lance.dataset(str(output))
    versions = dataset.versions()
    assert [v["version"] for v in versions] == [1, 2, 3], (
        f"time-travel fixture must keep exactly versions 1..3: {versions}")
    timestamps = [v["timestamp"] for v in versions]
    assert timestamps == sorted(timestamps) and len(set(timestamps)) == 3, (
        f"time-travel fixture commit times must be distinct and increasing: {timestamps}")
    assert all((b - a).total_seconds() >= 1 for a, b in zip(timestamps, timestamps[1:])), (
        f"time-travel fixture commits must be at least one second apart: {timestamps}")
    for version, (_, _, high, tag) in zip((1, 2, 3), COMMITS):
        table = dataset.checkout_version(version).to_table().sort_by("row_id")
        assert table["row_id"].to_pylist() == list(range(1, high + 1)), (
            f"version {version} rows differ from expected: {table}")
        assert table["tag"].to_pylist()[-1] == tag, f"version {version} tag differs: {table}"
    tags = {name: (ref["branch"], ref["version"]) for name, ref in dataset.tags.list().items()}
    assert tags == {"v1": (None, 1), "v2": (None, 2), "v3": (None, 3), BRANCH_TAG: (BRANCH, 3)}, (
        f"time-travel fixture tags differ: {tags}")
    assert list(dataset.branches.list()) == [BRANCH], f"time-travel fixture branches differ: {dataset.branches.list()}"
    branch_manifests = sorted((output / "tree" / BRANCH / "_versions").glob("*.manifest"))
    assert len(branch_manifests) == 2, f"branch {BRANCH} must carry versions 2 and 3: {branch_manifests}"
    for manifest in branch_manifests:
        assert BRANCH_PARENT_URI.encode() in manifest.read_bytes(), (
            f"{manifest} must reference the parent at {BRANCH_PARENT_URI}; a branch created elsewhere is unreadable there")
    for version in versions:
        print(f"time_travel.lance version {version['version']} committed at "
              f"{version['timestamp'].isoformat()}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("output", type=Path, nargs="?",
                        help="path of time_travel.lance (not used with --create-branch)")
    parser.add_argument("--check", action="store_true", help="verify the existing fixture")
    parser.add_argument("--create-branch", metavar="URI",
                        help="create the dev branch at the uploaded dataset URI instead of building")
    parser.add_argument("--storage-option", action="append", default=[], metavar="KEY=VALUE",
                        help="Lance storage option for --create-branch (repeatable)")
    args = parser.parse_args()
    if not args.create_branch and args.output is None:
        parser.error("the output path is required unless --create-branch is given")
    if args.create_branch:
        create_branch(args.create_branch, dict(item.split("=", 1) for item in args.storage_option))
    elif args.check:
        check(args.output)
    else:
        build(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
