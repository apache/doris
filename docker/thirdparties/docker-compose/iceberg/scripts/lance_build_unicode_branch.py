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

"""Generate the Lance regression fixture whose branch name is newer than the JDK's Unicode.

unicode_branch.lance is a root table of the preinstalled Directory catalog next to
time_travel.lance. Its branch is named "dev\\u1c89": U+1C89 was added in Unicode 16, so Lance's
Rust validation accepts it while JDK 17, on Unicode 13, leaves it unassigned. The REST server
serves the table as unicode_branch_managed, so Doris opens the branch by the URI it joins.

    version 1          create  row_id 1..8      fragment 0
    dev\\u1c89 version 2  append  row_id 201..204  on the branch only

vec = [row_id, row_id + 1, row_id + 2, row_id + 3] as float32, body = "doc <row_id>".

A Lance branch is a shallow clone that records its parent's absolute URI, so the branch is
created against the fixture's final location (see lance_build_search_snapshot.py):

  python3 lance_build_unicode_branch.py preinstalled_data/lance
  # upload unicode_branch.lance to s3://warehouse/lance/unicode_branch.lance
  python3 lance_build_unicode_branch.py --create-branch s3://warehouse/lance/unicode_branch.lance \\
      --storage-option endpoint=http://127.0.0.1:19000 --storage-option access_key_id=admin \\
      --storage-option secret_access_key=password --storage-option region=us-east-1 \\
      --storage-option allow_http=true
  # then sync tree/ and _refs/branches/ from that location back
  python3 lance_build_unicode_branch.py --check preinstalled_data/lance

Build it with pylance 12.0.0, the Lance the FE uses, not the writer pinned in
lance_fixture_requirements.txt: Lance 7 stores a non-ASCII branch directory percent-encoded
(tree/dev%E1%B2%89/), which Lance 12 does not find, while Lance 12 stores it as tree/dev\u1c89/.
The catalog build carries the fixture over as-is. --check runs with any reader.
"""
import argparse
import shutil
from pathlib import Path

import lance
import pyarrow as pa

UNICODE_BRANCH_DIR = "unicode_branch.lance"
BRANCH = "devᲉ"
PARENT_URI = "s3://warehouse/lance/unicode_branch.lance"
WRITER_VERSION = "12.0.0"
DIMENSION = 4


def rows_of(low: int, high: int) -> pa.Table:
    ids = list(range(low, high + 1))
    return pa.table({
        "row_id": pa.array(ids, pa.int32()),
        "vec": pa.array([[float(i + j) for j in range(DIMENSION)] for i in ids],
                        pa.list_(pa.float32(), DIMENSION)),
        "body": pa.array([f"doc {i}" for i in ids], pa.string()),
    })


def require_writer() -> None:
    if lance.__version__ != WRITER_VERSION:
        raise SystemExit(f"build {UNICODE_BRANCH_DIR} with pylance {WRITER_VERSION}, not {lance.__version__}")


def build(output: Path) -> None:
    require_writer()
    if output.exists():
        shutil.rmtree(output)
    lance.write_dataset(rows_of(1, 8), str(output), mode="create", data_storage_version="2.2")
    print(f"{UNICODE_BRANCH_DIR} written; create the branch with --create-branch {PARENT_URI}")


def create_branch(uri: str, storage_options: dict) -> None:
    require_writer()
    dataset = lance.dataset(uri, storage_options=storage_options)
    assert dataset.version == 1, "upload the main chain first"
    dataset.create_branch(BRANCH, 1)
    lance.write_dataset(rows_of(201, 204), f"{uri}/tree/{BRANCH}", mode="append",
                        data_storage_version="2.2", storage_options=storage_options)
    branch = lance.dataset(f"{uri}/tree/{BRANCH}", storage_options=storage_options)
    assert branch.version == 2, branch.version
    print(f"branch {BRANCH!r} created at {uri}; sync tree/ and _refs/branches/ back")


def row_ids(dataset) -> list:
    return sorted(dataset.to_table(columns=["row_id"])["row_id"].to_pylist())


def check(output: Path) -> None:
    dataset = lance.dataset(str(output))
    assert [v["version"] for v in dataset.versions()] == [1], dataset.versions()
    assert row_ids(dataset) == list(range(1, 9)), "main rows differ"
    assert list(dataset.branches.list()) == [BRANCH], dataset.branches.list()
    # The branch directory keeps the name as is; Lance 12 does not find a percent-encoded one.
    manifests = sorted((output / "tree" / BRANCH / "_versions").glob("*.manifest"))
    assert len(manifests) == 2, f"branch {BRANCH!r} must carry versions 1 and 2: {manifests}"
    for manifest in manifests:
        assert PARENT_URI.encode() in manifest.read_bytes(), f"{manifest} must reference {PARENT_URI}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path, nargs="?",
                        help="directory holding the fixture (not used with --create-branch)")
    parser.add_argument("--check", action="store_true", help="verify the existing fixture")
    parser.add_argument("--create-branch", metavar="URI",
                        help="create the branch at the uploaded dataset URI instead of building")
    parser.add_argument("--storage-option", action="append", default=[], metavar="KEY=VALUE",
                        help="Lance storage option for --create-branch (repeatable)")
    args = parser.parse_args()
    if not args.create_branch and args.root is None:
        parser.error("the fixture directory is required unless --create-branch is given")
    if args.create_branch:
        create_branch(args.create_branch, dict(item.split("=", 1) for item in args.storage_option))
    elif args.check:
        check(args.root / UNICODE_BRANCH_DIR)
        print(f"{UNICODE_BRANCH_DIR} checked")
    else:
        build(args.root / UNICODE_BRANCH_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
