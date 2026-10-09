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

"""Generate the Lance search-snapshot regression fixtures.

search_snapshot.lance, search_snapshot_pruned.lance and search_snapshot_evolved.lance are root
tables of the preinstalled Directory catalog next to time_travel.lance. They exercise
vector_search() and full_text_search() with the version, timestamp, tag and branch properties,
so they keep an uncompacted history in which indexes appear, stop covering new data, are
rebuilt, and outlive schema changes.

search_snapshot.lance, main chain:

    version 1  create        row_id 1..8    fragment 0
    version 2  append        row_id 9..16   fragment 1
    version 3  vector index  vec_idx (IVF_FLAT, l2) over fragments 0 and 1
    version 4  FTS index     body_idx over fragments 0 and 1
    version 5  append        row_id 17..24  fragment 2, covered by neither index
    version 6  delete        row_id 3 and 11
    version 7  vector index  vec_idx rebuilt over all fragments (a new index UUID)

Tag "rel" points at main version 5. Branch "dev" forks from version 4:

    dev version 5  append     row_id 101..108  fragment 2 (the same id as main's fragment 2)
    dev version 6  FTS index  body_idx rebuilt over all fragments, stored under tree/dev/

Tag "dev_rel" points at dev version 5, which is not the branch's latest version. So main and
dev both have versions 5 and 6 and a fragment 2, with different rows and indexes.

Every row has vec = [row_id, row_id + 1, row_id + 2, row_id + 3] as float32, so the exact
squared L2 distance between rows r and n is 4 * (n - r)^2, and body = "doc <row_id> <word>"
with <word> "lance" for even row_ids and "doris" for odd ones. The vector index has two IVF
partitions; the suites search with nprobes=2, which makes IVF_FLAT exact, so indexed and flat
searches return the same rows.

search_snapshot_pruned.lance keeps what cleanup leaves behind:

    version 1  create        row_id 1..8, removed by cleanup
    version 2  vector index  vec_idx, pinned by tag "kept"; its index files are then deleted
    version 3  vector index  vec_idx rebuilt

so version 1 is missing, and version 2 opens but its index files are gone.

search_snapshot_evolved.lance changes its schema after the vector index is built:

    version 1  create        row_id 1..8    fragment 0
    version 2  vector index  vec_idx over fragment 0
    version 3  add column    note = "note <row_id>"
    version 4  rename        vec -> embedding (same field id, so vec_idx still serves it)
    version 5  append        row_id 9..16   fragment 1, not covered by vec_idx

so a search must bind the column names, the index and the fragments of the version it reads.

A Lance branch is a shallow clone that records its parent's absolute URI, so the branch was
created against the fixture's final location s3://warehouse/lance/search_snapshot.lance (see
lance_build_time_travel.py for the same procedure):

  python3 lance_build_search_snapshot.py preinstalled_data/lance
  # upload search_snapshot.lance to s3://warehouse/lance/search_snapshot.lance
  python3 lance_build_search_snapshot.py --create-branch s3://warehouse/lance/search_snapshot.lance \
      --storage-option endpoint=http://127.0.0.1:19000 --storage-option access_key_id=admin \
      --storage-option secret_access_key=password --storage-option region=us-east-1 \
      --storage-option allow_http=true
  # then sync tree/, _refs/branches/ and _refs/tags/ from that location back

The suites hard-code the commit times this script prints, as the time-travel suites do.
Run it with the writer pinned in lance_fixture_requirements.txt.

Usage:
  python3 lance_build_search_snapshot.py preinstalled_data/lance
  python3 lance_build_search_snapshot.py --fixture evolved preinstalled_data/lance
  python3 lance_build_search_snapshot.py --check preinstalled_data/lance

--fixture limits a build to some of the fixtures. Rebuilding search_snapshot.lance changes the
commit times the suites hard-code and requires recreating its branch.
"""
import argparse
import shutil
import time
from datetime import timedelta
from pathlib import Path

import lance
import pyarrow as pa

MAIN_DIR = "search_snapshot.lance"
PRUNED_DIR = "search_snapshot_pruned.lance"
EVOLVED_DIR = "search_snapshot_evolved.lance"
BRANCH = "dev"
BRANCH_PARENT_URI = "s3://warehouse/lance/search_snapshot.lance"
DIMENSION = 4
COMMIT_GAP_SECONDS = 1.5
FTS_INDEX_PARAMS = {
    "base_tokenizer": "simple",
    "language": "English",
    "max_token_length": 40,
    "lower_case": True,
    "stem": False,
    "remove_stop_words": False,
    "ascii_folding": False,
    "with_position": True,
}


def rows_of(low: int, high: int) -> pa.Table:
    ids = list(range(low, high + 1))
    vectors = [[float(i + j) for j in range(DIMENSION)] for i in ids]
    return pa.table({
        "row_id": pa.array(ids, pa.int32()),
        "vec": pa.array(vectors, pa.list_(pa.float32(), DIMENSION)),
        "body": pa.array([f"doc {i} {'lance' if i % 2 == 0 else 'doris'}" for i in ids], pa.string()),
    })


def create_vector_index(dataset, replace: bool = False):
    dataset.create_index("vec", "IVF_FLAT", name="vec_idx", metric="l2", num_partitions=2,
                         sample_rate=256, replace=replace)


def create_fts_index(dataset, replace: bool = False):
    dataset.create_scalar_index("body", "INVERTED", name="body_idx", replace=replace, **FTS_INDEX_PARAMS)


def pause() -> None:
    time.sleep(COMMIT_GAP_SECONDS)


def build_main(output: Path) -> None:
    if output.exists():
        shutil.rmtree(output)
    uri = str(output)
    lance.write_dataset(rows_of(1, 8), uri, mode="create", data_storage_version="2.2")
    pause()
    lance.write_dataset(rows_of(9, 16), uri, mode="append", data_storage_version="2.2")
    pause()
    create_vector_index(lance.dataset(uri))
    pause()
    create_fts_index(lance.dataset(uri))
    pause()
    lance.write_dataset(rows_of(17, 24), uri, mode="append", data_storage_version="2.2")
    pause()
    lance.dataset(uri).delete("row_id IN (3, 11)")
    pause()
    create_vector_index(lance.dataset(uri), replace=True)
    dataset = lance.dataset(uri)
    assert dataset.version == 7, dataset.version
    dataset.tags.create("rel", 5)


def build_pruned(output: Path) -> None:
    if output.exists():
        shutil.rmtree(output)
    uri = str(output)
    lance.write_dataset(rows_of(1, 8), uri, mode="create", data_storage_version="2.2")
    create_vector_index(lance.dataset(uri))
    kept = lance.dataset(uri)
    kept_index = {index["name"]: index["uuid"] for index in kept.list_indices()}["vec_idx"]
    kept.tags.create("kept", 2)
    create_vector_index(lance.dataset(uri), replace=True)
    dataset = lance.dataset(uri)
    # The tag keeps version 2 through cleanup, as a tagged release would be kept.
    dataset.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True,
                                 error_if_tagged_old_versions=False)
    shutil.rmtree(output / "_indices" / kept_index)
    assert [v["version"] for v in lance.dataset(uri).versions()] == [2, 3]


def evolved_rows(low: int, high: int, schema: pa.Schema) -> pa.Table:
    rows = rows_of(low, high).rename_columns(["row_id", "embedding", "body"])
    notes = pa.array([f"note {i}" for i in range(low, high + 1)], pa.string())
    return rows.append_column("note", notes).cast(schema)


def build_evolved(output: Path) -> None:
    if output.exists():
        shutil.rmtree(output)
    uri = str(output)
    lance.write_dataset(rows_of(1, 8), uri, mode="create", data_storage_version="2.2")
    create_vector_index(lance.dataset(uri))
    lance.dataset(uri).add_columns({"note": "concat('note ', CAST(row_id AS STRING))"})
    lance.dataset(uri).alter_columns({"path": "vec", "name": "embedding"})
    renamed = lance.dataset(uri)
    lance.write_dataset(evolved_rows(9, 16, renamed.schema), uri, mode="append", data_storage_version="2.2")
    assert lance.dataset(uri).version == 5, lance.dataset(uri).version


FIXTURES = {"main": (MAIN_DIR, build_main), "pruned": (PRUNED_DIR, build_pruned),
            "evolved": (EVOLVED_DIR, build_evolved)}


def build(root: Path, fixtures: list) -> None:
    for name in fixtures:
        directory, builder = FIXTURES[name]
        builder(root / directory)
        print(f"{directory} written")
    if "main" in fixtures:
        print(f"create the branch with --create-branch {BRANCH_PARENT_URI}")


def create_branch(uri: str, storage_options: dict) -> None:
    """Forks dev at the dataset's final location, appends to it and rebuilds its FTS index there."""
    dataset = lance.dataset(uri, storage_options=storage_options)
    assert dataset.version == 7, "upload the main chain first"
    dataset.create_branch(BRANCH, 4)
    branch_uri = f"{uri}/tree/{BRANCH}"
    pause()
    lance.write_dataset(rows_of(101, 108), branch_uri, mode="append", data_storage_version="2.2",
                        storage_options=storage_options)
    pause()
    create_fts_index(lance.dataset(branch_uri, storage_options=storage_options), replace=True)
    branch = lance.dataset(branch_uri, storage_options=storage_options)
    assert branch.version == 6, branch.version
    dataset.tags.create("dev_rel", (BRANCH, 5))
    print(f"branch {BRANCH} and tag dev_rel created at {uri}; sync tree/, _refs/branches/ and _refs/tags/ back")


def row_ids(dataset) -> list:
    return sorted(dataset.to_table(columns=["row_id"])["row_id"].to_pylist())


def check(root: Path) -> None:
    output = root / MAIN_DIR
    dataset = lance.dataset(str(output))
    versions = dataset.versions()
    assert [v["version"] for v in versions] == list(range(1, 8)), versions
    timestamps = [v["timestamp"] for v in versions]
    assert all((b - a).total_seconds() >= 1 for a, b in zip(timestamps, timestamps[1:])), timestamps
    expected_rows = {
        1: list(range(1, 9)), 2: list(range(1, 17)), 3: list(range(1, 17)), 4: list(range(1, 17)),
        5: list(range(1, 25)), 6: [i for i in range(1, 25) if i not in (3, 11)],
        7: [i for i in range(1, 25) if i not in (3, 11)],
    }
    indexes = {}
    for version, rows in expected_rows.items():
        snapshot = dataset.checkout_version(version)
        assert row_ids(snapshot) == rows, f"version {version} rows differ"
        indexes[version] = {index["name"]: (index["uuid"], sorted(index["fragment_ids"]))
                             for index in snapshot.list_indices()}
    assert indexes[2] == {}
    assert set(indexes[3]) == {"vec_idx"} and indexes[3]["vec_idx"][1] == [0, 1]
    assert set(indexes[4]) == {"vec_idx", "body_idx"} and indexes[4]["body_idx"][1] == [0, 1]
    assert indexes[5] == indexes[4] and indexes[6]["vec_idx"] == indexes[4]["vec_idx"]
    assert indexes[7]["vec_idx"][0] != indexes[6]["vec_idx"][0], "version 7 must rebuild vec_idx"
    assert indexes[7]["vec_idx"][1] == [0, 1, 2]
    tags = {name: (ref["branch"], ref["version"]) for name, ref in dataset.tags.list().items()}
    assert tags == {"rel": (None, 5), "dev_rel": (BRANCH, 5)}, tags
    assert list(dataset.branches.list()) == [BRANCH], dataset.branches.list()
    branch_manifests = sorted((output / "tree" / BRANCH / "_versions").glob("*.manifest"))
    assert len(branch_manifests) == 3, f"branch {BRANCH} must carry versions 4 to 6: {branch_manifests}"
    for manifest in branch_manifests:
        assert BRANCH_PARENT_URI.encode() in manifest.read_bytes(), (
            f"{manifest} must reference the parent at {BRANCH_PARENT_URI}")
    assert (output / "tree" / BRANCH / "_indices").is_dir(), "dev must hold its rebuilt FTS index"

    pruned = lance.dataset(str(root / PRUNED_DIR))
    assert [v["version"] for v in pruned.versions()] == [2, 3], pruned.versions()
    kept = pruned.checkout_version(2)
    kept_index = kept.list_indices()[0]["uuid"]
    assert not (root / PRUNED_DIR / "_indices" / kept_index).exists(), "version 2 index files must be gone"
    assert (root / PRUNED_DIR / "_indices" / pruned.list_indices()[0]["uuid"]).is_dir()
    check_evolved(root / EVOLVED_DIR)
    for version in versions:
        print(f"{MAIN_DIR} version {version['version']} committed at {version['timestamp'].isoformat()}")


def check_evolved(output: Path) -> None:
    dataset = lance.dataset(str(output))
    assert [v["version"] for v in dataset.versions()] == list(range(1, 6)), dataset.versions()
    expected_columns = {
        1: ["row_id", "vec", "body"], 2: ["row_id", "vec", "body"], 3: ["row_id", "vec", "body", "note"],
        4: ["row_id", "embedding", "body", "note"], 5: ["row_id", "embedding", "body", "note"],
    }
    index_uuids = set()
    for version, columns in expected_columns.items():
        snapshot = dataset.checkout_version(version)
        assert snapshot.schema.names == columns, f"version {version} columns {snapshot.schema.names}"
        rows = list(range(1, 17)) if version == 5 else list(range(1, 9))
        assert row_ids(snapshot) == rows, f"version {version} rows differ"
        indexes = snapshot.list_indices()
        if version == 1:
            assert indexes == [], indexes
            continue
        assert [index["name"] for index in indexes] == ["vec_idx"], indexes
        assert sorted(indexes[0]["fragment_ids"]) == [0], indexes
        index_uuids.add(indexes[0]["uuid"])
    assert len(index_uuids) == 1, f"the rename must keep vec_idx: {index_uuids}"
    notes = dataset.checkout_version(3).to_table(columns=["row_id", "note"]).to_pylist()
    assert all(row["note"] == f"note {row['row_id']}" for row in notes), notes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path, nargs="?",
                        help="directory holding the fixtures (not used with --create-branch)")
    parser.add_argument("--check", action="store_true", help="verify the existing fixtures")
    parser.add_argument("--fixture", action="append", choices=sorted(FIXTURES), metavar="NAME",
                        help="build only this fixture (repeatable): " + ", ".join(sorted(FIXTURES)))
    parser.add_argument("--create-branch", metavar="URI",
                        help="create the dev branch at the uploaded dataset URI instead of building")
    parser.add_argument("--storage-option", action="append", default=[], metavar="KEY=VALUE",
                        help="Lance storage option for --create-branch (repeatable)")
    args = parser.parse_args()
    if not args.create_branch and args.root is None:
        parser.error("the fixture directory is required unless --create-branch is given")
    if args.create_branch:
        create_branch(args.create_branch, dict(item.split("=", 1) for item in args.storage_option))
    elif args.check:
        check(args.root)
    else:
        build(args.root, args.fixture or list(FIXTURES))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
