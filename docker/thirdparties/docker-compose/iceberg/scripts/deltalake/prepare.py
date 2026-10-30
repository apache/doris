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

"""Prepare pinned Spark dependencies and Delta golden tables for the optional Compose profile."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile
import urllib.request


DELTA_COMMIT = "cebdc44dc07d331383e72811177af4ffc8638990"
ARCHIVE_SHA256 = "a9b81e3e0a10c4b32afec823b5a2803eab57c06f542664478ecd438b2b982339"
ARTIFACTS = [
    ('io/delta/delta-spark_4.0_2.13/4.4.1/delta-spark_4.0_2.13-4.4.1.jar',
     'd0766b4b0ace348dca7a5851418767e1eea170c7303c4771cda2934f49627ce8'),
    ('io/delta/delta-storage/4.4.1/delta-storage-4.4.1.jar',
     'f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f'),
    ('io/delta/delta-kernel-api/4.4.1/delta-kernel-api-4.4.1.jar',
     '19c93b5e6740bb3001d3deca0b8c3fa5470197c520ffa36ffa58df98e91ae459'),
    ('io/delta/delta-kernel-defaults/4.4.1/delta-kernel-defaults-4.4.1.jar',
     '329995bbc9b2e5d74a9a7c9e968574c1e438bbb183aaf1b2c629a87ce8558fe2'),
    ('io/delta/delta-kernel-unitycatalog/4.4.1/delta-kernel-unitycatalog-4.4.1.jar',
     '4d92b9248f8a5f9b3043fea2bb7666f0db26f5491e279c30868280be35450368'),
    ('org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar',
     '01334a07e179cae2b7e251f24857f11e3759108c9d6a79dcbe362a4295227da2'),
    ('software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar',
     '890bf9f2a80488f7a8aa8277d64d13a9ea3a3cdff8607ef3b63a878d8b467b41'),
]

TABLES = [
    'table-with-columnmapping-mode-name',
    'table-with-columnmapping-mode-id',
    'data-reader-timestamp_ntz',
    'data-reader-timestamp_ntz-name-mode',
    'data-reader-timestamp_ntz-id-mode',
    'type-widening',
    'type-widening-nested',
    'dv-partitioned-with-checkpoint',
    'dv-with-columnmapping',
    'log-replay-dv-key-cases',
    'checkpoint',
    'multi-part-checkpoint',
    'only-checkpoint-files',
    'v2-checkpoint-json',
    'v2-checkpoint-parquet',
    'basic-with-vacuum-protocol-check-feature',
    'data-reader-primitives',
    'data-reader-nested-struct',
    'data-reader-array-primitives',
    'data-reader-array-complex-objects',
    'data-reader-map',
    'kernel-timestamp-INT96',
    'kernel-timestamp-TIMESTAMP_MICROS',
    'kernel-timestamp-TIMESTAMP_MILLIS',
    'decimal-various-scale-precision',
    'parquet-decimal-dictionaries',
    'parquet-decimal-dictionaries-v1',
    'parquet-decimal-dictionaries-v2',
    'collations-table',
    'spark-variant-checkpoint',
    'no-delta-log-folder',
]


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, destination, expected):
    if destination.is_file() and checksum(destination) == expected:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
        partial = Path(temporary.name)
    try:
        print(f"Downloading {destination.name}", flush=True)
        with urllib.request.urlopen(url, timeout=120) as response, partial.open("wb") as output:
            shutil.copyfileobj(response, output, 1024 * 1024)
        actual = checksum(partial)
        if actual != expected:
            raise ValueError(f"SHA-256 mismatch for {destination.name}: {actual}, expected {expected}")
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)


def prepare(output):
    output.mkdir(parents=True, exist_ok=True)
    maven = os.environ.get("MAVEN_REPOSITORY_URL", "https://repo.maven.apache.org/maven2").rstrip("/")
    for artifact, sha256 in ARTIFACTS:
        download(f"{maven}/{artifact}", output / "jars" / Path(artifact).name, sha256)
    archive = output / "delta-v4.4.1.tar.gz"
    download(f"https://codeload.github.com/delta-io/delta/tar.gz/{DELTA_COMMIT}", archive, ARCHIVE_SHA256)
    expected_manifest = {"commit": DELTA_COMMIT, "archive_sha256": ARCHIVE_SHA256, "tables": TABLES}
    marker = output / "golden-manifest.json"
    golden = output / "golden"
    if (marker.is_file() and json.loads(marker.read_text()) == expected_manifest
            and all((golden / table.lower().replace("-", "_")).is_dir() for table in TABLES)):
        print("Delta v4.4.1 dependencies and golden tables are ready", flush=True)
        return
    prefix = f"delta-{DELTA_COMMIT}/connectors/golden-tables/src/main/resources/golden/"
    copied = set()
    with tempfile.TemporaryDirectory(dir=output, prefix="golden-") as staging:
        staged = Path(staging) / "golden"
        staged.mkdir()
        with tarfile.open(archive) as bundle:
            for member in bundle:
                if member.name in (f"delta-{DELTA_COMMIT}/LICENSE.txt", f"delta-{DELTA_COMMIT}/NOTICE.txt"):
                    destination = Path(staging) / Path(member.name).name
                elif member.name.startswith(prefix):
                    relative = PurePosixPath(member.name[len(prefix):])
                    if member.isdir() or relative.parts[0] not in TABLES:
                        continue
                    assert ".." not in relative.parts and member.isfile(), member.name
                    table = relative.parts[0]
                    copied.add(table)
                    destination = staged / table.lower().replace("-", "_") / Path(*relative.parts[1:])
                else:
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(member) as source, destination.open("wb") as target:
                    shutil.copyfileobj(source, target)
        missing = set(TABLES) - copied
        if missing:
            raise ValueError(f"Missing golden tables in pinned archive: {sorted(missing)}")
        # This directory contains only generated fixtures, never container state or authored inputs.
        if golden.exists():
            shutil.rmtree(golden)
        staged.replace(golden)
        for name in ("LICENSE.txt", "NOTICE.txt"):
            (Path(staging) / name).replace(output / name)
    pending_marker = marker.with_suffix(".json.tmp")
    pending_marker.write_text(json.dumps(expected_manifest, indent=2) + "\n")
    pending_marker.replace(marker)
    print(f"Prepared {len(copied)} Delta v4.4.1 golden tables", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).resolve().parents[2] / "cache/deltalake")
    prepare(parser.parse_args().output.resolve())
