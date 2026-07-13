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

"""Build the Python complex-UDF archive with its jieba dependency."""

import argparse
import hashlib
from pathlib import Path, PurePosixPath
import tarfile
import zipfile


JIEBA_VERSION = "0.42.1"
JIEBA_ARCHIVE_SHA256 = "055ca12f62674fafed09427f176506079bc135638a14e23e25be909131928db2"
UDF_SOURCES = (
    "business_logic.py",
    "complex_udaf.py",
    "complex_udtf.py",
    "external_api.py",
    "nlp_chinese.py",
)
REQUIRED_JIEBA_FILES = (
    "jieba/__init__.py",
    "jieba/dict.txt",
    "jieba/analyse/idf.txt",
    "jieba/posseg/__init__.py",
)
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def parse_args():
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Build py_udf_complex.zip from a verified jieba source archive"
    )
    parser.add_argument(
        "--jieba-archive",
        required=True,
        type=Path,
        help="path to the jieba-0.42.1 source tar.gz",
    )
    parser.add_argument(
        "--output",
        default=script_dir / "py_udf_complex.zip",
        type=Path,
        help="output zip path",
    )
    return parser.parse_args()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_entries(script_dir, jieba_archive):
    actual_sha256 = sha256(jieba_archive)
    if actual_sha256 != JIEBA_ARCHIVE_SHA256:
        raise ValueError(
            "unexpected jieba archive SHA-256: "
            f"expected {JIEBA_ARCHIVE_SHA256}, got {actual_sha256}"
        )

    entries = {name: (script_dir / name).read_bytes() for name in UDF_SOURCES}
    entries["THIRD_PARTY_LICENSES/jieba.txt"] = (
        script_dir / "jieba.LICENSE"
    ).read_bytes()

    archive_prefix = PurePosixPath(f"jieba-{JIEBA_VERSION}") / "jieba"
    with tarfile.open(jieba_archive, "r:gz") as archive:
        for member in archive.getmembers():
            member_path = PurePosixPath(member.name)
            try:
                relative_path = member_path.relative_to(archive_prefix)
            except ValueError:
                continue
            if not member.isfile() or not relative_path.parts:
                continue

            # The case runs on CPython and does not exercise jieba's optional
            # Paddle mode. Exclude the Jython pickle models and lac_small to
            # keep the UDF copied to every BE small.
            if "lac_small" in relative_path.parts or relative_path.suffix == ".p":
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"cannot read {member.name} from jieba archive")
            entries[str(PurePosixPath("jieba") / relative_path)] = extracted.read()

    missing = [name for name in REQUIRED_JIEBA_FILES if name not in entries]
    if missing:
        raise ValueError(f"jieba archive is missing required files: {missing}")
    return entries


def directory_names(file_names):
    directories = set()
    for file_name in file_names:
        parent = PurePosixPath(file_name).parent
        while parent != PurePosixPath("."):
            directories.add(f"{parent}/")
            parent = parent.parent
    return sorted(directories, key=lambda name: (name.count("/"), name))


def write_zip(output, entries):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output.with_name(f".{output.name}.tmp")
    with zipfile.ZipFile(
        temporary_output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as target:
        for directory in directory_names(entries):
            info = zipfile.ZipInfo(directory, ZIP_TIMESTAMP)
            info.create_system = 3
            info.external_attr = (0o40755 << 16) | 0x10
            target.writestr(info, b"")

        for name in sorted(entries):
            info = zipfile.ZipInfo(name, ZIP_TIMESTAMP)
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            target.writestr(info, entries[name], compresslevel=9)
    temporary_output.replace(output)


def main():
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    entries = load_entries(script_dir, args.jieba_archive.resolve())
    write_zip(args.output.resolve(), entries)
    print(f"wrote {args.output} with {len(entries)} files")


if __name__ == "__main__":
    main()
