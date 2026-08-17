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

import pathlib
import re
import sys


INCLUDE = '#include "util/thrift_container_size.h"'
RESIZE = re.compile(r"^(?P<indent>\s*)(?P<container>.+)\.resize\((?P<size>_size\d*)\);$")
VECTOR_FIELD = re.compile(
    r"^(?P<indent>\s+)std::vector<(?P<value>.+)>  (?P<name>[A-Za-z_][A-Za-z0-9_]*);$"
)
LIFETIME_TRACKED_FIELDS = {
    # Parquet metadata loaders can retain many page-index objects together, so these reservations
    # must outlive the protocol. Other generated containers still need admission before resize but
    # are not retained by the metadata-loading path that requires aggregate lifetime accounting.
    "page_locations",
    "unencoded_byte_array_data_bytes",
    "null_pages",
    "min_values",
    "max_values",
    "null_counts",
    "repetition_level_histograms",
    "definition_level_histograms",
}


def add_include(lines):
    if INCLUDE in lines:
        return
    first_include = next(index for index, line in enumerate(lines) if line.startswith("#include "))
    lines.insert(first_include + 1, INCLUDE)


def instrument_source(path):
    lines = path.read_text().splitlines()
    output = []
    replacements = 0
    for line in lines:
        match = RESIZE.match(line)
        if match is not None:
            check = (
                f"{match['indent']}::doris::reserve_thrift_container_memory("
                f"iprot, &{match['container']}, {match['size']});"
            )
            if not output or output[-1] != check:
                output.append(check)
                replacements += 1
        output.append(line)

    if replacements != 0:
        add_include(output)
        path.write_text("\n".join(output) + "\n")


def instrument_header(path):
    lines = path.read_text().splitlines()
    output = []
    replacements = 0
    for line in lines:
        match = VECTOR_FIELD.match(line)
        if match is not None and match["name"] in LIFETIME_TRACKED_FIELDS:
            line = (
                f"{match['indent']}::doris::ThriftMemoryTrackedVector<{match['value']}>  "
                f"{match['name']};"
            )
            replacements += 1
        output.append(line)

    if replacements != 0:
        add_include(output)
        path.write_text("\n".join(output) + "\n")


def main() -> int:
    path = pathlib.Path(sys.argv[1])
    instrument_source(path)
    instrument_header(path.with_suffix(".h"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
