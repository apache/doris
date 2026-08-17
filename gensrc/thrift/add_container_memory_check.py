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


def main() -> int:
    path = pathlib.Path(sys.argv[1])
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

    if replacements == 0:
        return 0
    if INCLUDE not in output:
        own_header = next(index for index, line in enumerate(output) if line.startswith('#include "'))
        output.insert(own_header + 1, INCLUDE)
    path.write_text("\n".join(output) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
