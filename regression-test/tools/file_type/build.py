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

"""Compile the standalone FILE oracle against vanilla ORC and its dependencies."""

import argparse
import os
from pathlib import Path
import shlex
import subprocess


def main():
    source = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path,
                        default=source.parents[2] / "thirdparty/installed")
    parser.add_argument("--cxx", default=os.environ.get("CXX", "c++"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prefix = args.prefix.resolve()
    env = dict(os.environ, PKG_CONFIG_PATH=os.pathsep.join(
        str(prefix / lib / "pkgconfig") for lib in ("lib", "lib64")))
    flags = shlex.split(subprocess.check_output(
        ["pkg-config", "--static", "--cflags", "--libs", "protobuf", "zlib",
         "liblz4", "libzstd"], env=env, text=True))
    # Treat upstream headers as system headers while retaining warnings in this tool.
    flags = [option for flag in flags
             for option in (["-isystem", flag[2:]] if flag.startswith("-I") else [flag])]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    command = [args.cxx, "-std=c++20", "-O0", "-g", "-Wall", "-Wextra",
               str(source / "file_type_fixture.cpp"), "-o", str(args.output),
               "-isystem", str(prefix / "include"),
               "-Wl,--start-group", "-L" + str(prefix / "lib"),
               "-L" + str(prefix / "lib64"), "-lorc", "-lsnappy", *flags,
               "-Wl,--end-group", "-pthread"]
    print(shlex.join(command), flush=True)
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
