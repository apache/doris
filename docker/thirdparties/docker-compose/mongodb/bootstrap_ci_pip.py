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

"""Seed pip into the private fixture venv without a system ensurepip dependency."""

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.request import urlopen


WHEEL_NAME = "pip-25.3-py3-none-any.whl"
WHEEL_URL = ("https://files.pythonhosted.org/packages/44/3c/"
             "d717024885424591d5376220b5e836c2d5293ce2011523c9de23ff7bf068/" + WHEEL_NAME)
WHEEL_SIZE = 1778622
WHEEL_SHA256 = "9655943313a94722b7774661c21049070f6bbb0a1516bf02f7c8d5d9201514cd"


def verified_wheel(directory):
    directory.mkdir(parents=True, exist_ok=True)
    wheel = directory / WHEEL_NAME
    if wheel.exists():
        with wheel.open("rb") as source:
            data = source.read(WHEEL_SIZE + 1)
    else:
        with urlopen(WHEEL_URL, timeout=60) as source:
            data = source.read(WHEEL_SIZE + 1)
    if len(data) != WHEEL_SIZE or hashlib.sha256(data).hexdigest() != WHEEL_SHA256:
        raise RuntimeError("The pinned pip bootstrap wheel failed size/SHA-256 verification")
    if not wheel.exists():
        # Never leave a partial wheel that a retry could mistake for a verified cache entry.
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            try:
                temporary.write(data)
                temporary.close()
                temporary_path.replace(wheel)
            finally:
                temporary_path.unlink(missing_ok=True)
    return wheel


def main():
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("The fixture pip bootstrap must run inside a virtual environment")
    wheel = verified_wheel(Path(sys.prefix) / ".bootstrap")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(wheel)
    # The wheel's metadata is visible through PYTHONPATH, so force a real install into the venv.
    subprocess.run([sys.executable, "-m", "pip", "--isolated", "install", "--no-index", "--no-deps",
                    "--ignore-installed", "--prefix", sys.prefix, "--disable-pip-version-check", str(wheel)],
                   env=environment, check=True)


if __name__ == "__main__":
    main()
