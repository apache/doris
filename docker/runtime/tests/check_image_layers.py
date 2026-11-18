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

"""Reject removed private keys, Arthas and the other JDK in any shipped layer."""

import argparse
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def check_layers(image, java_version):
    (ROOT / "output").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=ROOT / "output", prefix="runtime-layers-") as work:
        archive = Path(work) / "image.tar"
        subprocess.run(["docker", "image", "save", "--output", str(archive), image], check=True)
        with tarfile.open(archive) as saved:
            manifest = json.load(saved.extractfile("manifest.json"))
            layers = {layer for entry in manifest for layer in entry["Layers"]}
            other_jdk = "jdk-8" if java_version == "17" else "jdk-17"
            forbidden = (
                "opt/apache-doris/fe/mysql_ssl_default_certificate",
                "opt/apache-doris/fe/arthas",
                f"usr/lib/jvm/{other_jdk}",
            )
            for name in layers:
                with tarfile.open(fileobj=saved.extractfile(name), mode="r|*") as layer:
                    for member in layer:
                        path = member.name.removeprefix("./").rstrip("/")
                        for prefix in forbidden:
                            if path == prefix or path.startswith(prefix + "/"):
                                raise RuntimeError(f"{image}: forbidden layer entry: {path}")
        print(f"{image}: checked {len(layers)} layers; no bundled default keys, Arthas or other JDK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("--java-version", choices=["8", "17"], default="17")
    arguments = parser.parse_args()
    check_layers(arguments.image, arguments.java_version)
