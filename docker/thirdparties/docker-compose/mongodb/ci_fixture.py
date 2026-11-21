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

"""Prepare the authenticated External CI topology and its private runner configuration."""

import argparse
import json
import os
from pathlib import Path
import secrets
import signal

from bson.codec_options import DatetimeConversion
import pymongo

from snapshot_fixture import Fixture
import verify_native_snapshot as snapshot
import verify_native_splits as splits


def private_json(path, value):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as output:
        os.fchmod(output.fileno(), 0o600)
        json.dump(value, output, indent=2)
    temporary.replace(path)


def prepare(fixture):
    readers = []
    state = fixture.state
    for index, uri in enumerate([state["replica_uri"], state["router_uris"][0]]):
        with pymongo.MongoClient(uri, datetime_conversion=DatetimeConversion.DATETIME_AUTO) as source:
            splits.initialize(source, state["shard_names"] if index else None)
            user, password = splits.install_reader(source, bool(index))
            reader = snapshot.query_uri(uri, user, password)
            readers.append(reader.replace("appName=" + snapshot.APPLICATION, "appName=" + splits.APPLICATION))
    state["split_reader_uris"] = readers
    fixture.save()
    # Publish only after both sources and their read-only identities are ready. A missing file
    # makes enabled CI suites fail explicitly instead of reading a stale or standalone source.
    private_json(fixture.directory / "regression.json", {
        "mongodb_split_uri": readers[1], "mongodb_replica_uri": readers[0], "mongodb_mongos_uri": readers[1]})


def terminate(signum, frame):
    raise SystemExit(143)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--advertised-host", help="host address reachable from FE/BE and bridge containers")
    parser.add_argument("--base-port", type=int, default=28100)
    parser.add_argument("--image", choices=["mongo:7.0.28", "mongo:8.0.20"], default="mongo:8.0.20")
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--network-mode", choices=["host", "bridge"], default="bridge")
    parser.add_argument("--remove", action="store_true")
    args = parser.parse_args()
    if not args.remove and not args.advertised_host:
        parser.error("--advertised-host is required when starting the fixture")
    args.local_bootstrap = True
    args.container_user = "0:0"
    fixture = Fixture(args)
    ready = fixture.directory / "regression.json"
    ready.unlink(missing_ok=True)
    if fixture.state.get("containers"):
        fixture.remove()
    if args.remove:
        return
    os.environ["MONGODB_SNAPSHOT_ADMIN_USER"] = "ci_fixture_admin"
    os.environ["MONGODB_SNAPSHOT_ADMIN_PASSWORD"] = secrets.token_hex(24)
    # The launcher execs this controller, so cancelling its tracked PID also cleans its containers.
    signal.signal(signal.SIGTERM, terminate)
    try:
        fixture.create()
        prepare(fixture)
    except BaseException:
        ready.unlink(missing_ok=True)
        if fixture.state.get("containers"):
            fixture.remove()
        raise
    print("Ready: External CI replica-set/mongos split and read-parameter fixtures", flush=True)


if __name__ == "__main__":
    main()
