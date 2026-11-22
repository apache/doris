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

"""Create or remove an authenticated, disposable replica-set and sharded fixture."""

import argparse
import base64
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import time
from urllib.parse import quote
import uuid

import pymongo
from pymongo.errors import PyMongoError


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--image", choices=["mongo:7.0.28", "mongo:8.0.20"], default="mongo:8.0.20")
    parser.add_argument("--base-port", type=int, default=28010)
    parser.add_argument("--advertised-host", default="127.0.0.1")
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--remove", action="store_true")
    return parser.parse_args()


class Fixture:
    def __init__(self, args):
        self.args = args
        self.directory = Path(args.state_dir).resolve()
        self.state_path = self.directory / "state.json"
        self.docker_command = shlex.split(args.docker_command)
        self.state = {}
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())

    def docker(self, *arguments):
        return subprocess.check_output(self.docker_command + list(arguments), text=True).strip()

    def save(self):
        # Create credential-bearing state with private permissions before writing any bytes.
        descriptor = os.open(self.state_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            json.dump(self.state, output, indent=2)

    def remove(self):
        for container in self.state.get("containers", []):
            self.docker("rm", "-f", container)
        self.state["containers"] = []
        self.save()
        print("Removed the fixture's recorded containers", flush=True)

    def connect(self, port, authenticated=True):
        options = dict(directConnection=True, serverSelectionTimeoutMS=1000,
                       appname="doris-snapshot-fixture-controller")
        if authenticated:
            options.update(username=self.state["username"], password=self.state["password"], authSource="admin")
        return pymongo.MongoClient("127.0.0.1", port, **options)

    @staticmethod
    def wait(client, predicate, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                result = predicate(client)
                if result:
                    return result
            except PyMongoError:
                pass
            time.sleep(0.25)
        raise RuntimeError("The controlled MongoDB fixture did not become ready")

    def start(self, suffix, port, arguments, binary="mongod"):
        name = self.state["prefix"] + "-" + suffix
        bind = ",".join(dict.fromkeys(["127.0.0.1", self.args.advertised_host]))
        command = ["run", "-d", "--name", name, "--network", "host", "--user",
                   f"{os.getuid()}:{os.getgid()}", "--tmpfs", "/data/db:rw,size=1g,mode=1777",
                   "-v", str(self.directory / "keyfile") + ":/run/mongodb-key:ro", "--entrypoint", binary,
                   self.args.image, "--bind_ip", bind, "--port", str(port), "--keyFile", "/run/mongodb-key",
                   "--setParameter", "enableTestCommands=1"]
        if binary == "mongod":
            command += ["--dbpath", "/data/db", "--wiredTigerCacheSizeGB", "0.25",
                        "--oplogSize", "64"]
        self.docker(*(command + arguments))
        self.state["containers"].append(name)
        self.save()

    def initialize_replica_set(self, ports, name, config_server=False):
        # Starting a process does not mean its socket is ready for the initiation quorum check.
        for port in ports:
            with self.connect(port, False) as member:
                self.wait(member, lambda client: client.admin.command("ping"))
        with self.connect(ports[0], False) as primary:
            configuration = {"_id": name, "members": [
                {"_id": index, "host": f"{self.args.advertised_host}:{port}"}
                for index, port in enumerate(ports)]}
            if config_server:
                configuration["configsvr"] = True
            primary.admin.command("replSetInitiate", configuration)
            self.wait(primary, lambda client: client.admin.command("hello").get("isWritablePrimary"))
            primary.admin.command("createUser", self.state["username"], pwd=self.state["password"], roles=["root"],
                                  writeConcern={"w": "majority"})

    def uri(self, ports, extra=""):
        hosts = ",".join(f"{self.args.advertised_host}:{port}" for port in ports)
        user = quote(self.state["username"], safe="")
        password = quote(self.state["password"], safe="")
        return f"mongodb://{user}:{password}@{hosts}/admin?retryReads=false{extra}"

    def create(self):
        if self.state.get("containers"):
            raise RuntimeError("The state directory already owns a fixture; remove it before recreating")
        username = os.environ["MONGODB_SNAPSHOT_ADMIN_USER"]
        password = os.environ["MONGODB_SNAPSHOT_ADMIN_PASSWORD"]
        if not 1024 < self.args.base_port < 65527:
            raise ValueError("base-port must leave eight valid fixture ports")
        # Fail before allocating containers if a requested listening port is already occupied.
        for port in range(self.args.base_port + 1, self.args.base_port + 9):
            with socket.socket() as probe:
                # Closed fixture connections can still leave TIME_WAIT sockets after removal.
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        key = self.directory / "keyfile"
        # Recreating a removed fixture must not try to overwrite its existing read-only key.
        if not key.exists():
            key.write_bytes(base64.b64encode(os.urandom(48)))
            key.chmod(0o400)
        token = uuid.uuid4().hex[:10]
        self.state = dict(prefix="doris-mongodb-snapshot-" + token, containers=[], image=self.args.image,
                          username=username, password=password)
        self.save()
        base = self.args.base_port
        replica_set, config_set = "snapshot_rs_" + token, "snapshot_cfg_" + token
        shards = ["snapshot_s0_" + token, "snapshot_s1_" + token]
        self.start("config", base + 1, ["--configsvr", "--replSet", config_set])
        for index in range(2):
            self.start("shard" + str(index), base + 2 + index, ["--shardsvr", "--replSet", shards[index]])
        for index in range(3):
            self.start("member" + str(index), base + 6 + index, ["--replSet", replica_set])
        self.initialize_replica_set([base + 1], config_set, True)
        for index in range(2):
            self.initialize_replica_set([base + 2 + index], shards[index])
        self.initialize_replica_set([base + 6, base + 7, base + 8], replica_set)
        for index in range(2):
            self.start("router" + str(index), base + 4 + index,
                       ["--configdb", f"{config_set}/{self.args.advertised_host}:{base + 1}"], "mongos")
        with self.connect(base + 4) as router:
            self.wait(router, lambda client: client.admin.command("ping"))
            for index in range(2):
                router.admin.command("addShard", f"{shards[index]}/{self.args.advertised_host}:{base + 2 + index}")
            router.admin.command("enableSharding", "doris_mongodb_snapshot", primaryShard=shards[0])
        self.state.update(replica_uri=self.uri([base + 6, base + 7, base + 8], "&replicaSet=" + replica_set),
                          router_uris=[self.uri([base + 4 + index], "&directConnection=true") for index in range(2)],
                          shard_uris=[self.uri([base + 2 + index], "&directConnection=true") for index in range(2)],
                          shard_names=shards)
        self.save()
        print("Ready: authenticated three-member replica set, two shards and two routers; state file written", flush=True)


def main():
    args = parse_args()
    fixture = Fixture(args)
    if args.remove:
        fixture.remove()
    else:
        fixture.create()


if __name__ == "__main__":
    main()
