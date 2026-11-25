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

"""Own two disposable loopback clusters for the default private RPC regression."""

import argparse
import json
import os
from pathlib import Path
import random
import resource
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time


PORTS = {
    "fe": {"http_port": 8030, "rpc_port": 9020, "query_port": 9030,
           "edit_log_port": 9010, "arrow_flight_sql_port": 8835},
    "be": {"be_port": 9060, "webserver_port": 8040, "heartbeat_service_port": 9050,
           "brpc_port": 8060, "arrow_flight_sql_port": 8815},
}


def package_root(home):
    for candidate in (Path(home) / "output", Path(home)):
        if all((candidate / role / "bin" / ("start_" + role + ".sh")).is_file()
               and (candidate / role / "lib" / ("doris-fe.jar" if role == "fe" else "doris_be")).is_file()
               for role in PORTS):
            return candidate.resolve()
    raise RuntimeError("Private RPC TLS regression requires local FE and BE packages; "
                       "build them first or set privateRpcTlsPackageHome")


def reserve_ports(used):
    for _ in range(100):
        # Stay below Linux's default ephemeral range so startup traffic cannot claim our ports.
        offset = random.SystemRandom().randrange(10000, 20000)
        ports = {role: {key: value + offset for key, value in values.items()}
                 for role, values in PORTS.items()}
        values = {port for part in ports.values() for port in part.values()}
        if used.intersection(values):
            continue
        sockets = []
        try:
            for port in values:
                listener = socket.socket()
                sockets.append(listener)
                # Doris listeners bind wildcard addresses even when priority_networks is loopback.
                listener.bind(("0.0.0.0", port))
            used.update(values)
            return ports
        except OSError:
            pass
        finally:
            for listener in sockets:
                listener.close()
    raise RuntimeError("No unused loopback port range for private RPC TLS regression")


def engine_environment():
    # Regression runners can carry CI/cloud credentials; the test engines must not inherit them.
    keep = {"PATH", "JAVA_HOME", "JDK_17", "LD_LIBRARY_PATH", "LANG", "LC_ALL", "TZ",
            "ASAN_OPTIONS", "UBSAN_OPTIONS"}
    return {key: value for key, value in os.environ.items() if key in keep}


def disable_core_dumps():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


class Fixture:
    def __init__(self, home, logs):
        self.package = package_root(home)
        self.logs = Path(logs).resolve()
        self.logs.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="doris-private-rpc-tls-"))
        self.nodes = []
        self.clusters = {}

    def prepare(self):
        certificates = self.root / "certificates"
        with (self.logs / "certificates.log").open("w") as output:
            subprocess.run(["bash", str(Path(__file__).parent / "tls/generate-tls-fixture.sh"),
                            str(certificates)], check=True, timeout=60,
                           env=engine_environment(), stdout=output, stderr=subprocess.STDOUT,
                           preexec_fn=disable_core_dumps)
        # The engines need node identities, not the CA signing keys used during fixture generation.
        for authority in ("ca", "untrusted-ca"):
            (certificates / (authority + ".key")).unlink()
        used = set()
        for name, tls in (("local", True), ("remote", False)):
            ports = reserve_ports(used)
            backends = [ports["be"]]
            if name == "local":
                # Two local BEs keep distributed joins from collapsing into a single worker.
                backends.append(reserve_ports(used)["be"])
            self.clusters[name] = {"host": "127.0.0.1", "ports": ports, "backends": backends}
            nodes = [("fe", "fe", ports["fe"])]
            nodes.extend(("be", "be" if index == 0 else "be" + str(index + 1), backend)
                         for index, backend in enumerate(backends))
            for role, component, node_ports in nodes:
                source = self.package / role
                node = self.root / name / component
                node.mkdir(parents=True)
                shutil.copytree(source / "bin", node / "bin",
                                ignore=shutil.ignore_patterns("*.pid", "*.pid.lock", "palo_env.sh"))
                for entry in ("lib", "plugins", "custom_lib", "webroot", "www", "dict"):
                    if (source / entry).exists():
                        (node / entry).symlink_to(source / entry, target_is_directory=True)
                for entry in ("conf", "log", "storage", "doris-meta"):
                    (node / entry).mkdir()
                logging = source / "conf/log4j2-spring.xml"
                if logging.is_file():
                    shutil.copyfile(logging, node / "conf/log4j2-spring.xml")
                if role == "be":
                    # start_be.sh points sanitizers here; ASAN aborts before main if they are missing.
                    for suppression in ("asan_suppr.conf", "ubsan_suppr.conf", "lsan_suppr.conf"):
                        shutil.copyfile(source / "conf" / suppression, node / "conf" / suppression)
                config = dict(node_ports, priority_networks="127.0.0.1/32",
                              enable_tls=str(tls).lower())
                if role == "fe":
                    config.update(JAVA_OPTS_FOR_JDK_17='"-Xms256m -Xmx1024m '
                                  '-XX:ActiveProcessorCount=2 -XX:-HeapDumpOnOutOfMemoryError"',
                                  meta_dir=node / "doris-meta", thrift_server_type="THREAD_POOL")
                else:
                    # Leave room for ASAN's startup accounting before reserving query memory.
                    config.update(storage_root_path=node / "storage", sys_log_dir=node / "log",
                                  mem_limit="8G", num_cores=2, enable_java_support="false",
                                  max_sys_mem_available_low_water_mark_bytes=0)
                if tls:
                    config.update(tls_verify_mode="verify_fail_if_no_peer_cert",
                                  tls_certificate_path=certificates / (role + ".pem"),
                                  tls_private_key_path=certificates / (role + ".key"),
                                  tls_ca_certificate_path=certificates / "ca.pem",
                                  tls_cert_refresh_interval_seconds=0,
                                  tls_excluded_protocols=("http,mysql,arrowflight,bdbje"
                                                          if role == "fe" else "http,arrowflight"))
                (node / "conf" / (role + ".conf")).write_text(
                    "".join(str(key) + " = " + str(value) + "\n" for key, value in config.items()))
                # Register before launching so partial startup failures also clean up daemon PIDs.
                self.nodes.append((name, role, node))

    @staticmethod
    def owned_pid(role, node):
        try:
            pid = int((node / "bin" / (role + ".pid")).read_text().strip())
            environment = (Path("/proc") / str(pid) / "environ").read_bytes().split(b"\0")
            return pid if ("DORIS_HOME=" + str(node)).encode() in environment else None
        except (OSError, ValueError):
            return None

    def start(self):
        self.prepare()
        for name, role, node in self.nodes:
            with (self.logs / (name + "-" + node.name + "-start.log")).open("w") as output:
                subprocess.run(["bash", str(node / "bin" / ("start_" + role + ".sh")),
                                "--daemon"], cwd=node, env=engine_environment(), check=True,
                               timeout=30, stdout=output, stderr=subprocess.STDOUT,
                               preexec_fn=disable_core_dumps)
            deadline = time.monotonic() + 30
            while self.owned_pid(role, node) is None:
                if time.monotonic() >= deadline:
                    raise RuntimeError("Private RPC fixture did not publish an owned " + role + " PID")
                time.sleep(0.1)
        self.verify_tls()
        self.register_backends()
        return self.clusters

    def register_backends(self):
        # JDBC probes session variables through the BE before opening a connection on a fresh FE.
        # Use the same MySQL CLI as pipeline deployment to register the first BE before JDBC runs.
        for name, endpoint in self.clusters.items():
            for index, backend in enumerate(endpoint["backends"]):
                command = ["mysql", "--no-defaults", "--protocol=TCP", "--host=127.0.0.1",
                           "--port=" + str(endpoint["ports"]["fe"]["query_port"]), "--user=root",
                           "--connect-timeout=3", "--execute=ALTER SYSTEM ADD BACKEND '127.0.0.1:"
                           + str(backend["heartbeat_service_port"]) + "'"]
                deadline = time.monotonic() + 120
                log_name = name + "-register-backend-" + str(index + 1) + ".log"
                with (self.logs / log_name).open("w") as output:
                    while True:
                        result = subprocess.run(command, env=engine_environment(), timeout=10,
                                                stdout=output, stderr=subprocess.STDOUT)
                        if result.returncode == 0:
                            break
                        if time.monotonic() >= deadline:
                            raise RuntimeError("Private RPC fixture could not register its backend")
                        time.sleep(0.5)

    def verify_tls(self):
        certificates = self.root / "certificates"
        ports = self.clusters["local"]["ports"]
        listeners = [ports["fe"]["rpc_port"]]
        listeners.extend(backend[key] for backend in self.clusters["local"]["backends"]
                         for key in ("be_port", "heartbeat_service_port", "brpc_port"))
        for port in listeners:
            context = ssl.create_default_context(cafile=str(certificates / "ca.pem"))
            # TLS 1.2 reports rejected client authentication before the handshake completes.
            context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(certificates / "fe.pem", certificates / "fe.key")
            deadline = time.monotonic() + 120
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=3) as tcp:
                        with context.wrap_socket(tcp, server_hostname="localhost"):
                            break
                except OSError:
                    if any(self.owned_pid(role, node) is None for _, role, node in self.nodes):
                        raise RuntimeError("Private RPC fixture engine exited during startup")
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Private RPC TLS listener did not authenticate a trusted peer")
                    time.sleep(0.5)
            for peer in (None, "untrusted"):
                rejected = ssl.create_default_context(cafile=str(certificates / "ca.pem"))
                rejected.minimum_version = rejected.maximum_version = ssl.TLSVersion.TLSv1_2
                if peer:
                    rejected.load_cert_chain(certificates / (peer + ".pem"),
                                             certificates / (peer + ".key"))
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=3) as tcp:
                        with rejected.wrap_socket(tcp, server_hostname="localhost"):
                            raise RuntimeError("Private RPC TLS listener admitted an unauthenticated peer")
                except (ssl.SSLError, ConnectionResetError):
                    pass

    def close(self):
        for _, role, node in reversed(self.nodes):
            pid = self.owned_pid(role, node)
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and any(
                self.owned_pid(role, node) is not None for _, role, node in self.nodes):
            time.sleep(0.1)
        for _, role, node in reversed(self.nodes):
            pid = self.owned_pid(role, node)
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and any(
                self.owned_pid(role, node) is not None for _, role, node in self.nodes):
            time.sleep(0.1)
        try:
            if any(self.owned_pid(role, node) is not None for _, role, node in self.nodes):
                raise RuntimeError("Private RPC fixture engine did not stop")
            for name, role, node in self.nodes:
                if (node / "log").exists():
                    shutil.copytree(node / "log", self.logs / (name + "-" + node.name), dirs_exist_ok=True)
        finally:
            # A full log disk must fail the test without retaining disposable private keys.
            shutil.rmtree(self.root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-home", required=True)
    parser.add_argument("--log-dir", required=True)
    args = parser.parse_args()

    def interrupt(*_):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, interrupt)
    fixture = Fixture(args.package_home, args.log_dir)
    try:
        print(json.dumps(fixture.start()), flush=True)
        # Closing the runner's pipe also tears down daemons after an unexpected JVM exit.
        sys.stdin.readline()
    except KeyboardInterrupt:
        pass
    finally:
        fixture.close()


if __name__ == "__main__":
    main()
