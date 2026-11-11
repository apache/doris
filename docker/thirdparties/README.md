<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Thirdparty Docker environments

Run a component with Bash 4 or newer and Docker Compose v2:

```bash
bash run-thirdparties-docker.sh -c iceberg
bash run-thirdparties-docker.sh -c iceberg --stop
```

## Rootless Docker on Linux

Install and start the current user's rootless Docker daemon first. Its setup
requires subordinate UID/GID ranges, `newuidmap`/`newgidmap`, and enabled
unprivileged user namespaces. Once configured, launching these environments
does not require host `sudo` or membership in the host `docker` group.

On Linux, the launcher requires a rootless daemon and rejects rootful daemons.
It detects the user's `$XDG_RUNTIME_DIR/docker.sock` socket unless an endpoint
or context was explicitly selected. Docker commands always run as the current
user; there is no automatic elevation or `DOCKER_USE_SUDO` switch.
The endpoint can also be selected explicitly:

```bash
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/docker.sock"
docker info --format '{{json .SecurityOptions}}' # must include name=rootless
bash run-thirdparties-docker.sh -c iceberg
```

On macOS, the launcher uses the current user's Docker Desktop endpoint.

Keep the working directory writable by the current user. The launcher cleans
fixture files owned by subordinate container UIDs from inside the same rootless
user namespace, using `alpine:3.20`. It does not change host ownership to root.

Hive, Kerberos and Iceberg REST's built-in HDFS require Docker Engine 29.5 or
newer for rootless host networking. Hive defaults to `HIVE_HOST_ALIAS=localhost`
and uses loopback for its same-host services. For Fluss, set `FLUSS_HOST_IP` to
a host address reachable by both containers and host clients when `IP_HOST`
is loopback. Iceberg REST HDFS uses host ports 20020, 20870, 20864, 20866 and
20867, so its DataNode block addresses are reachable by host clients.

Kerberos client configuration and keytabs remain under
`docker-compose/kerberos/two-kerberos-hives/`; use the printed `KRB5_CONFIG`
path for host clients. The rootless launcher does not write `/etc/hosts`,
`/etc/krb5.conf` or `/keytabs`. The DB2 image retains its container-level
`privileged` setting within the rootless daemon's user namespace.

`--reserve-ports` changes a host sysctl and is unavailable in rootless mode.
It is optional; an operator can reserve ports separately when required.
Image pulls and fixture downloads still require access to their registries
and artifact repositories. `--no-load-data` skips Hive fixture refresh;
combine it with `--hive-mode rebuild` to validate a fresh Hive installation
without downloading a regression-data baseline.
