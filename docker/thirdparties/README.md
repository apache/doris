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
and uses loopback for its same-host services. On Linux, when `IP_HOST` is loopback, Fluss
detects a host IPv4 address; set `FLUSS_HOST_IP` explicitly when a different
address is needed for both containers and host clients. Iceberg REST HDFS uses
host ports 20020, 20870, 20864, 20866 and
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


## Image registries

`custom_settings.env` declares the image-source configuration. Environment
variables take precedence over its defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `THIRDPARTY_IMAGE_MODE` | `native` | `native` preserves references; `mapped` resolves external images to the configured registry. |
| `THIRDPARTY_IMAGE_REGISTRY` | empty | Target registry host and optional port, without a scheme or project path. |
| `THIRDPARTY_IMAGE_REGISTRY_MAP` | empty | Comma-separated `source-registry=project[/prefix]` entries. |

For example, with matching proxy-cache projects in Harbor:

```bash
export THIRDPARTY_IMAGE_MODE=mapped
export THIRDPARTY_IMAGE_REGISTRY=harbor.example.com:443
export THIRDPARTY_IMAGE_REGISTRY_MAP='docker.io=dockerhub,ghcr.io=ghcr,quay.io=quay,icr.io=icr,cgr.dev=cgr,mcr.microsoft.com=mcr'
export CONTAINER_UID=external-test-

bash run-thirdparties-docker.sh -c mysql,hive3 --list-images
bash run-thirdparties-docker.sh -c mysql,hive3 --pull-images
bash run-thirdparties-docker.sh -c mysql,hive3
```

This maps `mysql:5.7.36` to
`harbor.example.com:443/dockerhub/library/mysql:5.7.36`. Tags and digests are
preserved. The target must already contain the images or have the corresponding
upstream proxy-cache projects configured. Ordinary projects containing copied
images work with the same path layout. Authentication and CA trust are managed
by the operator through Docker, not through these settings. IPv6 registry
literals are not supported by this mapping configuration.

`native` does not disable a daemon's existing Docker Hub registry mirror.
`mapped` uses explicit registry references and does not fall back to the original
image name. Missing mappings for selected components or their helpers fail
before deployment. Mapped starts fetch missing remote images, including build
bases and helpers, before stopping containers or changing fixture data. Cached
images follow Compose's default missing-image policy; tags are not forcibly
refreshed. Local build outputs such as `doris-kerberos-minimal` are not mapped or
pulled. `--stop` performs no registry probe or image prefetch.

`--list-images` prints a tab-separated inventory with component membership,
override variable, kind, source, and target, without accessing Docker. Without
`-c`, it lists the launcher's default components. `--pull-images` fetches missing
images without starting containers, building local images, or initializing data.
`--image-config-version` prints `1` without accessing Docker, so CI can detect
this interface before choosing it instead of a legacy source-rewriting path.

### Individual images and maintenance

`image-catalog.env` is the list of default images, their component membership,
and their override variables. A user override is applied before registry
mapping. An override already under the exact target registry is preserved,
including references to private projects:

```bash
export THIRDPARTY_CLICKHOUSE_IMAGE=harbor.example.com:443/private/clickhouse-server:23.8
```

Existing `FLUSS_SERVER_IMAGE`, `FLUSS_FLINK_IMAGE`,
`ICEBERG_POSTGRES_IMAGE`, and `THIRDPARTY_FILESYSTEM_IMAGE` overrides are
supported. Iceberg retains its platform-specific PostgreSQL default. Hive's
baseline probe and restore helper retains the original `alpine` reference via
`THIRDPARTY_HIVE_FILESYSTEM_IMAGE`; the cleanup and snapshot helper retains
`alpine:3.20` via `THIRDPARTY_FILESYSTEM_IMAGE`.

Add new images to the catalog and reference their exported variables in Compose
templates. Pass build bases through Dockerfile `ARG` and Compose `build.args`.
Standalone snapshot/export tools load the same settings. Source templates are
not scanned or rewritten for image mapping; use the launcher to initialize
image variables before operating its generated Compose configurations.

The image-source contract tests run without a Docker daemon (Compose v2 is used
only for interpolation):

```bash
python3 test/test_image_registry.py
```

These settings control container images only. Artifact downloads, fixture data,
cloud credentials, host aliases and cross-node service connectivity remain
separate deployment requirements.
