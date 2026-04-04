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

# Local single-container Doris (FE + BE)

For **local development only**: one container runs FE and BE together, with BE startup host checks relaxed via `SKIP_CHECK_ULIMIT=true`.

## Security

- Intended for **isolated machines** only.
- Default cluster auth is **root with empty password** (fresh metadata). The entrypoint also runs `CREATE USER IF NOT EXISTS 'admin'@'%' IDENTIFIED BY ''` and `GRANT ADMIN_PRIV` — some versions may reject an empty password; check the container log.
- **Do not** expose ports 9030/8030/8040 to untrusted networks.

## Build options

### 1) Prebuilt binary inside Docker (no `./build.sh`, recommended)

From the Doris repo root, pick a **released** version that exists on the official download host. The image architecture follows the build platform (`TARGETARCH`: `linux/arm64` → `arm64` package, `linux/amd64` → `x64` or `x64-noavx2`).

```bash
docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-prebuilt \
  --build-arg DORIS_VERSION=4.0.5 -t doris-local:4.0.5 .
```

- **CPU without AVX2** (amd64 package): add `--build-arg BINARY_VARIANT=noavx2`.
- **Cross-arch** (e.g. build `linux/arm64` on x86): use Buildx, for example:
  `docker buildx build --platform linux/arm64 ...`

### 2) Download tarball on the host, then docker build

Script selects **`arm64` / `x64` / `x64-noavx2`** from the current OS (on Linux x86_64 it checks `/proc/cpuinfo` for `avx2`). Override with `--suffix` if needed.

```bash
chmod +x docker/runtime/local-dev/download-binary.sh
./docker/runtime/local-dev/download-binary.sh -v 4.0.5
# Then run the printed docker build command (OUTPUT_PATH under .prebuilt/).
```

Artifacts go to `docker/runtime/local-dev/.prebuilt/` (ignored by git).

### 3) Local compile output / any directory with `fe/` and `be/`

```bash
docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-local \
  --build-arg OUTPUT_PATH=./output -t doris-local:dev .
```

Omitting `--target` is equivalent to `--target doris-local-dev-local` (default).

**Doris 2.0** needs JDK 8 in the image, for example:

```bash
docker build -f docker/runtime/local-dev/Dockerfile --target doris-local-dev-prebuilt \
  --build-arg JDK_IMAGE=openjdk:8u342-jdk \
  --build-arg DORIS_VERSION=2.0.15 -t doris-local:2.0.15 .
```

## Run

```bash
docker run --name doris-local \
  -p 9030:9030 -p 8030:8030 -p 8040:8040 \
  -v doris-meta:/opt/apache-doris/fe/doris-meta \
  -v doris-be-storage:/opt/apache-doris/be/storage \
  doris-local:4.0.5
```

- **JDBC / mysql client**: `127.0.0.1:9030`, user `root` or `admin`, empty password (if accepted).
- **FE HTTP UI**: `8030`. **BE HTTP** (e.g. stream load): `8040`.

Optional: `docker run ... --ulimit nofile=65535:65535` if you hit file descriptor limits despite `SKIP_CHECK_ULIMIT`.

If FE stays `UNKNOWN` or `SHOW FRONTENDS` never succeeds, wipe volumes and retry so first-boot `priority_networks` / `BE_IP` are applied: `docker rm -f doris-local && docker volume rm doris-meta doris-be-storage` (adjust names).

## Environment variables (entrypoint)

| Variable | Default | Meaning |
|----------|---------|---------|
| `SKIP_CHECK_ULIMIT` | `true` | Skip BE `start_be.sh` checks for `vm.max_map_count`, swap, `ulimit -n` |
| `MYSQL_HOST` | `127.0.0.1` | FE MySQL protocol host (inside container) |
| `MYSQL_PORT` | `9030` | Query port |
| `FE_HTTP_PORT` | `8030` | Used only for log hints while waiting for MySQL to accept connections |
| `BE_IP` | auto: first non-loopback from `hostname -I`, else `127.0.0.1` | Address used in `ALTER SYSTEM ADD BACKEND` (should match BE heartbeat source; Docker often needs the container eth0 IP) |
| `PRIORITY_NETWORKS` | auto from `BE_IP` (`x.x.x.0/24` or `127.0.0.1/24`) | Override `fe.conf`/`be.conf` `priority_networks` on first boot; semicolon-separated CIDRs allowed |
| `BE_HEARTBEAT_PORT` | `9050` | Must match `heartbeat_service_port` in `be.conf` |
| `MAX_WAIT_SECONDS` | `300` | Wait for FE / register BE |
