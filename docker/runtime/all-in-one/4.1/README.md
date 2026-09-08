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

# Doris all-in-one image (branch-4.1)

One FE and one BE in a single container, sized to be a test fixture in a
downstream project's CI. It is assembled from the official `apache/doris:fe-*`
and `apache/doris:be-*` images, so a new Doris release needs no repackaging of
anything here.

This directory targets the **4.1 release line only**. Other lines differ enough
in payload layout to deserve their own directory rather than a version switch.

## Two tags

| tag | covers | size |
|---|---|---|
| `apache/doris:all-in-one-<version>` | internal tables, Hive, Iceberg (including system tables), Paimon, JDBC catalogs, external-table writeback, Java UDF | 2.46 GB |
| `apache/doris:all-in-one-<version>-full` | the above plus Hudi, Trino connector, MaxCompute | 2.99 GB |

Pick `-full` only if the tests touch Hudi, the Trino connector or MaxCompute.

Both come up `healthy` in under 20 seconds. Sizes are the uncompressed layer
sum measured on 4.1.3/arm64, against 4.9 GB for the same payload untouched.
`docker image inspect --format '{{.Size}}'` reports 1.61 / 2.06 GiB for them;
`docker images` can print a much larger figure when the containerd image store
is enabled, because it adds the compressed blobs to the unpacked snapshot.

## Build

A plain run builds for the host architecture only. Multi-arch needs an explicit
`--platform`; the header the script prints says which it is doing.

```shell
# both tags, from the official 4.1.3 component images, host architecture only
./build.sh -v 4.1.3

# base tag only, then smoke test it
./build.sh -v 4.1.3 -f base -t

# from a locally built ./output instead
./build.sh -v dev -s local
```

`./build.sh --help` lists the rest. The build context is the repository root,
narrowed to a few KB by `Dockerfile.dockerignore`; run the script from anywhere.

## Multi-architecture

One tag can serve both architectures, the way `apache/doris:fe-4.1.3` does: the
tag points at an OCI image index listing one manifest per platform, and a pull
picks the matching entry.

```
apache/doris:all-in-one-4.1.3   ->  index
                                      |- linux/amd64
                                      \- linux/arm64
```

Nothing in the Dockerfile is architecture-specific beyond one JDK symlink;
`COPY --from=apache/doris:be-4.1.3` resolves to the target platform's variant on
its own, so no per-arch directories have to be staged by hand.

**Both architectures on one host.** Simple, but slow for whichever architecture
is foreign to the machine:

```shell
./build.sh -v 4.1.3 --platform linux/amd64,linux/arm64 --push
```

Keeping the result locally instead of pushing works only with the containerd
image store, which is where Docker has somewhere to put a second architecture.

**One architecture per host, joined afterwards.** No emulation, so this is much
faster and is the sane choice for a release:

```shell
# on an x86 host
./build.sh -v 4.1.3 --platform linux/amd64 --push -i myrepo/doris   # then retag ...-amd64
# on an arm host
./build.sh -v 4.1.3 --platform linux/arm64 --push -i myrepo/doris   # then retag ...-arm64
# anywhere
docker buildx imagetools create -t myrepo/doris:all-in-one-4.1.3 \
    myrepo/doris:all-in-one-4.1.3-amd64 myrepo/doris:all-in-one-4.1.3-arm64
```

**What emulation costs.** Measured on an Apple Silicon host building amd64:
the two `apt-get install` layers take about 18 minutes between them, plus a
one-off pull of that architecture's `be` image. Everything else is cheap --
the `COPY --from` steps are plain file copies at 8 seconds total and `strip`
takes under 2 seconds even emulated. If that ever needs to come down, the
runtime `apt` is the only target worth attacking, e.g. by basing the runtime
stage on a multi-arch image that already carries a JDK.

Note that a Doris BE built for a foreign architecture generally will not *run*
under emulation -- the official `apache/doris:be-4.1.3` amd64 image segfaults on
an Apple Silicon host just as this image does. Build for the other architecture
freely; test it on real hardware.

## Use

The image ships a `HEALTHCHECK`, so nothing downstream has to sleep.

```yaml
# docker compose
services:
  doris:
    image: apache/doris:all-in-one-4.1.3
    ports: ["9030:9030", "8030:8030", "8040:8040"]

  integration-test:
    image: my-project-tests:latest
    depends_on:
      doris:
        condition: service_healthy
```

```yaml
# GitHub Actions -- service containers are started and waited on automatically
services:
  doris:
    image: apache/doris:all-in-one-4.1.3
    ports: ['9030:9030', '8030:8030', '8040:8040']
```

| | |
|---|---|
| MySQL protocol | `9030`, user `root`, no password |
| FE HTTP | `8030`, `/api/health` needs no auth |
| BE HTTP | `8040`, stream load endpoint |
| readiness | docker health status turns `healthy` once FE is up and the BE is registered and alive |
| replicas | forced to 1, so `CREATE TABLE` needs no `replication_num` |
| persistence | none by default; mount `/opt/apache-doris/fe/doris-meta` and `/opt/apache-doris/be/storage` to keep state, the startup path is idempotent |
| config override | `-e FE_CONFIG_EXTRA=...` / `-e BE_CONFIG_EXTRA=...`, appended to the respective conf at startup |
| exit behaviour | fail-fast: if FE or BE exits, the container exits non-zero. `docker stop` shuts BE then FE down gracefully and exits 0 |
| logs | `docker logs` carries FE's console stream; the full logs live in `fe/log/fe.log` and `be/log/be.INFO` |

Heap and memory are tuned down for CI runners (FE `-Xmx2048m`, BE JNI heap
`-Xmx1024m`, BE `mem_limit = 40%`). On a larger machine, raise them with
`-e BE_CONFIG_EXTRA="mem_limit = 80%"` or rebuild with `--build-arg FE_HEAP=8192m`.

Heavy workloads may also need `vm.max_map_count` raised **on the host**
(`sysctl -w vm.max_map_count=2000000`); it is not a namespaced sysctl, so the
container cannot set it and `start_be.sh`'s check for it is skipped.

## How the two tags differ

BE loads JNI scanners by enumerating the directories under
`be/lib/java_extensions/` (`ScannerLoader.loadAllScannerJars`, called from
`be/src/util/jni-util.cpp` when the BE JVM starts). There is no list and no
config key, so a tag supports exactly the formats whose directory it ships.

The set of scanner classes BE can construct is closed — every one of them is
named as a string literal under `be/src/format_v2/jni/` and
`be/src/format/table/`:

| class | directory | tag |
|---|---|---|
| `PaimonJniScanner` | `paimon-scanner` | both |
| `IcebergSysTableJniScanner` | `iceberg-metadata-scanner` | both |
| `JdbcJniScanner` | `jdbc-scanner` | both |
| (`JniWriter`) | `java-writer` | both |
| `HadoopHudiJniScanner` | `hadoop-hudi-scanner` | `-full` |
| `TrinoConnectorJniScanner` | `trino-connector-scanner` | `-full` |
| `MaxComputeJniScanner` | `max-compute-connector` | `-full` |

Hive and Iceberg **data** reads are not in that table: their column decoding
runs in BE's native parquet/orc reader. They still go through JNI for file
system and catalog access, which is what the next section is about.

## The JNI baseline — do not prune these

Three directories look like external-table extras and are not:

- **`be/lib/java_extensions/preload-extensions`** (254 MB) — no Java sources at
  all, just a dependency bundle: parquet-hadoop-bundle, hadoop-common,
  hadoop-cos, libthrift, arrow, and the AWS SDK including
  `s3-tables-catalog-for-iceberg`. Every external-table read passes through it.
  `bin/start_be.sh` preloads it and requires `DORIS_PRELOAD_JAR` to stay first
  on the classpath.
- **`be/lib/java_extensions/java-udf`** (237 MB) — preloaded alongside it.
- **`be/lib/hadoop_hdfs`** (99 MB) — the JVM side of libhdfs, 157 jars. In 4.1
  libhdfs is linked statically into `doris_be` (hence the empty `native/`
  directory), but it is still a JNI wrapper and cannot open an HDFS file
  without them.

Together they are ~590 MB, a quarter of the base tag. `resource/prune.txt`
deliberately omits them and says so.

## What the build actually removes

| | base | -full |
|---|---|---|
| `strip --strip-debug` on `doris_be` | 2213 MB → 430 MB (450 MB on arm64) | same |
| `be/lib/meta_tool`, `be/lib/cdc_client`, `fe/arthas` | removed | removed |
| hudi / trino / maxcompute scanners | removed | kept |

Stripping is the single largest saving — more than every connector combined —
and `--strip-debug` keeps `.symtab`, so crash backtraces still resolve function
names and only lose file:line. Build with `--strip none` to keep the binary
byte-identical to the official image.

Both the strip and the pruning happen in the artifact stage, before the final
`COPY`. Doing either in the runtime stage would only add a whiteout layer and
the bytes would still ship.

FE is left alone: branch-4.1 has no FE plugin split, so all 634 jars in
`fe/lib` are on the startup classpath and pruning by jar name would be fragile
for little gain.

## Verifying a change to `resource/prune.txt`

`resource/smoke-test.sh <image:tag> [base|full]` starts the image, waits for
`healthy`, asserts the JNI baseline and the flavor payload are what they should
be, then exercises create / insert / stream load / aggregate / schema change.
It runs everything through `docker exec`, so the host needs only docker.

It does **not** read a real external table. To check that end of things, point
the built image at the fixtures under `docker/thirdparties` and run an Iceberg
or Hive query by hand.

## Layout

```
4.1/
├── Dockerfile                 three artifact sources -> strip + prune -> runtime
├── Dockerfile.dockerignore    keeps the repo-root context to a few KB
├── build.sh                   the only entry point you need
└── resource/
    ├── entrypoint.sh          FE -> readiness -> BE -> register -> fail-fast wait
    ├── health_check.sh        backs HEALTHCHECK
    ├── smoke-test.sh          guards prune.txt
    ├── prune.txt              what each flavor drops, and what must never be dropped
    └── conf/{fe_ci.conf,be_ci.conf}   appended to the upstream conf at build time
```
