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

# Building the Doris runtime images

The default base image contains the packages needed by the FE, BE and MS entry
points, one maintained JDK, and rebuilt `doris-debug` / `dorisctl` binaries.
Development packages, kernel headers, Python 2 and general profiling tools are
not included. The `all-in-one/` and `doris-compose/` build paths are maintained
separately.

## Build from a release archive

Run from any directory in the checkout:

```bash
bash docker/runtime/docker-build.sh -v 4.1.5
```

The script downloads the archive and its SHA512 checksum over HTTPS, verifies
the archive before extracting it, builds the base, and passes that same base
image to each component build. A checksum failure stops the build. The host
needs Docker with BuildKit, Bash, curl, tar, and either sha512sum or shasum.

The default working directory is `output/runtime-images/`. Extracted build
contexts are temporary and are removed on exit; source resource directories
are not modified. The downloaded archive and component image inspection JSON
are retained there. Output tags remain `doris.fe:<version>`,
`doris.be:<version>` and `doris.ms:<version>`. Releases without MS build only
FE and BE.

| Environment variable | Purpose |
| --- | --- |
| `DORIS_ARCH` | Target `amd64` or `arm64`; defaults to the host architecture. Foreign architectures require Docker emulation. |
| `DORIS_NO_AVX2=1` | Select the x64-noavx2 release archive. A native Linux x86_64 host without AVX2 selects it automatically. |
| `DORIS_BASE_IMAGE` | Tag for the newly built base and the base consumed by all component builds. |
| `DORIS_IMAGE_PREFIX` | Component tag prefix; defaults to `doris`. |
| `DORIS_IMAGE_WORK_DIR` | Download, staging and image metadata directory. |
| `DORIS_ARCHIVE` | Existing local release archive. It is still verified before extraction. |
| `DORIS_ARCHIVE_SHA512` | Expected 128-character digest from a trusted release checksum. When omitted, download the release checksum document. |
| `DORIS_DOWNLOAD_BASE_URL` | HTTPS release mirror; defaults to the Apache Doris release mirror. |
| `UBUNTU_IMAGE`, `JDK17_IMAGE`, `JDK8_IMAGE`, `GO_IMAGE` | Base-build input overrides, for example an approved mirror with the same digest. |

For an existing archive, obtain its expected digest from the trusted release
metadata, independently of the local file:

```bash
DORIS_ARCHIVE=/path/to/apache-doris-4.1.5-bin-arm64.tar.gz \
DORIS_ARCHIVE_SHA512=<expected-release-sha512> \
DORIS_ARCH=arm64 \
bash docker/runtime/docker-build.sh -v 4.1.5
```

## Base images and Java compatibility

The build script keeps the previous Java selection: Doris 3.x and newer use
Java 17; older releases use Java 8. The base tags are
`apache/doris:base-7.0-jdk17` and `apache/doris:base-7.0-jdk8`. Each image
contains only the selected JDK, at `/usr/lib/jvm/java`, with a compatibility
link at `/usr/lib/jvm/jdk-17` or `/usr/lib/jvm/jdk-8`.

Build a base independently:

```bash
docker build --pull --no-cache \
    --build-arg JAVA_VERSION=17 \
    -f docker/runtime/base-image/Dockerfile_base \
    -t apache/doris:base-7.0-jdk17 docker/runtime/base-image
```

The default base is already minimal. `Dockerfile_base_slim` is a compatibility
alias of that base, rather than a separate package list:

```bash
docker build --build-arg BASE_IMAGE=apache/doris:base-7.0-jdk17 \
    -f docker/runtime/base-image/Dockerfile_base_slim \
    -t apache/doris:base-7.0-jdk17-slim docker/runtime/base-image
```

For manual component builds, extract the release under
`<role>/resource/<arch>/apache-doris-<version>-bin-<release-arch>/<role>`.
For example:

```bash
docker build --build-arg DORIS_VERSION=4.1.5 \
    --build-arg BASE_IMAGE=apache/doris:base-7.0-jdk17 \
    -f docker/runtime/fe/Dockerfile -t doris.fe:4.1.5 docker/runtime/fe
```

Manual builds default to Java 17. For an older release, build the Java 8 base
and pass `JAVA_VERSION=8` or its explicit `BASE_IMAGE` to the component build.
Do not use `--pull` on a component build that consumes a locally built base.

## Runtime changes

- The MySQL CLI comes from `mysql-client-core-8.0`; its TLS options used by
  the entry points remain available.
- The full JDK retains tools such as `jcmd`. The vendor and patch update can
  change certificate trust and disabled algorithms; validate TLS, Kerberos and
  BE JNI integrations before rollout.
- The runtime does not contain gdb, perf, BCC, iperf3, patchelf, vim, development
  headers or Python 2. Use a separate diagnostic container when needed.
- FE images omit Arthas and its bundled dependencies.
- Both operator tools remain at `/opt/apache-doris/`. They are compiled from
  the pinned doris-operator source with the updated Go toolchain, x/net and edwards25519;
  Operator debug mode and compute-group discovery retain their commands.

## MySQL TLS certificates

FE images no longer contain `fe/mysql_ssl_default_certificate/`. This removes
the shared example private keys before the final image layer is created.
MySQL SSL remains disabled by default.

Deployments enabling MySQL SSL must mount their own server keystore and CA
truststore and set these existing FE configuration properties:

```properties
mysql_ssl_default_server_certificate = /run/secrets/mysql-server.p12
mysql_ssl_default_ca_certificate = /run/secrets/mysql-ca.p12
mysql_ssl_default_server_certificate_password = <server-keystore-password>
mysql_ssl_default_ca_certificate_password = <truststore-password>
```

Set the passwords to match the mounted stores. The CA truststore only needs
the trusted certificates, not a CA private key. Do not use the removed example
certificates as production credentials.

## Updating and verifying dependencies

`base-image/Dockerfile_base` pins the Ubuntu, Temurin 17, Temurin 8 and Go
multi-platform image digests, plus the doris-operator commit and source archive
SHA256. Update version and digest together, and update the source checksum
when changing the operator commit. Go's module checksum verification remains
enabled. The build keeps these inventories in each runtime image:

- `/usr/share/doris/runtime-packages.tsv`: installed OS package versions.
- `/usr/share/doris/debug-tools.buildinfo`: compiled Go versions and modules.

APT uses HTTPS and package signature verification. The base build upgrades APT
first, upgrades all retained packages, and checks the minimum APT and OpenSSL
patch versions. Cache cleanup alone does not apply security patches.

Refresh the pinned inputs regularly and rebuild the base with
`--pull --no-cache`; then rebuild every component against it. Rebuilding a
component against an old base does not update the base's packages. Inspect and
scan each resulting image by its recorded digest.

Run the release verification tests without Docker or network:

```bash
python3 docker/runtime/tests/test_docker_build.py -v
```

Inspect every shipped layer of a built image for removed artifacts:

```bash
python3 docker/runtime/tests/check_image_layers.py doris.fe:4.1.5
```

Before release, build both supported architectures and Java variants, scan
the resulting images including Go binaries, and exercise FE/BE/MS startup,
Operator debug mode, compute-group discovery, TLS and BE JNI. Check final
image layers for the removed private keys. Findings in Doris application jars,
including shaded dependencies, require separate dependency updates; an OS or
JDK upgrade does not replace them.
