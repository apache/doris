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

The default base uses Ubuntu 26.04 LTS. A second build entry point uses Red Hat
Universal Base Image (UBI) 9 Minimal. Both bases contain the packages needed by
the FE, BE and MS entry points, one maintained JDK, and rebuilt `doris-debug` / `dorisctl` binaries.
Development packages, kernel headers, Python 2 and general profiling tools are
not included. The `all-in-one/` and `doris-compose/` build paths use the same package-update
policy with their own runtime tools.

## Build from a release archive

Run from any directory in the checkout:

```bash
# Ubuntu 26.04
bash docker/runtime/docker-build.sh -v 4.1.5
# UBI 9
bash docker/runtime/docker-build-ubi9.sh -v 4.1.5
```

The script downloads the archive and its SHA512 checksum over HTTPS, verifies
the archive before extracting it, builds the base, and passes that same base
image to each component build. A checksum failure stops the build. The host
needs Docker with BuildKit, Bash, curl, tar, and either sha512sum or shasum.

The default working directories are `output/runtime-images/ubuntu26.04/` and
`output/runtime-images/ubi9/`. Extracted build
contexts are temporary and are removed on exit; source resource directories
are not modified. The downloaded archive and component image inspection JSON
are retained there. Output tags remain `doris.fe:<version>`,
`doris.be:<version>` and `doris.ms:<version>`. Releases without MS build only
FE and BE. UBI tags append `-ubi9`, for example `doris.be:4.1.5-ubi9`, so
building both distributions does not overwrite either image family.

| Environment variable | Purpose |
| --- | --- |
| `DORIS_ARCH` | Target `amd64` or `arm64`; defaults to the host architecture. Foreign architectures require Docker emulation. |
| `DORIS_NO_AVX2=1` | Select the x64-noavx2 release archive. A native Linux x86_64 host without AVX2 selects it automatically. |
| `DORIS_BASE_IMAGE` | Tag for the newly built base and the base consumed by all component builds. |
| `DORIS_IMAGE_PREFIX` | Component tag prefix; defaults to `doris`. |
| `DORIS_IMAGE_TAG_SUFFIX` | Override the component tag suffix (Ubuntu: empty; UBI: `-ubi9`). |
| `DORIS_IMAGE_WORK_DIR` | Download, staging and image metadata directory. |
| `DORIS_ARCHIVE` | Existing local release archive. It is still verified before extraction. |
| `DORIS_ARCHIVE_SHA512` | Expected 128-character digest from a trusted release checksum. When omitted, download the release checksum document. |
| `DORIS_DOWNLOAD_BASE_URL` | HTTPS release mirror; defaults to the Apache Doris release mirror. |
| `UBUNTU_IMAGE`, `JDK17_IMAGE`, `JDK8_IMAGE`, `GO_IMAGE` | Ubuntu and shared build input overrides, for example an approved mirror with the same digest. |
| `UBI9_IMAGE`, `JDK17_UBI9_IMAGE`, `JDK8_UBI9_IMAGE` | UBI Minimal and Temurin UBI 9 input overrides. |

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
`apache/doris:base-8.0-ubuntu26.04-jdk17` and
`apache/doris:base-8.0-ubuntu26.04-jdk8`; the UBI equivalents replace
`ubuntu26.04` with `ubi9`. Each image
contains only the selected JDK, at `/usr/lib/jvm/java`, with a compatibility
link at `/usr/lib/jvm/jdk-17` or `/usr/lib/jvm/jdk-8`.

Build a base independently:

```bash
docker build --pull --no-cache \
    --build-arg JAVA_VERSION=17 \
    -f docker/runtime/base-image/Dockerfile_base \
    -t apache/doris:base-8.0-ubuntu26.04-jdk17 docker/runtime/base-image
```

The same Dockerfile shares the JDK selection and operator-tool build between
both distributions. To build a UBI base independently:

```bash
docker build --pull --no-cache \
    --build-arg RUNTIME_DISTRO=ubi9 --build-arg JAVA_VERSION=17 \
    -f docker/runtime/base-image/Dockerfile_base \
    -t apache/doris:base-8.0-ubi9-jdk17 docker/runtime/base-image
```

Pass that UBI base through `BASE_IMAGE` for manual component builds.

The default base is already minimal. `Dockerfile_base_slim` is a compatibility
alias of that base, rather than a separate package list:

```bash
docker build --build-arg BASE_IMAGE=apache/doris:base-8.0-ubuntu26.04-jdk17 \
    -f docker/runtime/base-image/Dockerfile_base_slim \
    -t apache/doris:base-8.0-ubuntu26.04-jdk17-slim docker/runtime/base-image
```

For manual component builds, extract the release under
`<role>/resource/<arch>/apache-doris-<version>-bin-<release-arch>/<role>`.
For example:

```bash
docker build --build-arg DORIS_VERSION=4.1.5 \
    --build-arg BASE_IMAGE=apache/doris:base-8.0-ubuntu26.04-jdk17 \
    -f docker/runtime/fe/Dockerfile -t doris.fe:4.1.5 docker/runtime/fe
```

Manual builds default to Java 17. For an older release, build the Java 8 base
and pass `JAVA_VERSION=8` or its explicit `BASE_IMAGE` to the component build.
Do not use `--pull` on a component build that consumes a locally built base.

## Runtime changes

- The MySQL CLI comes from Ubuntu's `mysql-client-core` (MySQL 8.4) or
  UBI's `mysql` package (MySQL 8.0). Both retain the TLS options used by
  the entry points. UBI keeps `curl-minimal` to avoid conflicting with the
  preinstalled curl package.
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

`base-image/Dockerfile_base` pins the Ubuntu, UBI 9, Temurin 17, Temurin 8 and Go
multi-platform image digests, plus the doris-operator commit and source archive
SHA256. Update version and digest together, and update the source checksum
when changing the operator commit. Go's module checksum verification remains
enabled. The build keeps these inventories in each runtime image:

- `/usr/share/doris/runtime-packages.tsv`: installed OS package versions.
- `/usr/share/doris/debug-tools.buildinfo`: compiled Go versions and modules.

APT uses HTTPS and package signature verification. The base build upgrades APT
first, upgrades all retained packages, and checks the minimum APT and OpenSSL
patch versions for each supported Ubuntu release. The installer supports an
explicit Ubuntu 22.04/Jammy override as well as the 26.04 default. When using
Jammy, override the Ubuntu and JDK inputs together; for all-in-one or compose,
override its Temurin input. UBI uses a separate `microdnf` installer, updates
retained RPMs, and enables only the public UBI BaseOS and AppStream repositories.
The UBI installer restores the zoneinfo files removed from the minimal image,
which BE needs at startup. Its inventory records RPM epoch, version, release
and architecture. Cache
cleanup alone does not apply security patches.

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

## Host and binary compatibility

These are runtime images: the build scripts package existing Doris binaries
and do not change their compiler or glibc baseline. The host uses the image's
user-space libraries but supplies the kernel and CPU. A newer image can still
require newer kernel interfaces or container-runtime syscall policies.

UBI 9 on x86_64 requires **x86-64-v2**, including when selecting a Doris
`x64-noavx2` archive. A no-AVX2 archive does not remove the UBI requirement.
Ubuntu uses the ordinary `amd64` variant; this build does not opt into
`amd64v3` packages. UBI 9's glibc 2.34 can be older than the environment where
a custom Doris or JNI binary was built. Check all shipped native libraries,
including JNI and FoundationDB, against the selected runtime.

Before changing an existing deployment, validate its CPU features, kernel,
Docker/containerd/runc and seccomp versions, and cgroup configuration. The
Ubuntu 26.04 change in coreutils also requires exercising the shell entry
points. Retain the previous 22.04 images for deployments whose host baseline
has not been validated and for rollback. Do not repoint the old base-7.0 tags.
