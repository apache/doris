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

## Building the Docker image

### Download and copy the Doris code repo

clone [Doris code repo](https://github.com/apache/doris.git) and enter the doris/docker/runtime path.
your workspace should like this:

```
.
├── runtime
│   ├── README.md
│   ├── all-in-one
│   ├── base-image
│   ├── be
│   ├── broker
│   ├── docker-compose-demo
│   ├── doris-compose
│   ├── fe
│   ├── ms
```

### Download, extract archive and copy into resource directories

1. Go to the doris [official website](https://doris.apache.org/download) to download the binary package you need(pay attention to selecting the doris version and architecture you need), and extract archive binary

Here we take the build of x64 (avx2) and arm64 platforms of 3.0.2 version as an example.

```shell
$ wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.2-bin-x64.tar.gz && tar -zxvf apache-doris-3.0.2-bin-x64.tar.gz
# or
$ wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.2-bin-arm64.tar.gz && tar -zxvf apache-doris-3.0.2-bin-arm64.tar.gz
```

2. You need to copy the corresponding directories to different directories in sequence as below in the table:

amd64(avx2) platform: 

| doris type |                      doris package                       |                                    docker file path                                    |
|:----------:|:--------------------------------------------------------:|:--------------------------------------------------------------------------------------:|
|     fe     |              apache-doris-3.0.2-bin-x64/fe               |                runtime/fe/resource/amd64/apache-doris-3.0.2-bin-x64/fe                 |
|     be     |              apache-doris-3.0.2-bin-x64/be               |                runtime/be/resource/amd64/apache-doris-3.0.2-bin-x64/be                 |
|     ms     |              apache-doris-3.0.2-bin-x64/ms               |                runtime/ms/resource/amd64/apache-doris-3.0.2-bin-x64/ms                 |

arm64 platform:

| doris type |                       doris package                        |                                     docker file path                                     |
|:----------:|:----------------------------------------------------------:|:----------------------------------------------------------------------------------------:|
|     fe     |              apache-doris-3.0.2-bin-arm64/fe               |                runtime/fe/resource/arm64/apache-doris-3.0.2-bin-arm64/fe                 |
|     be     |              apache-doris-3.0.2-bin-arm64/be               |                runtime/be/resource/arm64/apache-doris-3.0.2-bin-arm64/be                 |
|     ms     |              apache-doris-3.0.2-bin-arm64/ms               |                runtime/ms/resource/arm64/apache-doris-3.0.2-bin-arm64/ms                 |

**NOTICE**

Only after doris 3.0, the storage-computing separation mode requires the ms (meta-service) component.

### Build base image

**NOTICE**

The below images depend on the base image `apache/doris:base-latest`. If your environment cannot access it, you can pre-build the base image. The base image contains the basic environment for doris to run, including JDK, openssl, etc.

1. As mentioned in the preparation steps above, the Dockerfile of the Base image is under the runtime/base-image path. To build it, make sure you have pulled the Doris code repo and execute the following command.

```shell
$ cd doris/runtime/base-image && docker build . -t doris-base:latest -f Dockerfile_base
```

2. Adjust the base image name used by the Dockerfile of the doris component and replace the base image you built yourself, As shown in the following example:

```dockerfile
# Adjust the base image here
FROM doris-base:latest:latest

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"

```

### Build Doris docker image

as the following commands, Docker will automatically confirm the architecture

```shell
$ cd doris/runtime/fe && docker build . -t doris.fe:3.0.2 -f Dockerfile --build-arg DORIS_VERSION=3.0.2 
$ cd doris/runtime/be && docker build . -t doris.be:3.0.2 -f Dockerfile --build-arg DORIS_VERSION=3.0.2 
$ cd doris/runtime/ms && docker build . -t doris.ms:3.0.2 -f Dockerfile --build-arg DORIS_VERSION=3.0.2 
```

### Latest update time

2025-02-12

## Running FE, BE and Meta Service without root

The standalone FE, BE and MS images run as `doris` (`10001:10001`) by default.
The MS image also runs Recycler under this identity. Build arguments
`DORIS_UID` and `DORIS_GID` customize these IDs. Image-owned configuration,
logs, metadata and storage directories are writable by this user. The SQL
administrator account is independent of the operating-system user. The image
provides `/etc/krb5.conf` as a link into the writable Doris configuration
directory, so Operator-mounted Kerberos configuration does not require writes
to `/etc` at startup.

The `docker-build.sh` helper forwards `DORIS_UID` and `DORIS_GID` environment
variables to all three image builds. Use the same IDs in the image and the
Kubernetes configuration, and keep them stable across releases.

For Kubernetes, configure each component's Pod security context, for example:

```yaml
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
```

Use the container security context to set `privileged: false`,
`allowPrivilegeEscalation: false` and drop all capabilities. With Doris Operator,
set BE `enableWorkloadGroup`
to `false` and `skipDefaultSystemInit` to `true` to avoid privileged cgroup
setup and the default system init container. Check the generated Pod to
confirm these settings; custom init containers must also be omitted.

With Operator 26.0.1, these settings are available without changing the
Operator. For a `DorisCluster`, set `feSpec.securityContext` and
`beSpec.securityContext`, and the corresponding `containerSecurityContext`
fields. For a `DorisDisaggregatedCluster`, configure `metaService`, `feSpec`
and every `computeGroups` entry. Set `skipDefaultSystemInit: true` and
`enableWorkloadGroup: false` on each BE/compute group. The Operator switch
controls cgroup setup; it does not disable query admission or memory controls.

For MS, `doris_cloud.conf` from a ConfigMap is copied into a writable file
before applying `FDB_ENDPOINT`. Without that environment variable, the
configured `fdb_cluster` is preserved. The same entrypoint supports the
packaged default (Meta Service and Recycler) or explicit `--meta-service`
and `--recycler` arguments. FoundationDB and object storage are external
dependencies with their own container-user and storage requirements.

New volumes still need to be writable: image ownership does not override a
mounted volume's permissions. Validate the actual StorageClass with a fresh
PVC. A successful deployment with local-path storage is not proof that every
CSI driver supports the same permissions. The non-root configuration can be
validated in a namespace enforcing the Kubernetes `restricted` Pod Security
Standard; a storage provisioner's infrastructure is managed separately.

Skipping system initialization does not tune the host. Doris recommends
`vm.max_map_count >= 2000000`, disabled swap and a sufficient file-descriptor
limit. A non-root image cannot change these host settings. Operator versions
that set `SKIP_CHECK_ULIMIT=true` bypass startup checks, but this does not
remove the underlying kernel limits or their effects under load. Persistent
volumes must be writable by the configured UID/GID; `fsGroup` requires volume
driver support. Existing root-owned data needs its permissions prepared
separately. Custom log, cache, Kerberos and keytab paths must also be readable
or writable as required by Doris. A read-only container root filesystem is
not implied by running without root.

Without a writable, delegated CPU cgroup, Workload Group CPU weights and CPU
hard limits are unavailable. Leave `doris_cgroup_cpu_path` unset in this mode.
Other Workload Group controls, including query admission and memory limits,
remain available; Kubernetes container CPU limits continue to apply.
