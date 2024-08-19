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

Here we take the build of x64 (avx2) and arm64 platforms of 2.1.5 version as an example.

```shell
$ wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-2.1.5-bin-x64.tar.gz && tar -zxvf apache-doris-2.1.5-bin-x64.tar.gz
# or
$ wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-2.1.5-bin-arm64.tar.gz && tar -zxvf apache-doris-2.1.5-bin-arm64.tar.gz
```

2. You need to copy the corresponding directories to different directories in sequence as below in the table:

amd64(avx2) platform: 

| doris type |                      doris package                       |                                    docker file path                                    |
|:----------:|:--------------------------------------------------------:|:--------------------------------------------------------------------------------------:|
|     fe     |              apache-doris-2.1.5-bin-x64/fe               |                runtime/fe/resource/amd64/apache-doris-2.1.5-bin-x64/fe                 |
|     be     |              apache-doris-2.1.5-bin-x64/be               |                runtime/be/resource/amd64/apache-doris-2.1.5-bin-x64/be                 |
|     ms     |              apache-doris-2.1.5-bin-x64/ms               |                runtime/ms/resource/amd64/apache-doris-2.1.5-bin-x64/ms                 |
|   broker   | apache-doris-2.1.5-bin-x64/extensions/apache_hdfs_broker | runtime/broker/resource/amd64/apache-doris-2.1.5-bin-x64/extensions/apache_hdfs_broker |

arm64 platform:

| doris type |                       doris package                        |                                     docker file path                                     |
|:----------:|:----------------------------------------------------------:|:----------------------------------------------------------------------------------------:|
|     fe     |              apache-doris-2.1.5-bin-arm64/fe               |                runtime/fe/resource/arm64/apache-doris-2.1.5-bin-arm64/fe                 |
|     be     |              apache-doris-2.1.5-bin-arm64/be               |                runtime/be/resource/arm64/apache-doris-2.1.5-bin-arm64/be                 |
|     ms     |              apache-doris-2.1.5-bin-arm64/ms               |                runtime/ms/resource/arm64/apache-doris-2.1.5-bin-arm64/ms                 |
|   broker   | apache-doris-2.1.5-bin-arm64/extensions/apache_hdfs_broker | runtime/broker/resource/arm64/apache-doris-2.1.5-bin-arm64/extensions/apache_hdfs_broker |

### Build base image

**NOTICE**

The below images depend on the base image selectdb/base. If your environment cannot access it, you can pre-build the base image. The base image contains the basic environment for doris to run, including JDK, openssl, etc.

1. As mentioned in the preparation steps above, the Dockerfile of the Base image is under the runtime/base-image path. To build it, make sure you have pulled the Doris code repo and execute the following command.

```shell
$ cd doris/runtime/base-image && docker build . -t doris-base:latest -f Dockerfile_base
```

2. Adjust the base image name used by the Dockerfile of the doris component and replace the base image you built yourself, As shown in the following example:

```dockerfile
...
# Adjust the base image here
FROM doris-base:latest:latest

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"
...
```

### Build Doris docker image

as the following commands, Docker will automatically confirm the architecture

```shell
$ cd doris/runtime/fe && docker build . -t doris.fe:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
$ cd doris/runtime/be && docker build . -t doris.be:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
$ cd doris/runtime/ms && docker build . -t doris.ms:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
$ cd doris/runtime/broker && docker build . -t doris.broker:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
```

### Latest update time

2024-8-12
