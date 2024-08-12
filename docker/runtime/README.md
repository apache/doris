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

## How to build a docker image

### Preparation 

#### Download and copy the Doris code repo

```console
$ cd /to/your/workspace/
$ git clone https://github.com/apache/doris.git
$ cd doris/docker
$ cp -r runtime/ /to/your/workspace/
```

After preparation, your workspace should like this:

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

#### Download, unzip and copy the antidote package

Here we take the build of x64 (avx2) and arm64 platforms of 2.1.5 version as an example.

1. Download binary

Go to the doris [official website](https://doris.apache.org/download) to download the binary package you need, and pay attention to selecting the doris version and architecture type you need.
  
```console
$ wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-2.1.5-bin-x64.tar.gz
# or
$ wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-2.1.5-bin-arm64.tar.gz
```
   
2. Unzip binary package

```console
$ tar -zxvf apache-doris-2.1.5-bin-x64.tar.gz
# or
$ tar -zxvf apache-doris-2.1.5-bin-arm64.tar.gz
```

3. copy the antidote package

You need to copy the corresponding directories to different files in sequence as shown in the table:

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



### Base image

**NOTICE**

The below images depend on the base image selectdb/base. If your environment cannot access it, you can pre-build the base image. The base image contains the basic environment for doris to run, including JDK, openssl, etc.

#### Build base image

As mentioned in the preparation steps above, the Dockerfile of the Base image is under the runtime/base-image path. To build it, make sure you have pulled the Doris code repo and execute the following command.

```console
$ cd /to/your/workspace/runtime/base-image && docker build . -t doris-base:latest -f Dockerfile_base
```

#### Adjust Dockerfile
Adjust the base image name used by the Dockerfile of the doris component and replace the base image you built yourself, As shown in the following example:

```dockerfile
...
# Adjust the base image here
FROM doris-base:latest:latest

ARG TARGETARCH

ARG DORIS_VERSION="x.x.x"
...
```


### Build Doris docker image

Execute the following command, Docker will automatically confirm the architecture type

```console
$ cd /to/your/workspace/runtime/fe && docker build . -t doris.fe:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
$ cd /to/your/workspace/runtime/be && docker build . -t doris.be:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
$ cd /to/your/workspace/runtime/ms && docker build . -t doris.ms:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
$ cd /to/your/workspace/runtime/broker && docker build . -t doris.broker:2.1.5 -f Dockerfile --build-arg DORIS_VERSION=2.1.5 
```

### Latest update time

2024-8-12
