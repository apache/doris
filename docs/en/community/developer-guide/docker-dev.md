---
{
    "title": "Doris Docker quick build development environment",
    "language": "en"
}

---

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

# Doris Docker quick build development environment

## Related detailed document navigation

- [Developing mirror compilation using Docker](../../docs/install/source-install/compilation.md#developing-mirror-compilation-using-docker-recommended)
- [Deploying Doris](../../docs/install/install-deploy.md#cluster-deployment)
- [VSCode Be Development Debugging](./be-vscode-dev.md)

## Environment preparation

- Install Docker
- VSCode
    - Remote plugin

## Run image

```bash
$ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apache/incubator-doris:build-env-ldb-toolchain-latest
```

note! [problems with mounting](../../docs/install/source-install/compilation.md)

> See the link above: It is recommended to run the image by mounting the local Doris source code directory as a volume .....
if you are developing on windows, mounting may cause cross-filesystem access problems, please consider setting it manually

create directory and download doris

```bash
su <your user>
mkdir code && cd code
git clone https://github.com/apache/doris.git
```

## Compile

```bash
sh build.sh
```

## Run

manually create `meta_dir` metadata storage location, default value is `${DORIS_HOME}/doris-meta`

```bash
mkdir meta_dir
```

launch FE

```bash
cd output/fe
sh bin/start_fe.sh --daemon
```

launch BE

```bash
cd output/be
sh bin/start_be.sh --daemon
```

mysql-client connect

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```
