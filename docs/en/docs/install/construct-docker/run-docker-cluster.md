---
{
    "title": "Deploy Docker Cluster",
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
# Deploy the Docker Cluster

## Background Description

This article will briefly describe how to quickly build a complete Doris test cluster through `docker run` or `docker-compose up` commands.

## Applicable scene

It is recommended to use Doris Docker in SIT or DEV environment to simplify the deployment process.

If you want to test a certain function point in the new version, you can use Doris Docker to deploy a Playground environment. Or when you want to reproduce a certain problem during debugging, you can also use the docker environment to simulate.

In the production environment, currently try to avoid using containerized solutions for Doris deployment.

## Software Environment

| Software | Version |
| -------------- | ----------- |
| Docker | 20.0 and above |
| docker-compose | 2.10 and above |

## Hardware Environment

| Configuration Type | Hardware Information | Maximum Running Cluster Size |
| -------- | -------- | ---------------- |
| Minimum configuration | 2C 4G | 1FE 1BE |
| Recommended configuration | 4C 16G | 3FE 3BE |

## Pre-Environment Preparation

The following command needs to be executed on the host machine

```shell
sysctl -w vm.max_map_count=2000000
```

## Docker Compose

Different platforms need to use different Image images. This article takes the `X86_64` platform as an example.

### Network Mode Description

There are two network modes applicable to Doris Docker.

1. HOST mode suitable for deployment across multiple nodes, this mode is suitable for deploying 1FE 1BE on each node.
2. The subnet bridge mode is suitable for deploying multiple Doris processes on a single node. This mode is suitable for single-node deployment (recommended). If you want to deploy multiple nodes, you need to deploy more components (not recommended).

For the sake of presentation, this chapter only demonstrates scripts written in subnet bridge mode.

### Interface Description

From the version of `Apache Doris 1.2.1 Docker Image`, the interface list of each process image is as follows:

| process name | interface name | interface definition | interface example |
|--------------|----------------|---------------|------------------|
| FE           | BE             | BROKER | FE_SERVERS | FE node main information | fe1:172.20.80.2:9010,fe2:172.20.80.3:9010,fe3:172.20.80.4:9010 |
| FE           | FE_ID          | FE node ID | 1 |
| BE           | BE_ADDR        | BE node main information | 172.20.80.5:9050 |
| BE           | NODE_ROLE      | BE node type | computation |
| BROKER       | BROKER_ADDR    | Main information of BROKER node | 172.20.80.6:8000 |

Note that the above interface must fill in the information, otherwise the process cannot be started.

> FE_SERVERS interface rules are: `FE_NAME:FE_HOST:FE_EDIT_LOG_PORT[,FE_NAME:FE_HOST:FE_EDIT_LOG_PORT]`
>
> The FE_ID interface rule is: an integer of `1-9`, where the FE number `1` is the Master node.
>
> BE_ADDR interface rule is: `BE_HOST:BE_HEARTBEAT_SERVICE_PORT`
>
> The NODE_ROLE interface rule is: `computation` or empty, where empty or other values indicate that the node type is `mix` type
>
> BROKER_ADDR interface rule is: `BROKER_HOST:BROKER_IPC_PORT`

### Script Template

#### Docker Run Command

1FE & 1BE Command Templates

Note that you need to modify `${intranet IP of the current machine}` to replace it with the intranet IP of the current machine

```shell
docker run -itd \
--name=fe \
--env FE_SERVERS="fe1:${intranet IP of the current machine}:9010" \
--env FE_ID=1 \
-p 8030:8030\
-p 9030:9030 \
-v /data/fe/doris-meta:/opt/apache-doris/fe/doris-meta \
-v /data/fe/log:/opt/apache-doris/fe/log \
--net=host \
apache/doris:2.0.0_alpha-fe-x86_64

docker run -itd \
--name=be\
--env FE_SERVERS="fe1:${intranet IP of the current machine}:9010" \
--env BE_ADDR="${Intranet IP of the current machine}:9050" \
-p 8040:8040 \
-v /data/be/storage:/opt/apache-doris/be/storage \
-v /data/be/log:/opt/apache-doris/be/log \
--net=host \
apache/doris:2.0.0_alpha-be-x86_64
```

3FE & 3BE Run command template if needed [click here](https://github.com/apache/doris/tree/master/docker/runtime/docker-compose-demo/build-cluster/rum-command/3fe_3be .sh) to access downloads.

#### Docker Compose Script

1FE & 1BE template

Note that you need to modify `${intranet IP of the current machine}` to replace it with the intranet IP of the current machine

```yaml
version: "3"
services:
   fe:
     image: apache/doris:2.0.0_alpha-fe-x86_64
     hostname: fe
     environment:
      - FE_SERVERS=fe1:${intranet IP of the current machine}:9010
      - FE_ID=1
     volumes:
      - /data/fe/doris-meta/:/opt/apache-doris/fe/doris-meta/
      - /data/fe/log/:/opt/apache-doris/fe/log/
     network_mode: host
   be:
     image: apache/doris:2.0.0_alpha-be-x86_64
     hostname: be
     environment:
      - FE_SERVERS=fe1:${intranet IP of the current machine}:9010
      - BE_ADDR=${intranet IP of the current machine}:9050
     volumes:
      - /data/be/storage/:/opt/apache-doris/be/storage/
      - /data/be/script/:/docker-entrypoint-initdb.d/
     depends_on:
       -fe
     network_mode: host
```

3FE & 3BE Docker Compose script template if needed [click here](https://github.com/apache/doris/tree/master/docker/runtime/docker-compose-demo/build-cluster/docker-compose/ 3fe_3be/docker-compose.yaml) access to download.

## Deploy Doris Docker

You can choose one of the two deployment methods:

1. Execute the `docker run` command to create a cluster
2. Save the `docker-compose.yaml` script and execute the `docker-compose up -d` command in the same directory to create a cluster

### Special Case Description

Due to the different ways of implementing containers internally on MacOS, it may not be possible to directly modify the value of `max_map_count` on the host during deployment. You need to create the following containers first:

```shel
docker run -it --privileged --pid=host --name=change_count debian nsenter -t 1 -m -u -n -i sh
```

The container was created successfully executing the following command:

```shell
sysctl -w vm.max_map_count=2000000
```

Then `exit` exits and creates the Doris Docker cluster.
