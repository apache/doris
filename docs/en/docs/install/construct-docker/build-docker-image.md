---
{
"title": "Build Docker Image",
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

# Build Docker Image 

This topic is about how to build a running image of Apache Doris through Dockerfile, so that an Apache Doris image can be quickly pulled in a container orchestration tool or during a quick test to complete the cluster creation.

## Software and Hardware Requirements

### Overview

Prepare the production machine before building a Docker image. The platform architecture of the Docker image will be the same as that of the machine. For example, if you use an X86_64 machine to build a Docker image, you need to download the Doris binary program of X86_64, and the Docker image built can only run on the X86_64 platform. The ARM platform (or M1), likewise.

### Hardware Requirements

Minimum configuration: 2C 4G

Recommended configuration: 4C 16G

### Software Requirements

Docker Version: 20.10 or newer

## Build Docker Image

During Dockerfile scripting, please note that:
> 1. Use the official OpenJDK image certified by Docker-Hub as the base parent image (Version: JDK 1.8).
> 2. Use the official binary package for download; do not use binary packages from unknown sources.
> 3. Use embedded scripts for tasks such as FE startup, multi-FE registration, FE status check, BE startup, registration of BE to FE, and BE status check.
> 4. Do not use `--daemon`  to start applications in Docker. Otherwise there will be exceptions during the deployment of orchestration tools such as K8S.

Apache Doris 1.2 and the subsequent versions support JavaUDF, so you also need a JDK environment for BE. The recommended images are as follows:

| Doris Program | Recommended Base Parent Image |
| ---------- | ----------------- |
| Frontend | openjdk:8u342-jdk |
| Backend | openjdk:8u342-jdk |
| Broker | openjdk:8u342-jdk |

### Script Preparation

In the Dockerfile script for compiling the Docker Image, there are two methods to load the binary package of the Apache Doris program:

1. Execute the download command via wget / curl when compiling, and then start the docker build process.
2. Download the binary package to the compilation directory in advance, and then load it into the docker build process through the ADD or COPY command.

Method 1 can produce a smaller Docker image, but if the docker build process fails, the download operation might be repeated and result in longer build time; Method 2 is more suitable for less-than-ideal network environments.

**The examples below are based on Method 2. If you prefer to go for Method 1, you may modify the steps accordingly.**

### Prepare Binary Package

Please noted that if you have a need for custom development, you need to modify the source code, [compile](../source-install/compilation-general.md) and package it, and then place it in the build directory.

If you have no such needs, you can just [download](https://doris.apache.org/download) the binary package from the official website.
### Steps

#### Build FE

The build environment directory is as follows:

```sql
└── docker-build                                       // build root directory
     └── fe                                            // FE build directory
         ├── Dockerfile                                // Dockerfile script
         └── resource                                  // resource directory
             ├── init_fe.sh                            // startup and registration script
             └── apache-doris-x.x.x-bin-x64.tar.gz     // binary package
```
1. Create a build environment directory

   ```shell
   mkdir -p ./docker-build/fe/resource
   ```

2. Download [official binary package](https://doris.apache.org/download)/compiled binary package

   Copy the binary package to the `./docker-build/fe/resource` directory

3. Write the Dockerfile script for FE

   You can refer to [Dockerfile](https://github.com/apache/doris/tree/master/docker/runtime/fe/Dockerfile).

   After writing, name it `Dockerfile` and save it to the `./docker-build/fe` directory.

4. Write the execution script of FE

   You can refer to [init_fe.sh](https://github.com/apache/doris/tree/master/docker/runtime/fe/resource/init_fe.sh).

   After writing, name it `init_fe.sh` and save it to the `./docker-build/fe/resouce` directory.

5. Execute the build

   Please note that `${fe-tagName}` needs to be replaced with the tag name you want to package and name, such as: `apache-doris:2.0.1-fe`

   Build FE:

   ```shell
   cd ./docker-build/fe
   docker build . -t ${fe-tagName}
   ```
   
   After the build process is completed, you will see the prompt  `Success`. Then, you can check the built image using the following command.

   ```shell
   docker images
   ```

#### Build BE

1. Create a build environment directory

```shell
mkdir -p ./docker-build/be/resource
```
2. The build environment directory is as follows:

   ```sql
   └── docker-build                                            // build root directory
       └── be                                                  // BE build directory
           ├── Dockerfile                                      // Dockerfile script
           └── resource                                        // resource directory
               ├── entry_point.sh                              // entry point script
               ├── init_be.sh                                  // startup and registration script
               └── apache-doris-x.x.x-bin-x64.tar.gz           // binary package
   ```

3. Write the Dockerfile script for BE

   You can refer to [Dockerfile](https://github.com/apache/doris/tree/master/docker/runtime/be/Dockerfile).

   After writing, name it `Dockerfile` and save it to the `./docker-build/be` directory

4. Write the execution script of BE

   You can refer to [init_be.sh](https://github.com/apache/doris/tree/master/docker/runtime/be/resource/init_be.sh).

   After writing, name it `init_be.sh` and save it to the `./docker-build/be/resouce` directory.

5. Execute the build

   Please note that `${be-tagName}` needs to be replaced with the tag name you want to package and name, such as: `apache-doris:2.0.1-be`

   Build BE:

   ```shell
   cd ./docker-build/be
   docker build . -t ${be-tagName}
   ```

   After the build process is completed, you will see the prompt  `Success`. Then, you can check the built image using the following command.

   ```shell
   docker images
   ```
#### Build Broker

1. Create a build environment directory

```shell
mkdir -p ./docker-build/broker/resource
```

2. The build environment directory is as follows:

   ```sql
   └── docker-build                                     // build root directory
       └── broker                                       // BROKER build directory
           ├── Dockerfile                               // Dockerfile script
           └── resource                                 // resource directory
               ├── init_broker.sh                       // startup and registration script
               └── apache-doris-x.x.x-bin-x64.tar.gz    // binary package
   ```

3. Write the Dockerfile script for Broker

   You can refer to [Dockerfile](https://github.com/apache/doris/tree/master/docker/runtime/broker/Dockerfile).

   After writing, name it `Dockerfile` and save it to the `./docker-build/broker` directory

4. Write the execution script of BE

   You can refer to [init_broker.sh](https://github.com/apache/doris/tree/master/docker/runtime/broker/resource/init_broker.sh).

   After writing, name it `init_broker.sh` and save it to the `./docker-build/broker/resouce` directory.

5. Execute the build

   Please note that `${broker-tagName}` needs to be replaced with the tag name you want to package and name, such as: `apache-doris:2.0.1-broker`

   Build Broker:

   ```shell
   cd ./docker-build/broker
   docker build . -t ${broker-tagName}
   ```

   After the build process is completed, you will see the prompt  `Success`. Then, you can check the built image using the following command.

   ```shell
   docker images
   ```

## Push Image to DockerHub or Private Warehouse

Log into your DockerHub account

```
docker login
```

If the login succeeds, you will see the prompt `Success` , and then you can push the Docker image to the warehouse.

```shell
docker push ${tagName}
```
