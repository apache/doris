---
{
"title": "Construct Docker Image",
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

# Construct Docker Image 

This document mainly introduces how to make a running image of Apache Doris through Dockerfile, so that an Apache Doris Image can be quickly pulled in a containerized orchestration tool or during a quick test to complete the cluster creation.

## Software and hardware requirements

### Overview

Before making a Docker image, the production machine must be prepared in advance. The platform architecture of the machine determines the applicable platform architecture of the Docker Image after production. For example, the X86_64 machine needs to download the Doris binary program of X86_64. The image after production can only be used on the X86_64 platform run on. The ARM platform (M1 is regarded as ARM) is the same.

### Hardware requirements

Minimum configuration: 2C 4G

Recommended configuration: 4C 16G

### Software Requirements

Docker Version: 20.10 and later

## Docker Image build

Dockerfile scripting needs to pay attention to the following points:
> 1. The base parent image uses the official OpenJDK image certified by Docker-Hub, and the version uses JDK 1.8
> 2. The application uses the official binary package for download by default, do not use binary packages from unknown sources
> 3. Embedded scripts are required to complete tasks such as FE startup, multi-FE registration, status check and BE startup, registration of BE to FE, status check, etc.
> 4. The application should not be started using `--daemon` when starting in Docker, otherwise there will be exceptions during the deployment of orchestration tools such as K8S

Since Apache Doris 1.2 began to support JavaUDF capability, BE also needs a JDK environment. The recommended mirror is as follows:

| Doris Program | Recommended Base Parent Image |
| ---------- | ----------------- |
| Frontend | openjdk:8u342-jdk |
| Backend | openjdk:8u342-jdk |
| Broker | openjdk:8u342-jdk |

### Script preparation

In the Dockerfile script for compiling the Docker Image, there are two ways to load the binary package of the Apache Doris program:

1. Execute the download command at compile time via wget / curl, and then complete the docker build process
2. Download the binary package to the compilation directory in advance, and then load it into the docker build process through the ADD or COPY command

Using the former will make the Docker Image Size smaller, but if the build fails, the download operation may be repeated, resulting in a long build time, while the latter is more suitable for a build environment where the network environment is not very good. The image built by the latter is slightly larger than the former, but not much larger.

**To sum up, the examples in this document are subject to the second method. If you have the first request, you can customize and modify it according to your own needs. **

### Prepare binary package

It should be noted that if there is a need for customized development, you need to modify the source code and then [compile](../source-install/compilation) to package it, and then place it in the build directory.

If there is no special requirement, just [download](https://doris.apache.org/download) the binary package provided by the official website.
### Build Steps

#### Build FE

The build environment directory is as follows:

```sql
└── docker-build                                       // build root directory
     └── fe                                            // FE build directory
         ├── dockerfile                                // dockerfile script
         └── resource                                  // resource directory
             ├── init_fe.sh                            // startup and registration script
             └── apache-doris-x.x.x-bin-fe.tar.gz      // binary package
```
1. Create a build environment directory

   ```shell
   mkdir -p ./docker-build/fe/resource
   ```

2. Download [official binary package](https://doris.apache.org/download)/compiled binary package

   Copy the binary package to the `./docker-build/fe/resource` directory

3. Write FE's Dockerfile script

   ```powershell
   # select the base image
   FROM openjdk:8u342-jdk
   
   # Set environment variables
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/opt/apache-doris/fe/bin:$PATH"
   
   # Download the software to the mirror and replace it as needed
   ADD ./resource/apache-doris-fe-${x.x.x}-bin.tar.gz /opt/
   
   RUN apt-get update && \
       apt-get install -y default-mysql-client && \
       apt-get clean && \
       mkdir /opt/apache-doris && \
       cd /opt && \
       mv apache-doris-fe-${x.x.x}-bin /opt/apache-doris/fe
   
   ADD ./resource/init_fe.sh /opt/apache-doris/fe/bin
   RUN chmod 755 /opt/apache-doris/fe/bin/init_fe.sh
   
   ENTRYPOINT ["/opt/apache-doris/fe/bin/init_fe.sh"]
   ```

   After writing, name it `Dockerfile` and save it to the `./docker-build/fe` directory

4. Write the execution script of FE

   You can refer to the content of copying [init_fe.sh](https://github.com/apache/doris/tree/master/docker/runtime/fe/resource/init_fe.sh)

   After writing, name it `init_fe.sh` and save it to the `./docker-build/fe/resouce` directory

5. Execute the build

   It should be noted that `${tagName}` needs to be replaced with the tag name you want to package and name, such as: `apache-doris:1.1.3-fe`

   Build FEs:

   ```shell
   cd ./docker-build/fe
   docker build . -t ${fe-tagName}
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
           ├── dockerfile                                      // dockerfile script
           └── resource                                        // resource directory
               ├── init_be.sh                                  // startup and registration script
               └── apache-doris-x.x.x-bin-x86_64/arm-be.tar.gz // binary package
   ```

3. Write BE's Dockerfile script

   ```powershell
   # select the base image
   FROM openjdk:8u342-jdk
   
   # Set environment variables
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/opt/apache-doris/be/bin:$PATH"
   
   # Download the software to the mirror and replace it as needed
   ADD ./resource/apache-doris-be-${x.x.x}-bin-x86_64.tar.gz /opt/
   
   RUN apt-get update && \
       apt-get install -y default-mysql-client && \
       apt-get clean && \
       mkdir /opt/apache-doris && \
       cd /opt && \
       mv apache-doris-be-${x.x.x}-bin-x86_64 /opt/apache-doris/be
   
   ADD ./resource/init_be.sh /opt/apache-doris/be/bin
   RUN chmod 755 /opt/apache-doris/be/bin/init_be.sh
   
   ENTRYPOINT ["/opt/apache-doris/be/bin/init_be.sh"]
   ```

   After writing, name it `Dockerfile` and save it to the `./docker-build/be` directory

4. Write the execution script of BE

   You can refer to the content of copying [init_be.sh](https://github.com/apache/doris/tree/master/docker/runtime/be/resource/init_be.sh)

   After writing, name it `init_be.sh` and save it to the `./docker-build/be/resouce` directory

5. Execute the build

   It should be noted that `${tagName}` needs to be replaced with the tag name you want to package and name, such as: `apache-doris:1.1.3-be`

   Build BEs:

   ```shell
   cd ./docker-build/be
   docker build . -t ${be-tagName}
   ```

   After the construction is complete, there will be a prompt of `Success`. At this time, the following command can be used to view the Image mirror that has just been built

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
           ├── dockerfile                               // dockerfile script
           └── resource                                 // resource directory
               ├── init_broker.sh                       // startup and registration script
               └── apache-doris-x.x.x-bin-broker.tar.gz // binary package
   ```

3. Write Broker's Dockerfile script

   ```powershell
   # select the base image
   FROM openjdk:8u342-jdk
   
   # Set environment variables
   ENV JAVA_HOME="/usr/local/openjdk-8/" \
       PATH="/opt/apache-doris/broker/bin:$PATH"
   
   # Download the software to the mirror, where the broker directory is synchronously compressed to the binary package of FE, which needs to be decompressed and repackaged by itself, and can be replaced as needed
   ADD ./resource/apache_hdfs_broker.tar.gz /opt/
   
   RUN apt-get update && \
       apt-get install -y default-mysql-client && \
       apt-get clean && \
       mkdir /opt/apache-doris && \
       cd /opt && \
       mv apache_hdfs_broker /opt/apache-doris/broker
   
   ADD ./resource/init_broker.sh /opt/apache-doris/broker/bin
   RUN chmod 755 /opt/apache-doris/broker/bin/init_broker.sh
   
   ENTRYPOINT ["/opt/apache-doris/broker/bin/init_broker.sh"]
   ```

   After writing, name it `Dockerfile` and save it to the `./docker-build/broker` directory

4. Write the execution script of BE

   You can refer to the content of copying [init_broker.sh](https://github.com/apache/doris/tree/master/docker/runtime/broker/resource/init_broker.sh)

   After writing, name it `init_broker.sh` and save it to the `./docker-build/broker/resouce` directory

5. Execute the build

   It should be noted that `${tagName}` needs to be replaced with the tag name you want to package and name, such as: `apache-doris:1.1.3-broker`

   Build Broker:

   ```shell
   cd ./docker-build/broker
   docker build . -t ${broker-tagName}
   ```

   After the construction is complete, there will be a prompt of `Success`. At this time, the following command can be used to view the Image mirror that has just been built

   ```shell
   docker images
   ```

## Push the image to DockerHub or private warehouse

Log in to your DockerHub account

```
docker login
```

A successful login will prompt `Success` related prompts, and then push them to the warehouse

```shell
docker push ${tagName}
```
