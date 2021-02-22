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

# Apache Doris (incubating)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Doris is an MPP-based interactive SQL data warehousing for reporting and analysis.
Its original name was Palo, developed in Baidu. After donated to Apache Software Foundation, it was renamed Doris.

## 1. License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## 2. Technology
Doris mainly integrates the technology of Google Mesa and Apache Impala, and it is based on a column-oriented storage engine and can communicate by MySQL client.

## 3. User cases
Doris not only provides high concurrent low latency point query performance, but also provides high throughput queries of ad-hoc analysis.

Doris not only provides batch data loading, but also provides near real-time mini-batch data loading.

Doris also provides high availability, reliability, fault tolerance, and scalability.

The simplicity (of developing, deploying and using) and meeting many data serving requirements in single system are the main features of Doris (refer to [Overview](https://github.com/apache/incubator-doris/wiki/Doris-Overview)).

## 4. Compile and install

Currently only supports Docker environment and Linux OS, such as Ubuntu and CentOS.

### 4.1 Compile in Docker environment (Recommended)

We offer a docker image as a Doris compilation environment. You can compile Doris from source in it and run the output binaries in other Linux environments.

Firstly, you need to install and start docker service.

And then you could build Doris as following steps:

#### Step1: Pull the docker image with Doris building environment

```
$ docker pull apachedoris/doris-dev:build-env-1.2
```

You can check it by listing images, for example:

```
$ docker images
REPOSITORY              TAG                 IMAGE ID            CREATED             SIZE
apachedoris/doris-dev   build-env-1.2       69cf7fff9d10        2 weeks ago         4.12GB
```
> NOTE: You may have to use different images to compile from source.
>
> | image version | commit id | release version |
> |---|---|---|
> | apachedoris/doris-dev:build-env | before [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) | 0.8.x, 0.9.x |
> | apachedoris/doris-dev:build-env-1.1 | [ff0dd0d](https://github.com/apache/incubator-doris/commit/ff0dd0d2daa588f18b6db56f947e813a56d8ec81) or later | 0.10.x or 0.11.x |
> | apachedoris/doris-dev:build-env-1.2 | [1648226](https://github.com/apache/incubator-doris/commit/1648226927c5b4e33f33ce2e12bf0e06369b7f6e) or later | 0.12.x or later |




#### Step2: Run the Docker image

You can run the image directly:

```
$ docker run -it apachedoris/doris-dev:build-env-1.2
```

Or if you want to compile the source located in your local host, you can map the local directory to the image by running:

```
$ docker run -it -v /your/local/path/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apachedoris/doris-dev:build-env-1.2
```

#### Step3: Download Doris source

Now you should be attached in docker environment.

You can download Doris source by release package or by git clone in image.

(If you already downloaded the source in your local host and map it to the image in Step2, you can skip this step.)

```
$ wget https://dist.apache.org/repos/dist/dev/incubator/doris/xxx.tar.gz
or
$ git clone https://github.com/apache/incubator-doris.git
```

#### Step4: Build Doris

Enter Doris source path and build Doris.

```
$ sh build.sh
```

After successfully building, it will install binary files in the directory `output/`.

### 4.2 For Linux OS

#### Prerequisites

You should install the following softwares:

```
GCC 7.3.0+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.4.3+
```

Then set them to environment variable PATH and set JAVA_HOME.

If your GCC version is lower than 7.3.0, you can run:

```
sudo yum install devtoolset-7-toolchain -y
```

and then, set the path of GCC (e.g `/opt/rh/devtoolset-7/root/usr/bin`) to the environment variable PATH.

#### Compile and install

Run the following script, it will compile thirdparty libraries and build whole Doris.

```
sh build.sh
```

After successfully building, it will install binary files in the directory `output/`.

## 5. License Notice

Some of the third-party dependencies' license are not compatible with Apache 2.0 License. So you may have to disable
some features of Doris to be complied with Apache 2.0 License. Details can be found in `thirdparty/LICENSE.txt`

## 6. Reporting Issues

If you find any bugs, please file a [GitHub issue](https://github.com/apache/incubator-doris/issues).

## 7. Links

* Doris official site - <http://doris.incubator.apache.org>
* User Manual (GitHub Wiki) - <https://github.com/apache/incubator-doris/wiki>
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Gitter channel - <https://gitter.im/apache-doris/Lobby> - Online chat room with Doris developers.
* Overview - <https://github.com/apache/incubator-doris/wiki/Doris-Overview>
* Compile and install - <https://github.com/apache/incubator-doris/wiki/Doris-Install>
* Getting start - <https://github.com/apache/incubator-doris/wiki/Getting-start>
* Deploy and Upgrade - <https://github.com/apache/incubator-doris/wiki/Doris-Deploy-%26-Upgrade>
* User Manual - <https://github.com/apache/incubator-doris/wiki/Doris-Create%2C-Load-and-Delete>
* FAQs - <https://github.com/apache/incubator-doris/wiki/Doris-FAQ>
