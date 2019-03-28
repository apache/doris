# Apache Doris (incubating)
[![Join the chat at https://gitter.im/apache-doris/Lobby](https://badges.gitter.im/apache-doris/Lobby.svg)](https://gitter.im/apache-doris/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Doris is an MPP-based interactive SQL data warehousing for reporting and analysis. It is open-sourced by Baidu. 

## 1. License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## 2. Technology
Doris mainly integrates the technology of Google Mesa and Apache Impala, and it based on a column-oriented storage engine and can communicate by MySQL client.

## 3. User cases
Doris not only provides high concurrent low latency point query performance, but also provides high throughput queries of ad-hoc analysis. 

Doris not only provides batch data loading, but also provides near real-time mini-batch data loading. 

Doris also provides high availability, reliability, fault tolerance, and scalability. 

The simplicity (of developing, deploying and using) and meeting many data serving requirements in single system are the main features of Doris (refer to [Overview](https://github.com/apache/incubator-doris/wiki/Doris-Overview)).

## 4. Compile and install

Currently only supports Docker environment and Linux OS, such as Ubuntu and CentOS.

### 4.1 Compile in Docker environment (Recommended)

We offer a docker images as a Doris compilation environment. You can compile Doris from source in it and run the output binaries in other Linux environment.

Firstly, you must be install and start docker service.

And then you could build Doris as following steps:

#### Step1: Pull the docker image with Doris building environment

```
$ docker pull apachedoris/doris-dev:build-env
```

You can check it by listing images, for example:

```
$ docker images
REPOSITORY              TAG                 IMAGE ID            CREATED             SIZE
apachedoris/doris-dev   build-env           f8bc5d4024e0        21 hours ago        3.28GB
```

#### Step2: Run the Docker image 

You can run the image directyly:

```
$ docker run -it apachedoris/doris-dev:build-env
```

Or if you want to compile the source located in your local host, you can map the local directory to the image by running:

```
$ docker run -it -v /your/local/path/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ apachedoris/doris-dev:build-env
```

#### Step3: Download Doris source

Now you should in docker environment.

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

You must be install following softwares:

```
GCC 5.3.1+, Oracle JDK 1.8+, Python 2.7+, Apache Maven 3.5+, CMake 3.4.3+
```

After you installed above all, you also must be set them to environment variable PATH and set JAVA_HOME.

If your GCC version is less than 5.3.1, you can run:

```
sudo yum install devtoolset-4-toolchain -y
```

and then, set the path of GCC (e.g /opt/rh/devtoolset-4/root/usr/bin) to the environment variable PATH.

#### Compile and install

Run following script, it will compile thirdparty libraries and build whole Doris.

```
sh build.sh
```

After successfully building, it will install binary files in the directory `output/`.

## 5. Reporting Issues

If you find any bugs, please file a [GitHub issue](https://github.com/apache/incubator-doris/issues).

## 6. Links

* Doris official site - <http://doris.incubator.apache.org>
* User Manual (GitHub Wiki) - <https://github.com/apache/incubator-doris/wiki>
* Developer Mailing list - Subscribe to <dev@doris.incubator.apache.org> to discuss with us.
* Gitter channel - <https://gitter.im/apache-doris/Lobby> - Online chat room with Doris developers.
* Overview - <https://github.com/apache/incubator-doris/wiki/Doris-Overview>
* Compile and install - <https://github.com/apache/incubator-doris/wiki/Doris-Install>
* Getting start - <https://github.com/apache/incubator-doris/wiki/Getting-start>
* Deploy and Upgrade - <https://github.com/apache/incubator-doris/wiki/Doris-Deploy-%26-Upgrade>
* User Manual - <https://github.com/apache/incubator-doris/wiki/Doris-Create%2C-Load-and-Delete>
* FAQs - <https://github.com/apache/incubator-doris/wiki/Doris-FAQ>

