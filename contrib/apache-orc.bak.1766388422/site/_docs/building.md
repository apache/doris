---
layout: docs
title: Building ORC
permalink: /docs/building.html
dockerUrl: https://github.com/apache/orc/blob/main/docker
---

## Building both C++ and Java

The C++ library is supported on the following operating systems:

* CentOS 7
* Debian 10 to 11
* MacOS 11.6 and 12.5
* Ubuntu 18.04 to 22.04

You'll want to install the usual set of developer tools, but at least:

* cmake
* g++ or clang++
* java ( >= 1.8)
* make
* maven ( >= 3)

For each version of Linux, please check the corresponding Dockerfile, which
is in the docker subdirectory, for the list of packages required to build ORC:

* [CentOS 7]({{ page.dockerUrl }}/centos7/Dockerfile)
* [Debian 10]({{ page.dockerUrl }}/debian10/Dockerfile)
* [Debian 11]({{ page.dockerUrl }}/debian11/Dockerfile)
* [Ubuntu 18]({{ page.dockerUrl }}/ubuntu18/Dockerfile)
* [Ubuntu 20]({{ page.dockerUrl }}/ubuntu20/Dockerfile)
* [Ubuntu 22]({{ page.dockerUrl }}/ubuntu22/Dockerfile)

To build a normal release:

~~~ shell
% mkdir build
% cd build
% cmake ..
% make package test-out
~~~

ORC's C++ build supports three build types, which are controlled by adding
`-DCMAKE_BUILD_TYPE=<type>` to the cmake command.

* **RELWITHDEBINFO** (default) - Optimized with debug information
* **DEBUG** - Unoptimized with debug information
* **RELEASE** - Optimized with no debug information

If your make command fails, it is useful to see the actual commands that make
is invoking:

~~~ shell
% make package test-out VERBOSE=1
~~~

## Building just Java

You'll need to install:

* java (>= 1.8)
* maven (>= 3)

To build:

~~~ shell
% cd java
% ./mvnw package
~~~

## Building just C++

~~~ shell
% mkdir build
% cd build
% cmake .. -DBUILD_JAVA=OFF
% make package test-out
~~~

## Specify third-party libraries for C++ build

~~~ shell
% mkdir build
% cd build
% cmake .. -DSNAPPY_HOME=<PATH> \
           -DZLIB_HOME=<PATH> \
           -DLZ4_HOME=<PATH> \
           -DGTEST_HOME=<PATH> \
           -DPROTOBUF_HOME=<PATH>
% make package test-out
~~~
