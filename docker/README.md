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

## Doris Develop Environment based on docker

### Preparation

1. Download the Doris code repo

    ```console
    $ cd /to/your/workspace/
    $ git clone https://github.com/apache/incubator-doris.git
    ```

    You can remove the `.git` dir in `incubator-doris/` to make the dir size smaller.
    So that the following generated docker image can be smaller.

2. Copy Dockerfile

    ```console
    $ cd /to/your/workspace/
    $ cp incubator-doris/docker/Dockerfile ./
    ```

After preparation, your workspace should like this:

```
.
├── Dockerfile
├── incubator-doris
│   ├── be
│   ├── bin
│   ├── build.sh
│   ├── conf
│   ├── DISCLAIMER-WIP
│   ├── docker
│   ├── docs
│   ├── env.sh
│   ├── fe
│   ├── ...
```

### Build docker image

```console
$ cd /to/your/workspace/
$ docker build -t doris:v1.0  .
```

> `doris` is docker image repository name and `v1.0` is tag name, you can change them to whatever you like.

### Use docker image

This docker image you just built does not contain Doris source code repo. You need
to download it first and map it to the container. (You can just use the one you
used to build this image before)

```console
$ docker run -it -v /your/local/path/incubator-doris/:/root/incubator-doris/ doris:v1.0
$ docker run -it -v /your/local/.m2:/root/.m2 -v /your/local/incubator-doris-DORIS-x.x.x-release/:/root/incubator-doris-DORIS-x.x.x-release/ doris:v1.0
```

Then you can build source code inside the container.

```console
$ cd /root/incubator-doris/
$ sh build.sh
```

**NOTICE**

The default JDK version is openjdk 11, if you want to use openjdk 8, you can run the command:

```console
$ alternatives --set java java-1.8.0-openjdk.x86_64
$ alternatives --set javac java-1.8.0-openjdk.x86_64
$ export JAVA_HOME=/usr/lib/jvm/java-1.8.0
```

The version of jdk you used to run FE must be the same version you used to compile FE.

### Latest update time

2022-1-23
