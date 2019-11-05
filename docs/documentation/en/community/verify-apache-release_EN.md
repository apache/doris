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

# Verify Apache Release

To verify the release, following checklist can used to reference:

1. [ ] Download links are valid.
2. [ ] Checksums and PGP signatures are valid.
3. [ ] DISCLAIMER-WIP is included.
4. [ ] Source code artifacts have correct names matching the current release.
5. [ ] LICENSE and NOTICE files are correct for the repository.
6. [ ] All files have license headers if necessary.
7. [ ] No compiled archives bundled in source archive.
8. [ ] Building is OK.

## 1. Download source package, signature file, hash file and KEYS

Download all artifacts, take a.b.c-incubating as an example:

```
wget https://dist.apache.org/repos/dist/dev/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz

wget https://dist.apache.org/repos/dist/dev/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.sha512

wget https://dist.apache.org/repos/dist/dev/incubator/doris/a.b.c-incubating/apache-doris-a.b.c-incubating-src.tar.gz.asc

wget https://dist.apache.org/repos/dist/dev/incubator/doris/KEYS
```

## 2. Verify signature and hash

GnuPG is recommended, which can install by yum install gnupg or apt-get install gnupg.

```
gpg --import KEYS
gpg --verify apache-doris-a.b.c-incubating-src.tar.gz.asc apache-doris-a.b.c-incubating-src.tar.gz
sha512sum --check apache-doris-a.b.c-incubating-src.tar.gz.sha512
```

## 3. Verify license header

Apache RAT is recommended to verify license headder, which can dowload as following command.

```
wget http://mirrors.tuna.tsinghua.edu.cn/apache//creadur/apache-rat-0.12/apache-rat-0.12-bin.tar.gz
tar zxvf apache -rat -0.12 -bin.tar.gz
```

Given your source dir is apache-doris-a.b.c-incubating-src, you can check with following command.
It will output a file list which don't include ASF license header, and these files used other licenses.

```
/usr/java/jdk/bin/java  -jar apache-rat-0.12/apache-rat-0.12.jar -a -d apache-doris-a.b.c-incubating-src -E apache-doris-a.b.c-incubating-src/.rat-excudes 
```

## 4. Verify building

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

You can run image directyly:

```
$ docker run -it apachedoris/doris-dev:build-env
```

#### Step3: Download Doris source
Now you should in docker environment, and you can download Doris source by release package or by git clone in image.
(If you have downloaded source and it is not in image, you can map its path to image in Step2.)

```
$ wget https://dist.apache.org/repos/dist/dev/incubator/doris/xxx.tar.gz
```

#### Step4: Build Doris
Now you can enter Doris source path and build Doris.

```
$ cd incubator-doris
$ sh build.sh
```

After successfully building, it will install binary files in the directory output/.

For more detail, you can refer to README.md in source package.
