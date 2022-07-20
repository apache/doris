---
{
    "title": "Setting Up dev env for FE - IntelliJ IDEA",
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

# Setting Up Development Environment for FE using IntelliJ IDEA

## 1. Environmental Preparation

* JDK1.8+
* IntelliJ IDEA
* Maven (Optional, IDEA shipped embedded Maven3)

Git clone codebase from https://github.com/apache/incubator-doris.git

Use IntelliJ IDEA to open the code `FE` directory

### Thrift

If your are only interested in FE module, and for some reason you can't or don't want to compile full thirdparty libraries，

the minimum tool required for FE module is `thrift`, so you can manually install `thrift` and copy or create a link of the executable `thrift` command to `./thirdparty/installed/bin`.

```
Doris build against `thrift` 0.13.0 ( note : `Doris` 0.15 and later version build against `thrift` 0.13.0 , the previous version is still `thrift` 0.9.3)   

Windows: 
   1. Download：`http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
   2. Copy：copy the file to `./thirdparty/installed/bin`
   
MacOS: 
   1. Download：`brew install thrift@0.13.0`
   2. Establish soft connection： 
      `mkdir -p ./thirdparty/installed/bin`
      # For ARM macOS
      `ln -s /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift ./thirdparty/installed/bin/thrift`
      # For Intel macOS
      `ln -s /usr/local/Cellar/thrift@0.13.0/0.13.0/bin/thrift ./thirdparty/installed/bin/thrift`
   
Note：The error that the version cannot be found may be reported when MacOS execute `brew install thrift@0.13.0`. The solution is execute at the terminal as follows:
   1. `brew tap-new $USER/local-tap`
   2. `brew extract --version='0.13.0' thrift $USER/local-tap`
   3. `brew install thrift@0.13.0`
Reference link: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`
```

### generate sources

Go to `./fe` folder and run the following maven command to generate sources.

```
mvn generate-sources
```

If fails, run following command.

```
mvn clean install -DskipTests
```

You can also use IDE embedded GUI tools to run maven command to generate sources

![](/images/gen_code.png)

If you are developing on the OS which lack of support to run `shell script` and `make` such as Windows, a workround here 
is generate codes in Linux and copy them back. Using Docker should also be an option.

#### arm mac compile failed

An error would occur if you generated sources using maven on arm mac. Detailed error messages are as follows.

```
[ERROR] Failed to execute goal org.xolstice.maven.plugins:protobuf-maven-plugin:0.6.1:compile (grpc-build) on project apm-network: Unable to resolve artifact: Missing:
[ERROR] 1) com.google.protobuf:protoc:exe:osx-aarch_64:3.14.0
[ERROR] 1 required artifact is missing.
```

Since protobuf v3.14.0 and protoc-gen-grpc-java v1.30.0 don't come up with osx-aarch_64 version, given the advice by [grpc_community], you'd better manually download the corresponding osx_x86 version and then translate them by Rosseta2.

1. open `doris/fe/fe-core/pom.xml`
2. change `<protocArtifact>com.google.protobuf:protoc:${protobuf.version}</protocArtifact>` to `<protocArtifact>com.google.protobuf:protoc:3.14.0:exe:osx-x86_64</protocArtifact>`
3. change `<pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}</pluginArtifact>` to `<pluginArtifact>io.grpc:protoc-gen-grpc-java:1.30.0:exe:osx-x86_64</pluginArtifact>`
4. open terminal and paste `softwareupdate --install-rosetta`

### help document

If a help document has not been generated, go to the docs directory and run `sh build_help_zip.sh`，
   
Then copy help-resource.zip from build to fe/fe-core/target/classes

## 2. Debug

1. Import `./fe` into IDEA

2. Follow the picture to create the folders (The directory may exist in the new version. If it exists, skip it, otherwise create it.)

![](/images/DEBUG4.png)

3. Build `ui` project , and copy files from directory `ui/dist` into directory `webroot` ( you can skip this step , if you don't need `Doris` UI )

## 3. Custom FE configuration

Copy below content into `conf/fe.conf` and tune it to fit your environment

(Note: If developed using`Mac`, since`docker for Mac`does not support`Host`mode,`be`needs to be exposed using`-p` and `fe.conf` `priority_networks` configured to be accessible within the container, such as WIFI Ip).

## 4. Setting Environment Variables

Follow the picture to set runtime Environment Variables in IDEA

![](/images/DEBUG5.png)

## 5. Config options

Because part of the dependency is `provided`, idea needs to do a special config. Click on the right `Modify Options` in the `Run/Debug Configurations` setting. Check the `Add Dependencies with "Provided" scope to classpath` option.

![](/images/idea_options.png)

## 6. Start FE

Having fun with Doris FE!
