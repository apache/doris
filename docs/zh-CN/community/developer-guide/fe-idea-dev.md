---
{
    "title": "FE 开发环境搭建 - IntelliJ IDEA",
    "language": "zh-CN"
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

# 使用 IntelliJ IDEA 搭建 FE 开发环境

## 1.环境准备

从 https://github.com/apache/incubator-doris.git 下载源码到本地

安装 JDK1.8+ ，使用 IntelliJ IDEA 打开 FE.

### thrift

如果仅进行fe开发而没有编译过thirdparty，则需要安装thrift，并将thrift 复制或者连接到 `thirdparty/installed/bin` 目录下

安装 `thrift` 0.13.0 版本(注意：`Doris` 0.15 和最新的版本基于 `thrift` 0.13.0 构建, 之前的版本依然使用`thrift` 0.9.3 构建)

#### Windows 下载

1. 下载：`http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
2. 拷贝：将文件拷贝至 `./thirdparty/installed/bin`

#### MacOS 下载

下载：`brew install thrift@0.13.0`

注：macOS执行 `brew install thrift@0.13.0` 可能会报找不到版本的错误，解决方法如下，在终端执行：

1. `brew tap-new $USER/local-tap`
2. `brew extract --version='0.13.0' thrift $USER/local-tap`
3. `brew install thrift@0.13.0`

参考链接: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`

#### 建立软链接

位于 Doris **根**目录下

`mkdir -p ./thirdparty/installed/bin`

ARM架构macOS

`ln -s /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift ./thirdparty/installed/bin/thrift`

Intel架构macOS

`ln -s /usr/local/Cellar/thrift@0.13.0/0.13.0/bin/thrift ./thirdparty/installed/bin/thrift`

### 自动生成代码：

如果是Mac 或者 Linux 环境 可以通过 如下命令生成

```
cd fe
mvn generate-sources
```

如果出现错误，则执行：

```
cd fe && mvn clean install -DskipTests
```

或者通过图形界面运行 maven 命令生成

![](/images/gen_code.png)

如果使用windows环境可能会有make命令和sh脚本无法执行的情况 可以通过拷贝linux上的 `fe/fe-core/target/generated-sources` 目录拷贝到相应的目录的方式实现，也可以通过docker 镜像挂载本地目录之后，在docker 内部生成自动生成代码，可以参照编译一节

#### arm mac compile failed

如果在m1 mac上进行自动生成代码会出现如下错误

```
[ERROR] Failed to execute goal org.xolstice.maven.plugins:protobuf-maven-plugin:0.6.1:compile (grpc-build) on project apm-network: Unable to resolve artifact: Missing:
[ERROR] 1) com.google.protobuf:protoc:exe:osx-aarch_64:3.14.0
[ERROR] 1 required artifact is missing.
```

ptotobuf3.14.0和protoc-gen-grpc-java1.30.0没有aarch64的版本，根据[grpc社区issue](https://github.com/grpc/grpc-java/issues/7690)的建议下载x86版本并使用rosetta转译

1. 打开`doris/fe/fe-core/pom.xml`
2. 将`<protocArtifact>com.google.protobuf:protoc:${protobuf.version}</protocArtifact>`修改成`<protocArtifact>com.google.protobuf:protoc:3.14.0:exe:osx-x86_64</protocArtifact>`
3. 将`<pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}</pluginArtifact>`修改成`<pluginArtifact>io.grpc:protoc-gen-grpc-java:1.30.0:exe:osx-x86_64</pluginArtifact>`
4. 打开终端输入`softwareupdate --install-rosetta`

### help文档

如果还未生成过help文档，需要跳转到docs目录，执行`sh build_help_zip.sh`，

然后将build中的help-resource.zip拷贝到fe/fe-core/target/classes中

## 2.调试

1. 用idea导入fe工程

2. 在fe目录下创建下面红框标出的目录（在新版本中该目录可能存在，如存在则跳过，否则创建）

![](/images/DEBUG4.png)

3. 编译`ui`项目，将 `ui/dist/`目录中的文件拷贝到`webroot`中（如果你不需要看`Doris` UI，这一步可以跳过）

## 3.配置conf/fe.conf

配置在 `conf/fe.conf`，你可以根据自己的需要进行修改

(注意：如果使用`Mac`开发，由于`docker for Mac`不支持`Host`模式，需要使用`-p`方式暴露`be`端口，同时`fe.conf`的`priority_networks`配置为容器内可访问的Ip，例如WIFI的Ip)

## 4.设置环境变量

在IDEA中设置运行环境变量

![](/images/DEBUG5.png)

## 5.配置options

由于部分依赖使用了`provided`，idea需要做下特殊配置，在`Run/Debug Configurations`设置中点击右侧`Modify options`，勾选`Add dependencies with "provided" scope to classpath`选项

![](/images/idea_options.png)

## 6.启动fe

下面你就可以愉快的启动，调试你的FE了

