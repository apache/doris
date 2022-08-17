---
{
    "title": "C++ 代码格式化",
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

# C++ 代码格式化

Doris使用clang-format进行代码格式化，并在build-support目录下提供了封装脚本：

* `clang-format.sh`.

    格式化 `be/src` 和 `be/test` 目录下的 C/C++ 代码。

* `check-format.sh`.

    检查 `be/src` 和 `be/test` 目录下的 C/C++ 代码格式，并将 diff 输出，但不会修改文件内容。

## 代码风格定制

Doris的代码风格在Google Style的基础上稍有改动，定制为 `.clang-format` 文件，位于Doris根目录。

目前，`.clang-format` 配置文件适配clang-format-13.0.1以上的版本。

`.clang-format-ignore` 文件中记录了不希望被格式化的代码。这些代码通常来自第三方代码库，建议保持原有代码风格。

## 环境准备

需要下载安装clang-format，也可使用IDE或Editor提供的clang-format插件，下面分别介绍。

### 下载安装clang-format

推荐使用 NPM 安装 clang-format 14（不同版本的 clang-format 可能产生不同的代码格式，建议使用 14 版本）：

`npm install clang-format@1.6.0`

Ubuntu: `apt-get install clang-format` 

当前版本为10.0，也可指定旧版本，例如: `apt-get install clang-format-9`，建议源码编译14.0版本。

Mac: `brew install clang-format`

Centos 7: 

centos yum安装的clang-format版本过老，支持的StyleOption太少，建议源码编译14.0版本。

LDB toolchain:

如果使用 [LDB toolchain](/docs/install/source-install/compilation-with-ldb-toolchain)，
最新版本的 [LDB toolchain](https://github.com/amosbird/ldb_toolchain_gen/releases)（>= v0.11）已经包含了预编译的clang-format
13.0.1的二进制文件。

### clang-format插件

Clion IDE可使用插件"ClangFormat"，`File->Setting->Plugins`搜索下载。但版本无法和
clang-format程序的版本匹配，从支持的StyleOption上看，应该是低于clang-format-9.0。

## 使用方式

### 命令行运行

cd到Doris根目录下，然后执行如下命令:

`build-support/clang-format.sh`

> 注：`clang-format.sh`脚本要求您的机器上安装了python 3

### 在IDE或Editor中使用clang-format

#### Clion

Clion如果使用插件，点击`Reformat Code`即可。

#### VS Code

VS Code需安装扩展程序Clang-Format，但需要自行提供clang-format执行程序的位置。

打开VS Code配置页面，直接搜索"clang_format"，填上

```
"clang_format_path":  "$clang-format path$",
"clang_format_style": "file"
```

然后，右键点击`Format Document`即可。
