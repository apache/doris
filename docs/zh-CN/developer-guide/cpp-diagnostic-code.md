---
{
    "title": "C++ 代码分析",
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

# C++ 代码分析

Doris支持使用[Clangd](https://clangd.llvm.org/)和[Clang-Tidy](https://clang.llvm.org/extra/clang-tidy/)进行代码静态分析。Clangd和Clang-Tidy在[LDB-toolchain](https://doris.apache.org/zh-CN/installing/compilation-with-ldb-toolchain)中已经内置，另外也可以自己安装或者编译。

### Clang-Tidy
Clang-Tidy中可以做一些代码分析的配置,配置文件`.clang-tidy`在Doris根目录下。

### 在VSCODE中配置Clangd

首先需要安装clangd插件，然后在`settings.json`中编辑或者直接在首选项中更改插件配置。相比于vscode-cpptools，clangd可以为vscode提供更强大和准确的代码转跳，并且集成了clang-tidy的分析和快速修复功能。

```json
    "clangd.path": "ldb_toolchain/bin/clangd", //clangd的路径
    "clangd.arguments": [
        "--background-index",
        "--clang-tidy", //开启clang-tidy
        "--compile-commands-dir=doris/be/build_RELEASE/", //会用到cmake生成的compile_commands.json,所以需要先编译一次生成该文件
        "--completion-style=detailed",
        "-j=5", //clangd分析文件的并行数
        "--all-scopes-completion",
        "--pch-storage=memory",
        "--pretty",
        "-log=verbose",
        "--query-driver=ldb_toolchain/bin/*" //编译器路径
    ],
    "clangd.trace": "/home/disk2/pxl/dev/baidu/bdg/doris/core/output/clangd-server.log" //clangd的日志路径,可以自己设定
```
