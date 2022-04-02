---
{
    "title": "C++ Code Diagnostic",
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

# C++ Code Diagnostic

Doris support to use [Clangd](https://clangd.llvm.org/) and [Clang-Tidy](https://clang.llvm.org/extra/clang-tidy/) to diagnostic code. Clangd and Clang-Tidy already has in [LDB-toolchain](https://doris.apache.org/zh-CN/installing/compilation-with-ldb-toolchain)，also can install by self.

### Clang-Tidy
Clang-Tidy can do some diagnostic cofig, config file `.clang-tidy` is in Doris root path. Compared with vscode-cpptools, clangd can provide more powerful and accurate code jumping for vscode, and integrates the analysis and quick-fix functions of clang-tidy.

### Enable clangd on VSCODE

First we should install clangd plugin, then edit `settings.json` or just change config on gui.

```json
    "clangd.path": "ldb_toolchain/bin/clangd", //clangd path
    "clangd.arguments": [
        "--background-index",
        "--clang-tidy", //enable clang-tidy
        "--compile-commands-dir=doris/be/build_RELEASE/", //clangd should read compile_commands.json create by cmake, so you should compile once
        "--completion-style=detailed",
        "-j=5", //clangd diagnostic parallelism
        "--all-scopes-completion",
        "--pch-storage=memory",
        "--pretty",
        "-log=verbose",
        "--query-driver=ldb_toolchain/bin/*" //path of compiler
    ],
    "clangd.trace": "/home/disk2/pxl/dev/baidu/bdg/doris/core/output/clangd-server.log" //clangd log path
```
