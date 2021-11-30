---
{
    "title": "C++ Format Code",
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

# C++ code formatting

Doris uses clang-format for code formatting, and provides a package script in the build-support directory:

* `clang-format.sh`.

    Format the C/C++ code in the `be/src` and `be/test` directories.

* `check-format.sh`.

    Check the C/C++ code format in the `be/src` and `be/test` directories, and output diff, but the content of the file will not be modified.

## Code style customization

The code style of Doris is slightly modified on the basis of Google Style and is customized as a `.clang-format` file located in the root directory of Doris.

Currently, the `.clang-format` configuration file is adapted to versions above clang-format-8.0.1.

The code that you do not want to be formatted is recorded in the `.clang-format-ignore` file. These codes usually come from third-party code bases, and it is recommended to keep the original code style.

## Environmental preparation

You need to download and install clang-format, or you can use the clang-format plug-in provided by IDE or Editor, as described below.

### Download and install clang-format

Ubuntu: `apt-get install clang-format`

The current version is 10.0, you can also specify the old version, for example: `apt-get install clang-format-9`

Mac: `brew install clang-format`

Centos 7:

The version of clang-format installed by centos yum is too old and supports too few StyleOptions. It is recommended to compile version 10.0 from source code.

### clang-format plugin

Clion IDE can use the plug-in "ClangFormat", search and download in `File->Setting->Plugins`. But the version can’t match
The version of the clang-format program matches, judging from the supported StyleOption, it should be lower than clang-format-9.0.

## How to use

### Command line operation

cd to the root directory of Doris, and then execute the following command:

`build-support/clang-format.sh`

> Note: The `clang-format.sh` script requires python 3 to be installed on your machine

### Use clang-format in IDE or Editor

#### Clion

If Clion uses a plug-in, just click `Reformat Code`.

#### VS Code

VS Code needs to install the extension Clang-Format, but you need to provide the location of the clang-format execution program.

Open the VS Code configuration page, directly search for "clang_format", and fill in

```
"clang_format_path": "$clang-format path$",
"clang_format_style": "file"
```

Then, right-click on `Format Document`.
