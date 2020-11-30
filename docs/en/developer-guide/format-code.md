---
{
    "title": "Format Code",
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

# Format Code
Doris use `Clang-format` to automatically check the format of your source code.

## Code Style
Doris Code Style is based on Google's, makes a few changes. The customized .clang-format
file is in the root dir of Doris.
Now, .clang-format file only works on clang-format-8.0.1+.

## Preparing
You should install clang-format, or you can use clang-format plugins which support by IDEs or Editors.

### Install clang-format
Ubuntu: `apt-get install clang-format` 

Mac: `brew install clang-format`

The current release is 10.0, you can specify old version, e.g.
 
 `apt-get install clang-format-9`

Centos 7: 

The version of clang-format installed by yum is too old. Compiling clang from source
is recommended.

### Clang-format plugins
Clion IDE supports the plugin "ClangFormat", you can search in `File->Setting->Plugins`
 and download it.
But the version is not match with clang-format. Judging from the options supported, 
the version is lower than clang-format-9.0.

## Usage

### CMD
Change directory to the root directory of Doris sources and run the following command:
`build-support/clang-format.sh`

NOTE: Python3 is required to run the `clang-format.sh` script.

### Using clang-format in IDEs or Editors
#### Clion
If using the plugin 'ClangFormat' in Clion, choose `Reformat Code` or press the keyboard 
shortcut.

#### VS Code
VS Code needs install the extension 'Clang-Format', and specify the executable path of 
clang-format in settings.

Open the vs code configuration page and search `clang_format`, fill the box as follows.

```
"clang_format_path":  "$clang-format path$",
"clang_format_style": "file"
```
Then, right click the file and choose `Format Document`.
