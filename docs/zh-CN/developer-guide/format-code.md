---
{
    "title": "代码格式化",
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

# 代码格式化
为了自动格式化代码，推荐使用clang-format进行代码格式化。

## 代码风格定制
Doris的代码风格在Google Style的基础上稍有改动，定制为.clang-format文件，位于Doris根目录。

目前，.clang-format配置文件适配clang-format-8.0.1以上的版本。

## 环境准备
需要下载安装clang-format，也可使用IDE或Editor提供的clang-format插件，下面分别介绍。

### 下载安装clang-format
Ubuntu: `apt-get install clang-format` 

当前版本为10.0，也可指定旧版本，例如: `apt-get install clang-format-9`

Centos 7: 

centos yum安装的clang-format版本过老，支持的StyleOption太少，建议源码编译10.0版本。

### clang-format插件
Clion IDE可使用插件"ClangFormat"，`File->Setting->Plugins`搜索下载。但版本无法和
clang-format程序的版本匹配，从支持的StyleOption上看，应该是低于clang-format-9.0。

## 使用方式

### 命令行运行
`clang-format --style=file -i $File$` 

`--sytle=file`就会自动找到.clang-format文件，根据文件Option配置来格式化代码。

批量文件clang-format时，需注意过滤不应该格式化的文件。例如，只格式化*.h/*.cpp，并排除某些文件夹：

`find . -type f -not \( -wholename ./env/* \) -regextype posix-egrep -regex
 ".*\.(cpp|h)" | xargs clang-format -i -style=file`

### 在IDE或Editor中使用clang-format
#### Clion
Clion如果使用插件，点击`Reformat Code`即可。
#### VS Code
VS Code需安装扩展程序Clang-Format，但需要自行提供clang-format执行程序的位置。

```
"clang-format.executable":  "$clang-format path$",
"clang-format.style": "file"
```
然后，点击`Format Document`即可。