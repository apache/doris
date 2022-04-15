---
{
    "title": "贡献 UDF ",
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

# 贡献 UDF

该手册主要讲述了外部用户如何将自己编写的 UDF 函数贡献给 Doris 社区。

## 前提条件

1. UDF 函数具有通用性

   这里的通用性主要指的是：UDF 函数在某些业务场景下，被广泛使用。也就是说 UDF 函数具有复用价值，可被社区内其他用户直接使用。

   如果你不确定自己写的 UDF 函数是否具有通用性，可以发邮件到 `dev@doris.apache.org` 或直接创建 ISSUE 发起讨论。

2. UDF 已经完成测试，并正常运行在用户的生产环境中

## 准备工作

1. UDF 的 source code
2. UDF 的使用手册

### 源代码

在 `contrib/udf/src/` 下创建一个存放 UDF 函数的文件夹，并将源码和 CMAKE 文件存放在此处。待贡献的源代码应该包含: `.h` , `.cpp`, `CMakeFile.txt`。这里以 udf_samples 为例，首先在 `contrib/udf/src/` 路径下创建一个新的文件夹，并存放源码。

```
   ├──contrib
   │  └── udf
   │    ├── CMakeLists.txt
   │    └── src
   │       └── udf_samples
   │           ├── CMakeLists.txt
   │           ├── uda_sample.cpp
   │           ├── uda_sample.h
   │           ├── udf_sample.cpp
   │           └── udf_sample.h

```

1. CMakeLists.txt

   用户的 `CMakeLists.txt` 放在此处后，需要进行少量更改。去掉 `include udf` 和 `udf lib` 即可。去掉的原因是，在 `contrib/udf` 层级的 CMake 文件中，已经声明了。

### 使用手册

使用手册需要包含：UDF 函数含义说明，适用的场景，函数的语法，如何编译 UDF ，如何在 Doris 集群中使用 UDF， 以及使用示例。

1. 使用手册需包含中英文两个版本，并分别存放在 `docs/zh-CN/extending-doris/udf/contrib` 和 `docs/en/extending-doris/udf/contrib` 下。

    ```
    ├── docs
    │   └── zh-CN
    │       └──extending-doris
    │          └──udf
    │            └──contrib
    │              ├── udf-simple-manual.md
 
    ``` 

    ```
    ├── docs
    │   └── en
    │       └──extending-doris
    │          └──udf
    │            └──contrib
    │              ├── udf-simple-manual.md
    ```

2. 将两个使用手册的文件，加入中文和英文的 sidebar 中。

    ```
    vi docs/.vuepress/sidebar/zh-CN.js
    {
        title: "用户贡献的 UDF",
        directoryPath: "contrib/",
        children:
        [
            "udf-simple-manual",
        ],
    },
    ```

    ```
    vi docs/.vuepress/sidebar/en.js
    {
        title: "Users contribute UDF",
        directoryPath: "contrib/",
        children:
        [
            "udf-simple-manual",
        ],
    },

    ```

## 贡献 UDF 到社区

当你符合前提条件并准备好代码，文档后就可以将 UDF 贡献到 Doris 社区了。在  [Github](https://github.com/apache/incubator-doris) 上面提交 Pull Request (PR) 即可。具体提交方式见：[Pull Request (PR)](https://help.github.com/articles/about-pull-requests/)。

最后，当 PR 评审通过并 Merge 后。恭喜你，你的 UDF 已经贡献给 Doris 社区，你可以在 [Doris 官网](/zh-CN) 的生态扩展部分查看到啦~。
