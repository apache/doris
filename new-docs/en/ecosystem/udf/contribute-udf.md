---
{
    "title": "Contribute UDF",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Contribute UDF

This manual mainly introduces how external users can contribute their own UDF functions to the Doris community.

## Prerequisites

1. UDF function is universal

    The versatility here mainly refers to: UDF functions are widely used in certain business scenarios. Such UDF functions are valuable and can be used directly by other users in the community.

    If you are not sure whether the UDF function you wrote is universal, you can send an email to `dev@doris.apache.org` or directly create an ISSUE to initiate the discussion.

2. UDF has completed testing and is running normally in the user's production environment

## Ready to work

1. UDF source code
2. User Manual of UDF

### Source code

Create a folder for UDF functions under `contrib/udf/src/`, and store the source code and CMAKE files here. The source code to be contributed should include: `.h`, `.cpp`, `CMakeFile.txt`. Taking udf_samples as an example here, first create a new folder under the `contrib/udf/src/` path and store the source code.

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

    After the user's `CMakeLists.txt` is placed here, a small amount of changes are required. Just remove `include udf` and `udf lib`. The reason for the removal is that it has been declared in the CMake file at the `contrib/udf` level.

### manual

The user manual needs to include: UDF function definition description, applicable scenarios, function syntax, how to compile UDF, how to use UDF in Doris, and use examples.

1. The user manual must contain both Chinese and English versions and be stored under `docs/zh-CN/extending-doris/contrib/udf` and `docs/en/extending-doris/contrib/udf` respectively.

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

2. Add the two manual files to the sidebar in Chinese and English.

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

## Contribute UDF to the community

When you meet the conditions and prepare the code, you can contribute UDF to the Doris community after the document. Simply submit the request (PR) on [Github](https://github.com/apache/incubator-doris). See the specific submission method: [Pull Request (PR)](https://help.github.com/articles/about-pull-requests/).

Finally, when the PR assessment is passed and merged. Congratulations, your UDF becomes a third-party UDF supported by Doris. You can check it out in the extended functions section of [Doris official website](http://doris.apache.org/master/zh-CN/)~.
