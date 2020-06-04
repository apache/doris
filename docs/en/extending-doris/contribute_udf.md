---
{
    "title": "Contribute UDF",
    "language": "en"
}
---

<! -
One license to the Apache Software Foundation (ASF)
Or more contributor license agreement. Please refer to the announcement document
Distribute with this work for more information
About copyright rights. ASF license this file
According to Apache License Version 2.0 (hereinafter referred to as "
"License"); You must not use this document unless it is in compliance
With license. You can obtain a licensed copy at:

Http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed in writing,
The software distributed under the license will be distributed to
"As is", no guarantees or conditions are provided
Explicit or implicit types. Please refer to the license
Language-specific permissions and restrictions
According to permission.
->

# Contribute UDF

This manual mainly introduces how external users can contribute their own UDF functions to the Doris community.

# Prerequisites

1. UDF function is universal

The versatility here mainly refers to: UDF functions are widely used in certain business scenarios. Such UDF functions are valuable and can be used directly by other users in the community.

If you are not sure whether the UDF function you wrote is universal, you can send an email to `dev@doris.apache.org` or directly create an ISSUE to initiate the discussion.

2. UDF has completed testing and is running normally in the user's production environment

# Ready to work

1. UDF source code
2. User Manual of UDF

## Source code

The placement path should be under `custom_udf/src/my_udf`. Here with udf_samples, first create a new folder under the `custom_udf/src/` path and store the original code.

```

    ├── custom_udf
    │   ├── CMakeLists.txt
    │   └── src
    │       └── udf_samples
    │           ├── CMakeLists.txt
    │           ├── uda_sample.cpp
    │           ├── uda_sample.h
    │           ├── udf_sample.cpp
    │           └── udf_sample.h

```

## manual

The user manual needs to include: UDF function definition description, applicable scenarios, function syntax, how to compile UDF, how to use UDF in Doris, and use examples.

1. The user manual must contain both Chinese and English versions, and be stored under `docs/zh-CN/extending-doris/third-party-udf/` and `docs/en/extending-doris/third-party-udf`, respectively.

    ```
    ├── docs
    │   └── zh-CN
    │       └──extending-doris
    │          └──third-party-udf
    │             ├── udf simple 使用手册

    ```

    ```
    ├── docs
    │   └── en
    │       └──extending-doris
    │          └──third-party-udf
    │             ├── udf simple manual
    ```

2. Add the two manual files to the sidebar in Chinese and English.

    ```
    vi docs/.vuepress/sidebar/zh-CN.js
    {
        title: "第三方 UDF",
        directoryPath: "third-party-udf/",
        children:
        [
            "udf simple 使用手册",
        ],
    },
    ```

    ```
    vi docs/.vuepress/sidebar/en.js
    {
        title: "Third-party UDF",
        directoryPath: "third-party-udf/",
        children:
        [
            "udf simple manual",
        ],
    },

    ```

# Contribute UDF to the community

    When you meet the conditions and prepare the code, you can contribute UDF to the Doris community after the document. Simply submit the request (PR) on [Github] (https://github.com/apache/incubator-doris). See the specific submission method: [Pull Request (PR)] (https://help.github.com/articles/about-pull-requests/).

    Finally, when the PR assessment is passed and merged. Congratulations, your UDF becomes a third-party UDF supported by Doris. You can check it out in the extended functions section of [Doris official website] (http://doris.apache.org/master/zh-CN/)~.
