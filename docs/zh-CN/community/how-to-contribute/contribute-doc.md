---
{
    "title": "文档贡献",
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

# Doris 文档贡献

这里我们主要介绍 Doris 的文档怎么修改和贡献，

怎么去提交你的文档修改，请参照

[为 Doris 做贡献](./)

[代码提交指南](./pull-request)

历史版本的文档，直接在 [apache/doris-website](https://github.com/apache/doris-website) 上提交 PR 即可，如果是最新版本的，需要在 [apache/doris-website](https://github.com/apache/doris-website)  和 [apache/doris](https://github.com/apache/doris)  代码库上同时提交修改。

下面介绍 Doris Website站点的目录结构，以方便用户修改提交文档

## Doris Website 目录结构

```
.
├── README.md
├── babel.config.js
├── blog
│   ├── 1.1 Release.md
│   ├── Annoucing.md
│   ├── jd.md
│   ├── meituan.md
│   ├── release-note-0.15.0.md
│   ├── release-note-1.0.0.md
│   └── xiaomi.md
├── build.sh
├── community
│   ├── design
│   │   ├── Flink-doris-connector-Design.md
│   │   ├── doris_storage_optimization.md
│   │   ├── grouping_sets_design.md
│   │   └── metadata-design.md
│   ├── ......
├── docs
│   ├── admin-manual
│   │   ├── cluster-management
│   │   ├── config
│   │   ├── data-admin
│   │   ├── http-actions
│   │   ├── maint-monitor
│   │   ├── multi-tenant.md
│   │   ├── optimization.md
│   │   ├── privilege-ldap
│   │   ├── query-profile.md
│   │   └── sql-interception.md
│   ├── ......
├── docusaurus.config.js
├── i18n
│   └── zh-CN
│       ├── code.json
│       ├── docusaurus-plugin-content-blog
│       ├── docusaurus-plugin-content-docs
│       ├── docusaurus-plugin-content-docs-community
│       └── docusaurus-theme-classic
├── package.json
├── sidebars.json
├── sidebarsCommunity.json
├── src
│   ├── components
│   │   ├── Icons
│   │   ├── More
│   │   ├── PageBanner
│   │   └── PageColumn
│   ├── ......
├── static
│   ├── images
│   │   ├── Bloom_filter.svg.png
│   │   ├── .....
│   └── js
│       └── redirect.js
├── tree.out
├── tsconfig.json
├── versioned_docs
│   ├── version-0.15
│   │   ├── administrator-guide
│   │   ├── best-practices
│   │   ├── extending-doris
│   │   ├── getting-started
│   │   ├── installing
│   │   ├── internal
│   │   ├── sql-reference
│   │   └── sql-reference-v2
│   └── version-1.0
│       ├── administrator-guide
│       ├── benchmark
│       ├── extending-doris
│       ├── faq
│       ├── getting-started
│       ├── installing
│       ├── internal
│       ├── sql-reference
│       └── sql-reference-v2
├── versioned_sidebars
│   ├── version-0.15-sidebars.json
│   └── version-1.0-sidebars.json
├── versions.json

```

目录结构说明：

1. 博客目录

   - 英文博客目录在根目录下的blog下面，所有博客的英文文件放到这个目录下
   - 中文博客的目录在 `i18n/zh-CN/docusaurus-plugin-content-blog` 目录下，所有中文博客文件放到这个下面
   - 中英文博客的文件名称要一致

2. 文档内容目录

   - 最新版本的英文文档内容在根目录下的docs下面

   - 英文文档的版本在根目录下的 `versioned_docs/` 下面

     - 这个目录只放历史版本的文档

       ```
       .
       ├── version-0.15
       │   ├── administrator-guide
       │   ├── best-practices
       │   ├── extending-doris
       │   ├── getting-started
       │   ├── installing
       │   ├── internal
       │   ├── sql-reference
       │   └── sql-reference-v2
       └── version-1.0
           ├── administrator-guide
           ├── benchmark
           ├── extending-doris
           ├── faq
           ├── getting-started
           ├── installing
           ├── internal
           ├── sql-reference
           └── sql-reference-v2
       ```

     - 英文文档的版本控制在根目录下的 `versioned_sidebars` 下面

       ```
       .
       ├── version-0.15-sidebars.json
       └── version-1.0-sidebars.json
       ```

       这里的 json 文件按照对应版本的目录结构进行编写

   - 中文文档在 `i18n/zh-CN/docusaurus-plugin-content-docs`

     - 在这个下面对应不同的版本目录及版本对应的 json 文件 ，如下效果

       current是当前最新版本的文档，示例中对应的是 1.1 版本，修改的时候，根据要修改的文档版本，在对应目录下找到相应的文件修改，提交即可。

       ```
       .
       ├── current
       │   ├── admin-manual
       │   ├── advanced
       │   ├── benchmark
       │   ├── data-operate
       │   ├── data-table
       │   ├── ecosystem
       │   ├── faq
       │   ├── get-starting
       │   ├── install
       │   ├── sql-manual
       │   └── summary
       ├── current.json
       ├── version-0.15
       │   ├── administrator-guide
       │   ├── best-practices
       │   ├── extending-doris
       │   ├── getting-started
       │   ├── installing
       │   ├── internal
       │   ├── sql-reference
       │   └── sql-reference-v2
       ├── version-0.15.json
       ├── version-1.0
       │   ├── administrator-guide
       │   ├── benchmark
       │   ├── extending-doris
       │   ├── faq
       │   ├── getting-started
       │   ├── installing
       │   ├── internal
       │   ├── sql-reference
       │   └── sql-reference-v2
       └── version-1.0.json
       ```

     - Version Json 文件

       Current.json 对应的是最新版本文档的中文翻译内容，例如：

       ```
       {
         "version.label": {
           "message": "1.1",
           "description": "The label for version current"
         },
         "sidebar.docs.category.Getting Started": {
           "message": "快速开始",
           "description": "The label for category Getting Started in sidebar docs"
         }
         .....
       }
       ```

       这里的 `sidebar.docs.category.Getting Started` 和根目录下的 `sidebars.json`  里的 `label` 对应

       例如刚才这个 `sidebar.docs.category.Getting Started` ，是由 `sidebar` 前缀和 `sidebars.json` 里面的结构对应的

       首先是 `sidebar + "." + docs +  ".'" + [ type ] + [ label ] ` 组成.

       ```json
       {
           "docs": [
               {
                   "type": "category",
                   "label": "Getting Started",
                   "items": [
                       "get-starting/get-starting"
                   ]
               },
               {
                   "type": "category",
                   "label": "Doris Introduction",
                   "items": [
                       "summary/basic-summary"
                   ]
               }
             .....
       }
       ```

     - 在中文的 version json 文件中支持 label 的翻译，不需要描述文档层级关系，文档层级关系是在 `sidebar.json` 文件里描述的

     - 所有的文档必须有英文的，中文才能显示，如果英文没写，可以创建一个空文件，不然中文文档也显示不出来，这个适用于所有博客、文档、社区内容

3. 社区文档

   这块的文档不区分版本，是通用的

   - 英文文档在根目录下的 `community/` 目录下面。

   - 中文文档在  `i18n/zh-CN/docusaurus-plugin-content-docs-community/` 目录下面。

   - 社区文档的目录结构控制在根目录下的 `sidebarsCommunity.json` 文件中，

   - 社区文档目录结构对应的中文翻译在 `i18n/zh-CN/docusaurus-plugin-content-docs-community/current.json` 文件中

     ```json
     {
       "version.label": {
         "message": "Next",
         "description": "The label for version current"
       },
       "sidebar.community.category.How to Contribute": {
         "message": "贡献指南",
         "description": "The label for category How to Contribute in sidebar community"
       },
       "sidebar.community.category.Release Process & Verification": {
         "message": "版本发布与校验",
         "description": "The label for category Release Process & Verification in sidebar community"
       },
       "sidebar.community.category.Design Documents": {
         "message": "设计文档",
         "description": "The label for category Design Documents in sidebar community"
       },
       "sidebar.community.category.Developer Guide": {
         "message": "开发者手册",
         "description": "The label for category Developer Guide in sidebar community"
       }
     }
     ```

4. 图片

   所有图片都在 `static/images `目录下面

## 如何编写命令帮助手册

命令帮助手册文档，是指在 `docs/sql-manual` 下的文档。这些文档用于两个地方：

1. 官网文档展示。
2. HELP 命令的输出。

为了支持 HELP 命令输出，这些文档需要严格按照以下格式排版编写，否则无法通过准入检查。

以 `SHOW ALTER` 命令示例如下：

```
---
{
    "title": "SHOW-ALTER",
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

## SHOW-ALTER

### Nameo

SHOW ALTER

### Description

（描述命令语法。）

### Example

（提供命令示例。）

### Keywords

SHOW, ALTER

### Best Practice

（最佳实践（如有））

```

注意，不论中文还是英文文档，以上标题都是用英文，并且注意标题的层级。

## 文档多版本

网站文档支持通过 html 标签标记版本。可以通过 `<version>` 标签标记文档中的某段内容是从哪个版本开始的，或者从哪个版本移除。

### 参数介绍

| 参数 | 说明 | 值 |
|---|---|---|
| since | 从该版本支持 | 版本号 |
| deprecated | 从该版本移除 | 版本号 |
| comment | 注释 |  |
| type | 有默认和行内两种样式 | 不传值表示默认样式，传inline表示行内样式 |

注意：`<version>` 标签前后要有空行，避免样式渲染异常。

### 单标签

```

<version since="1.1">

Apache Doris was first born as Palo project for Baidu's ad reporting business,
 officially open-sourced in 2017, donated by Baidu to the Apache Foundation 
 for incubation in July 2018, and then incubated and operated by members of 
 the incubator project management committee under the guidance of 
 Apache mentors. Currently, the Apache Doris community has gathered 
 more than 300 contributors from nearly 100 companies in different 
 industries, and the number of active contributors is close to 100 per month. 
 Apache Doris has graduated from Apache incubator successfully and 
 become a Top-Level Project in June 2022.

</version>

```

渲染样式：

<version since="1.1">

Apache Doris was first born as Palo project for Baidu's ad reporting business,
 officially open-sourced in 2017, donated by Baidu to the Apache Foundation 
 for incubation in July 2018, and then incubated and operated by members of 
 the incubator project management committee under the guidance of 
 Apache mentors. Currently, the Apache Doris community has gathered 
 more than 300 contributors from nearly 100 companies in different 
 industries, and the number of active contributors is close to 100 per month. 
 Apache Doris has graduated from Apache incubator successfully and 
 become a Top-Level Project in June 2022.

</version>

### 多标签

```

<version since="1.2" deprecated="1.5">

# Usage Scenarios

As shown in the figure below, after various data integration and processing, the data sources are usually stored in the real-time data warehouse Doris and the offline data lake or data warehouse (in Apache Hive, Apache Iceberg or Apache Hudi).
![Image description](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/sekvbs5ih5rb16wz6n9k.png)

Apache Doris is widely used in the following scenarios:

</version>

```

渲染样式：

<version since="1.2" deprecated="1.5">

# Usage Scenarios

As shown in the figure below, after various data integration and processing, the data sources are usually stored in the real-time data warehouse Doris and the offline data lake or data warehouse (in Apache Hive, Apache Iceberg or Apache Hudi).
![Image description](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/sekvbs5ih5rb16wz6n9k.png)

Apache Doris is widely used in the following scenarios:

</version>

### 包含注释

```

<version since="1.3" comment="This is comment, Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. ">

-   Frontend（FE）: It is mainly responsible for user request access, query parsing and planning, management of metadata, and node management-related work.
-   Backend（BE）: It is mainly responsible for data storage and query plan execution.

Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. And these two types of processes guarantee high availability of services and high reliability of data through consistency protocols. This highly integrated architecture design greatly reduces the operation and maintenance cost of a distributed system.

</version>

```

渲染样式：

<version since="1.3" comment="This is comment, Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. ">

-   Frontend（FE）: It is mainly responsible for user request access, query parsing and planning, management of metadata, and node management-related work.
-   Backend（BE）: It is mainly responsible for data storage and query plan execution.

Both types of processes are horizontally scalable, and a single cluster can support up to hundreds of machines and tens of petabytes of storage capacity. And these two types of processes guarantee high availability of services and high reliability of data through consistency protocols. This highly integrated architecture design greatly reduces the operation and maintenance cost of a distributed system.

</version>

### 行内标签

```
In terms of the storage engine, Doris uses columnar storage to encode and compress and read data by column, <version since="1.0" type="inline" > enabling a very high compression ratio while reducing a large number of scans of non-relevant data,</version> thus making more efficient use of IO and CPU resources.
```

渲染样式：

In terms of the storage engine, Doris uses columnar storage to encode and compress and read data by column, <version since="1.0" type="inline" > enabling a very high compression ratio while reducing a large number of scans of non-relevant data,</version> thus making more efficient use of IO and CPU resources.


