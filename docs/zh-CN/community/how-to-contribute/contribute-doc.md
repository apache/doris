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