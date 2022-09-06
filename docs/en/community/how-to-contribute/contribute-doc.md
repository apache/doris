---
{
    "title": "Docs Contribute",
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

# Doris Documentation Contributions

Here we mainly introduce how to modify and contribute to Doris' documents.

How to submit your document modification, please refer to

[Contribute to Doris](./)

[Code Submission Guidelines](./pull-request)

Documents of historical versions can be submitted directly on [apache/doris-website](https://github.com/apache/doris-website) PR, if it is the latest version, it needs to be in [apache/doris-website] (https://github.com/apache/doris-website) and the [apache/doris](https://github.com/apache/doris) codebase at the same time commit changes.

The following introduces the directory structure of the Doris Website site to facilitate users to modify and submit documents

## Doris Website Directory Structure



````
.
├── README.md
├── babel.config.js
├── blog
│ ├── 1.1 Release.md
│ ├── Annoucing.md
│ ├── jd.md
│ ├── meituan.md
│ ├── release-note-0.15.0.md
│ ├── release-note-1.0.0.md
│ └── xiaomi.md
├── build.sh
├── community
│ ├── design
│ │ ├── Flink-doris-connector-Design.md
│ │ ├── doris_storage_optimization.md
│ │ ├── grouping_sets_design.md
│ │ └── metadata-design.md
│ ├──  
├── docs
│ ├── admin-manual
│ │ ├── cluster-management
│ │ ├── config
│ │ ├── data-admin
│ │ ├── http-actions
│ │ ├── maint-monitor
│ │ ├── multi-tenant.md
│ │ ├── optimization.md
│ │ ├── privilege-ldap
│ │ ├── query-profile.md
│ │ └── sql-interception.md
│ ├──  
├── docusaurus.config.js
├── i18n
│ └── en-US
│ ├── code.json
│ ├── docusaurus-plugin-content-blog
│ ├── docusaurus-plugin-content-docs
│ ├── docusaurus-plugin-content-docs-community
│ └── docusaurus-theme-classic
├── package.json
├── sidebars.json
├── sidebarsCommunity.json
├── src
│ ├── components
│ │ ├── Icons
│ │ ├── More
│ │ ├── PageBanner
│ │ └── PageColumn
│ ├──  
├── static
│ ├── images
│ │ ├── Bloom_filter.svg.png
│ │ ├── .....
│ └── js
│ └── redirect.js
├── tree.out
├── tsconfig.json
├── versioned_docs
│ ├── version-0.15
│ │ ├── administrator-guide
│ │ ├── best-practices
│ │ ├── extending-doris
│ │ ├── getting-started
│ │ ├── installing
│ │ ├── internal
│ │ ├── sql-reference
│ │ └── sql-reference-v2
│ └── version-1.0
│ ├── administrator-guide
│ ├── benchmark
│ ├── extending-doris
│ ├── faq
│ ├── getting-started
│ ├── installing
│ ├── internal
│ ├── sql-reference
│ └── sql-reference-v2
├── versioned_sidebars
│ ├── version-0.15-sidebars.json
│ └── version-1.0-sidebars.json
├── versions.json

````

Directory structure description:

1. Blog Directory

   - The English blog directory is under the blog in the root directory, and the English files of all blogs are placed in this directory
   - The directory of the Chinese blog is in the `i18n/zh-CN/docusaurus-plugin-content-blog` directory, all Chinese blog files are placed under this
   - The file names of Chinese and English blogs should be the same

2. Document Content Directory

   - The latest version of the English document content is under docs in the root directory

   - The version of the English documentation is under `versioned_docs/` in the root directory

     - This directory only holds documents from historical versions

       ````
       .
       ├── version-0.15
       │ ├── administrator-guide
       │ ├── best-practices
       │ ├── extending-doris
       │ ├── getting-started
       │ ├── installing
       │ ├── internal
       │ ├── sql-reference
       │ └── sql-reference-v2
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
       ````

     - Versioning of English documents is under `versioned_sidebars` in the root directory

       ````
       .
       ├── version-0.15-sidebars.json
       └── version-1.0-sidebars.json
       ````

       The json file here is written according to the directory structure of the corresponding version

   - Chinese documentation at `i18n/zh-CN/docusaurus-plugin-content-docs`

     - Below this corresponds to different version directories and json files corresponding to the version, as follows

       current is the current latest version of the document. The example corresponds to version 1.1. When modifying, according to the document version to be modified, find the corresponding file modification in the corresponding directory and submit it.

       ````
       .
       ├── current
       │ ├── admin-manual
       │ ├── advanced
       │ ├── benchmark
       │ ├── data-operate
       │ ├── data-table
       │ ├── ecosystem
       │ ├── faq
       │ ├── get-starting
       │ ├── install
       │ ├── sql-manual
       │ └── summary
       ├── current.json
       ├── version-0.15
       │ ├── administrator-guide
       │ ├── best-practices
       │ ├── extending-doris
       │ ├── getting-started
       │ ├── installing
       │ ├── internal
       │ ├── sql-reference
       │ └── sql-reference-v2
       ├── version-0.15.json
       ├── version-1.0
       │ ├── administrator-guide
       │ ├── benchmark
       │ ├── extending-doris
       │ ├── faq
       │ ├── getting-started
       │ ├── installing
       │ ├── internal
       │ ├── sql-reference
       │ └── sql-reference-v2
       └── version-1.0.json
       ````

     - Version Json file

       Current.json corresponds to the Chinese translation of the latest version of the document, for example:

       ````json
       {
         "version.label": {
           "message": "1.1",
           "description": "The label for version current"
         },
         "sidebar.docs.category.Getting Started": {
           "message": "Quick Start",
           "description": "The label for category Getting Started in sidebar docs"
         }
         .....
       }
       ````

       Here `sidebar.docs.category.Getting Started` corresponds to `label` in `sidebars.json` in the root directory

       For example, the `sidebar.docs.category.Getting Started` just now corresponds to the `sidebar` prefix and the structure in `sidebars.json`

       The first is `sidebar + "." + docs + ".'" + [ type ] + [ label ] `.

       ````json
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
       ````

     - Support label translation in the Chinese version json file, no need to describe the document hierarchy, which is described in the `sidebar.json` file

     - All documents must be in English, and Chinese can only be displayed. If English is not written, you can create an empty file, otherwise Chinese documents will not be displayed. This applies to all blogs, documents, and community content

2. Community Documentation

   This document does not distinguish between versions and is generic

   - English documentation is under the `community/` directory in the root directory.

   - Chinese documentation is under `i18n/zh-CN/docusaurus-plugin-content-docs-community/` directory.

   - The directory structure of community documents is controlled in the `sidebarsCommunity.json` file in the root directory,

   - The Chinese translation corresponding to the community documentation directory structure is in the `i18n/zh-CN/docusaurus-plugin-content-docs-community/current.json` file

     ````json
     {
       "version.label": {
         "message": "Next",
         "description": "The label for version current"
       },
       "sidebar.community.category.How to Contribute": {
         "message": "Contribution Guidelines",
         "description": "The label for category How to Contribute in sidebar community"
       },
       "sidebar.community.category.Release Process & Verification": {
         "message": "Version release and verification",
         "description": "The label for category Release Process & Verification in sidebar community"
       },
       "sidebar.community.category.Design Documents": {
         "message": "Design document",
         "description": "The label for category Design Documents in sidebar community"
       },
       "sidebar.community.category.Developer Guide": {
         "message": "Developer's Manual",
         "description": "The label for category Developer Guide in sidebar community"
       }
     }
     ````

3. Pictures

   All images are in the `static/images` directory