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

# Doris Document

[Vuepress](https://github.com/vuejs/vuepress.git) is used as our document site generator. Configurations are in `./docs/.vuepress` folder.

## Getting Started

Download and install [nodejs](http://nodejs.cn/download/)

```bash
npm config set registry https://registry.npm.taobao.org // Only if you are in Mainland China.
cd docs && npm install
npm run dev
```

Open your browser and navigate to `localhost:8080/en/` or `localhost:8080/zh-CN/`.

## Docs' Directories

```bash
  .
  ├─ docs/
  │  ├─ .vuepress
  │  │  ├─ dist // Built site files.
  │  │  ├─ public // Assets
  │  │  ├─ sidebar // Side bar configurations.
  │  │  │  ├─ en.js
  │  │  │  └─ zh-CN.js
  │  ├─ theme // Global styles and customizations.
  │  └─ config.js // Vuepress configurations.
  ├─ zh-CN/
  │  ├─ xxxx.md
  │  └─ README.md // Will be rendered as entry page.
  └─ en/
     ├─ one.md
     └─ README.md // Will be rendered as entry page.
```

## Start Writing

1. Write markdown files in multi languages and put them in separated folders `./en/` and `./zh-CN/`. **But they should be with the same name.**

    ```bash
    .
    ├─ en/
    │  ├─ one.md
    │  └─ two.md
    └─ zh-CN/
    │  ├─ one.md
    │  └─ two.md
    ```

2. Frontmatters like below should always be on the top of each file:

    ```markdown
    ---
    {
        "title": "Backup and Recovery", // sidebar title
        "language": "en" // writing language
    }
    ---
    ```

3. Assets are in `.vuepress/public/`.

    Assuming that there exists a png `.vuepress/public/images/image_x.png`, then it can be used like:

    ```markdown
    ![alter text](/images/image_x.png)
    ```

4. Remember to update the sidebar configurations in `.vuepress/sidebar/` after adding a new file or a folder.

    Assuming that the directories are:

    ```bash
    .
    ├─ en/
    │  ├─ subfolder
    │  │  ├─ one.md
    │  │  └─ two.md
    │  └─ three.md
    └─ zh-CN/
       ├─ subfolder
       │  ├─ one.md
       │  └─ two.md
       └─ three.md
    ```

    Then the sidebar configurations would be like:

    ```javascript
    // .vuepress/sidebar/en.js`
    module.exports = [
      {
        title: "subfolder name",
        directoryPath: "subfolder/",
        children: ["one", "two"]
      },
      "three"
    ]
    ```

    ```javascript
    // .vuepress/sidebar/zh-CN.js
    module.exports = [
      {
        title: "文件夹名称",
        directoryPath: "subfolder/",
        children: ["one", "two"]
      },
      "three"
    ]
    ```

5. Run `npm run lint` before starting a PR.

  Surely that there will be lots of error logs if the mardown files are not following the rules, and these logs will all be printed in the console:

```shell

en/administrator-guide/alter-table/alter-table-bitmap-index.md:92 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "    ```"]
en/administrator-guide/alter-table/alter-table-rollup.md:45 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-rollup.md:77 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-rollup.md:178 MD046/code-block-style Code block style [Expected: fenced; Actual: indented]
en/administrator-guide/alter-table/alter-table-schema-change.md:50 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-schema-change.md:82 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-schema-change.md:127 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-schema-change.md:144 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-schema-change.md:153 MD040/fenced-code-language Fenced code blocks should have a language specified [Context: "```"]
en/administrator-guide/alter-table/alter-table-schema-change.md:199 MD046/code-block-style Code block style [Expected: fenced; Actual: indented]
en/administrator-guide/backup-restore.md:45:1 MD029/ol-prefix Ordered list item prefix [Expected: 1; Actual: 2; Style: 1/1/1]
en/administrator-guide/backup-restore.md:57:1 MD029/ol-prefix Ordered list item prefix [Expected: 1; Actual: 2; Style: 1/1/1]
en/administrator-guide/backup-restore.md:61:1 MD029/ol-prefix Ordered list item prefix [Expected: 1; Actual: 3; Style: 1/1/1]
npm ERR! code ELIFECYCLE
npm ERR! errno 1
npm ERR! docs@ lint: `markdownlint '**/*.md' -f`
npm ERR! Exit status 1
npm ERR! 
npm ERR! Failed at the docs@ lint script.

```

## FullText search

We use [Algolia DocSearch](https://docsearch.algolia.com/) as our fulltext search engine.

One thing we need to do is that [Config.json From DocSearch](https://github.com/algolia/docsearch-configs/blob/master/configs/apache_doris.json) should be updated if a new language or branch is created.

For more detail of the docsearch's configuration, please refer to [Configuration of DocSearch](https://docsearch.algolia.com/docs/config-file)

## Deployment

Just start a PR, and all things will be done automatically.

## What Travis Does

Once a PR accepted, travis ci will be triggered to build and deploy the whole website within its own branch. Here is what `.travis.yml` does:

1. Prepare nodejs and vuepress enviorment.

2. Use current branch's name as the relative url path in `.vuepress/config.js`(which is the `base` property).

3. Build the documents into a website all by vuepress.

4. Fetch asf-site repo to local directory, and copy `.vupress/dist/` into `{BRANCH}/`.

5. Push the new site to asf-site repo with `GitHub Token`(which is preset in Travis console as a variable used in .travis.yml).

## asf-site repository

Finally the asf-site repository will be like:

```bash
.
├─ master/
│  ├─ en/
│  │  ├─ subfolder
│  │  │  ├─ one.md
│  │  └─ three.md
│  └─ zh-CN/
│      ├─ subfolder
│      │  ├─ one.md
│      └─ three.md
├─ incubating-0.11/
│  ├─ en/
│  │  ├─ subfolder
│  │  │  ├─ one.md
│  │  └─ three.md
│  └─ zh-CN/
│      ├─ subfolder
│      │  ├─ one.md
│      └─ three.md
├─ index.html // user entry, and auto redirected to master folder
└─ versions.json // all versions that can be seleted on the website are defined here
```

And the `versions.json` is like:

```json
{
  "en": [
    {
      "text": "Versions", // dropdown label
      "items": [
        {
          "text": "master", // dropdown-item label
          "link": "/../master/en/installing/compilation.html", // entry page for this version
          "target": "_blank"
        },
        {
          "text": "branch-0.11",
          "link": "/../branch-0.11/en/installing/compilation.html",
          "target": "_blank"
        }
      ]
    }
  ],
  "zh-CN": [
    {
      "text": "版本",
      "items": [
        {
          "text": "master",
          "link": "/../master/zh-CN/installing/compilation.html",
          "target": "_blank"
        },
        {
          "text": "branch-0.11",
          "link": "/../branch-0.11/zh-CN/installing/compilation.html",
          "target": "_blank"
        }
      ]
    }
  ]
}
```
