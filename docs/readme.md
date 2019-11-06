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

## Philosophy

**write once, use everywhere**

Documentations will be written once, and will be converted to other format according to different application scenarios.

## Implementation

```
         +---------------+
         | Documentation |
         +-------+-------+
                 |
         +-------+-------+
         |  Doc Builder  |
         +-------+-------+
                 |
    +--------------------------------+
    |            |                   |
+---+---+    +---+----+        +-----+----+
|  PDF  |    |  HTML  |  ....  | Help Doc |
+-------+    +--------+        +----------+

```

> Documentation：Text contents which is written by human. And this is the only place for documentation. 
> Doc Builder: Tools that convert documentations to other format, such as PDF, HTML. There could be many tools, and we can use different tools to convert documentation to different formats. 

## Organization

> `docs/documentation`: Root directory for documentation. And for different languages, there is a root directory for it. For example, `docs/documentation/cn` is the Chinese documentation's root directory.
> `docs/scripts`: Place of `Doc Builder`.
> `docs/resources`: Resources that are referenced in documentation, such as pictures.
> `docs/website`: A website for documentations built with [Sphinx](http://www.sphinx-doc.org) using a theme provided by [Read-the-Docs](https://readthedocs.org/).

## Constraints

1. All documents are written in Markdown format, and file name is end with ".md".
2. All documents are started with level 1 title `# Title`, and should have only one level 1 title.
3. Names of file and directory are in lowercase letters, and use dashes as separator. 
4. Documentation can be constructed as a directory or a single Markdown file, these two formats equal  with each other in logical. Relationship is represented by parent-child directory in directory format, and by title level in file format. It is recommended to use directory format to manage a large documentation, because it is easy to maintain.
3. A directory corresponds to a title, and readme.md in this directory is its content. Other documents in this directory is its sub-sections.
4. For manual like section, such as function description, there should be `Description`, `Syntax`, `Examples` section in documents.

## Level Directories

1. doris-concepts
2. installing
3. getting-started
4. administrator-guide
5. sql-references
6. best-practices
7. internals
8. community

Each directory, or its sub directories should contain a file `index.rst`, for constructing the navibar of the website. For example:

```
documentation/
└── cn
    ├── administrator-guide
    │   ├── index.rst
    │   ├── http-actions
    │   │   └── index.rst
    │   ├── load-data
    │   │   ├── index.rst
    │   ├── operation
    │   │   ├── index.rst
    ├── extending-doris
    │   ├── index.rst
    └── sql-reference
        ├── index.rst
        │   ├── date-time-functions
        │   │   ├── index.rst
```

## Docs Styles

There are some styles need to be followed.

### SQL-Statement

Docs under `documentation/cn/sql-reference/sql-statements/` must obey the following style

```
# TITLE(capital)

## description

    The description of this doc. The "## description" must be reserved, with a following empty line.

## keyword

    The keyword of this doc. Usually, this can be the title of this doc.
    The "## keyword" must  be reserved, with a following empty line.
```

