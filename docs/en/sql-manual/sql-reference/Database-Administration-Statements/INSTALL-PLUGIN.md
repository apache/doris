---
{
    "title": "INSTALL-PLUGIN",
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

## INSTALL-PLUGIN

### Name

INSTALL PLUGIN

### Description

This statement is used to install a plugin.

grammar:

```sql
INSTALL PLUGIN FROM [source] [PROPERTIES ("key"="value", ...)]
````

source supports three types:

1. An absolute path to a zip file.
2. An absolute path to a plugin directory.
3. Point to a zip file download path with http or https protocol

### Example

1. Install a local zip file plugin:

    ```sql
    INSTALL PLUGIN FROM "/home/users/doris/auditdemo.zip";
    ````

2. Install the plugin in a local directory:

    ```sql
    INSTALL PLUGIN FROM "/home/users/doris/auditdemo/";
    ````

3. Download and install a plugin:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
    ````

4. Download and install a plugin, and set the md5sum value of the zip file at the same time:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip" PROPERTIES("md5sum" = "73877f6029216f4314d712086a146570");
    ````

### Keywords

    INSTALL, PLUGIN

### Best Practice

