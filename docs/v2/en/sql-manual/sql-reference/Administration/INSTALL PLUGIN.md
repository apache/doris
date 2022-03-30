---
{
    "title": "INSTALL PLUGIN",
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

# INSTALL PLUGIN
## description

    To install a plugin

    Syntax

        INSTALL PLUGIN FROM [source]
        
        source supports 3 kinds:
        
        1. Point to a zip file with absolute path.
        2. Point to a plugin dir with absolute path.
        3. Point to a http/https download link of zip file.
        
        PROPERTIES supports setting some configurations of the plugin, such as setting the md5sum value of the zip file.

## example

    1. Intall a plugin with a local zip file:

        INSTALL PLUGIN FROM "/home/users/doris/auditdemo.zip";

    2. Intall a plugin with a local dir:

        INSTALL PLUGIN FROM "/home/users/doris/auditdemo/";

    3. Download and install a plugin:

        INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
        
    4. Download and install a plugin, and set the md5sum value of the zip file: 
               
        INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip" PROPERTIES("md5sum" = "73877f6029216f4314d712086a146570"); 
        
## keyword
    INSTALL,PLUGIN
