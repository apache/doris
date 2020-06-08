---
{
    "title": "INSTALL PLUGIN",
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

# INSTALL PLUGIN
## description

    该语句用于安装一个插件。

    语法

        INSTALL PLUGIN FROM [source]
        
        source 支持三种类型：
        
        1. 指向一个 zip 文件的绝对路径。
        2. 指向一个插件目录的绝对路径。
        3. 指向一个 http 或 https 协议的 zip 文件下载路径

## example

    1. 安装一个本地 zip 文件插件：

        INSTALL PLUGIN FROM "/home/users/doris/auditdemo.zip";

    2. 安装一个本地目录中的插件：

        INSTALL PLUGIN FROM "/home/users/doris/auditdemo/";

    2. 下载并安装一个插件：

        INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
        
## keyword
    INSTALL,PLUGIN
