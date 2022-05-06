---
{
    "title": "插件开发手册",
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

# Doris 插件框架

## 介绍

Doris 的插件框架支持在运行时添加/卸载自定义插件，而不需要重启服务，用户可以通过开发自己的插件来扩展Doris的功能。

例如，审计插件作用于 Doris 请求执行后，可以获取到一次请求相关的信息（访问用户，请求IP，SQL等...），并将信息写入到指定的表中。

与UDF的区别：
* UDF是函数，用于在SQL执行时进行数据计算。插件是附加功能，用于为Doris扩展自定义的功能，例如：支持不同的存储引擎，支持不同的导入方式，插件并不会参与执行SQL时的数据计算。
* UDF的执行周期仅限于一次SQL执行。插件的执行周期可能与Doris进程相同。
* 使用场景不同。如果您需要执行SQL时支持特殊的数据算法，那么推荐使用UDF，如果您需要在Doris上运行自定义的功能，或者是启动一个后台线程执行任务，那么推荐使用插件。

目前插件框架仅支持审计类插件。

> 注意:
> Doris的插件框架是实验性功能, 目前只支持FE插件，且默认是关闭的，可以通过FE配置`plugin_enable=true`打开

## 插件

一个FE的插件可以使一个**zip压缩包**或者是一个**目录**。其内容至少包含两个文件：`plugin.properties` 和 `.jar` 文件。`plugin.properties`用于描述插件信息。

文件结构如下：

```
# plugin .zip
auditodemo.zip:
    -plugin.properties
    -auditdemo.jar
    -xxx.config
    -data/
    -test_data/

# plugin local directory
auditodemo/:
    -plugin.properties
    -auditdemo.jar
    -xxx.config
    -data/
    -test_data/
```

`plugin.properties` 内容示例:

```
### required:
#
# the plugin name
name = audit_plugin_demo
#
# the plugin type
type = AUDIT
#
# simple summary of the plugin
description = just for test
#
# Doris's version, like: 0.11.0
version = 0.11.0

### FE-Plugin optional:
#
# version of java the code is built against
# use the command "java -version" value, like 1.8.0, 9.0.1, 13.0.4
java.version = 1.8.31
#
# the name of the class to load, fully-qualified.
classname = AuditPluginDemo

### BE-Plugin optional:
# the name of the so to load
soName = example.so
```

## 编写插件

插件的开发环境依赖Doris的开发编译环境。所以请先确保Doris的开发编译环境运行正常。

`fe_plugins` 目录是 FE 插件的根模块。这个根模块统一管理插件所需的依赖。添加一个新的插件，相当于在这个根模块添加一个子模块。

### 创建插件模块

我们可以通过以下命令在 `fe_plugins` 目录创建一个子模块用户实现创建和创建工程。其中 `doris-fe-test` 为插件名称。

```
mvn archetype: generate -DarchetypeCatalog = internal -DgroupId = org.apache -DartifactId = doris-fe-test -DinteractiveMode = false
```

这个命令会创建一个新的 maven 工程，并且自动向 `fe_plugins/pom.xml` 中添加一个子模块：

```
    .....
    <groupId>org.apache</groupId>
    <artifactId>doris-fe-plugins</artifactId>
    <packaging>pom</packaging>
    <version>1.0-SNAPSHOT</version>
    <modules>
        <module>auditdemo</module>
        # new plugin module
        <module>doris-fe-test</module>
    </modules>
    .....
```

新的工程目录结构如下：

```
-doris-fe-test/
-pom.xml
-src/
    ---- main/java/org/apache/
    ------- App.java # mvn auto generate, ignore
    ---- test/java/org/apache
```

接下来我们在 `main` 目录下添加一个 `assembly` 目录来存放 `plugin.properties` 和 `zip.xml`。最终的工程目录结构如下：

```
-doris-fe-test/
-pom.xml
-src/
---- main/
------ assembly/
-------- plugin.properties
-------- zip.xml
------ java/org/apache/
--------App.java # mvn auto generate, ignore
---- test/java/org/apache
```

### 添加 zip.xml

`zip.xml` 用于描述最终生成的 zip 压缩包中的文件内容。（如 .jar file, plugin.properties 等等）

```
<assembly>
    <id>plugin</id>
    <formats>
        <format>zip</format>
    </formats>
    <!-IMPORTANT: must be false->
    <includeBaseDirectory>false</includeBaseDirectory>
    <fileSets>
        <fileSet>
            <directory>target</directory>
            <includes>
                <include>*.jar</include>
            </ ncludes>
            <outputDirectory>/</outputDirectory>
        </fileSet>

        <fileSet>
            <directory>src/main/assembly</directory>
            <includes>
                <include>plugin.properties</include>
            </includes>
            <outputDirectory>/</outputDirectory>
        </fileSet>
    </fileSets>
</assembly>
```

### 更新 pom.xml

接下来我们需要更新子模块的 `pom.xml` 文件，添加 doris-fe 依赖：

```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xmlns="http://maven.apache.org/POM/4.0.0"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <groupId>org.apache</groupId>
        <artifactId>doris-fe-plugins</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>

    <artifactId>auditloader</artifactId>
    <packaging>jar</packaging>

    <dependencies>
        <!-- doris-fe dependencies -->
        <dependency>
            <groupId>org.apache</groupId>
            <artifactId>doris-fe</artifactId>
        </dependency>

        <!-- other dependencies -->
        <dependency>
            ...
        </dependency>
    </dependencies>

    <build>
        <finalName>auditloader</finalName>
        <plugins>
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>2.4.1</version>
                <configuration>
                    <appendAssemblyId>false</appendAssemblyId>
                    <descriptors>
                        <descriptor>src/main/assembly/zip.xml</descriptor>
                    </descriptors>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

</project>
```

### 实现插件

之后我们就可以开始进行插件功能的开发了。插件需要实现 `Plugin` 接口。具体可以参阅 Doris 自带的 `auditdemo` 插件示例代码。

### 编译

在编译插件之前，需要先执行 `sh build.sh --fe` 进行 Doris FE 代码的编译，并确保编译成功。

之后，执行 `sh build_plugin.sh` 编译所有插件。最终的产出会存放在 `fe_plugins/output` 目录中。

或者也可以执行 `sh build_plugin.sh --plugin your_plugin_name` 来仅编译指定的插件。

### 另一种开发方式

您可以直接通过修改自带的 `auditdemo` 插件示例代码进行开发。

## 部署

插件可以通过以下三种方式部署。

* 将 `.zip` 文件放在 Http 或 Https 服务器上。如：`http://xxx.xxx.com/data/my_plugin.zip`, Doris 会下载这个文件。同时需要在properties中设置md5sum的值，或者放置一个和 `.zip` 文件同名的 md5 文件，如 `http://xxx.xxxxxx.com/data/my_plugin.zip.md5`。其中内容为 .zip 文件的 MD5 值。
* 本地 `.zip` 文件。 如：`/home/work/data/plugin.zip`。如果该插件仅用于 FE，则需部署在所有 FE 节点相同的目录下。否则，需要在所有 FE 和 BE 节点部署。
* 本地目录。如：`/home/work/data/plugin/`。相当于 `.zip` 文件解压后的目录。如果该插件仅用于 FE，则需部署在所有 FE 节点相同的目录下。否则，需要在所有 FE 和 BE 节点部署。

注意：需保证部署路径在整个插件生命周期内有效。

## 安装和卸载插件

通过如下命令安装和卸载插件。更多帮助请参阅 `HELP INSTALL PLUGIN;` `HELP IUNNSTALL PLUGIN;` `HELP SHOW PLUGINS;`

```
mysql> install plugin from "/home/users/doris/auditloader.zip";
Query OK, 0 rows affected (0.09 sec)

mysql> show plugins\G
*************************** 1. row ***************************
       Name: auditloader
       Type: AUDIT
Description: load audit log to olap load, and user can view the statistic of queries
    Version: 0.12.0
JavaVersion: 1.8.31
  ClassName: AuditLoaderPlugin
     SoName: NULL
    Sources: /home/users/doris/auditloader.zip
     Status: INSTALLED
 Properties: {}
*************************** 2. row ***************************
       Name: AuditLogBuilder
       Type: AUDIT
Description: builtin audit logger
    Version: 0.12.0
JavaVersion: 1.8.31
  ClassName: org.apache.doris.qe.AuditLogBuilder
     SoName: NULL
    Sources: Builtin
     Status: INSTALLED
 Properties: {}   
2 rows in set (0.00 sec)

mysql> uninstall plugin auditloader;
Query OK, 0 rows affected (0.05 sec)

mysql> show plugins;
Empty set (0.00 sec)
```
