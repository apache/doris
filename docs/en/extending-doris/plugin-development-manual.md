---
{
    "title": "Plugin Development Manual",
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

# Doris Plugin Framework

## Introduction

Doris plugin framework supports install/uninstall custom plugins at runtime without restart the Doris service. Users can extend Doris's functionality by developing their own plugins.

For example, the audit plugin worked after a request execution, it can obtain information related to a request (access user, request IP, SQL, etc...) and write the information into the specified table.

Differences from UDF:
* UDF is a function used for data calculation when SQL is executed. Plugin is additional function that is used to extend Doris with customized function, such as support different storage engines and different import ways, and plugin doesn't participate in data calculation when executing SQL.
* The execution cycle of UDF is limited to a SQL execution. The execution cycle of plugin may be the same as the Doris process.
* The usage scene is different. If you need to support special data algorithms when executing SQL, then UDF is recommended, if you need to run custom functions on Doris, or start a background thread to do tasks, then the use of plugin is recommended.

Currently the plugin framework only supports audit plugins.

> Note:
> Doris plugin framework is an experimental feature, currently only supports FE plugin, and is closed by default, can be opened by FE configuration `plugin_enable = true`.

## Plugin

A FE Plugin can be a **.zip package** or a **directory**, which contains at least two parts: the `plugin.properties` and `.jar` files. The `plugin.properties` file is used to describe the plugin information.

The file structure of a Plugin looks like this:

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

`plugin.properties` example:

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

## Write A Plugin

The development environment of the FE plugin depends on the development environment of Doris. So please make sure Doris's compilation and development environment works normally.

`fe_plugins` is the parent module of the fe plugins. It can uniformly manage the third-party library information that the plugin depends on. Adding a plugin can add a submodule implementation under `fe_plugins`.

### Create module

We can add a submodule in the `fe_plugins` directory to implement Plugin and create a project:

```
mvn archetype: generate -DarchetypeCatalog = internal -DgroupId = org.apache -DartifactId = doris-fe-test -DinteractiveMode = false
```

The command produces a new mvn project, and a new submodule is automatically added to `fe_plugins/pom.xml`:

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

The new plugin project file structure is as follows:

```
-doris-fe-test/
-pom.xml
-src/
    ---- main/java/org/apache/
    ------- App.java # mvn auto generate, ignore
    ---- test/java/org/apache
```

We will add an assembly folder under main to store `plugin.properties` and `zip.xml`. After completion, the file structure is as follows:

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

### Add zip.xml

`zip.xml`, used to describe the content of the final package of the plugin (.jar file, plugin.properties):

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


### Update pom.xml

Then we need to update `pom.xml`, add doris-fe dependency, and modify maven packaging way:

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

### Implement plugin

Then we can implement Plugin according to the needs. Plugins need to implement the `Plugin` interface. For details, please refer to the `auditdemo` plugin sample code that comes with Doris.

### Compile

Before compiling the plugin, you must first execute `sh build.sh --fe` of Doris to complete the compilation of Doris FE.

Finally, execute `sh build_plugin.sh` in the ${DORIS_HOME} path and you will find the `your_plugin_name.zip` file in `fe_plugins/output`

Or you can execute `sh build_plugin.sh --plugin your_plugin_name` to only build your plugin.
 
### Other way

The easiest way, you can implement your plugin by modifying the example `auditdemo`

## Deploy

Doris's plugin can be deployed in three ways:

* Http or Https .zip, like `http://xxx.xxxxxx.com/data/plugin.zip`, Doris will download this .zip file. At the same time, the value of md5sum needs to be set in properties, or an md5 file with the same name as the `.zip` file needs to be placed, such as `http://xxx.xxxxxx.com/data/my_plugin.zip.md5`. The content is the MD5 value of the .zip file.
* Local .zip, like `/home/work/data/plugin.zip`. If the plug-in is only used for FE, it needs to be deployed in the same directory of all FE nodes. Otherwise, it needs to be deployed on all FE and BE nodes.
* Local directory, like `/home/work/data/plugin`, .zip decompressed folder. If the plug-in is only used for FE, it needs to be deployed in the same directory of all FE nodes. Otherwise, it needs to be deployed on all FE and BE nodes.

Note: Need to ensure that the plugin .zip file is available in the life cycle of doris!

## Install and Uninstall

Install and uninstall the plugin through the install/uninstall statements. More details, see `HELP INSTALL PLUGIN;` `HELP IUNNSTALL PLUGIN;` `HELP SHOW PLUGINS;` 

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
