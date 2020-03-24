# README
[TOC]
## Introduction
fe_plugins is the parent module of the fe plugins. It can uniformly manage the third-party library information that
 the plugin depends on. Adding a plugin can add a submodule implementation under fe_plugins

## Plugin

A FE Plugin can be a .zip package or a directory, which contains at least two parts: the plugin.properties and .jar files. The plugin.properties file is used to describe the plugin information.

The file structure of a Plugin looks like this:
```
# plugin .zip
auditodemo.zip:
-plugin.properties
-auditdemo.jar
-xxx.config
-data \
-test_data

# plugin local directory
auditodemo /:
-plugin.properties
-auditdemo.jar
-xxx.config
-data \
-test_data
```

plugin.properties example:

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
classname = plugin.AuditPluginDemo

### BE-Plugin optional:
# the name of the so to load
soName = example.so
```

## Write A Plugin
### create module
We can add a submodule in the fe_plugins directory to implement Plugin and create a project:
```
mvn archetype: generate -DarchetypeCatalog = internal -DgroupId = org.apache -DartifactId = doris-fe-test -DinteractiveMode = false
```

The command produces a new mvn project, and a new submodule is automatically added to fe_plugins / pom.xml:
```
    .....
    <groupId> org.apache </ groupId>
    <artifactId> doris-fe-plugins </ artifactId>
    <packaging> pom </ packaging>
    <version> 1.0-SNAPSHOT </ version>
    <modules>
        <module> auditdemo </ module>
        # new plugin module
        <module> doris-fe-test </ module>
    </ modules>

    <properties>
        <doris.home> $ {basedir} /../../ </ doris.home>
    </ properties>
    .....
```

The new plugin project file structure is as follows:
```
-doris-fe-test /
-pom.xml
-src /
---- main / java / org / apache /
------App.java # mvn auto generate, ignore
---- test / java / org / apache
```

We will add an assembly folder under main to store plugin.properties and zip.xml. After completion, the file structure is as follows:
```
-doris-fe-test /
-pom.xml
-src /
---- main /
------ assembly /
-------- plugin.properties
-------- zip.xml
------ java / org / apache /
--------App.java # mvn auto generate, ignore
---- test / java / org / apache
```

### add zip.xml
zip.xml, used to describe the content of the final package of the plugin (.jar file, plugin.properties):

```
<assembly>
    <id> plugin </ id>
    <formats>
        <format> zip </ format>
    </ formats>
    <!-IMPORTANT: must be false->
    <includeBaseDirectory> false </ includeBaseDirectory>
    <fileSets>
        <fileSet>
            <directory> target </ directory>
            <includes>
                <include> *. jar </ include>
            </ includes>
            <outputDirectory> / </ outputDirectory>
        </ fileSet>

        <fileSet>
            <directory> src / main / assembly </ directory>
            <includes>
                <include> plugin.properties </ include>
            </ includes>
            <outputDirectory> / </ outputDirectory>
        </ fileSet>
    </ fileSets>
</ assembly>
```


### update pom.xml
Then we need to update pom.xml, add doris-fe dependency, and modify maven packaging way:
```
<? xml version = "1.0" encoding = "UTF-8"?>
<project xmlns = "http://maven.apache.org/POM/4.0.0"
         xmlns: xsi = "http://www.w3.org/2001/XMLSchema-instance"
         xsi: schemaLocation = "http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <groupId> org.apache </ groupId>
        <artifactId> doris-fe-test </ artifactId>
        <version> 1.0-SNAPSHOT </ version>
    </ parent>
    <modelVersion> 4.0.0 </ modelVersion>

<! --- IMPORTANT UPDATE! --->
    <artifactId> doris-fe-test </ artifactId>
    <packaging> jar </ packaging>
    <dependencies>
        <dependency>
            <groupId> org.apache </ groupId>
            <artifactId> doris-fe </ artifactId>
        </ dependency>
    </ dependencies>
    <build>
        <finalName> doris-fe-test </ finalName>
        <plugins>
            <plugin>
                <artifactId> maven-assembly-plugin </ artifactId>
                <version> 2.4.1 </ version>
                <configuration>
                    <appendAssemblyId> false </ appendAssemblyId>
                    <descriptors>
                        <descriptor> src / main / assembly / zip.xml </ descriptor>
                    </ descriptors>
                </ configuration>
                <executions>
                    <execution>
                        <id> make-assembly </ id>
                        <phase> package </ phase>
                        <goals>
                            <goal> single </ goal>
                        </ goals>
                    </ execution>
                </ executions>
            </ plugin>
        </ plugins>
    </ build>
<! --- IMPORTANT UPDATE! --->
</ project>
```

### implement plugin
Then we can happily implement Plugin according to the needs

### compile
Before compiling the plugin, you must first execute `sh build.sh --fe` of Doris to complete the compilation of Doris FE.

Finally, execute `sh build_plugin.sh` in the ${DORIS_HOME} path and you will find the plugin .zip file in fe_plugins/output 
 
### other way
The easiest way, you can implement your plugin by modifying an example

## Deploy
Doris's plugin can be deployed in three ways:

* Http or Https .zip, like http://xxx.xxxxxx.com/data/plugin.zip, Doris will download this .zip file
* Local .zip, like /home/work/data/plugin.zip, need to be deployed on all FE and BE nodes
* Local directory, like /home/work/data/plugin, .zip decompressed folder, need to be deployed on all FE, BE nodes

Note: Need to ensure that the plugin .zip file is available in the life cycle of doris!

## Use

Install and uninstall the plugin through the install/uninstall statements:

```
mysql>
mysql>
mysql> install plugin from "/home/users/seaven/auditdemo.zip";
Query OK, 0 rows affected (0.09 sec)

mysql>
mysql>
mysql>
mysql> show plugins;
+ ------------------- + ------- + --------------- + ----- ---- + ------------- + ------------------------ + ------ -+ --------------------------------- + ----------- +
Name | Type | Description | Version | JavaVersion | ClassName | SoName | Sources |
+ ------------------- + ------- + --------------- + ----- ---- + ------------- + ------------------------ + ------ -+ --------------------------------- + ----------- +
audit_plugin_demo | AUDIT | just for test | 0.11.0 | 1.8.31 | plugin.AuditPluginDemo | NULL | /home/users/hekai/auditdemo.zip | INSTALLED |
+ ------------------- + ------- + --------------- + ----- ---- + ------------- + ------------------------ + ------ -+ --------------------------------- + ----------- +
1 row in set (0.02 sec)
mysql>
mysql>
mysql>
mysql> uninstall plugin audit_plugin_demo;
Query OK, 0 rows affected (0.05 sec)

mysql> show plugins;
Empty set (0.00 sec)

mysql>

```