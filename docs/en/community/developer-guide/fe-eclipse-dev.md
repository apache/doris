---
{
    "title": "Setting FE dev env - Eclipse",
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

# Setting FE dev env using Eclipse

## Preparation

* JDK 1.8+
* Maven 3.x+
* Eclipse, with [M2Eclipse](http://www.eclipse.org/m2e/) installed

### Code Generation

The FE module requires part of the generated code, such as Thrift, Protobuf, Jflex, CUP and other frameworks. 

#### Windows

1. Under Linux, enter the source code directory `fe` and execute the following command:

   ```
    mvn  generate-sources
   ```
    
2. If use windows as development environment, then package the generated `fe/fe-core/target/generated-sources` directory:

   `fe/fe-core/target/ && tar czf java.tar.gz generated-sources/`

3. Copy `java.tar.gz` to the `fe/fe-core/target/` directory of the development environment and unzip

    ```
    cp java.tar.gz /path/to/doris/fe/fe-core/target/
    cd /path/to/doris/fe/fe-core/target/ && tar xzf java.tar.gz
    ```

#### MacOS

We can use maven to do code generation or in other words build FE project
directly on MacOS.

1. Install apache thrift from source code or `brew` if you don't have one
2. Create a directory `thirdparty/installed/bin`, and link command thrift into
	 it.

	```
	mkdir -p thirdparty/installed/bin
	ln -s ${thrift_installed_full_path} thirdparty/installed/bin/thrift
	```
3. Call `maven` to build FE project, if something went wrong, check you
	 `$JAVA_HOME`, java version and newly installed command `thrift` work
	 correctly.

	```
	cd fe && mvn package -DskipTests=true -Dos.arch=x86_64
	```

Option `-Dos.arch=x86_64` in step 3 is for compatibility of Apple's M series
CPU.

Note: 
0. cup and jfex use java jar to do code generation, the process is platform
	 independent.
1. Code generation of protobuf is done by `protoc-jar-maven-plugin`, which uses 
	 precompiled binaries of different archs to make it seems platform independent.
2. thrift is the only one relies on `thirdparty/installed`, which needs to be
	 built from source. We will make it independent on `thirdparty/installed`
	 someday (TODO).

## Import FE project

### Import as eclipse project

1. In the `fe/` directory of the development environment, execute the following command to generate the Eclipse project file:

    `cd /path/to/doris/fe/ && mvn -npr eclipse:eclipse -Dskip.plugin=true`
    
    After the execution is completed, the `.project` and `.classpath` files will be generated in the `fe/` directory.

2. Import FE project

    * Open Eclipse, choose `File -> Import`.
    * Choose `General -> Existing Projects into Workspace`.
    * `Select root directory` and choose `fe/` directory, click `Finish` to finish.
    * Right click the project, and choose `Build Path -> Configure Build Path`.
    * In the `Java Build Path` dialog, choose the `Source` tab, click `Add Folder`, and select the `java/` directory that was copied and unzipped before adding.
    * Click `Apply and Close` to finish.

At this point, FE project import is complete. The project directory in Eclipse is roughly as follows:

![](/images/eclipse-import-fe-project-1.png)

### Import as maven project

We are able to build FE with maven, if we have done all the operations related
to MacOS. And we are now of course able to import FE project as a maven project
in eclipse.

In eclipse menu `File` select `Import -> Maven -> Existing Maven Projects`,
Choose doris fe directory to finish import. It's recommended to use working set
to manage the all modules of FE.

Enjoy developing and debugging FE in eclipse!

## Run Unit Test

Right-click on the unit test file you want to run and select `Run As -> JUnit Test`. (If you want to debug, select `Debug As -> JUnit Test`).

If the following error occurs:

```
java.lang.Exception: Method xxxx should have no parameters
```

Then right-click the unit test file and select `Run As -> Run Configurations...`. (If you want to debug, select `Debug As -> Debug Configurations...`).

Add to the `VM arguments` in the `Arguments` tab:

```
-javaagent:${settings.localRepository}/org/jmockit/jmockit/1.48/jmockit-1.48.jar
```

Among them, `${settings.localRepository}` should be replaced with the path of the maven library path, such as:

```
-javaagent:/Users/cmy/.m2/repository/org/jmockit/jmockit/1.48/jmockit-1.48.jar
```

Then just run `Run/Debug`.

## Run FE

You can directly start an FE process in Eclipse to facilitate debugging the code.

1. Create a runtime directory

    ```
    mkdir /path/to/doris/fe/run/
    cd /path/to/doris/fe/run/
    mkdir conf/log/doris-meta/
    ```
    
2. Create configuration file
    
    Create the configuration file `fe.conf` in the `conf/` directory created in the first step. You can directly copy `conf/fe.conf` in the source directory and make simple changes.
    
3. Find the `src/main/java/org/apache/doris/PaloFe.java` file in Eclipse, right-click and select `Run As -> Run Configurations...`. Add the following environment variables to the `Environment` tab:

    * `DORIS_HOME: /path/to/doris/fe/run/`
    * `PID_DIR: /path/to/doris/fe/run/`
    * `LOG_DIR: /path/to/doris/fe/run/log`

4. Right-click `PaloFe.java` and select `Run As -> Java Application` to start FE.

To run a UT, a FE service will be started at first. And then, UT cases execute as client and test corresponding logics. When errors occur, only client logs will be print in UT logs. If you need more information from **server logs**, you can view logs in `${DORIS_HOME}/fe/mocked`.

## Code Update

### Imported as eclipse project
1. Update lexical and grammar files or proto and thrift files

    If you modified `fe/src/main/cup/sql_parser.cup` or `fe/src/main/jflex/sql_scanner.flex` file or proto and thrift files. You need to execute the following commands in the `fe/` directory:
    
    ```
    mvn  generate-sources
    ```
    
    Then refresh the project in Eclipse.
        
2. Update maven dependencies

    If you update the dependency in `fe/pom.xml`, you need to execute the following command in the `fe/` directory:

    `mvn -npr eclipse:eclipse -Dskip.plugin=true`
    
    Then refresh the project in Eclipse. If it cannot be updated, it is recommended to delete the project and import it again according to this document.

### Imported as maven project

1. Update lexical and grammar files or proto and thrift files
	```
	cd fe && mvn package -DskipTests=true -Dos.arch=x86_64
	```
2. Update maven dependencies, in eclipse `Package Explorer` right click on the
	 project `maven -> update project...`

3. Refresh project in eclipse

## Imports Order

In order to maintain the Imports order of Java, please perform the following operations to set the Imports Order of the project.

1. Create the file `fe_doris.importorder` and write the following:

    ```
    #Organize Import Order
    #Wed Jul 01 16:42:47 CST 2020
    4=javax
    3=java
    2=org
    1=com
    0=org.apache.doris
    ```

2. Open Eclipse Preferences, select `Java -> Code Style -> Organize Imports`. Click `Import` to import the above file.
