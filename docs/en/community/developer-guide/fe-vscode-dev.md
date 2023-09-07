---
{
    "title": "FE development and debugging environment - Visual Studio Code (VSCode)",
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

# Apache Doris Be development and debugging in VS Code

Some developers are building FE development environment on a development machine/WSL/docker, but this kind of development environment is not supported for local development, some developers are used to use VSCode to configure remote develop and debug.

## Preparation

* JDK11+ (Java Extension Pack need JDK11+) (author is creating a `lib` directory under `home`, and install [JDK11](https://github.com/adoptium/temurin11-binaries/releases/) and JDK8 in it, and use them for `Extensions` and `Compilation`)
* VSCode
  + Extension Pack for Java
  + Remote Extensions

## Download code for compilation

1. https://github.com/apache/doris.git Download the doris source code

2. use VSCode to open the code `/fe` directory

## Setting for VSCode

Create `settings.json` in `.vscode/` , and set settings:

* `"java.configuration.runtimes"`
* `"java.jdt.ls.java.home"` -- must set it to the directory of JDK11+, used for vscode-java plugin
* `"maven.executable.path"` -- maven path，for maven-language-server plugin

example:

```json
{
    "java.configuration.runtimes": [
        {
            "name": "JavaSE-1.8",
            "path": "/!!!path!!!/jdk-1.8.0_191"
        },
        {
            "name": "JavaSE-11",
            "path": "/!!!path!!!/jdk-11.0.14.1+1",
            "default": true
        },
    ],
    "java.jdt.ls.java.home": "/!!!path!!!/jdk-11.0.14.1+1",
    "maven.executable.path": "/!!!path!!!/maven/bin/mvn"
}
```

## Build

Other articles have already explained:
* [Build with LDB toolchain ](/docs/install/source-install/compilation-with-ldb-toolchain)
* ......

In order to debug, you need to add debugging parameters when fe starts, such as 

```bash
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
```

In `doris/output/fe/bin/start_fe.sh` , after `$JAVA $final_java_opt` add this param.
