---
{
  "title": "Setting Up Dev Env on Mac for Doris FE - IntelliJ IDEA",
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

## Open the FE directory of the Doris code

**Here we do not directly open the root directory of the Doris project, but open the FE directory (very important!! In order not to conflict with CLion**

![deployment1](/images/mac-idea-deployment1.png)

## Generate FE code

1. Open the IDEA terminal and go to the root directory of the code to execute

   `sh generated-source.sh`

   Just wait for Done to be displayed
    
    ![deployment2](/images/mac-idea-deployment2.png)

2. Copy help-resource.zip

    ```
    Enter the dorisdocs directory and execute the following command
    cd doris/docs
    sh build_help_zip.sh
    cp -r build/help-resource.zip ../fe/fe-core/target/classes
    ```

## Configure Debug FE

- select edit configuration

  ![deployment3](/images/mac-idea-deployment3.png)

- Add DorisFE configuration

  Add an application configuration with the + sign in the upper left corner. For specific configuration, refer to the figure below

  ![deployment4](/images/mac-idea-deployment4.png)

  - Select the fe directory under the source code directory as the working directory
  - Refer to the environment variables of export in `fe/bin/start_fe.sh` in the root directory of the Doris code to configure the environment variables.
    The Doris directory value of the environment variable points to the directory copied by myself in the preparation work.
    - Environment variable reference:
    ```
    JAVA_OPTS=-Xmx8092m;
    LOG_DIR=~/DorisDev/doris-run/fe/log;
    PID_DIR=~/DorisDev/doris-run/fe/log;
    DORIS_HOME=~/DorisDev/doris-run/fe
    ```
    ![deployment5](/images/mac-idea-deployment5.png)

## Start FE

Click Run or Debug to start compiling, and fe will start after compiling

![deployment6](/images/mac-idea-deployment6.png)
