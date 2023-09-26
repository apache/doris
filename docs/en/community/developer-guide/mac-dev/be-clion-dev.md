---
{
    'title': 'Setting Up Dev Env on Mac for Doris BE - CLion', 
    'language': 'en'
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

## Open the Doris code root directory

![deployment1](/images/mac-clion-deployment1.png)

## Configure CLion

1. Configure Toolchain

   Refer to the figure below, after configuring all the detections successfully, there will be no problem

    ![deployment2](/images/mac-clion-deployment2.png)
   
2. Configure CMake

    Refer to the configuration below

    ![deployment3](/images/mac-clion-deployment3.png)

   After the configuration is completed and confirmed, the CMake file will be automatically loaded for the first time. If it is not automatically loaded, you can manually right-click `$DORIS_HOME/be/CMakeLists.txt` and select Load

## Configure Debug BE

select edit configuration

  ![deployment4](/images/mac-clion-deployment4.png)

Add environment variables to doris_be

Refer to the environment variables of export in `be/bin/start_be.sh` in the root directory of the Doris code to configure the environment variables.
The Doris directory value of the environment variable points to the directory copied by myself in the preparation work.

Environment variable reference:

```
JAVA_OPTS=-Xmx1024m -DlogPath=$DORIS_HOME/log/jni.log -Dsun.java.command=DorisBE -XX:-CriticalJNINatives -DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000;
LOG_DIR=~/DorisDev/doris-run/be/log;
NLS_LANG=AMERICAN_AMERICA.AL32UTF8;
ODBCSYSINI=~/DorisDev/doris-run/be/conf;
PID_DIR=~/DorisDev/doris-run/be/log;
UDF_RUNTIME_DIR=~/DorisDev/doris-run/be/lib/udf-runtime;
DORIS_HOME=~/DorisDev/doris-run/be
```

![deployment5](/images/mac-clion-deployment5.png)
![deployment6](/images/mac-clion-deployment6.png)


## Start BE

Click Run or Debug to start compiling, and be will start after compiling

![deployment7](/images/mac-clion-deployment7.png)
