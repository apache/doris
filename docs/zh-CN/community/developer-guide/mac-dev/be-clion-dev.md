---
{
    'title': 'Doris BE Mac 开发环境搭建 - CLion', 
    'language': 'zh-CN'
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

## 打开 Doris 代码根目录

![deployment1](/images/mac-clion-deployment1.png)

## 配置 CLion

1. 配置工具链

    参考下图，配置好全部检测成功就没问题了

    ![deployment2](/images/mac-clion-deployment2.png)
   
2. 配置 CMake

    参考下图配置

    ![deployment3](/images/mac-clion-deployment3.png)

    配置完成确认后第一次会自动加载 CMake 文件，若没有自动加载，可手动右键点击 `$DORIS_HOME/be/CMakeLists.txt` 选择加载

## 配置 Debug BE

选择编辑配置

  ![deployment4](/images/mac-clion-deployment4.png)

给 doris_be 添加环境变量

参照 Doris 代码根目录下的 `be/bin/start_be.sh` 中 export 的环境变量进行环境变量配置。 
其中环境变量的Doris目录值指向准备工作里里自己copy出来的目录。

环境变量参考：

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


## 启动Debug

点击 Run 或者 Debug 就会开始编译，编译完 be 就会启动

![deployment7](/images/mac-clion-deployment7.png)
