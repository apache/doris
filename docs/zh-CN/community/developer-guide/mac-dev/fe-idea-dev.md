---
{
  "title": "Doris FE Mac 开发环境搭建 - IntelliJ IDEA",
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

## 打开 Doris 代码的 FE 目录

**这里我们不要直接打开Doris项目根目录，要打开FE的目录（很重要！！为了不和CLion发生冲突**

![deployment1](/images/mac-idea-deployment1.png)

## 生成 FE 代码

1. 打开 IDEA 终端，到代码根目录下执行
   `sh generated-source.sh`

    等待显示 Done 就可以了
    
    ![deployment2](/images/mac-idea-deployment2.png)
2. Copy help-resource.zip 

    ```
    进入doris/docs目录，执行以下命令
    cd doris/docs
    sh build_help_zip.sh
    cp -r build/help-resource.zip ../fe/fe-core/target/classes
    ```

## 配置 Debug FE

- 选择编辑配置

  ![deployment3](/images/mac-idea-deployment3.png)

- 添加 DorisFE 配置

  左上角 + 号添加一个应用程序的配置，具体配置参考下图

  ![deployment4](/images/mac-idea-deployment4.png)

  - 工作目录选择源码目录下的 fe 目录
  - 参照 Doris 代码根目录下的 `fe/bin/start_fe.sh` 中 export 的环境变量进行环境变量配置。 
    其中环境变量的Doris目录值指向准备工作里里自己copy出来的目录。
    - 环境变量参考：
    ```
    JAVA_OPTS=-Xmx8092m;
    LOG_DIR=~/DorisDev/doris-run/fe/log;
    PID_DIR=~/DorisDev/doris-run/fe/log;
    DORIS_HOME=~/DorisDev/doris-run/fe
    ```
    ![deployment5](/images/mac-idea-deployment5.png)

## 启动 FE

点击 Run 或者 Debug 就会开始编译，编译完 fe 就会启动

![deployment6](/images/mac-idea-deployment6.png)
