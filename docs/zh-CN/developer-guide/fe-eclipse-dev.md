---
{
    "title": "FE 开发环境搭建 - Eclipse",
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

# 使用 Eclipse 搭建 FE 开发环境

## 环境准备

* JDK 1.8+
* Maven 3.x+
* Eclipse，并已安装 [M2Eclipse](http://www.eclipse.org/m2e/)

### 代码生成

FE 模块需要部分生成代码，如 Thrift、Protobuf, jflex, cup 等框架的生成代码。这部分需要在 Linux 或者 Mac环境生成。

1. 在 Linux 下， 进入 `fe 目录下执行以下命令：
   ```
   mvn  generate-sources
   ```
    
2. 如果使用window开发 需要将生成的 `fe/fe-core/target/generated-sources` 目录打包：

    `fe/fe-core/target/ && tar czf java.tar.gz generated-sources/`

3. 将 `java.tar.gz` 拷贝到开发环境的 `fe/fe-core/target/` 目录下，并解压

    ```
    cp java.tar.gz /path/to/doris/fe/fe-core/target/
    cd /path/to/doris/fe/fe-core/target/ && tar xzf java.tar.gz
    ```

## 导入 FE 工程

1. 在开发环境的 `fe/` 目录下，执行以下命令生成 Eclipse 工程文件：

    `cd /path/to/doris/fe/ && mvn -npr eclipse:eclipse -Dskip.plugin=true`
    
    执行完成后，会在 `fe/` 目录下生成 `.project` 和 `.classpath` 文件

2. 导入 FE 工程

    * 打开 Eclipse，选择 `File -> Import`。
    * 选择 `General -> Existing Projects into Workspace`。
    * `Select root directory` 选择 `fe/` 目录，点击 `Finish` 完成导入。
    * 右击工程，选择 `Build Path -> Configure Build Path`。
    * 在 `Java Build Path` 对话框中，选择 `Source` 标签页，点击 `Add Folder`，勾选添加之前拷贝并解压的 `java/` 目录。
    * 点击 `Apply and Close` 完成。

至此，FE 导入完成。Eclipse 中的工程目录大致如下：

![](/images/eclipse-import-fe-project-1.png)

## 运行单元测试

在想要运行的单元测试文件上右击，选择 `Run As -> JUnit Test`。（如果要单步调试，则选择 `Debug As -> JUnit Test`）。

如果出现以下错误：

```
java.lang.Exception: Method xxxx should have no parameters
```

则右击单元测试文件，选择 `Run As -> Run Configurations...`。（如果要单步调试，则选择 `Debug As -> Debug Configurations...`）。

在 `Arguments` 标签页中的 `VM arguments` 中添加：

```
-javaagent:${settings.localRepository}/org/jmockit/jmockit/1.48/jmockit-1.48.jar
```

其中 `${settings.localRepository}` 要换成 maven lib 库的路径，如：

```
-javaagent:/Users/cmy/.m2/repository/org/jmockit/jmockit/1.48/jmockit-1.48.jar
```

之后在运行 `Run/Debug` 即可。

## 运行 FE

可以在 Eclipse 中直接启动一个 FE 进程，方便对代码进行调试。

1. 创建一个运行目录：

    ```
    mkdir /path/to/doris/fe/run/
    cd /path/to/doris/fe/run/
    mkdir conf/ log/ palo-meta/
    ```
    
2. 创建配置文件
    
    在第一步创建的 `conf/` 目录下创建配置文件 `fe.conf`。你可以直接将源码目录下 `conf/fe.conf` 拷贝过来并做简单修改。
    
3. 在 Eclipse 中找到 `src/main/java/org/apache/doris/PaloFe.java` 文件，右击选择 `Run As -> Run Configurations...`。在 `Environment` 标签页中添加如下环境变量：

    * `DORIS_HOME: /path/to/doris/fe/run/`
    * `PID_DIR: /path/to/doris/fe/run/`
    * `LOG_DIR: /path/to/doris/fe/run/log`

4. 右击 `PaloFe.java`，选择 `Run As -> Java Application`，则可以启动 FE。

## 代码更新

1. 更新词法、语法文件或者thrift 和proto 文件

    如果修改了 `fe/fe-core/src/main/cup/sql_parser.cup` 或者 `fe/fe-core/src/main/jflex/sql_scanner.flex`文件或者proto 和thrift 文件。则需在 `fe` 目录下执行以下命令：
    
    ```
    mvn  generate-sources
    ```
    
    之后在 Eclipse 中刷新工程即可。
    
2. 更新 maven 依赖

    如果更新了 `fe/pom.xml` 中的依赖，则需在 `fe/` 目录下执行以下命令：

    `mvn -npr eclipse:eclipse -Dskip.plugin=true`
    
    之后在 Eclipse 中刷新工程即可。如无法更新，建议删除工程，并按照该文档重新导入一遍即可。

## Import 顺序

为了保持 Java 的 Import 顺序，请执行如下操作设定项目的 Import Order

1. 创建文件 `fe_doris.importorder` 并写入以下内容：

    ```
    #Organize Import Order
    #Wed Jul 01 16:42:47 CST 2020
    4=javax
    3=java
    2=org
    1=com
    0=org.apache.doris
    ```

2. 打开 Eclipse 的偏好设置（Preferences），选择 `Java -> Code Style -> Organize Imports`。点击 `Import` 导入上述文件。
