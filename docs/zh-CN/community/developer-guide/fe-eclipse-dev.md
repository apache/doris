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

#### windows下开发获取生成代码步骤

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

#### mac下开发获取生成代码步骤

mac下可以直接使用maven构建的步骤生成代码, 或者说可以直接编译. 

1. 安装thrift的解释器(0.13.0), 如果没有的话到官网下载源码进行编译安装或者直接时
	 使用`brew` 安装一个.

2. 创建一个文件夹`thirdparty/installed/bin`, 然后将thrift 命令建立一个软链到这个
	 路径下(当然你可以copy二进制).

	```
	mkdir -p thirdparty/installed/bin
	ln -s ${thrift_installed_full_path} thirdparty/installed/bin/thrift
	```

3. 调用maven直接进行构建, 如果出现一些错误请检查`$JAVA_HOME`路径以及java版本以及
	 thrift是否能正常正确运行.

	```
	cd fe && mvn package -DskipTests=true -Dos.arch=x86_64
	```

上述第3步中`-Dos.arch=x86_64` 是为了兼容苹果的m系列处理器(`os.arch=aarch64`),
protobuf会使用x86_64架构的protoc二进制进行代码生成, 如果是使用m系列处理器的mac,
有roseta做兼容所以不会有问题.

Note: 
0. cup和jfex均使用java的jar包程序进行编译, 代码生成的流程可以平台无关
1. protobuf文件使用了现成的开源插件`protoc-jar-maven-plugin`进行跨平台的生成,
	 本质上是下载已经编译好的对应平台二进制, 进行protobuf代码生成.
2. thrift是目前(2022-06-26-Sun) FE在maven构建上唯一一个依赖
	 `thirdparty/installed`的工具. 目前还没有使用类似protobuf的生成插件替换(TODO).

## 导入 FE 工程

### 使用eclipse工程导入

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


### 使用maven工程导入

经过前边mac相关的操作之后, 我们应该能够直接本地maven构建了. 能够maven构建的项目
是可以使用eclipse m2e 插件直接导入的.

在eclipse File 菜单中依次选择`Import -> Maven -> Existing Maven Projects`
然后选择doris fe文件夹即可完成导入. 导入时建议选择working set管理FE的多个module.

至此, 我们已经可以使用eclipse进行FE的开发调试.

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

FE的单元测试会首先启动一个FE服务，然后由测试用例作为客户端执行相应的测试逻辑。在UT报错时，UT的日志只会打印相应的客户端日志，如果需要**查看服务端日志**，可以在路径`${DORIS_HOME}/fe/mocked`下查看。

## 运行 FE

可以在 Eclipse 中直接启动一个 FE 进程，方便对代码进行调试。

1. 创建一个运行目录：

    ```
    mkdir /path/to/doris/fe/run/
    cd /path/to/doris/fe/run/
    mkdir conf/log/doris-meta/
    ```
    
2. 创建配置文件
    
    在第一步创建的 `conf/` 目录下创建配置文件 `fe.conf`。你可以直接将源码目录下 `conf/fe.conf` 拷贝过来并做简单修改。
    
3. 在 Eclipse 中找到 `src/main/java/org/apache/doris/PaloFe.java` 文件，右击选择 `Run As -> Run Configurations...`。在 `Environment` 标签页中添加如下环境变量：

    * `DORIS_HOME: /path/to/doris/fe/run/`
    * `PID_DIR: /path/to/doris/fe/run/`
    * `LOG_DIR: /path/to/doris/fe/run/log`

4. 右击 `PaloFe.java`，选择 `Run As -> Java Application`，则可以启动 FE。

## 代码更新

### eclipse工程

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

### maven工程

1. 更新词法、语法文件或者thrift 和proto 文件 在fe目录下命令行执行一次
	```
	cd fe && mvn package -DskipTests=true -Dos.arch=x86_64
	```
2. 更新maven依赖, 直接在eclipse里`Package Explorer` 右键选中maven项目
	 `maven -> update project...`

3. 在eclipse中刷新工程.

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
