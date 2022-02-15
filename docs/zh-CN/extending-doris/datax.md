---
{
    "title": "DataX doriswriter",
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

# DataX doriswriter

[DataX](https://github.com/alibaba/DataX) doriswriter 插件，用于通过 DataX 同步其他数据源的数据到 Doris 中。

这个插件是利用Doris的Stream Load 功能进行数据导入的。需要配合 DataX 服务一起使用。

## 关于 DataX

DataX 是阿里云 DataWorks数据集成 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS 等各种异构数据源之间高效的数据同步功能。

更多信息请参阅: `https://github.com/alibaba/DataX/`

## 使用手册

DataX doriswriter 插件代码 [这里](https://github.com/apache/incubator-doris/tree/master/extension/DataX)。

这个目录包含插件代码以及 DataX 项目的开发环境。

doriswriter 插件依赖的 DataX 代码中的一些模块。而这些模块并没有在 Maven 官方仓库中。所以我们在开发 doriswriter 插件时，需要下载完整的 DataX 代码库，才能进行插件的编译和开发。

### 目录结构

1. `doriswriter/`

    这个目录是 doriswriter 插件的代码目录。这个目录中的所有代码，都托管在 Apache Doris 的代码库中。

    doriswriter 插件帮助文档在这里：`doriswriter/doc`

2. `init-env.sh`

    这个脚本主要用于构建 DataX 开发环境，他主要进行了以下操作：
    
    1. 将 DataX 代码库 clone 到本地。
    2. 将 `doriswriter/` 目录软链到 `DataX/doriswriter` 目录。
    3. 在 `DataX/pom.xml` 文件中添加 `<module>doriswriter</module>` 模块。
    4. 将 `DataX/core/pom.xml` 文件中的 httpclient 版本从 4.5 改为 4.5.13.

        > httpclient v4.5 在处理 307 转发时有bug。

    这个脚本执行后，开发者就可以进入 `DataX/` 目录开始开发或编译了。因为做了软链，所以任何对 `DataX/doriswriter` 目录中文件的修改，都会反映到 `doriswriter/` 目录中，方便开发者提交代码。

### 编译

1. 运行 `init-env.sh`
2. 按需修改 `DataX/doriswriter` 中的代码。
3. 编译 doriswriter：

    1. 单独编译 doriswriter 插件:

        `mvn clean install -pl plugin-rdbms-util,doriswriter -DskipTests`

    2. 编译整个 DataX 项目:

        `mvn package assembly:assembly -Dmaven.test.skip=true`

        产出在 `target/datax/datax/`.

        > hdfsreader, hdfswriter and oscarwriter 这三个插件需要额外的jar包。如果你并不需要这些插件，可以在 `DataX/pom.xml` 中删除这些插件的模块。

	3. 编译错误

		如遇到如下编译错误：

		```
		Could not find artifact com.alibaba.datax:datax-all:pom:0.0.1-SNAPSHOT ...
		```

		可尝试以下方式解决：

		1. 下载 [alibaba-datax-maven-m2-20210928.tar.gz](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/alibaba-datax-maven-m2-20210928.tar.gz)
		2. 解压后，将得到的 `alibaba/datax/` 目录，拷贝到所使用的 maven 对应的 `.m2/repository/com/alibaba/` 下。
		3. 再次尝试编译。

4. 按需提交修改。

### 示例

doriswriter 插件的使用说明请参阅 [这里](https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md)
