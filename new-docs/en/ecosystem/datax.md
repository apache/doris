---
{
    "title": "DataX doriswriter",
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

# DataX doriswriter

[DataX](https://github.com/alibaba/DataX) doriswriter plug-in, used to synchronize data from other data sources to Doris through DataX.

The plug-in uses Doris' Stream Load function to synchronize and import data. It needs to be used with DataX service.

## About DataX

DataX is an open source version of Alibaba Cloud DataWorks data integration, an offline data synchronization tool/platform widely used in Alibaba Group. DataX implements efficient data synchronization functions between various heterogeneous data sources including MySQL, Oracle, SqlServer, Postgre, HDFS, Hive, ADS, HBase, TableStore (OTS), MaxCompute (ODPS), Hologres, DRDS, etc.

More details can be found at: `https://github.com/alibaba/DataX/`

## Usage

The code of DataX doriswriter plug-in can be found [here](https://github.com/apache/incubator-doris/tree/master/extension/DataX).

This directory is the doriswriter plug-in development environment of Alibaba DataX.

Because the doriswriter plug-in depends on some modules in the DataX code base, and these module dependencies are not submitted to the official Maven repository, when we develop the doriswriter plug-in, we need to download the complete DataX code base to facilitate our development and compilation of the doriswriter plug-in.

### Directory structure

1. `doriswriter/`

    This directory is the code directory of doriswriter, and this part of the code should be in the Doris code base.

    The help doc can be found in `doriswriter/doc`

2. `init-env.sh`

    The script mainly performs the following steps:

    1. Git clone the DataX code base to the local
    2. Softlink the `doriswriter/` directory to `DataX/doriswriter`.
    3. Add `<module>doriswriter</module>` to the original `DataX/pom.xml`
    4. Change httpclient version from 4.5 to 4.5.13 in DataX/core/pom.xml

        > httpclient v4.5 can not handle redirect 307 correctly.

    After that, developers can enter `DataX/` for development. And the changes in the `DataX/doriswriter` directory will be reflected in the `doriswriter/` directory, which is convenient for developers to submit code.

### How to build

1. Run `init-env.sh`
2. Modify code of doriswriter in `DataX/doriswriter` if you need.
3. Build doriswriter

    1. Build doriswriter along:

        `mvn clean install -pl plugin-rdbms-util,doriswriter -DskipTests`

    2. Build DataX:

        `mvn package assembly:assembly -Dmaven.test.skip=true`

        The output will be in `target/datax/datax/`.

        > hdfsreader, hdfswriter and oscarwriter needs some extra jar packages. If you don't need to use these components, you can comment out the corresponding module in DataX/pom.xml.

	3. Compilation error

        If you encounter the following compilation errors:

        ```
        Could not find artifact com.alibaba.datax:datax-all:pom:0.0.1-SNAPSHOT ...
        ```

        You can try the following solutions:

        1. Download [alibaba-datax-maven-m2-20210928.tar.gz](https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/alibaba-datax-maven-m2-20210928.tar.gz)
        2. After decompression, copy the resulting `alibaba/datax/` directory to `.m2/repository/com/alibaba/` corresponding to the maven used.
        3. Try to compile again.

4. Commit code of doriswriter in `doriswriter` if you need.

### Example

For instructions on using the doriswriter plug-in, please refer to [here](https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md).
