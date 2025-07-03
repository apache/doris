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

# How to use:

	1. mvn clean install -U
    2. mvn package
	3. java --add-opens=java.base/java.nio=ALL-UNNAMED -cp target/java-0.1.jar doris.arrowflight.demo.Main "sql" "fe_ip" "fe_arrow_flight_port" "fe_query_port"

# What can this demo do:

	This is a java demo for doris arrow flight sql, you can use this to test various connection
    methods for  sending queries to the doris arrow flight server, help you understand how to use arrow flight sql
    and test performance. You should install maven prior to run this demo.

# Performance test

    Section 6.2 of https://github.com/apache/doris/issues/25514 is the performance test
    results of the doris arrow flight sql using java.

# Notes

    If the following error occurs:
    - `module java.base does not "opens java.nio" to unnamed module`
    - `module java.base does not "opens java.nio" to org.apache.arrow.memory.core`
    - `java.lang.NoClassDefFoundError: Could not initialize class org.apache.arrow.memory.util.MemoryUtil (Internal; Prepare)`

    First, add `--add-opens=java.base/java.nio=ALL-UNNAMED` to `JAVA_OPTS_FOR_JDK_17` in fe/conf/fe.conf. Then add `--add-opens=java.base/java.nio=ALL-UNNAMED` in the Java command. If debugging in IntelliJ IDEA, add `--add-opens=java.base/java.nio=ALL-UNNAMED` in `Build and run` of `Run/Debug Configurations`.

    For more details, refer to [JDBC Connector with Arrow Flight SQL] and [Java Usage] in the document https://doris.apache.org/zh-CN/docs/dev/db-connect/arrow-flight-sql-connect