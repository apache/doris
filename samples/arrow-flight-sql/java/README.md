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
	3. java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp java-0.1.jar doris.arrowflight.demo.Main "sql" "fe_ip" "fe_arrow_flight_port" "fe_query_port"

# What can this demo do:

	This is a java demo for doris arrow flight sql, you can use this to test various connection
    methods for  sending queries to the doris arrow flight server, help you understand how to use arrow flight sql
    and test performance. You should install maven prior to run this demo.

# Performance test

    Section 6.2 of https://github.com/apache/doris/issues/25514 is the performance test
    results of the doris arrow flight sql using java.