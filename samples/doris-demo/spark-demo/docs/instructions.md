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

# Instructions for use

This series of sample codes mainly shows how to use spark and spark doris connector to read data from doris table.
here give some code examples using java and scala language.

```java
org.apache.doris.demo.spark.DorisSparkConnectionExampleJava
org.apache.doris.demo.spark.DorisSparkConnectionExampleScala
```

**Note:** Because the Spark doris connector jar file is not in the Maven central repository, you need to compile it separately and add to the classpath of your project. Refer to the compilation and use of Spark doris connector: 

[Spark doris connector](https://doris.apache.org/master/zh-CN/extending-doris/spark-doris-connector.html)


1. First, create a table in doris with any mysql client

   ```sql
   CREATE TABLE `example_table` (
   `id` bigint(20) NOT NULL COMMENT "ID",
   `name` varchar(100) NOT NULL COMMENT "Name",
   `age` int(11) NOT NULL COMMENT "Age"
   ) ENGINE = OLAP
   UNIQUE KEY(`id`)
   COMMENT "example table"
   DISTRIBUTED BY HASH(`id`) BUCKETS 1
   PROPERTIES (
   "replication_num" = "1",
   "in_memory" = "false",
   "storage_format" = "V2"
   );
   ```

2. Insert some test data to example_table

   ```sql
   insert into example_table values(1,"xx1",21);
   insert into example_table values(2,"xx2",21);
   ```

3. Set doris config in this class

   change the Doris DORIS_DB, DORIS_TABLE, DORIS_FE_IP, DORIS_FE_HTTP_PORT,
   DORIS_FE_QUERY_PORT, DORIS_USER, DORIS_PASSWORD config in this class

4. Run this class, you should see the output:

   ```shell
    +---+----+---+
    | id|name|age|
    +---+----+---+
    |  1|  xx1| 21|
    |  2|  xx2| 21|
    +---+----+---+
   ```
