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

# Introduction

This series of sample codes mainly explain how to use Flink jdbc and Flink doris connector to read data from doris from
the perspective of Flink framework and Flink doris connector, construct a datastream, realize analysis, and give code
examples in combination with actual usage scenarios.

# Method to realize

1.flink doris connector(recommend)

Realized through flink doris connector

**Note:** Because the Flink doris connector jar package is not in the Maven central warehouse, you need to compile it
separately and add it to the classpath of your project. Refer to the compilation and use of Flink doris connector:
[Flink doris connector]: https://doris.apache.org/master/zh-CN/extending-doris/flink-doris-connector.html

2.flink jdbc connector

First. Load doris related configuration

  ``` 
        properties.put("url", "jdbc:mysql://ip:9030");
        properties.put("username", "root");
        properties.put("password", "");
        properties.put("sql", "select * from db.tb");
 ```
Second. Custom source

```java
org.apache.doris.demo.flink.analyze.DorisSource
```



