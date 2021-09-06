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

This series of sample codes mainly explain how to use the Flink connector to read Kafka data from the perspective of the
Flink framework, and use the StreamLoad method to store doris (DorisSink method), and give code examples.

# Code example

```
org.apache.doris.demo.flink.kafka.FlinkKafka2Doris
```

Custom doris sink

```java
org.apache.doris.demo.flink.DorisSink
```

Sample program

```
   public class DorisSink extends RichSinkFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
    @Override
    public void invoke(String value, Context context) throws Exception {
        DorisStreamLoad.LoadResponse loadResponse = dorisStreamLoad.loadBatch(value, columns, jsonFormat);
        .....
    }
}
```




