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

This series of sample codes mainly explain how to use Flink to read HDFS files (including txt, csv) from the perspective
of the Flink framework, and write them to Doris through streamLoad(Doris method), and give code examples.

# Method to realize

1.flink readTextFile(recommend)

```java
org.apache.doris.demo.flink.hdfs.FlinkReadTextHdfs2Doris
```

Sample program

```
DataStreamSource<String> dataStreamSource = blinkStreamEnv.readTextFile("hdfs://xxx:8020/test/txt");
...
```

2.flink Custom hdfs source Implementation code

```java
org.apache.doris.demo.flink.hdfs.HdfsSource
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



