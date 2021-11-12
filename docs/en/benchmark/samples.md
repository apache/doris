---
{
    "title": "Samples",
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

# Samples

Doris provides a wealth of usage samples, which can help Doris users quickly get started to experience the features of Doris.

## Description

The sample codes are stored in the [`samples/`](https://github.com/apache/incubator-doris/tree/master/samples) directory of the Doris code base.

```
├── connect
├── doris-demo
├── insert
└── mini_load
```

* `connect/`

     This catalog mainly shows the code examples of connecting Doris in various programming languages.
    
* `doris-demo/`

     The code examples of the multiple functions of Doris are shown mainly in the form of Maven project. Such as spark-connector and flink-connector usage examples, integration with the Spring framework, Stream Load examples, and so on.
    
* `insert/`

     This catalog shows some code examples of importing data through python or shell script calling Doris's Insert command.
    
* `miniload/`

     This catalog shows the code example of calling mini load through python to import data. However, because the mini load function has been replaced by the stream load function, it is recommended to use the stream load function for data import.