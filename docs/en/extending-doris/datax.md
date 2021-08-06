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

## Compile and install

The plug-in code is located [here](https://github.com/apache/incubator-doris/tree/master/extension/DataX).

The plug-in uses Doris' Stream Load function to synchronize and import data. It needs to be used with DataX service.

Please refer to the [README](https://github.com/apache/incubator-doris/blob/master/extension/DataX/README) document to compile and install.

## Plug-in use

For instructions on using the doriswriter plug-in, please refer to [here](https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md).