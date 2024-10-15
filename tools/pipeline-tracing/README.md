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

# Pipeline Tracing and Display Tool

In the Pipeline execution engine, we split the execution plan tree of each Instance into multiple small Pipeline Tasks and execute them under our custom Pipeline scheduler. Therefore, in an environment with a large number of Pipeline Tasks executing, how these Tasks are scheduled across threads and CPU cores is an important factor for execution performance. We have developed a specialised tool to observe the scheduling process on a particular query or time period, which we call "Pipeline Tracing".

This tool converts record files to proper JSON format for visualization.

## How to Use

```shell
python3 origin-to-show.py -s <SOURCE_FILE> -d <DEST>.json
```
to transfer record file `<SOURCE_FILE>` to `<DEST>.json`. Then it could be visualized.

```shell
python3 origin-to-show.py --help
```
for help details.