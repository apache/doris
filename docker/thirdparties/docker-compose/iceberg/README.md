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


```
data:
    input: 初始化docker使用的数据
        script: 初始化iceberg表使用的脚本
        warehouse: 初始化iceberg表的数据文件
        iceberg_rest_mode=memeory: 初始化iceberg表的元数据文件
    output: docker输出的数据

tools:
    gen_data.py: 生成随机数据
    save_docker.sh: 保存当前docker状态的脚本
```
