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

# 基本配置

## brpc_max_body_size

  这个配置主要用来修改 brpc 的参数 max_body_size ，默认配置是 64M。一般发生在 multi distinct + 无 group by + 超过1T 数据量的情况下。尤其如果发现查询卡死，且 BE 出现类似 body_size is too large 的字样。

  由于这是一个 brpc 的配置，用户也可以在运行中直接修改该参数。通过访问 http://host:brpc_port/flags 修改。
