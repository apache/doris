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
# MySQL-Flink-Doris

1. `docker-compose.yaml` 脚本中需要自行下载 `Flink-Doris-Connector`、`Flink-MySQL-Connector` 的 JAR 包，然后按目录进行映射。
2. 当前版本里可以采网络桥接模式来配置网络，也可以使用 host 模式来进行网络配置，需要进行参数调整。
3. 启动以后按照 MySQL-init、Doris-init、Flink-init 的顺序进行执行即可。
