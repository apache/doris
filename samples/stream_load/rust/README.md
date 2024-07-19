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

# Doris Stream Load in Rust

## How to use:

1. **Create a table in Doris:**
   ```sql
   CREATE TABLE `db0`.`t_user` (
       `id` int,
       `name` string
   )
   DUPLICATE KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 1
   PROPERTIES (
       "replication_num" = "1"
   );
   ```

2. Build the project:
   ```shell
   cargo build
   ```

3. Run the application:
    ```shell
    cargo run
    ```

## What can this demo do:
	This is a rust demo for doris stream load, you can use this code load your data into doris by stream load.
