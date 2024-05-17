// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
suite("test_create_upgrade_wg") {
    sql "ADMIN SET FRONTEND CONFIG ('enable_workload_group' = 'true');"

    sql "create workload group if not exists normal " +
            "properties ( " +
            "    'cpu_share'='10', " +
            "    'memory_limit'='30%', " +
            "    'enable_memory_overcommit'='true' " +
            ");"

    sql "create workload group if not exists upgrade_g1 " +
            "properties ( " +
            "    'cpu_share'='11', " +
            "    'memory_limit'='0.2%', " +
            "    'enable_memory_overcommit'='true' " +
            ");"

    sql "create workload group if not exists upgrade_g2 " +
            "properties ( " +
            "    'cpu_share'='12', " +
            "    'memory_limit'='0.3%', " +
            "    'enable_memory_overcommit'='true' " +
            ");"
}