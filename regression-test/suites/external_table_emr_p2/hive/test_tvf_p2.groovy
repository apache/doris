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

suite("test_tvf_p2", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hdfsPort = context.config.otherConfigs.get("extHdfsPort")

        qt_eof_check """select * from hdfs(
            "uri" = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/parquet/bad_store_sales.parquet",
            "fs.defaultFS" = "hdfs://${nameNodeHost}:${hdfsPort}",
            "format" = "parquet")
            where ss_store_sk = 4 and ss_addr_sk is null order by ss_item_sk"""
    }
}
