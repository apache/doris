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

suite("test_up_down_hive_auth","p0,mtmv,restart_fe,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    String suiteName = "auth_up_down_hive"
    String hivePrefix = "hive2";
    String catalogName = "${hivePrefix}_${suiteName}_catalog"
    String userName = "${hivePrefix}_${suiteName}_user"

    def res = sql """show grants for ${userName}"""
    logger.info("res: " + res.toString())
    assertTrue(res.toString().contains("${catalogName}.tpch1_parquet.customer"))
}
