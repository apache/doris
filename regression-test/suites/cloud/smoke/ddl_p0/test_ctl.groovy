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

suite("smoke_test_ctl", "smoke") {
    if (context.config.cloudVersion != null && !context.config.cloudVersion.isEmpty()
            && compareCloudVersion(context.config.cloudVersion, "3.0.0") >= 0) {
        log.info("case: smoke_test_ctl, cloud version ${context.config.cloudVersion} bigger than 3.0.0, skip".toString());
        return
    }
    try {
        sql """
    CREATE TABLE IF NOT EXISTS `test_ctl` (
      `test_varchar` varchar(150) NULL,
      `test_datetime` datetime NULL,
      `test_default_timestamp` datetime DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`test_varchar`)
    DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
    """

        sql """ 
    CREATE TABLE IF NOT EXISTS `test_ctl1` LIKE `test_ctl`
    """

        qt_select """SHOW CREATE TABLE `test_ctl1`"""
    } finally {
        sql """ DROP TABLE IF EXISTS test_ctl """

        sql """ DROP TABLE IF EXISTS test_ctl1 """
    }

}
