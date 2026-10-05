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

import org.apache.doris.regression.suite.Suite


// Check whether the sql result is equal to a certain value
// Usage: isSqlValueEqualToTarget(xxxSql, "", 10000, 30)
Suite.metaClass.isSqlValueEqualToTarget = {
    String statement, String expectedValue, int durationInMs, int maxRetryCnt /* param */ ->
        Suite suite = delegate as Suite

        int waitCnt = 0

        do {
            def sqlRes = sql statement
            suite.getLogger().info("sql res ${sqlRes}, expected value [[${expectedValue}]]"
                    .toString())

            if (expectedValue.isEmpty() && sqlRes.size() == 0) {
                return true
            }

            if (sqlRes.size() == 1 && sqlRes[0].size() == 1) {
                if (sqlRes[0][0].toString() == expectedValue) {
                    return true
                }
            }

            Thread.sleep(durationInMs)
            waitCnt = waitCnt + 1
            suite.getLogger().info("retried times: ${waitCnt}".toString())
        } while (waitCnt < maxRetryCnt)
        return false
}

logger.info("Added 'isSqlValueEqualToTarget' function to Suite")

// Check whether the sql result is greater than to a certain value
// Usage: isSqlValueGreaterThanTarget(xxxSql, 1, 10000, 30)
Suite.metaClass.isSqlValueGreaterThanTarget = {
    String statement, int expectedValue, int durationInMs, int maxRetryCnt /* param */ ->
        Suite suite = delegate as Suite

        int waitCnt = 0

        do {
            def sqlRes = sql statement
            suite.getLogger().info("sql res: ${sqlRes}, expected >= [[${expectedValue}]]"
                    .toString())

            if (sqlRes.size() == 1 && sqlRes[0].size() == 1) {
                if (sqlRes[0][0] >= expectedValue) {
                    return true
                }
            }

            Thread.sleep(durationInMs)
            waitCnt = waitCnt + 1
            suite.getLogger().info("retried times: ${waitCnt}".toString())
        } while (waitCnt < maxRetryCnt)
        return false
}

logger.info("Added 'isSqlValueGreaterThanTarget' function to Suite")

