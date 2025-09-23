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
import java.sql.SQLException

suite("too_many_versions_detection") {
    sql """ DROP TABLE IF EXISTS t """

    sql """
        create table t(a int)
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a)
        BUCKETS 10 PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    for (int i = 1; i <= 2000; i++) {
        sql """ INSERT INTO t VALUES (${i}) """
    }

    try {
        sql """ INSERT INTO t VALUES (2001) """
        assertTrue(false, "Expected TOO_MANY_VERSION error but none occurred")
    } catch (SQLException e) {
        logger.info("Exception caught: ${e.getMessage()}")
        def expectedError = "failed to init rowset builder. version count: 2001, exceed limit: 2000, tablet:"
        assertTrue(e.getMessage().contains(expectedError),
            "Expected TOO_MANY_VERSION error with message containing '${expectedError}', but got: ${e.getMessage()}")
    }

    sql """ DROP TABLE IF EXISTS t """
}