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

suite("test_incr_without_row_binlog") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS base_incr_no_binlog"
    sql """
        CREATE TABLE base_incr_no_binlog (
            k1 INT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    test {
        sql """
            SELECT k1, v1, __DORIS_BINLOG_OP__
            FROM base_incr_no_binlog@incr("incrementType" = "DETAIL")
        """
        exception "errCode = 2, detailMessage = INCR query requires ROW binlog enabled on base table."
    }
}
