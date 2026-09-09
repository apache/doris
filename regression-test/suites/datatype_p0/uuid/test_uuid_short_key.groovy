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

// Checklist: B07 F06 F07.
suite("test_uuid_short_key", "p0") {

    sql "DROP TABLE IF EXISTS uuid_short_key"
    sql """CREATE TABLE uuid_short_key (u UUID NOT NULL,id INT)
           DUPLICATE KEY(u) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1','short_key'='1')"""
    sql """INSERT INTO uuid_short_key SELECT
           CAST(CONCAT('8000000000000000',LPAD(HEX(number),16,'0')) AS UUID),number
           FROM numbers('number'='16384')"""
    sql "SET enable_profile = true"
    sql "SET enable_sql_cache = false"
    sql "SET profile_level = 2"
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    String token = "uuid_short_key_${UUID.randomUUID()}"
    qt_range """/* ${token} */ SELECT COUNT(*),MIN(u),MAX(u),SUM(id) FROM uuid_short_key
                WHERE u >= CAST('80000000000000000000000000003000' AS UUID)
                  AND u < CAST('80000000000000000000000000003010' AS UUID)"""
    checkProfileCounters(token, ['RowsKeyRangeFiltered'], ['RowsInvertedIndexFiltered','RowsBloomFilterFiltered'])
    qt_reference """SELECT COUNT(*),MIN(u),MAX(u),SUM(id) FROM uuid_short_key
                    WHERE id >= 12288 AND id < 12304"""

}
