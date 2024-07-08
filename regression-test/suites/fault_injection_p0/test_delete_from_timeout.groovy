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

suite("test_delete_from_timeout","nonConcurrent") {

    def tableName = "test_delete_from_timeout"
 
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `col1` BOOLEAN NOT NULL,
        `col2` DECIMAL(17, 1) NOT NULL,
        `col3` INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`col1`, `col2`, `col3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `col3`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1")
        """

    GetDebugPoint().clearDebugPointsForAllBEs()

    try {
        sql "insert into ${tableName} values(1, 99.9, 234);"
        GetDebugPoint().enableDebugPointForAllBEs("DeleteHandler::generate_delete_predicate.inject_failure",
            [error_code: -1900 /* DELETE_INVALID_CONDITION */, error_msg: "data type is float or double."])
        test {
            sql """delete from ${tableName} where col1 = "false" and col2 = "-9999782574499444.2" and col3 = "-25"; """
            exception "data type is float or double."
        }

        GetDebugPoint().clearDebugPointsForAllBEs()

        GetDebugPoint().enableDebugPointForAllBEs("DeleteHandler::generate_delete_predicate.inject_failure",
            [error_code: -1903 /* DELETE_INVALID_PARAMETERS */, error_msg: "invalid parameters for store_cond. condition_size=1"])
        test {
            sql """delete from ${tableName} where col1 = "false" and col2 = "-9999782574499444.2" and col3 = "-25"; """
            exception "invalid parameters for store_cond. condition_size=1"
        }
    } catch (Exception e) {
        logger.info(e.getMessage())
        AssertTrue(false) 
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("DeleteHandler::generate_delete_predicate.inject_failure")
    }
}