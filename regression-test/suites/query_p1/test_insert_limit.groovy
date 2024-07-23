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

suite("test_insert_limit") {
    sql "set enable_agg_state=true"
    sql "DROP TABLE IF EXISTS `tbl_test_insert_limit`"
    sql """
        create table `tbl_test_insert_limit`(
            k1 int null,
            k2 agg_state<group_concat(string)> generic
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        """

    sql """
        insert into `tbl_test_insert_limit` values(1, group_concat_state('abcd'));
    """

    def error = ""
    for (i in 1..24) {
        test {
            sql " insert into `tbl_test_insert_limit` select * from tbl_test_insert_limit; "
            check{result, exception, startTime, endTime ->
                if (exception != null) {
                    error = exception
                }
            }
        }

        if (error != "") {
            break
        }
    }

    assertTrue(error != "")

    qt_select """
        select k1, length(k2) from `tbl_test_insert_limit`;
    """

    sql "DROP TABLE IF EXISTS `tbl_test_insert_limit`"
}
