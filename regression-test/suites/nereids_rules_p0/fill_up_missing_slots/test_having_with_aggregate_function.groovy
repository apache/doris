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

suite("test_having_project") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS t
       """
    sql """
        create table t(id smallint) distributed by random properties('replication_num'='1');
    """

    qt_having_project_1 """
        SELECT 1 AS c1 FROM t HAVING count(1) >= 0
    """

    qt_having_project_2 """
        SELECT 1 AS c1 FROM t HAVING count(1) > 0
    """

    test {
        sql "SELECT 1 AS c1 FROM t HAVING count(1) > 0 OR c1 IS NOT NULL"
        exception "HAVING expression 'c1' must appear in the GROUP BY clause or be used in an aggregate function"
    }
}
