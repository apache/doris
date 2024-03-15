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

suite("test_subquery2", "arrow_flight_sql") {
    
    sql """DROP TABLE IF EXISTS subquerytest2"""
    
    sql """
        CREATE TABLE subquerytest2 (birth int)UNIQUE KEY(birth)DISTRIBUTED BY HASH (birth)
        BUCKETS 1 PROPERTIES("replication_allocation" = "tag.location.default: 1");
    """

    sql """insert into subquerytest2 values (2)"""


    qt_sql_1 """select i from (select 'abc' as i, sum(birth) as j from  subquerytest2) as tmp"""

    qt_sql_2 """select substring(i, 2) from (select 'abc' as i, sum(birth) as j from  subquerytest2) as tmp"""

    qt_sql_3 """select count(1) from (select 'abc' as i, sum(birth) as j from  subquerytest2) as tmp"""

    sql """DROP TABLE subquerytest2"""
    
}