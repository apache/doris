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

suite("test_left_join_null", "query") {

    def tbName1 = "dept_emp"
    def tbName2 = "departments"

    sql "drop table if exists ${tbName1}"
    sql "drop table if exists ${tbName2}"

    sql """
           CREATE TABLE IF NOT EXISTS ${tbName1} (
              `emp_no` int NOT NULL,
              `dept_no` char(4) NOT NULL,
              `from_date` date NOT NULL,
              `to_date` date NOT NULL
            ) 
            unique KEY (`emp_no`,`dept_no`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`emp_no`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
         """

    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
              `dept_no` char(4) NOT NULL,
              `dept_name` varchar(40) NOT NULL
            ) 
             UNIQUE KEY (`dept_no`)
             COMMENT "OLAP"
            DISTRIBUTED BY HASH(`dept_no`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
            """
    sql """insert into ${tbName2} values ('1001','test')"""

    qt_select """
                with test as (
                    SELECT dept_no from dept_emp
                 )
                 SELECT * from departments a LEFT JOIN test b on a.dept_no=b.dept_no
              """

}
