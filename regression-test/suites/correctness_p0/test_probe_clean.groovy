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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_probe_clean") {
   
sql """ drop table IF EXISTS clearblocktable1; """ 
sql """ 
 CREATE TABLE IF NOT EXISTS clearblocktable1 (
            `col_int_undef_signed` INT NULL COMMENT "",
            `col_int_undef_signed_not_null` INT NOT NULL COMMENT "",
            `col_date_undef_signed_not_null` date(11)  NOT NULL COMMENT "",

            ) ENGINE=OLAP
            DUPLICATE KEY(`col_int_undef_signed`)
            DISTRIBUTED BY HASH(`col_int_undef_signed`) BUCKETS 1
            PROPERTIES (
            'replication_num' = '1'
);
"""


sql """ 
insert into clearblocktable1 values(1,1,'2020-01-01');
"""
sql """ 
drop table IF EXISTS clearblocktable2;
""" 
sql """
CREATE TABLE IF NOT EXISTS clearblocktable2 (
            `col_int_undef_signed` INT NULL COMMENT "",
            `col_int_undef_signed_not_null` INT NOT NULL COMMENT "",
            `col_date_undef_signed_not_null` date(11)  NOT NULL COMMENT "",

            ) ENGINE=OLAP
            DUPLICATE KEY(`col_int_undef_signed`)
            DISTRIBUTED BY HASH(`col_int_undef_signed`) BUCKETS 1
            PROPERTIES (
            'replication_num' = '1'
);
"""
 
sql """
insert into clearblocktable2 values(1,1,'2020-01-01');
"""

sql """
set enable_pipeline_x_engine=true, enable_pipeline_engine=true;
"""
qt_select_pipelineX """ 

SELECT YEAR(ifnull(clearblocktable1.`col_date_undef_signed_not_null`, clearblocktable1.`col_date_undef_signed_not_null`)) AS field1 , 
CASE  WHEN clearblocktable1.`col_int_undef_signed` != clearblocktable1.`col_int_undef_signed` * (8 + 1) THEN -5.2 ELSE clearblocktable1.`col_int_undef_signed` END AS field2 
FROM clearblocktable1 INNER JOIN clearblocktable2 ON clearblocktable2.`col_int_undef_signed` = clearblocktable1.`col_int_undef_signed` WHERE clearblocktable1.`col_int_undef_signed_not_null` <> 7;

"""

sql """
set enable_pipeline_x_engine=false,enable_pipeline_engine=true;
"""
qt_select_pipeline """ 

SELECT YEAR(ifnull(clearblocktable1.`col_date_undef_signed_not_null`, clearblocktable1.`col_date_undef_signed_not_null`)) AS field1 , 
CASE  WHEN clearblocktable1.`col_int_undef_signed` != clearblocktable1.`col_int_undef_signed` * (8 + 1) THEN -5.2 ELSE clearblocktable1.`col_int_undef_signed` END AS field2 
FROM clearblocktable1 INNER JOIN clearblocktable2 ON clearblocktable2.`col_int_undef_signed` = clearblocktable1.`col_int_undef_signed` WHERE clearblocktable1.`col_int_undef_signed_not_null` <> 7;

"""

qt_select_non_pipeline """

SELECT YEAR(ifnull(clearblocktable1.`col_date_undef_signed_not_null`, clearblocktable1.`col_date_undef_signed_not_null`)) AS field1 , 
CASE  WHEN clearblocktable1.`col_int_undef_signed` != clearblocktable1.`col_int_undef_signed` * (8 + 1) THEN -5.2 ELSE clearblocktable1.`col_int_undef_signed` END AS field2 
FROM clearblocktable1 INNER JOIN clearblocktable2 ON clearblocktable2.`col_int_undef_signed` = clearblocktable1.`col_int_undef_signed` WHERE clearblocktable1.`col_int_undef_signed_not_null` <> 7;
"""
}
