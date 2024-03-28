
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


suite("test_vexcept_not_open") {
    sql """
    drop table if EXISTS tbl_vexcept;
    """
    sql """ 
    CREATE table tbl_vexcept (c1 varchar(31), c2 varchar(10)) ENGINE=OLAP DUPLICATE KEY( c1 ) COMMENT "OLAP"  DISTRIBUTED BY HASH( c1 ) BUCKETS  auto PROPERTIES ( "replication_num" = "1"  );
    """

    sql """
    insert into tbl_vexcept select 'ab', 'abc';
    """

    qt_select """
    SELECT c2 LIKE 'ab' FROM ( SELECT c2  FROM tbl_vexcept EXCEPT  SELECT 'ab' c2 ) AS t1;
    """
}