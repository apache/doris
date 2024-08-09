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

suite("test_xor_pred") {
    sql """ drop table if exists dbxor; """
    sql """
    CREATE TABLE IF NOT EXISTS dbxor (
              `k1` boolean NULL COMMENT "",
              `k2` boolean NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """set enable_nereids_planner=true,enable_fold_constant_by_be = false; """
    qt_sql_const """
        select true xor true,false xor true,true xor false,false xor false,null xor false,null xor true,true xor null ,false xor null,null xor null;
    """
    sql """ 
        insert into dbxor values(true,true),(true,false),(false,true),(false,false),(true,null),(false,null),(null,true),(null,false),(null,null);
    """
    qt_sql_select """
        select k1,k2 , k1 xor k2, not (k1 xor k2) from dbxor order by k1,k2;
    """

    test {
        sql """
            select 1 xor 0;
        """
        exception("Can not find the compatibility function signature: xor(TINYINT, TINYINT)")
    }
}
