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

suite("test_predicate_with_implicit_cast") {
    def tableName1 = "test_decimalv3_to_decimalv3_1"
    def tableName2 = "test_decimalv3_to_decimalv3_2"

    def prepare_test_decimalV3_to_decimalV3 = {
        sql "drop table if exists ${tableName1};"
        sql """
            create table ${tableName1}(
                a DECIMAL(2, 1), 
                b DECIMAL(3, 2)
            ) 
            ENGINE=OLAP
            DUPLICATE KEY(a) COMMENT "OLAP"
            DISTRIBUTED BY HASH(a) BUCKETS auto
            PROPERTIES ( "replication_num" = "1" );
        """
        sql "drop table if exists ${tableName2};"
        sql """
            create table ${tableName2}(
                c DECIMAL(3, 2)
            ) 
            ENGINE=OLAP
            DUPLICATE KEY(c) COMMENT "OLAP"
            DISTRIBUTED BY HASH(c) BUCKETS auto
            PROPERTIES ( "replication_num" = "1" );
        """
    }

    def q01 = {
        qt_decimalv3_1 "SELECT * FROM ${tableName1}, ${tableName2} WHERE a=c;"
        qt_decimalv3_2 "SELECT * FROM ${tableName1}, ${tableName2} WHERE a IN (c);"
        qt_decimalv3_3 "SELECT * FROM ${tableName1}, ${tableName2} WHERE cast(a as DecimalV3(3,2))=c;"
    }

    prepare_test_decimalV3_to_decimalV3()

    sql """
        insert into ${tableName1} values (5.7, 5.70);
    """

    sql """
        insert into ${tableName2} values(5.70);
    """

    sql """set enable_fallback_to_original_planner=false;"""

    // with nereids planner
    q01()
}
