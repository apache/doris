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

suite("test_predicate", "arrow_flight_sql") {
    sql """drop table if exists t1;"""
    sql """
            create table t1 (
                k1 int null,
                k2 int null,
                k3 int null,
                k4 int null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """
    sql """ insert into t1 values(1,2,3,null); """

    qt_null_or_null "select * from t1 where abs(k1)=null or abs(k1)=null;"
    qt_true_or_null "select * from t1 where abs(k1)=1 or abs(k1)=null;"
    qt_null_or_true "select * from t1 where abs(k1)=null or abs(k1)=1;"
    qt_false_or_null "select * from t1 where abs(k1)!=1 or abs(k1)=null;"
    qt_null_or_false "select * from t1 where abs(k1)=null or abs(k1)!=1;"
    qt_false_or_true "select * from t1 where abs(k1)!=1 or abs(k1)=1;"
    qt_true_or_false "select * from t1 where abs(k1)=1 or abs(k1)!=1;"

    qt_null_and_null "select * from t1 where abs(k1)=null and abs(k1)=null;"
    qt_true_and_null "select * from t1 where abs(k1)=1 and abs(k1)=null;"
    qt_null_and_true "select * from t1 where abs(k1)=null and abs(k1)=1;"
    qt_false_and_null "select * from t1 where abs(k1)!=1 and abs(k1)=null;"
    qt_null_and_false "select * from t1 where abs(k1)!=null and abs(k1)=1;"
    qt_false_and_true "select * from t1 where abs(k1)!=1 and abs(k1)=1;"
    qt_true_and_false "select * from t1 where abs(k1)=1 and abs(k1)!=1;"

    qt_not_false "select * from t1 where not (abs(k1)!=1);"
    qt_not_null "select * from t1 where not (abs(k1)!=1 or null);"

    qt_false_is_null "select * from t1 where (abs(k1)!=1) is null;"
    qt_null_is_null "select * from t1 where (abs(k1)!=1 or null) is null;"

    qt_false_eq_false "select * from t1 where (abs(k1)!=1)=false;"
    qt_null_eq_false "select * from t1 where (abs(k1)!=1 or null)=false;"
}
