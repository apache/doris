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

suite("extract_in") {
    sql """
        drop table if exists t;
        CREATE TABLE `t` (
            `col1` int(11) NULL COMMENT "",
            `col2` int(11) NULL COMMENT "",
            `col3` int(11) NULL COMMENT "",
            `A` int(11) NULL COMMENT "",
            `B` int(11) NULL COMMENT "",
            `C` int(11) NULL COMMENT "",
            `D` int(11) NULL COMMENT ""
            ) 
            DISTRIBUTED BY HASH(`col1`) BUCKETS 10
            PROPERTIES ('replication_num' = '1');

        insert into t values (7, 8, 9, 10, 11,12, 13);
        set ignore_shape_nodes="PhysicalDistribute,PhysicalProject";
    """

    qt_1 "explain shape plan select * from t where col1 = 1 or col1 = 2 or col1 = 3 and (col2 = 4)"
    qt_2 "explain shape plan select * from t where col1 = 1 and col1 = 3 and col2 = 3 or col2 = 4"
    
    qt_3 "explain shape plan select * from t where (col1 = 1 or col1 = 2) and  (col2 = 3 or col2 = 4)"

    qt_5 "explain shape plan select * from t where col1 = 1 or (col1 = 2 and (col1 = 3 or col1 = 4 or col1 = 5))"
    qt_6 "explain shape plan select * from t where col1 = 1 or col1 = 2 or col1 in (1, 2, 3)"
    qt_7 "explain shape plan select * from t where A = 1 or A = 2 or abs(A)=5 or A in (1, 2, 3) or B = 1 or B = 2 or B in (1, 2, 3) or B+1 in (4, 5, 7)"
    qt_8 "explain shape plan select * from t where col1 = 1 or (col1 = 2 and (col1 = 3 or col1 = '4' or col1 = 5.0))"

    //not rewrite to col2 in (1, 2) or  col1 in (1, 2)
    qt_ensure_order "explain shape plan select * from t where col1 IN (1, 2) OR col2 IN (1, 2)"

    qt_9 "explain shape plan select * from t where col1=1 and (col2=1 or col2=2)"

    qt_10_recursive "explain shape plan select * from t where col1=1 or (col2 = 2 and (col3=4 or col3=5))"
    qt_11_multi_in "explain shape plan select * from t where (a=1 and b=2 and c=3) or (a=2 and b=2 and c=4)"
    qt_12 "explain shape plan select * from t where a in (1, 2) and a in (3, 4)"
    
    // no need to extract a in (1, 2)
    qt_13 "explain shape plan select * from t where a like 'xyz%' or a=1 or a=2"
    
    // "a in (1, 3)"" is not hold, because  (a=1 and abs(a)=2) is false.
    // but "a in (1, 3)" is useful, becase it filters some rows in t
    qt_14_no_rewrite "explain shape plan select * from t where (a=1 and abs(a)=2) or a=3"

    // no extraction a in (...) because d=1
    qt_15 "explain shape plan select * from t where d=1 or (a=1 and b=2) or (a=2 and c=3)"

    qt_16 "explain shape plan select * from t where a=1 or a=1 or a=1"
    
    // do not extract 'a in (2, 3)'', because it alreay exists
    qt_17 "explain shape plan select * from t where (a=1 and b=2) or (a in (2, 3) and ((a=2 and c=3) or (a=3 and d=4)))"


}