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

suite("test_triangle") {
    sql """ drop table if exists test_triangle; """
    sql """ create table test_triangle(
        k1 int,
        v1 double
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """
    sql """ insert into test_triangle values
        (1,PI() / 4),
        (2,PI() / 2),
        (3,PI()),
        (4,0),
        (5,1),
        (6,-1),
        (7,NULL),
        (8,0.5),
        (9,-0.5),
        (10,10),
        (11,-10),
        (12,1E-7),
        (13,1E-7),
        (14,1E7),
        (15,-1E7),
        (16,-PI()),
        (17,-PI()/2)
    """

    qt_test "select k1,COT(v1) from test_triangle order by k1;"
    qt_test "select k1,SEC(v1) from test_triangle order by k1;"
    qt_test "select k1,CSC(v1) from test_triangle order by k1;"
}

