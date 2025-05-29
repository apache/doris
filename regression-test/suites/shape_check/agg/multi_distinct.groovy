/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
suite('multi_distinct') {
    sql """
    CREATE TABLE IF NOT EXISTS t (
            k1 int NOT NULL, 
            k2 int NOT NULL,
            d1 int NOT NULL,
            v1 int NOT NULL, 
            v2 int NOT NULL
        )  
        DUPLICATE KEY (k1, k2) 
        DISTRIBUTED BY HASH(d1) BUCKETS 48
        PROPERTIES ( "replication_num"="1");

    insert into t values (1, 2, 3, 4, 5);
    """

    qt_shape_2distinct_use_multi """
    explain shape plan
    select count(distinct v1), count(distinct v2) from t group by k1;
    """

    qt_shape_1distinct_use_3_phaseAgg """
    explain shape plan
    select count(distinct v1) from t group by k1;
    """

    qt_shape_1distinct_use_multi """
    explain shape plan
    select count(distinct v1), sum(v2) from t group by k1;
    """

    qt_shape_1_multi_distinct_by_user """
    explain shape plan
    select multi_distinct_count(v1) from t group by k1;
    """

    qt_shape_2_multi_distinct_by_user """
    explain shape plan
    select multi_distinct_count(v1), multi_distinct_sum(v2) from t group by k1;
    """
}