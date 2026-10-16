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

suite("test_if_nullable_condition") {
    sql "drop table if exists test_if_nullable_condition"
    sql """
        create table test_if_nullable_condition (
            id int,
            b boolean not null,
            p boolean not null,
            f boolean not null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql """
        insert into test_if_nullable_condition values
            (1, true, false, false),
            (2, true, true, false)
    """

    // nullif(b, p) is [true, NULL] and reuses b as the nested column of its result.
    // IF treats the NULL condition as false; that normalization must not be written into
    // the column shared with the else branch and with the other projected columns.
    sql "set short_circuit_evaluation = false"
    qt_if_nullif """
        select id, if(nullif(b, p), f, b) as r
        from test_if_nullable_condition order by id
    """
    qt_if_nullif_projection """
        select id, b, p, f, nullif(b, p) as cond, if(nullif(b, p), f, b) as r
        from test_if_nullable_condition order by id
    """

    sql "set short_circuit_evaluation = true"
    qt_if_nullif_short_circuit """
        select id, if(nullif(b, p), f, b) as r
        from test_if_nullable_condition order by id
    """
    qt_if_nullif_projection_short_circuit """
        select id, b, p, f, nullif(b, p) as cond, if(nullif(b, p), f, b) as r
        from test_if_nullable_condition order by id
    """
}
