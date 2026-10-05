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

suite("load") {

    sql """ DROP TABLE IF EXISTS table_200_undef_partitions2_keys3_properties4_distributed_by521; """

    sql """ CREATE TABLE IF NOT EXISTS table_200_undef_partitions2_keys3_properties4_distributed_by521
    (
        col_int_undef_signed_not_null                               int           not null,
        col_date_undef_signed_not_null                              date          not null,
        col_bigint_undef_signed_not_null_index_inverted             bigint        not null,
        col_bigint_undef_signed_not_null                            bigint        not null,
        col_int_undef_signed                                        int null,
        col_int_undef_signed_index_inverted                         int null,
        col_int_undef_signed_not_null_index_inverted                int           not null,
        col_bigint_undef_signed                                     bigint null,
        col_bigint_undef_signed_index_inverted                      bigint null,
        col_date_undef_signed                                       date null,
        col_date_undef_signed_index_inverted                        date null,
        col_date_undef_signed_not_null_index_inverted               date          not null,
        col_varchar_10__undef_signed                                varchar(10) null,
        col_varchar_10__undef_signed_index_inverted                 varchar(10) null,
        col_varchar_10__undef_signed_not_null                       varchar(10)   not null,
        col_varchar_10__undef_signed_not_null_index_inverted        varchar(10)   not null,
        col_varchar_1024__undef_signed                              varchar(1024) null,
        col_varchar_1024__undef_signed_index_inverted               varchar(1024) null,
        col_varchar_1024__undef_signed_not_null                     varchar(1024) not null,
        col_varchar_1024__undef_signed_not_null_index_inverted      varchar(1024) not null,
        col_array_bigint__undef_signed                              array<BIGINT> null,
        col_array_bigint__undef_signed_index_inverted               array<BIGINT> null,
        col_array_bigint__undef_signed_not_null                     array<BIGINT> not null,
        col_array_bigint__undef_signed_not_null_index_inverted      array<BIGINT> not null,
        col_array_varchar_64___undef_signed                         array< varchar (64)> null,
        col_array_varchar_64___undef_signed_index_inverted          array< varchar (64)> null,
        col_array_varchar_64___undef_signed_not_null                array< varchar (64)> not null,
        col_array_varchar_64___undef_signed_not_null_index_inverted array< varchar (64)> not null,
        col_array_date__undef_signed                                array< date > null,
        col_array_date__undef_signed_index_inverted                 array< date > null,
        col_array_date__undef_signed_not_null                       array< date > not null,
        col_array_date__undef_signed_not_null_index_inverted        array< date > not null,
        pk                                                          int,
        INDEX                                                       col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_bigint_undef_signed_index_inverted_idx (`col_bigint_undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_bigint_undef_signed_not_null_index_inverted_idx (`col_bigint_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_varchar_10__undef_signed_index_inverted_idx (`col_varchar_10__undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_varchar_10__undef_signed_not_null_index_inverted_idx (`col_varchar_10__undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_array_bigint__undef_signed_index_inverted_idx (`col_array_bigint__undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_array_bigint__undef_signed_not_null_index_inverted_idx (`col_array_bigint__undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_array_varchar_64___undef_signed_index_inverted_idx (`col_array_varchar_64___undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_array_varchar_64___undef_signed_not_null_index_inverted_idx (`col_array_varchar_64___undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX                                                       col_array_date__undef_signed_index_inverted_idx (`col_array_date__undef_signed_index_inverted`) USING INVERTED,
        INDEX                                                       col_array_date__undef_signed_not_null_index_inverted_idx (`col_array_date__undef_signed_not_null_index_inverted`) USING INVERTED
    ) engine=olap
    UNIQUE KEY(col_int_undef_signed_not_null, col_date_undef_signed_not_null, col_bigint_undef_signed_not_null_index_inverted, col_bigint_undef_signed_not_null)
    PARTITION BY             RANGE(col_int_undef_signed_not_null, col_date_undef_signed_not_null) (
                    PARTITION p VALUES LESS THAN ('-1', '1997-12-11'),
                    PARTITION p0 VALUES LESS THAN ('4', '2023-12-11'),
                    PARTITION p1 VALUES LESS THAN ('6', '2023-12-15'),
                    PARTITION p2 VALUES LESS THAN ('7', '2023-12-16'),
                    PARTITION p3 VALUES LESS THAN ('8', '2023-12-25'),
                    PARTITION p4 VALUES LESS THAN ('8', '2024-01-18'),
                    PARTITION p5 VALUES LESS THAN ('10', '2024-02-18'),
                    PARTITION p6 VALUES LESS THAN ('1147483647', '2056-12-31'),
                    PARTITION p100 VALUES LESS THAN ('2147483647', '9999-12-31')
                )

    distributed by hash(col_bigint_undef_signed_not_null_index_inverted)
    properties("enable_unique_key_merge_on_write" = "true", "replication_num" = "1"); """
}
