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
    sql 'drop database if exists nereids_insert_into_table_test'
    sql 'create database nereids_insert_into_table_test'
    sql 'use nereids_insert_into_table_test'

    sql '''
        create table src (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(9, 3) null,
            `kchr` char(10) null,
            `kvchr` varchar(10) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2` datetimev2(0) null,
            `kdcml32v3` decimalv3(7, 3) null,
            `kdcml64v3` decimalv3(10, 5) null,
            `kdcml128v3` decimalv3(20, 8) null,
            `kaint` array<int> null,
            `kmintint` map<int, int> null,
            `kjson` json null
        ) engine=OLAP
        duplicate key(id)
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    streamLoad {
        table "src"
        db "nereids_insert_into_table_test"
        set 'column_separator', ';'
        set 'columns', '''
            id, kbool, ktint, ksint, kint, kbint, klint, kfloat, kdbl, kdcml, kchr, kvchr, kstr,
            kdt, kdtv2, kdtm, kdtmv2, kdcml32v3, kdcml64v3, kdcml128v3, kaint
            '''
        file "src.dat"
    }

    def files = [
            'agg_nop_t', 'agg_t', 'agg_type_cast',
            'dup_nop_t', 'dup_t', 'dup_type_cast',
            'uni_nop_t', 'uni_t', 'uni_type_cast',
            'map_t', 'random_t', 'json_t'
    ]

    for (String file in files) {
        def str = new File("""${context.file.parent}/ddl/${file}.sql""").text
        for (String table in str.split(';')) {
            sql table
        }
        sleep(100)
    }
}