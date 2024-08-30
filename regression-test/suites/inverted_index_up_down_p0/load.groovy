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

suite("test_upgrade_downgrade_prepare_inverted_index","p0,inverted_index,restart_fe") {
    for (version in ["V1", "V2"]) {
        sql "drop table if exists t_up_down_inverted_index${version}_duplicate"
        sql String.format('''
        create table t_up_down_inverted_index%s_duplicate(
        a int,
        b int,
        c int,
        en varchar(1024),
        ch varchar(1024),
        index idx_a(a),
        index idx_en(en) using inverted properties("parser" = "english", "support_phrase" = "false"),
        index idx_ch(ch) using inverted properties("parser" = "chinese")
        )
        DUPLICATE KEY(a, b)
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "%s"
        );''', version, version)
        sql String.format('''insert into t_up_down_inverted_index%s_duplicate values
            (1,1,1,"I see","我明白了"),
            (2,2,2,"I quit","我不干了"),
            (3,3,3,"Let go","放手"),
            (4,4,4,"Me too","我也是"),
            (5,5,5,"My god","天哪"),
            (6,6,6,"No way","不行"),
            (7,7,7,"Come on","来吧赶快"),
            (8,8,8,"Hold on","等一等"),
            (9,9,9,"Not bad","还不错"),
            (1,1,1,"I agree","我同意")''', version)

        sql "drop table if exists t_up_down_inverted_index${version}_unique"
        sql String.format('''
        create table t_up_down_inverted_index%s_unique(
        a int,
        b int,
        c int,
        d date,
        en varchar(1024),
        ch varchar(1024),
        index idx_d(d),
        index idx_ch(ch) using inverted properties("parser" = "unicode", "support_phrase" = "true"),
        index idx_a(a)
        )
        UNIQUE KEY(a, b) 
        PARTITION BY RANGE(`a`)
        (
        PARTITION `p0` VALUES LESS THAN (100),
        PARTITION `p1` VALUES LESS THAN (200),
        PARTITION `p2` VALUES LESS THAN (300),
        PARTITION `p3` VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(`a`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "%s"
        );''', version, version)

        sql String.format('''insert into t_up_down_inverted_index%s_unique values
            (1,1,1,"2022-1-22","I see","我明白了"),
            (2,2,2,"2022-2-22","I quit","我不干了"),
            (3,3,3,"2022-3-22","Let go","放手"),
            (4,4,4,"2022-4-22","Me too","我也是"),
            (5,5,5,"2022-5-22","My god","天哪"),
            (6,6,6,"2022-6-22","No way","不行"),
            (7,7,7,"2022-7-22","Come on","来吧赶快"),
            (8,8,8,"2022-8-22","Hold on","等一等"),
            (9,9,9,"2022-9-22","Not bad","还不错"),
            (10,10,10,"2022-10-22","I agree","我同意")''', version)

        sql "drop table if exists t_up_down_inverted_index${version}_agg"
        sql String.format('''
        create table t_up_down_inverted_index%s_agg(
        a varchar(255),
        b int,
        c int,
        d int sum,
        e int max,
        f int min,
        index idx_a(a) 
        )
        AGGREGATE KEY(a, b, c)
        DISTRIBUTED BY HASH(`a`) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "inverted_index_storage_format" = "%s"
        );''', version, version)
        def values = new ArrayList()
        for (int i = 0; i < 1000; i++ ) {
            values.add(String.format("(\"%d\",%d,%d, %d, %d, %d)", i, i, i, i, i, i))
        }
        for (int i = 0; i < 10; i++ ) {
            sql String.format("insert into t_up_down_inverted_index%s_agg values%s", version, values.join(","))
        }
    }
}
