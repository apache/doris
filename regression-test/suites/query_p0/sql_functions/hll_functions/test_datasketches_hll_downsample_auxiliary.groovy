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

suite("test_datasketches_hll_downsample_auxiliary") {
    // datasketches-cpp 46025e9 Hll4Array(lgK=8), coupons pair(1,20), pair(3,21).
    // The complete register array is f0,f0 followed by 126 zero bytes.
    def compact = "concat('0a0107080328000286068707080800400000000300c06f400000000000000000fe00000002000000', 'f0f0', repeat('00',126), '0100005003000054')"
    def updatable = "concat('0a0107080320000286068707080800400000000300c06f400000000000000000fe00000002000000', 'f0f0', repeat('00',126), '00000000010000500000000003000054', repeat('00',16))"
    sql 'DROP TABLE IF EXISTS test_ds_hll_downsample_auxiliary'
    sql """CREATE TABLE test_ds_hll_downsample_auxiliary (id INT, sk STRING)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO test_ds_hll_downsample_auxiliary VALUES
        (1,datasketches_hll_downsample(unhex(${compact}),8,'HLL_6')),
        (2,unhex(${compact})), (3,unhex(${updatable}))"""
    order_qt_input """SELECT id,length(sk) FROM test_ds_hll_downsample_auxiliary ORDER BY id"""
    for (int k : [4,8,21]) {
        for (String type : ['HLL_4','HLL_6','HLL_8']) {
            order_qt_results """SELECT id,
                hex(datasketches_hll_downsample(sk,${k},'${type}')) =
                    hex(IF(id=3 AND ${k}>=8 AND '${type}'='HLL_4', sk,
                        datasketches_hll_downsample(unhex(${compact}),${k},'${type}'))),
                hex(datasketches_hll_downsample(CAST(sk AS VARBINARY),${k},'${type}')) =
                    hex(datasketches_hll_downsample(sk,${k},'${type}')),
                hex(datasketches_hll_downsample(CAST(sk AS VARCHAR(65533)),${k},'${type}')) =
                    hex(datasketches_hll_downsample(sk,${k},'${type}'))
                FROM test_ds_hll_downsample_auxiliary ORDER BY id"""
        }
    }
    order_qt_noop """SELECT id,datasketches_hll_downsample(sk,21,'HLL_4')=sk
        FROM test_ds_hll_downsample_auxiliary WHERE id IN (2,3) ORDER BY id"""
    order_qt_serialization """SELECT
        hex(datasketches_hll_downsample(sk,4))=
            hex(datasketches_hll_downsample(unhex(${compact}),4))
        FROM test_ds_hll_downsample_auxiliary WHERE id=3 ORDER BY id"""
    order_qt_aggregate """SELECT id,
        datasketches_hll_union_agg(sk)=
            datasketches_hll_union_agg(unhex(${compact}))
        FROM test_ds_hll_downsample_auxiliary WHERE id IN (2,3) GROUP BY id ORDER BY id"""

    // Only register 0 is AUX_TOKEN; auxCount=1, but its compact entry was replaced by EMPTY.
    def bad = "concat('0a01070803280002000000000000f03f0000000200e06f400000000000000000ff00000001000000', '0f', repeat('00',131))"
    for (int k : [4,21]) {
        for (String type : ['HLL_4','HLL_6','HLL_8']) {
            test {
                sql "SELECT datasketches_hll_downsample(unhex(${bad}),${k},'${type}')"
                exception 'compact auxiliary map contains an EMPTY entry'
            }
            test {
                sql "SELECT datasketches_hll_downsample(CAST(unhex(${bad}) AS VARBINARY),${k},'${type}')"
                exception 'compact auxiliary map contains an EMPTY entry'
            }
        }
    }
    test {
        sql "SELECT datasketches_hll_union_agg(unhex(${bad}))"
        exception 'compact auxiliary map contains an EMPTY entry'
    }

    // Complete updatable lgK=8 HLL_4 SET from update(int32_t i), i in [0,10).
    // Only change the count; all 32 table slots remain present and ten are occupied.
    def setImage = '03010708050000010a0000000000000081bc5d0600000000000000000000000000000000862ff90d00000000000000000000000000000000cbd7c20400000000000000000000000000000000c1e9170500000000d21673070000000034a2610e7581660700000000000000000000000000000000fc2d420a7b65e6082bf2fb06000000000000000000000000'
    for (String count : ['01000000','0b000000']) {
        def malformed = "unhex(concat(substr('${setImage}',1,16),'${count}',substr('${setImage}',25)))"
        for (int k : [4,8,21]) {
            test {
                sql "SELECT datasketches_hll_downsample(${malformed},${k},'HLL_6')"
                exception 'coupon count does not match occupied slots'
            }
            test {
                sql "SELECT datasketches_hll_downsample(CAST(${malformed} AS VARBINARY),${k},'HLL_4')"
                exception 'coupon count does not match occupied slots'
            }
        }
        test {
            sql "SELECT datasketches_hll_union_agg(${malformed})"
            exception 'coupon count does not match occupied slots'
        }
    }
}
