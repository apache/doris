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

import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.doris.regression.action.ProfileAction

suite("test_timestamp_ns_index") {
    sql "set enable_profile = true"
    sql "set profile_level = 2"
    sql "set enable_condition_cache = false"
    sql "set enable_sql_cache = false"

    def profileAction = new ProfileAction(context)
    def assertProfileCounterPositive = { String token, String counterName ->
        String profileString = profileAction.getProfileBySql(token, [counterName])
        Pattern pattern = Pattern.compile(Pattern.quote(counterName) + ":\\s*([0-9,]+)")
        Matcher matcher = pattern.matcher(profileString)
        long total = 0
        while (matcher.find()) {
            total += Long.parseLong(matcher.group(1).replace(",", ""))
        }
        assertTrue(total > 0, "Expected ${counterName} to be positive, profile: ${profileString}")
    }

    sql "drop table if exists timestamp_ns_index"
    sql """
        create table timestamp_ns_index (
            id int,
            dt timestamp_ns,
            index idx_dt(dt) using inverted
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "inverted_index_storage_format" = "V3"
        )
    """
    sql """
        insert into timestamp_ns_index values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.000000001'),
        (4, '2262-04-11 23:47:16.854775807'),
        (5, null)
    """
    sql """
        insert into timestamp_ns_index
        select number + 100,
               microseconds_add(cast('2024-01-01 00:00:00.000000001' as timestamp_ns),
                                cast(number as int))
        from numbers("number" = "4096")
    """

    order_qt_eq "select id, dt from timestamp_ns_index where dt = '1970-01-01 00:00:00.000000001' order by id"
    order_qt_range """
        select id, dt from timestamp_ns_index
        where dt >= '1970-01-01 00:00:00.000000000'
          and dt <= '1970-01-01 00:00:00.000000001'
        order by id
    """
    order_qt_in """
        select id, dt from timestamp_ns_index
        where dt in ('1677-09-21 00:12:43.145224192', '2262-04-11 23:47:16.854775807')
        order by id
    """
    order_qt_v3_inverted_index_null_result """
        select id from timestamp_ns_index where dt is null order by id
    """

    def v3InvertedIndexToken = "timestamp_ns_v3_inverted_index_" + UUID.randomUUID().toString()
    sql """
        select /* ${v3InvertedIndexToken} */ id from timestamp_ns_index
        where dt = '2024-01-01 00:00:00.002048001'
    """
    assertProfileCounterPositive(v3InvertedIndexToken, "RowsInvertedIndexFiltered")

    def v3InvertedNullToken = "timestamp_ns_v3_inverted_null_" + UUID.randomUUID().toString()
    sql """
        select /* ${v3InvertedNullToken} */ id from timestamp_ns_index where dt is null
    """
    assertProfileCounterPositive(v3InvertedNullToken, "RowsInvertedIndexFiltered")

    sql "drop table if exists timestamp_ns_explicit_bloom_index"
    sql """
        create table timestamp_ns_explicit_bloom_index (
            id bigint not null,
            dt timestamp_ns,
            index idx_dt_bloom(dt) using bloomfilter
                properties("bloom_filter_fpp" = "0.01")
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        insert into timestamp_ns_explicit_bloom_index
        select number,
               if(number % 1024 = 100,
                  null,
                  microseconds_add(cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
                                   cast(number * 2 as int)))
        from numbers("number" = "4096")
    """
    order_qt_explicit_bloom_filter_null_result """
        select id from timestamp_ns_explicit_bloom_index where dt is null order by id
    """

    def explicitBloomFilterToken =
            "timestamp_ns_explicit_bloom_filter_" + UUID.randomUUID().toString()
    sql """
        select /* ${explicitBloomFilterToken} */ count(*)
        from timestamp_ns_explicit_bloom_index
        where dt = '1970-01-01 00:00:00.000001001'
    """
    assertProfileCounterPositive(explicitBloomFilterToken, "RowsBloomFilterFiltered")

    sql "drop table if exists timestamp_ns_pruning_index"
    sql """
        create table timestamp_ns_pruning_index (
            dt_key timestamp_ns not null,
            id bigint not null,
            dt_inverted timestamp_ns,
            dt_bloom timestamp_ns not null,
            dt_zonemap timestamp_ns not null,
            index idx_dt_inverted(dt_inverted) using inverted
        )
        duplicate key(dt_key, id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "bloom_filter_columns" = "dt_bloom",
            "inverted_index_storage_format" = "SNII",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        insert into timestamp_ns_pruning_index (dt_key, id, dt_inverted, dt_bloom, dt_zonemap)
        select microseconds_add(cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
                                cast(number as int)),
               number,
               if(number % 1024 = 100,
                  null,
                  microseconds_add(cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
                                   cast(number as int))),
               microseconds_add(cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
                                cast(number * 2 as int)),
               microseconds_add(cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
                                cast(number as int))
        from numbers("number" = "4096")
    """

    order_qt_key_index_result """
        select id, dt_key from timestamp_ns_pruning_index
        where dt_key = '1970-01-01 00:00:00.002048001'
        order by id
    """
    order_qt_inverted_index_result """
        select id, dt_inverted from timestamp_ns_pruning_index
        where dt_inverted = '1970-01-01 00:00:00.002048001'
        order by id
    """
    order_qt_snii_inverted_index_range_result """
        select id, dt_inverted from timestamp_ns_pruning_index
        where dt_inverted >= '1970-01-01 00:00:00.002047001'
          and dt_inverted <= '1970-01-01 00:00:00.002049001'
        order by id
    """
    order_qt_snii_inverted_index_null_result """
        select id from timestamp_ns_pruning_index where dt_inverted is null order by id
    """
    order_qt_bloom_filter_result """
        select count(*) from timestamp_ns_pruning_index
        where dt_bloom = '1970-01-01 00:00:00.000001001'
    """
    order_qt_zone_map_result """
        select count(*) from timestamp_ns_pruning_index
        where dt_zonemap = '1970-01-01 00:00:01.000000001'
    """

    def keyIndexToken = "timestamp_ns_key_index_" + UUID.randomUUID().toString()
    sql """
        select /* ${keyIndexToken} */ id from timestamp_ns_pruning_index
        where dt_key = '1970-01-01 00:00:00.002048001'
    """
    assertProfileCounterPositive(keyIndexToken, "RowsKeyRangeFiltered")

    def invertedIndexToken = "timestamp_ns_inverted_index_" + UUID.randomUUID().toString()
    sql """
        select /* ${invertedIndexToken} */ id from timestamp_ns_pruning_index
        where dt_inverted = '1970-01-01 00:00:00.002048001'
    """
    assertProfileCounterPositive(invertedIndexToken, "RowsInvertedIndexFiltered")

    def sniiInvertedNullToken = "timestamp_ns_snii_inverted_null_" + UUID.randomUUID().toString()
    sql """
        select /* ${sniiInvertedNullToken} */ id from timestamp_ns_pruning_index
        where dt_inverted is null
    """
    assertProfileCounterPositive(sniiInvertedNullToken, "RowsInvertedIndexFiltered")

    def bloomFilterToken = "timestamp_ns_bloom_filter_" + UUID.randomUUID().toString()
    sql """
        select /* ${bloomFilterToken} */ count(*) from timestamp_ns_pruning_index
        where dt_bloom = '1970-01-01 00:00:00.000001001'
    """
    assertProfileCounterPositive(bloomFilterToken, "RowsBloomFilterFiltered")

    def zoneMapToken = "timestamp_ns_zone_map_" + UUID.randomUUID().toString()
    sql """
        select /* ${zoneMapToken} */ count(*) from timestamp_ns_pruning_index
        where dt_zonemap = '1970-01-01 00:00:01.000000001'
    """
    assertProfileCounterPositive(zoneMapToken, "RowsStatsFiltered")

    sql "drop table if exists timestamp_ns_invalid_ngram_index"
    test {
        sql """
            create table timestamp_ns_invalid_ngram_index (
                id int,
                dt timestamp_ns,
                index idx_dt_ngram(dt) using ngram_bf
                    properties("gram_size" = "2", "bf_size" = "512")
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "is not supported in ngram_bf index"
    }

    sql "drop table if exists timestamp_ns_invalid_ann_index"
    test {
        sql """
            create table timestamp_ns_invalid_ann_index (
                id int,
                dt timestamp_ns not null,
                index idx_dt_ann(dt) using ann properties(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "1"
                )
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num" = "1")
        """
        exception "ANN index column must be array type"
    }

    sql "set enable_profile = false"
}
