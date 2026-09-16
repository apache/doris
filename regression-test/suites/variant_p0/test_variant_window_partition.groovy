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

// GROUP BY, ORDER BY, hash join and window ORDER BY accept Variant, and they all use the same
// canonical equality and comparison. Window PARTITION BY used to be the one remaining context that
// rejected it, so these queries pin that a Variant partition key groups rows by canonical value:
// values of different JSON kinds ("a", true, 1) never share a partition, and equal values do.
suite("test_variant_window_partition", "p0") {
    sql "drop table if exists test_variant_window_partition"

    sql """
        create table test_variant_window_partition (
            id int,
            v variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """

    sql """
        insert into test_variant_window_partition values
            (1, parse_to_variant('{"k": 1}')),
            (2, parse_to_variant('{"k": 1}')),
            (3, parse_to_variant('{"k": 2}')),
            (4, parse_to_variant('{"k": "a"}')),
            (5, parse_to_variant('{"k": true}')),
            (6, parse_to_variant('{"k": true}')),
            (7, parse_to_variant('{"k": 1}')),
            (8, parse_to_variant('{"k": true}'))
    """

    // PARTITION BY a Variant sub-path.
    order_qt_partition_by_subpath """
        select id, v['k'] as k, row_number() over (partition by v['k'] order by id) as rn
        from test_variant_window_partition
    """

    // PARTITION BY the whole Variant value.
    order_qt_partition_by_whole """
        select id, row_number() over (partition by v order by id) as rn
        from test_variant_window_partition
    """

    // rank() with a Variant partition key, which is also the shape CreatePartitionTopNFromWindow
    // rewrites into a PartitionTopN operator.
    order_qt_partition_rank """
        select id, rank() over (partition by v['k'] order by id) as rk
        from test_variant_window_partition
    """

    // A Variant partition key combined with a scalar one.
    order_qt_partition_mixed_keys """
        select id, row_number() over (partition by v['k'], id % 2 order by id) as rn
        from test_variant_window_partition
    """
}
