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

// map_agg/map_agg_v2 expose their MAP key/value as independent top-level Any(0)/Any(1)
// arguments. The default decimal v3 precision promotion must keep those logical groups
// independent: merging them into one wider type would widen the DECIMAL(38,0) key to a
// scale that cannot hold a 38-digit integral key (so the entry disappears as a NULL key)
// and truncate twelve fractional digits from the DECIMAL(38,18) value before aggregation.
suite("nereids_agg_fn_map_decimal_precision") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "drop table if exists map_agg_dec_precision"
    sql """
        create table map_agg_dec_precision (
            g int,
            k decimal(38, 0),
            v decimal(38, 18)
        )
        duplicate key(g)
        distributed by hash(g) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into map_agg_dec_precision values
            (1, 99999999999999999999999999999999999999, 0.125000000000000000),
            (2, 12345678901234567890123456789012345678, 2.000000000000000001),
            (2, 12345678901234567890123456789012345678, 3.000000000000000000)
    """

    // the 38-digit integral key must be preserved, and the value must keep all 18
    // fractional digits
    order_qt_map_agg_decimal_precision """
        select map_agg(k, v) from map_agg_dec_precision where g = 1;
    """

    order_qt_map_agg_v2_decimal_precision """
        select map_agg_v2(k, v) from map_agg_dec_precision where g = 1;
    """

    // duplicate keys are overwritten by the last value, still without precision loss
    order_qt_map_agg_group_by_decimal_precision """
        select g, map_agg(k, v) from map_agg_dec_precision group by g order by g;
    """
}
