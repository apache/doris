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

suite("ann_index_only_scan_debug_point", "nonConcurrent") {
    sql "unset variable all;"
    sql "set enable_segment_limit_pushdown=true;"
    sql "set experimental_enable_virtual_slot_for_cse=true;"
    sql "set enable_no_need_read_data_opt=true;"
    sql "set parallel_pipeline_task_num=1;"
    sql "set enable_sql_cache=false;"
    sql "set enable_condition_cache=false;"

    sql "drop table if exists ann_index_only_scan_debug_point"
    sql """
        create table ann_index_only_scan_debug_point (
            id int not null,
            embedding array<float> not null,
            comment string not null,
            value int null,
            index idx_comment(`comment`) using inverted properties("parser" = "english"),
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    sql """
        insert into ann_index_only_scan_debug_point values
        (0, [39.906116, 10.495334, 54.08394, 88.67262, 55.243687, 10.162686, 36.335983, 38.684258], 'alpha people', 100),
        (1, [62.759315, 97.15586, 25.832521, 39.604908, 88.76715, 72.64085, 9.688437, 17.721428], 'beta people', 101),
        (2, [15.447449, 59.7771, 65.54516, 12.973712, 99.685135, 72.080734, 85.71118, 99.35976], 'gamma', 102),
        (3, [72.26747, 46.42257, 32.368374, 80.50209, 5.777631, 98.803314, 7.0915947, 68.62693], 'delta', 103),
        (4, [22.098177, 74.10027, 63.634556, 4.710955, 12.405106, 79.39356, 63.014366, 68.67834], 'epsilon', 104),
        (5, [27.53003, 72.1106, 50.891026, 38.459953, 68.30715, 20.610682, 94.806274, 45.181377], 'zeta people', 105),
        (6, [77.73215, 64.42907, 71.50025, 43.85641, 94.42648, 50.04773, 65.12575, 68.58207], 'eta', 106),
        (7, [2.1537063, 82.667885, 16.171143, 71.126656, 5.335274, 40.286068, 11.943586, 3.69409], 'theta', 107),
        (8, [54.435013, 56.800594, 59.335514, 55.829235, 85.46627, 33.388138, 11.076194, 20.480877], 'iota', 108),
        (9, [76.197945, 60.623528, 84.229805, 31.652937, 71.82595, 48.04684, 71.29212, 30.282396], 'kappa', 109);
    """

    sql "drop table if exists ann_index_only_scan_remap_debug_point"
    sql """
        create table ann_index_only_scan_remap_debug_point (
            id int not null,
            pad_int int not null,
            pad_text string not null,
            embedding array<float> not null,
            value int not null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    sql """
        insert into ann_index_only_scan_remap_debug_point values
        (1, 10, 'a', [0.0, 0.0, 0.0], 100),
        (2, 20, 'b', [0.1, 0.0, 0.0], 200),
        (3, 30, 'c', [0.2, 0.0, 0.0], 300),
        (4, 40, 'd', [0.3, 0.0, 0.0], 400),
        (5, 50, 'e', [0.4, 0.0, 0.0], 500),
        (6, 60, 'f', [0.5, 0.0, 0.0], 600),
        (7, 70, 'g', [0.6, 0.0, 0.0], 700),
        (8, 80, 'h', [0.7, 0.0, 0.0], 800),
        (9, 90, 'i', [0.8, 0.0, 0.0], 900),
        (10, 100, 'j', [0.9, 0.0, 0.0], 1000);
    """

    sql "drop table if exists ann_index_only_scan_ip_debug_point"
    sql """
        create table ann_index_only_scan_ip_debug_point (
            id int not null,
            embedding array<float> not null,
            value int null,
            index ann_embedding(`embedding`) using ann properties(
                "index_type"="hnsw",
                "metric_type"="inner_product",
                "dim"="8"
            )
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    sql """
        insert into ann_index_only_scan_ip_debug_point
        select id, embedding, value from ann_index_only_scan_debug_point;
    """

    def v = "[26.360261917114258,7.05784273147583,32.361351013183594,86.39714050292969,58.79527282714844,27.189321517944336,99.38946533203125,80.19270324707031]"

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "segment_iterator._read_columns_by_index", [column_name: "embedding"])

        sql """
            select id
            from ann_index_only_scan_debug_point
            order by l2_distance_approximate(embedding, ${v})
            limit 5;
        """

        sql """
            select id
            from ann_index_only_scan_debug_point
            where l2_distance_approximate(embedding, ${v}) < 170.0
            order by id;
        """

        sql """
            select id, l2_distance_approximate(embedding, ${v}) as dist
            from ann_index_only_scan_debug_point
            where l2_distance_approximate(embedding, ${v}) < 170.0
            order by id;
        """

        sql """
            select id
            from ann_index_only_scan_debug_point
            where l2_distance_approximate(embedding, ${v}) < 170.0
              and comment match_any 'people'
            order by id;
        """

        sql """
            select id, inner_product_approximate(embedding, ${v}) as score
            from ann_index_only_scan_ip_debug_point
            where inner_product_approximate(embedding, ${v}) > 1000.0
            order by id;
        """

        sql """
            select id
            from ann_index_only_scan_remap_debug_point
            where l2_distance_approximate(embedding, [0.0, 0.0, 0.0]) < 1.0
            order by id;
        """

        test {
            sql """
                select id, embedding
                from ann_index_only_scan_debug_point
                where l2_distance_approximate(embedding, ${v}) < 170.0
                order by id;
            """
            exception "does not need to read data"
        }

        test {
            sql """
                select id, l2_distance_approximate(embedding, ${v}) as dist
                from ann_index_only_scan_debug_point
                where l2_distance_approximate(embedding, ${v}) > 120.0
                order by id;
            """
            exception "does not need to read data"
        }

        test {
            sql """
                select id, inner_product_approximate(embedding, ${v}) as score
                from ann_index_only_scan_ip_debug_point
                where inner_product_approximate(embedding, ${v}) < 16175.99
                order by id;
            """
            exception "does not need to read data"
        }

        test {
            sql """
                select id
                from ann_index_only_scan_debug_point
                where array_size(embedding) > 5
                  and l2_distance_approximate(embedding, ${v}) > 120.0
                order by l2_distance_approximate(embedding, ${v})
                limit 5;
            """
            exception "does not need to read data"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    sql "drop table if exists ann_index_only_scan_no_ann_debug_point"
    sql """
        create table ann_index_only_scan_no_ann_debug_point (
            id int not null,
            embedding array<float> not null,
            value int null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """

    sql """
        insert into ann_index_only_scan_no_ann_debug_point
        select id, embedding, value from ann_index_only_scan_debug_point;
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "segment_iterator._read_columns_by_index", [column_name: "embedding"])

        test {
            sql """
                select id
                from ann_index_only_scan_no_ann_debug_point
                where l2_distance_approximate(embedding, ${v}) < 999.0
                order by id;
            """
            exception "does not need to read data"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }
}
