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

// Reproduces DORIS-25915:
//   When VARIANT has typed paths that exceed variant_max_subcolumns_count and
//   variant_enable_typed_paths_to_sparse=true, timestamptz values that fall to
//   the sparse path are read back as only the timezone suffix (e.g. "+00:00")
//   instead of the full timestamp.
suite("test_variant_timestamptz_sparse", "p0"){
    sql " set time_zone = '+08:00' "
    sql " set default_variant_enable_doc_mode = false "

    sql "DROP TABLE IF EXISTS test_variant_timestamptz_sparse_repro"

    sql """
        CREATE TABLE test_variant_timestamptz_sparse_repro (
          pk int,
          var VARIANT<
            'c1':bigint,
            'c2':bigint,
            'd1':date,
            'd2':date,
            'dt0':datetime(0),
            'dt0n':datetime(0),
            'dt3':datetime(3),
            'dt3n':datetime(3),
            'dt6':datetime(6),
            'dt6n':datetime(6),
            'ts0':timestamptz(0),
            'ts0n':timestamptz(0),
            'ts3':timestamptz(3),
            'ts3n':timestamptz(3),
            'ts6':timestamptz(6),
            'ts6n':timestamptz(6),
            'v1':string,
            'v2':string,
            properties("variant_max_subcolumns_count" = "2","variant_enable_typed_paths_to_sparse" = "true")
          > NULL,
          INDEX c1 (`var`) USING INVERTED PROPERTIES("field_pattern" = "c1"),
          INDEX c2 (`var`) USING INVERTED PROPERTIES("field_pattern" = "c2"),
          INDEX d1 (`var`) USING INVERTED PROPERTIES("field_pattern" = "d1"),
          INDEX d2 (`var`) USING INVERTED PROPERTIES("field_pattern" = "d2"),
          INDEX dt0 (`var`) USING INVERTED PROPERTIES("field_pattern" = "dt0"),
          INDEX dt0n (`var`) USING INVERTED PROPERTIES("field_pattern" = "dt0n"),
          INDEX dt3 (`var`) USING INVERTED PROPERTIES("field_pattern" = "dt3"),
          INDEX dt3n (`var`) USING INVERTED PROPERTIES("field_pattern" = "dt3n"),
          INDEX dt6 (`var`) USING INVERTED PROPERTIES("field_pattern" = "dt6"),
          INDEX dt6n (`var`) USING INVERTED PROPERTIES("field_pattern" = "dt6n"),
          INDEX v1 (`var`) USING INVERTED PROPERTIES("field_pattern" = "v1"),
          INDEX v2 (`var`) USING INVERTED PROPERTIES("field_pattern" = "v2")
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_variant_timestamptz_sparse_repro VALUES
        (1, '{"c1":1699516324,"c2":1690122421,"d1":"2024-02-25","d2":"2024-11-27","dt0":"2002-12-25 12:42:19","dt0n":"2015-02-05 02:36:13","dt3":"2015-02-22 02:09:01","dt3n":"2015-09-16 02:55:07","dt6":"2001-09-19 09:53:52","dt6n":"2003-12-21 02:29:00","ts0":"2003-12-21 02:29:00 +00:00","ts0n":"2003-12-21 02:29:00 +00:00","ts3":"2003-12-21 02:29:00 +00:00","ts3n":"2003-12-21 02:29:00 -05:00","ts6":"2003-12-21 02:29:00 -05:00","ts6n":"2003-12-21 02:29:00 +03:00","v1":"2024-12-10 10:16:19","v2":"2023-12-31 23:59:59"}');
    """

    sql "SYNC"

    // Pre-fix: every column that fell to sparse returned just the timezone
    // suffix ("+08:00"). Post-fix: each ts* path round-trips as a full timestamp.
    qt_select_all """
        SELECT
          pk,
          CAST(var['ts0']  AS string) AS str_ts0,
          CAST(var['ts0n'] AS string) AS str_ts0n,
          CAST(var['ts3']  AS string) AS str_ts3,
          CAST(var['ts3n'] AS string) AS str_ts3n,
          CAST(var['ts6']  AS string) AS str_ts6,
          CAST(var['ts6n'] AS string) AS str_ts6n
        FROM test_variant_timestamptz_sparse_repro
        ORDER BY pk;
    """

    qt_select_cast_ts0 """
        SELECT
          pk,
          CAST(var['ts0'] AS timestamptz(0)) AS cast_ts0,
          CAST(var['ts3'] AS timestamptz(3)) AS cast_ts3,
          CAST(var['ts6'] AS timestamptz(6)) AS cast_ts6
        FROM test_variant_timestamptz_sparse_repro
        ORDER BY pk;
    """
}
