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



suite("test_timestamptz_sort") {

    sql " set time_zone = '+08:00'; "

    sql """
        DROP TABLE IF EXISTS `timestamptz_sort_test`;
    """
    sql """
        CREATE TABLE timestamptz_sort_test (id INT, tz timestamptz) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """

    sql """
        insert into timestamptz_sort_test values 
        (1, cast("2020-01-01 00:00:00 +03:00" as timestamptz)),
        (2, cast("2020-06-01 12:00:00 +05:00" as timestamptz)) , 
        (3, cast("2019-12-31 23:59:59 +00:00" as timestamptz));
    """

    qt_sort_asc """
        select * from timestamptz_sort_test order by tz asc;
    """

    qt_sort_desc """
        select * from timestamptz_sort_test order by tz desc;
    """

    qt_sort_limit """
        select * from timestamptz_sort_test order by tz asc limit 2;
    """

    qt_sort_offset_limit """
        select * from timestamptz_sort_test order by tz asc limit 1 offset 1;
    """

    // Exercise BE topn runtime predicate on TIMESTAMPTZ.
    // topn_filter_ratio is amplified so that the FE generates a TopnFilter targeting the OlapScan
    // regardless of row-count statistics; without TIMESTAMPTZ support in RuntimePredicate this
    // query throws "meet invalid type, type=TIMESTAMPTZ" on the BE.
    sql " set topn_filter_ratio = 1000000; "

    // Larger dataset to exercise the BE topn runtime predicate over many segments / pages,
    // so that the threshold tightens repeatedly and storage-layer pruning kicks in.
    sql """ DROP TABLE IF EXISTS timestamptz_sort_big; """
    sql """
        CREATE TABLE timestamptz_sort_big (id INT, tz timestamptz)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO timestamptz_sort_big
        SELECT
            cast(number as INT),
            cast(from_unixtime(number) as timestamptz)
        FROM numbers("number" = "10000");
    """

    // The plan must actually contain the TopN runtime predicate (TOPN OPT on OlapScanNode);
    // otherwise the BE path under test is dormant and the query trivially passes for the wrong
    // reason. We assert it on both asc and desc so flipping the order is also covered.
    explain {
        sql "select * from timestamptz_sort_big order by tz asc, id asc limit 10;"
        contains("TOPN OPT:")
    }
    explain {
        sql "select * from timestamptz_sort_big order by tz desc, id asc limit 10;"
        contains("TOPN OPT:")
    }

    qt_big_topn_asc """
        select * from timestamptz_sort_big order by tz asc, id asc limit 10;
    """

    qt_big_topn_desc """
        select * from timestamptz_sort_big order by tz desc, id asc limit 10;
    """

    qt_big_topn_offset """
        select * from timestamptz_sort_big order by tz asc, id asc limit 10 offset 50;
    """

    qt_big_count """
        select count(*) from timestamptz_sort_big;
    """

    // Mixed timezones: verify TopN orders by the absolute UTC instant, not the local display.
    // Two pairs deliberately share local clock time but differ by offset; the +00:00 row is the
    // *later* instant for the noon pair, and the +08:00 row is the *later* instant for the midnight
    // pair. The sort must reflect the actual instant, not the local string.
    sql """ DROP TABLE IF EXISTS timestamptz_sort_mixed; """
    sql """
        CREATE TABLE timestamptz_sort_mixed (id INT, tz timestamptz)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        insert into timestamptz_sort_mixed values
            (1,  cast("2020-01-01 12:00:00 +00:00" as timestamptz)),
            (2,  cast("2020-01-01 12:00:00 +08:00" as timestamptz)),
            (3,  cast("2020-01-01 12:00:00 -05:00" as timestamptz)),
            (4,  cast("2020-01-01 12:00:00 +05:30" as timestamptz)),
            (5,  cast("2020-01-01 00:00:00 +08:00" as timestamptz)),
            (6,  cast("2020-01-01 00:00:00 +00:00" as timestamptz)),
            (7,  cast("2020-01-01 00:00:00 -08:00" as timestamptz)),
            (8,  cast("2019-12-31 23:59:59 +00:00" as timestamptz)),
            (9,  cast("2020-06-01 12:00:00 +05:00" as timestamptz)),
            (10, cast("2020-06-01 12:00:00 -05:00" as timestamptz));
    """

    explain {
        sql "select * from timestamptz_sort_mixed order by tz asc, id asc limit 5;"
        contains("TOPN OPT:")
    }

    qt_mixed_asc """
        select * from timestamptz_sort_mixed order by tz asc, id asc limit 5;
    """

    qt_mixed_desc """
        select * from timestamptz_sort_mixed order by tz desc, id asc limit 5;
    """

    // Combined large-scale + nullable: 10k rows with periodic NULLs scattered through, exercising
    // the topn runtime predicate together with NULLS FIRST / NULLS LAST semantics over many pages.
    sql """ DROP TABLE IF EXISTS timestamptz_sort_big_null; """
    sql """
        CREATE TABLE timestamptz_sort_big_null (id INT, tz timestamptz NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO timestamptz_sort_big_null
        SELECT
            cast(number as INT),
            case when number % 97 = 0 then null
                 else cast(from_unixtime(number) as timestamptz)
            end
        FROM numbers("number" = "10000");
    """

    explain {
        sql "select * from timestamptz_sort_big_null order by tz asc nulls first, id asc limit 10;"
        contains("TOPN OPT:")
    }
    explain {
        sql "select * from timestamptz_sort_big_null order by tz asc nulls last, id asc limit 10;"
        contains("TOPN OPT:")
    }

    qt_big_null_asc_first """
        select * from timestamptz_sort_big_null order by tz asc nulls first, id asc limit 10;
    """

    qt_big_null_asc_last """
        select * from timestamptz_sort_big_null order by tz asc nulls last, id asc limit 10;
    """

    qt_big_null_desc_first """
        select * from timestamptz_sort_big_null order by tz desc nulls first, id asc limit 10;
    """

    qt_big_null_desc_last """
        select * from timestamptz_sort_big_null order by tz desc nulls last, id asc limit 10;
    """

    qt_big_null_count """
        select count(*), count(tz) from timestamptz_sort_big_null;
    """
}
