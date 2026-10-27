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

suite("test_spatial_wkb_compatibility") {
    // Legacy STRING functions keep their historical XY interpretation of EWKB dimension flags.
    // Typed functions reject PostGIS EWKB flags/SRID; ISO Z/M is retained separately.
    ["0101000080", "0101000040", "01010000C0", "01010000A0E6100000",
     "0101000060E6100000", "01010000E0E6100000"].eachWithIndex { header, index ->
        def ewkb = header + "000000000000F03F0000000000000040" + "00000000000008400000000000001040"
        "qt_ewkb_${index}"("""select ST_AsText(ST_GeometryFromWKB('${ewkb}')),
            ST_AsText(ST_GeomFromWKB('${ewkb}')),
            ST_GeometryFromWKBTyped('${ewkb}') is null,
            ST_GeogFromWKB('${ewkb}') is null""")
    }
    // ISO WKB dimensional type codes were never part of the legacy XY contract.
    qt_iso_dimensions """select ST_GeometryFromWKB(
        '01E9030000000000000000F03F00000000000000400000000000000840') is null,
        ST_GeometryFromWKBTyped(
        '01E9030000000000000000F03F00000000000000400000000000000840') is null"""
    qt_spherical_dateline """select ST_Intersects(ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'), ST_GeogFromWKB('010100000000000000008066400000000000000000')),
        ST_Disjoint(ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'), ST_GeogFromWKB('010100000000000000008066400000000000000000')), ST_Distance(ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'), ST_GeogFromWKB('010100000000000000008066400000000000000000')),
        ST_Intersects(ST_GeogFromWKB('01030000000100000005000000000000000040654000000000000024c000000000004065c000000000000024c000000000004065c0000000000000244000000000004065400000000000002440000000000040654000000000000024c0'), ST_GeogFromWKB('010100000000000000000000000000000000000000')), ST_Intersects(ST_GeogFromWKB('01030000000100000005000000000000000040654000000000000024c000000000004065c000000000000024c000000000004065c0000000000000244000000000004065400000000000002440000000000040654000000000000024c0'), ST_GeogFromWKB('010100000000000000008066400000000000000000'))"""
    qt_open_geography_line """select ST_Intersects(ST_GeogFromWKB('01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e03f000000000000e0bf000000000000e03f000000000000e03f000000000000e0bf000000000000e03f000000000000e0bf000000000000e0bf'), ST_GeogFromWKB('01020000000400000000000000000000c0000000000000000000000000000000c000000000000000400000000000000040000000000000004000000000000000400000000000000000')),
        ST_Intersects(ST_GeogFromWKB('01020000000400000000000000000000c0000000000000000000000000000000c000000000000000400000000000000040000000000000004000000000000000400000000000000000'), ST_GeogFromWKB('01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e03f000000000000e0bf000000000000e03f000000000000e03f000000000000e0bf000000000000e03f000000000000e0bf000000000000e0bf'))"""
    qt_geography_position_aliases """select ST_Intersects(ST_GeogFromWKB('010100000000000000008066400000000000000000'), ST_GeogFromWKB('010100000000000000008066c00000000000000000')),
        ST_Disjoint(ST_GeogFromWKB('010100000000000000008066400000000000000000'), ST_GeogFromWKB('010100000000000000008066c00000000000000000')),
        ST_Intersects(ST_GeogFromWKB('010100000000000000000000000000000000805640'), ST_GeogFromWKB('010100000000000000008056400000000000805640')),
        ST_Within(ST_GeogFromWKB('0101000000000000000000f03f0000000000000040'), ST_GeogFromWKB('0101000000000000000000f03f0000000000000040')), ST_Contains(ST_GeogFromWKB('0101000000000000000000f03f0000000000000040'), ST_GeogFromWKB('0101000000000000000000f03f0000000000000040'))"""
    qt_geography_cardinal_bearings """select round(ST_Azimuth(ST_GeogFromWKB('010100000000000000000024400000000000804640'), ST_GeogFromWKB('010100000000000000000024400000000000004740')), 12),
        round(ST_Azimuth(ST_GeogFromWKB('010100000000000000000024400000000000804640'), ST_GeogFromWKB('010100000000000000000024400000000000004640')), 12),
        round(ST_Azimuth(ST_GeogFromWKB('010100000000000000000000000000000000000000'), ST_GeogFromWKB('0101000000000000000000f03f0000000000000000')), 12),
        round(ST_Angle(ST_GeogFromWKB('010100000000000000000024400000000000004740'), ST_GeogFromWKB('010100000000000000000024400000000000804640'), ST_GeogFromWKB('010100000000000000000024400000000000004640')), 12)"""
    // Recognized default CRS case aliases retain the same calculation and write identity.
    qt_default_geography_crs_case """select
        ST_Distance(cast(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')
            as GEOGRAPHY('ogc:crs84', 'spherical')),
            cast(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')
            as GEOGRAPHY('OgC:CrS84', 'spherical'))),
        ST_AsText(cast(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')
            as GEOGRAPHY('ogc:crs84', 'spherical')))"""

    // Native spatial keys have no SQL equality/order contract; raw bytes remain accessible.
    [geometry: "ST_GeometryFromWKBTyped", geography: "ST_GeogFromWKB"].each { kind, constructor ->
        def point1 = "0101000000000000000000F03F0000000000000040"
        def point2 = "010100000000000000000008400000000000001040"
        def point3 = "010100000000000000000014400000000000001840"
        def nullableShapes = """select number as id, ${constructor}(case number % 3
            when 0 then null when 1 then '${point1}' else '${point2}' end) as shape
            from numbers("number" = "6")"""
        def encodedShapes = """select number as id, ${constructor}(case number % 3
            when 0 then null when 1 then '${point1}'
            else '00000000013FF00000000000004000000000000000' end) as shape
            from numbers("number" = "6")"""
        for (String operation : ["union", "union distinct", "intersect", "except"]) {
            test {
                sql """with shapes as (${encodedShapes})
                    select shape from shapes where id = 1 ${operation}
                    select shape from shapes where id = 2"""
                exception "GROUP BY and DISTINCT do not support spatial keys"
            }
        }
        for (String aggregate : ["array_agg", "collect_list"]) {
            test {
                sql """with shapes as (${encodedShapes})
                    select ${aggregate}(distinct shape) from shapes"""
                exception "${aggregate} DISTINCT does not support spatial arguments"
            }
        }
        test {
            sql """with shapes as (${encodedShapes}) select collect_list(distinct shape, 2) from shapes"""
            exception "collect_list DISTINCT does not support spatial arguments"
        }
        "order_qt_spatial_union_all_${kind}"("""with shapes as (${encodedShapes})
            select ST_AsText(shape), hex(ST_AsBinary(shape)) from (
                select shape from shapes where id = 1 union all
                select shape from shapes where id = 2) s""")
        "qt_spatial_plain_lists_${kind}"("""with shapes as (${encodedShapes})
            select size(array_agg(shape)), size(collect_list(shape)), size(collect_list(shape, 2))
            from shapes""")
        for (String query : [
                "select shape, count(*) from shapes group by shape",
                "select id, shape, count(*) from shapes group by id, shape",
                "select shape, count(*) from shapes group by grouping sets ((shape), ())",
                "select distinct shape from shapes",
                "select distinct id, shape from shapes",
                "select array(shape), count(*) from shapes group by array(shape)"]) {
            test {
                sql "with shapes as (${encodedShapes}) ${query}"
                exception "GROUP BY and DISTINCT do not support spatial keys"
            }
        }
        for (String expression : ["min(shape)", "max(shape)", "min(shape) over ()", "max(shape) over ()",
                "count(distinct shape)", "count(distinct id, shape)", "count(distinct shape) over ()"]) {
            test {
                sql "with shapes as (${encodedShapes}) select ${expression} from shapes"
                exception "does not support spatial arguments"
            }
        }
        // A no-FROM single row has no DISTINCT plan; use a real multi-row input.
        for (String query : ["select distinct ${constructor}(NULL) from numbers('number'='3')",
                "select count(*) from numbers('number'='3') group by ${constructor}(NULL)",
                "select min(${constructor}(NULL))", "select max(${constructor}(NULL))",
                "select count(distinct ${constructor}(NULL))"]) {
            test {
                sql query
                exception "support spatial"
            }
        }
        "order_qt_explicit_wkb_group_${kind}"("""with shapes as (${encodedShapes})
            select hex(ST_AsBinary(shape)), count(*) from shapes group by hex(ST_AsBinary(shape))""")
        "order_qt_explicit_wkb_distinct_${kind}"("""with shapes as (${encodedShapes})
            select distinct hex(ST_AsBinary(shape)) from shapes""")
        "order_qt_ordinary_key_spatial_value_${kind}"("""with shapes as (${nullableShapes})
            select id % 3, ST_AsText(any_value(shape)), count(shape) from shapes group by id % 3""")
        // The shared key dispatcher is also used by nullable and NULL-safe equijoins.
        "qt_single_join_${kind}"("""with shapes as (${nullableShapes})
            select count(*) from shapes l join shapes r on l.shape = r.shape""")
        "qt_single_null_safe_join_${kind}"("""with shapes as (${nullableShapes})
            select count(*) from shapes l join shapes r on l.shape <=> r.shape""")

        // ANY_VALUE keeps an owning spatial state without defining spatial ordering.
        "qt_single_value_aggregates_${kind}"("""with shapes as (${nullableShapes})
            select ST_AsText(any_value(shape)) in ('POINT (1 2)', 'POINT (3 4)') from shapes""")
        "qt_single_value_nulls_${kind}"("""select
            any_value(${constructor}(null)) is null from numbers("number" = "3")""")
        ["ndv", "approx_count_distinct", "histogram", "collect_set"].each { aggregate ->
            test {
                sql """with shapes as (${nullableShapes}) select ${aggregate}(shape) from shapes"""
                exception "does not support spatial arguments"
            }
        }
        ["histogram", "collect_set"].each { aggregate ->
            test {
                sql """with shapes as (${nullableShapes}) select ${aggregate}(shape, 2) from shapes"""
                exception "does not support spatial arguments"
            }
        }

        // Keep two WHEN clauses and distinct input columns so CASE survives CaseWhenToIf.
        "order_qt_case_materialization_${kind}"("""with shapes as (
            select number as id,
                ${constructor}(if(number % 2 = 0, '${point1}', '${point2}')) as g1,
                ${constructor}(if(number % 2 = 0, '${point2}', '${point3}')) as g2,
                ${constructor}(if(number % 2 = 0, '${point3}', '${point1}')) as g3,
                ${constructor}(if(number % 2 = 0, null, '${point2}')) as nullable_g
            from numbers("number" = "6"))
            select id,
                ST_AsText(case when id % 3 = 0 then g1 when id % 3 = 1 then g2 else g3 end),
                ST_AsText(case when id % 3 = 0 then g1 when id % 3 = 1 then nullable_g else g3 end),
                ST_AsText(case when id % 3 = 0 then ${constructor}('${point1}')
                    when id % 3 = 1 then ${constructor}(null) else ${constructor}('${point3}') end)
            from shapes""")

        // 300 one-row batches cross the production analytic trim frontier of 257 blocks.
        def windowShape = { id -> """${constructor}(case when (${id}) % 7 = 0 then null
            when (${id}) % 2 = 0 then '${point1}' else '${point2}' end)""" }
        "qt_window_trim_${kind}"("""select /*+ SET_VAR(batch_size=1, parallel_pipeline_task_num=1) */
            count(*), count(first_shape), count(last_shape),
            sum(if(ST_AsText(first_shape) <=> ST_AsText(expected_first), 0, 1)),
            sum(if(ST_AsText(last_shape) <=> ST_AsText(expected_last), 0, 1))
            from (select number,
                first_value(${windowShape('number')}) over
                    (order by number rows between 1 preceding and current row) as first_shape,
                last_value(${windowShape('number')}) over
                    (order by number rows between 1 preceding and current row) as last_shape,
                ${windowShape('if(number = 0, 0, number - 1)')} as expected_first,
                ${windowShape('number')} as expected_last
                from numbers("number" = "300")) w""")
    }
    qt_ordinary_min_max """select min(number), max(number), count(distinct number)
        from numbers('number'='3')"""


    // Typed Geography topology and proximity, with raw ISO WKB fixtures.
    qt_geography_complete_contains """select ST_Contains(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000')), ST_Within(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000')), ST_Contains(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000')), ST_Contains(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('010200000002000000000000000000e03f000000000000e03f000000000000f83f000000000000e03f'))"""
    qt_geography_complete_touches """select ST_Touches(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('010200000002000000000000000000f03f000000000000f0bf000000000000f03f000000000000f03f')), ST_Touches(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000')), ST_Touches(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('010300000001000000050000000000000000000040000000000000000000000000000010400000000000000000000000000000104000000000000000400000000000000040000000000000004000000000000000400000000000000000')), ST_Touches(ST_GeogFromWKB('010300000001000000050000000000000000000040000000000000000000000000000010400000000000000000000000000000104000000000000000400000000000000040000000000000004000000000000000400000000000000000'), ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'))"""
    qt_geography_topological_equals """select ST_Equals(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('01020000000300000000000000000000400000000000000000000000000000f03f000000000000000000000000000000000000000000000000')), ST_Equals(ST_GeogFromWKB('010100000000000000008066400000000000000000'), ST_GeogFromWKB('010100000000000000008066c00000000000000000')), ST_Equals(ST_GeogFromWKB('0102000000020000000000000000002440000000000080464000000000000024400000000000004740'), ST_GeogFromWKB('0102000000030000000000000000002440000000000000474000000000000024400000000000c0464000000000000024400000000000804640')), ST_Equals(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('010300000001000000050000000000000000000040000000000000004000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000000004000000000000000400000000000000040'))"""
    qt_geography_partial_overlap """select ST_Overlaps(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('010200000002000000000000000000f03f000000000000000000000000000008400000000000000000')), ST_Overlaps(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('010200000002000000000000000000f03f000000000000f0bf000000000000f03f000000000000f03f')), ST_Overlaps(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000')), ST_Overlaps(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('01030000000100000005000000000000000000f03f00000000000000000000000000000840000000000000000000000000000008400000000000000040000000000000f03f0000000000000040000000000000f03f0000000000000000')), ST_Overlaps(ST_GeogFromWKB('010300000001000000050000000000000000000000000000000000000000000000000000400000000000000000000000000000004000000000000000400000000000000000000000000000004000000000000000000000000000000000'), ST_GeogFromWKB('010300000001000000050000000000000000000040000000000000000000000000000010400000000000000000000000000000104000000000000000400000000000000040000000000000004000000000000000400000000000000000'))"""
    qt_geography_meridian_boundaries """select ST_Touches(ST_GeogFromWKB('010300000001000000050000000000000000002440000000000080464000000000000026400000000000804640000000000000264000000000000047400000000000002440000000000000474000000000000024400000000000804640'), ST_GeogFromWKB('01020000000200000000000000000024406666666666a6464000000000000024403333333333f34640')), ST_Contains(ST_GeogFromWKB('010300000001000000050000000000000000002440000000000080464000000000000026400000000000804640000000000000264000000000000047400000000000002440000000000000474000000000000024400000000000804640'), ST_GeogFromWKB('01020000000200000000000000000024406666666666a6464000000000000024403333333333f34640'))"""
    qt_geography_dwithin_zero """select ST_DWithin(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'), ST_GeogFromWKB('010200000002000000000000000000f03f000000000000f0bf000000000000f03f000000000000f03f'), 0), ST_DWithin(ST_GeogFromWKB('010100000000000000008066400000000000000000'), ST_GeogFromWKB('010100000000000000008066c00000000000000000'), 0), ST_DWithin(ST_GeogFromWKB('01020000000200000000000000000000000000000000000000000000000000f03f0000000000000000'), ST_GeogFromWKB('010200000002000000000000000000000011ea2d819997713d000000000000f03f11ea2d819997713d'), 0)"""
    qt_geography_dwithin_threshold """select ST_DWithin(a,b,ST_Distance(a,b)-1), ST_DWithin(a,b,ST_Distance(a,b)), ST_DWithin(a,b,ST_Distance(a,b)+1) FROM (SELECT ST_GeogFromWKB('010100000000000000000000000000000000000000') a, ST_GeogFromWKB('0101000000000000000000f03f0000000000000000') b) points"""
    qt_geography_new_relation_nulls """select ST_Equals(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'),CAST(NULL AS GEOGRAPHY)), ST_Overlaps(CAST(NULL AS GEOGRAPHY),ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000')), ST_DWithin(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'),ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000000000400000000000000000'),CAST(NULL AS DOUBLE))"""
    order_qt_geography_dwithin_dynamic_radius """select number, ST_DWithin(ST_GeogFromWKB('010100000000000000000000000000000000000000'), ST_GeogFromWKB('0101000000000000000000f03f0000000000000000'), number * 100000) from numbers('number'='3') order by number"""
    test {
        sql "select ST_DWithin('not_wkb','not_wkb',1)"
        exception "requires typed spatial arguments"
    }
    test {
        sql "select ST_Equals('not_wkb','not_wkb')"
        exception "requires typed spatial arguments"
    }
    test {
        sql "select ST_Overlaps('not_wkb','not_wkb')"
        exception "requires typed spatial arguments"
    }
    test {
        sql "select ST_DWithin(ST_GeogFromWKB('010100000000000000000000000000000000000000'), ST_GeogFromWKB('010100000000000000000000000000000000000000'), if(number=0, 0, -1)) from numbers('number'='2')"
        exception "finite and nonnegative"
    }
    test {
        sql "select ST_Equals(ST_GeogFromWKB('010300000000000000'), ST_GeogFromWKB('010100000000000000000000000000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_Equals(ST_GeogFromWKB('01E9030000000000000000F03F00000000000000400000000000000840'), ST_GeogFromWKB('010100000000000000000000000000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_Equals(ST_GeogFromWKB('0104000000010000000101000000000000000000F03F0000000000000040'), ST_GeogFromWKB('010100000000000000000000000000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_Overlaps(ST_GeogFromWKB('010300000000000000'), ST_GeogFromWKB('010100000000000000000000000000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_Overlaps(ST_GeogFromWKB('01E9030000000000000000F03F00000000000000400000000000000840'), ST_GeogFromWKB('010100000000000000000000000000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_Overlaps(ST_GeogFromWKB('0104000000010000000101000000000000000000F03F0000000000000040'), ST_GeogFromWKB('010100000000000000000000000000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_DWithin(ST_GeogFromWKB('010300000000000000'), ST_GeogFromWKB('010100000000000000000000000000000000000000'), 1)"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_DWithin(ST_GeogFromWKB('01E9030000000000000000F03F00000000000000400000000000000840'), ST_GeogFromWKB('010100000000000000000000000000000000000000'), 1)"
        exception "does not support this Geography WKB shape"
    }
    test {
        sql "select ST_DWithin(ST_GeogFromWKB('0104000000010000000101000000000000000000F03F0000000000000040'), ST_GeogFromWKB('010100000000000000000000000000000000000000'), 1)"
        exception "does not support this Geography WKB shape"
    }
    // Raw Geometry keeps its own identity and is not silently treated as default Geography.
    for (String function : ["ST_Equals", "ST_Overlaps", "ST_DWithin"]) {
        String arguments = "ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040'), " +
                "ST_GeometryFromWKBTyped('0101000000000000000000F03F0000000000000040')" +
                (function == "ST_DWithin" ? ", 1" : "")
        test {
            sql "select ${function}(${arguments})"
            exception "requires GEOGRAPHY(OGC:CRS84, spherical)"
        }
    }
    // Same raw-degree great circle after a rotation; nearby distinct curves stay distinct.
    qt_geography_rotated_raw_arc """select ST_Equals(a, b), ST_Contains(a, b),
        ST_Contains(b, a), ST_Within(a, b), ST_Within(b, a), ST_Equals(a, c)
        from (select ST_GeogFromWKB('010200000002000000000000000080464000000000008046c00000000000e060400000000000804640') a,
            ST_GeogFromWKB('010200000003000000000000000080464000000000008046c0000000000080564000000000000000000000000000e060400000000000804640') b,
            ST_GeogFromWKB('010200000003000000000000000080464000000000008046c000000000008056401656e79eaf03d23c0000000000e060400000000000804640') c) shapes"""
    // A retraced closed line has an empty boundary; split internal nodes remain interior.
    qt_geography_retraced_atomic_edges """select ST_Intersects(a, b), ST_Contains(a, b),
        ST_Within(b, a), ST_Distance(a, b) = 0, ST_DWithin(a, b, 0),
        ST_Touches(a, c), ST_Touches(a, d)
        from (select ST_GeogFromWKB('010200000003000000000000000000244000000000000000000000000000002440000000000000f03f00000000000024400000000000000000') a,
            ST_GeogFromWKB('0102000000030000000000000000002440000000000000d03f0000000000002440000000000000e03f0000000000002440000000000000e83f') b,
            ST_GeogFromWKB('0102000000020000000000000000002240000000000000000000000000000026400000000000000000') c,
            ST_GeogFromWKB('0102000000020000000000000000002240000000000000000000000000000024400000000000000000') d) shapes"""

    qt_geography_near_antipodal_point """select
        ST_Intersects(ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640'), ST_GeogFromWKB('010100000000000000008046400000000000000000')), ST_Intersects(ST_GeogFromWKB('010100000000000000008046400000000000000000'), ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640')),
        ST_Disjoint(ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640'), ST_GeogFromWKB('010100000000000000008046400000000000000000')), ST_Contains(ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640'), ST_GeogFromWKB('010100000000000000008046400000000000000000')), ST_Within(ST_GeogFromWKB('010100000000000000008046400000000000000000'), ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640')),
        ST_Touches(ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640'), ST_GeogFromWKB('010100000000000000008046400000000000000000')), ST_Touches(ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640'), ST_GeogFromWKB('0101000000fcffffffff7f46c000000000008046c0')),
        ST_Intersects(ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640'), ST_GeogFromWKB('010100000000000000008046401656e79eaf03d23c'))"""
    qt_geography_circle_coverage """select ST_Equals(ST_GeogFromWKB('01020000000500000000000000004065c000000000000000000000000000004ec000000000000000000000000000004e4000000000000000000000000000406540000000000000000000000000004065c00000000000000000'), ST_GeogFromWKB('010200000004000000000000000000000000000000000000000000000000005e4000000000000000000000000000005ec0000000000000000000000000000000000000000000000000')), ST_Equals(ST_GeogFromWKB('010200000004000000000000000000000000000000000000000000000000005e4000000000000000000000000000005ec0000000000000000000000000000000000000000000000000'), ST_GeogFromWKB('01020000000500000000000000004065c000000000000000000000000000004ec000000000000000000000000000004e4000000000000000000000000000406540000000000000000000000000004065c00000000000000000')),
        ST_Intersects(ST_GeogFromWKB('01020000000600000000000000000054c0000000000000000000000000008051c0000000000000000000000000008051c00000000000002440000000000080514000000000000024400000000000805140000000000000000000000000000054400000000000000000'), ST_GeogFromWKB('010200000002000000000000000000f0bf0000000000000000000000000000f03f0000000000000000')), ST_Contains(ST_GeogFromWKB('01020000000600000000000000000054c0000000000000000000000000008051c0000000000000000000000000008051c00000000000002440000000000080514000000000000024400000000000805140000000000000000000000000000054400000000000000000'), ST_GeogFromWKB('010200000002000000000000000000f0bf0000000000000000000000000000f03f0000000000000000'))"""
    // Proximity must use the same raw arc with or without an explicit midpoint.
    qt_geography_near_antipodal_proximity """select
        ST_Distance(a, p) > 0 and ST_Distance(a, p) < 0.112,
        abs(ST_Distance(a, p) - ST_Distance(b, p)) < 0.000001,
        abs(ST_Distance(p, a) - ST_Distance(p, b)) < 0.000001,
        ST_DWithin(a, p, 1), ST_DWithin(p, a, 1),
        ST_DWithin(b, p, 1), ST_DWithin(p, b, 1),
        ST_DWithin(a, p, 0), ST_DWithin(p, a, 0)
        from (select ST_GeogFromWKB('010200000002000000fcffffffff7f46c000000000008046c0ffffffffffdf60400000000000804640') a,
            ST_GeogFromWKB('010200000003000000fcffffffff7f46c000000000008046c000000000008046400000000000000000ffffffffffdf60400000000000804640') b,
            ST_GeogFromWKB('010100000000000000008046408dedb5a0f7c6b03e') p) shapes"""

    // Equivalent raw ring subdivision must preserve interior and boundary predicates.
    order_qt_geography_stable_polygon_interior """select
        ST_Contains(a, p), ST_Contains(b, p), ST_Within(p, a), ST_Within(p, b),
        ST_Intersects(a, p), ST_Intersects(p, a), ST_Disjoint(a, p), ST_Disjoint(p, a),
        ST_Touches(a, p), ST_Contains(a, q), ST_Contains(a, m), ST_Touches(a, m),
        ST_Contains(a, l), ST_Contains(b, l), ST_Equals(a, b), ST_Overlaps(a, b),
        ST_Contains(t, s), ST_Contains(t, p)
        from (select ST_GeogFromWKB('01030000000100000004000000fcffffffff7f46c000000000008046c0ffffffffffdf6040000000000080464000000000008046400000000000804640fcffffffff7f46c000000000008046c0') a,
            ST_GeogFromWKB('01030000000100000005000000fcffffffff7f46c000000000008046c000000000008046400000000000000000ffffffffffdf6040000000000080464000000000008046400000000000804640fcffffffff7f46c000000000008046c0') b,
            ST_GeogFromWKB('010100000000000000008046408dedb5a0f7c6b03e') p,
            ST_GeogFromWKB('010100000000000000008046408dedb5a0f7c6b0be') q,
            ST_GeogFromWKB('010100000000000000008046400000000000000000') m,
            ST_GeogFromWKB('01020000000200000000000000008046408dedb5a0f7c6b03e00000000008046408dedb5a0f7c6c03e') l,
            ST_GeogFromWKB('01030000000100000004000000fcffffffff7f46c000000000008046c0ffffffffffdf6040000000000080464000000000008046408dedb5a0f7c6b03efcffffffff7f46c000000000008046c0') t,
            ST_GeogFromWKB('010100000000000000008046408dedb5a0f7c6a03e') s) shapes"""

    // Area uses the same minor arcs whether the near-antipodal edge is subdivided or reversed.
    order_qt_geography_stable_polygon_area """select
        abs(ST_Area_Square_Meters(a) / ST_Area_Square_Meters(b) - 1) < 1e-12,
        abs(ST_Area_Square_Km(a) / ST_Area_Square_Km(b) - 1) < 1e-12,
        abs(ST_Area_Square_Meters(a) / ST_Area_Square_Meters(c) - 1) < 1e-12,
        abs(ST_Area_Square_Meters(a) / (ST_Area_Square_Km(a) * 1000000) - 1) < 1e-12
        from (select ST_GeogFromWKB('01030000000100000004000000fcffffffff7f46c000000000008046c0ffffffffffdf6040000000000080464000000000008046400000000000804640fcffffffff7f46c000000000008046c0') a,
            ST_GeogFromWKB('01030000000100000005000000fcffffffff7f46c000000000008046c000000000008046400000000000000000ffffffffffdf6040000000000080464000000000008046400000000000804640fcffffffff7f46c000000000008046c0') b,
            ST_GeogFromWKB('01030000000100000004000000fcffffffff7f46c000000000008046c000000000008046400000000000804640ffffffffffdf60400000000000804640fcffffffff7f46c000000000008046c0') c) shapes"""
    order_qt_geography_near_antipodal_bearing """select ST_Azimuth(a, b) = 0,
        ST_Azimuth(b, a) = 0, abs(ST_Azimuth(a, c) - pi()) < 1e-14,
        ST_Angle(n, a, b) = 0, abs(ST_Angle(n, a, c) - pi()) < 1e-14
        from (select ST_GeogFromWKB('010100000000000000000000000000000000804640') a,
            ST_GeogFromWKB('01010000000000000000806640ffffffffff7f46c0') b,
            ST_GeogFromWKB('010100000000000000008066c001000000008046c0') c,
            ST_GeogFromWKB('010100000000000000000000000000000000004740') n) shapes"""

    // Distinct raw endpoints can collide after degrees-to-radians conversion.
    order_qt_geography_rounded_endpoint_identity """select
        ST_Distance(p, l) < 1, ST_Distance(l, p) < 1,
        ST_Distance(p, l) = ST_Distance(l, p), ST_DWithin(p, l, 1), ST_DWithin(l, p, 1),
        ST_Intersects(p, l), ST_Intersects(l, p), ST_Disjoint(p, l),
        ST_Touches(p, l), ST_Contains(l, p), ST_Within(p, l), ST_Touches(e, l), ST_Equals(p, e)
        from (select ST_GeogFromWKB('010100000000000000000000000000000000004d40') p,
            ST_GeogFromWKB('01020000000200000000000000000000000100000000004d4000000000008066400000000000004dc0') l,
            ST_GeogFromWKB('010100000000000000000000000100000000004d40') e) shapes"""
    // Admit the same tiny hole with an original, subdivided or reversed near-antipodal shell.
    order_qt_geography_stable_hole_admission """select
        ST_Intersects(a, p), ST_Intersects(p, a), ST_Intersects(b, p), ST_Intersects(c, p),
        ST_Contains(a, q), ST_Contains(b, q), ST_Contains(c, q), ST_Touches(a, v),
        ST_Distance(a, p) > 0, ST_Distance(p, a) > 0, ST_DWithin(a, p, 1),
        abs(ST_Area_Square_Meters(a) / ST_Area_Square_Meters(b) - 1) < 1e-12,
        abs(ST_Area_Square_Km(a) / ST_Area_Square_Km(c) - 1) < 1e-12
        from (select ST_GeogFromWKB('01030000000200000004000000fcffffffff7f46c000000000008046c0ffffffffffdf6040000000000080464000000000008046400000000000804640fcffffffff7f46c000000000008046c0040000006b4029ffff7f4640314514eef032ae3e95bfd60000804640314514eef032ae3e000000000080464082b861ca7674b23e6b4029ffff7f4640314514eef032ae3e') a,
            ST_GeogFromWKB('01030000000200000005000000fcffffffff7f46c000000000008046c000000000008046400000000000000000ffffffffffdf6040000000000080464000000000008046400000000000804640fcffffffff7f46c000000000008046c0040000006b4029ffff7f4640314514eef032ae3e95bfd60000804640314514eef032ae3e000000000080464082b861ca7674b23e6b4029ffff7f4640314514eef032ae3e') b,
            ST_GeogFromWKB('01030000000200000004000000fcffffffff7f46c000000000008046c000000000008046400000000000804640ffffffffffdf60400000000000804640fcffffffff7f46c000000000008046c0040000006b4029ffff7f4640314514eef032ae3e95bfd60000804640314514eef032ae3e000000000080464082b861ca7674b23e6b4029ffff7f4640314514eef032ae3e') c,
            ST_GeogFromWKB('010100000000000000008046408dedb5a0f7c6b03e') p,
            ST_GeogFromWKB('010100000000000000008046408dedb5a0f7c6a03e') q,
            ST_GeogFromWKB('0101000000000000000080464082b861ca7674b23e') v) shapes"""

    // Preserve a valid raw minor arc even when endpoint conversion yields antipodal vectors.
    order_qt_geography_stable_line_admission """select
        ST_Distance(a, p) = 0, ST_Distance(p, a) = 0, ST_Distance(c, p) = 0,
        ST_DWithin(a, p, 0), ST_DWithin(p, a, 0), ST_DWithin(c, p, 1),
        abs(ST_Length(a) - ST_Length(b)) < 0.000001,
        abs(ST_Length(c) - ST_Length(b)) < 0.000001,
        ST_Contains(a, p), ST_Within(p, a), ST_Touches(a, p), ST_Equals(a, b),
        ST_Area_Square_Meters(a) = 0, ST_Area_Square_Km(c) = 0
        from (select ST_GeogFromWKB('0102000000020000000000000000003e400000000000004d400000000000c062c00100000000004dc0') a,
            ST_GeogFromWKB('0102000000030000000000000000003e400000000000004d400000000000003e4000000000008056c00000000000c062c00100000000004dc0') b,
            ST_GeogFromWKB('0102000000020000000000000000c062c00100000000004dc00000000000003e400000000000004d40') c,
            ST_GeogFromWKB('0101000000000000000000000000000000008056c0') p) shapes"""
    // Distinct raw extents stay unequal even when their endpoints round to one S2Point.
    order_qt_geography_unequal_rounded_line_extents """select id,
        ST_Equals(a, b), ST_Equals(b, a), ST_Intersects(a, p), ST_Intersects(b, p),
        ST_Equals(a, c), ST_Equals(c, a)
        from (
            select 1 id,
                ST_GeogFromWKB('01020000000200000000000000000000000000000000004d4000000000000000000000000000804d40') a,
                ST_GeogFromWKB('01020000000200000000000000000000000100000000004d4000000000000000000000000000804d40') b,
                ST_GeogFromWKB('01020000000300000000000000000000000000000000004d4000000000000000000100000000004d4000000000000000000000000000804d40') c,
                ST_GeogFromWKB('010100000000000000000000000000000000004d40') p
            union all select 2,
                ST_GeogFromWKB('01020000000200000000000000000000000000000000004d40000000000000f03f0000000000804d40'),
                ST_GeogFromWKB('01020000000200000000000000000000000100000000004d40000000000000f03f0000000000804d40'),
                ST_GeogFromWKB('010200000002000000000000000000f03f0000000000804d4000000000000000000000000000004d40'),
                ST_GeogFromWKB('010100000000000000000000000000000000004d40')
            union all select 3,
                ST_GeogFromWKB('0102000000020000000000000000004d4000000000000000000000000000804d400000000000000000'),
                ST_GeogFromWKB('0102000000020000000100000000004d4000000000000000000000000000804d400000000000000000'),
                ST_GeogFromWKB('0102000000030000000000000000004d4000000000000000000100000000004d4000000000000000000000000000804d400000000000000000'),
                ST_GeogFromWKB('01010000000000000000004d400000000000000000')
        ) shapes order by id"""
    // Directional coverage and positive overlap must retain raw edges that round to one S2 point.
    order_qt_geography_raw_line_coverage """select id,
        ST_Contains(a, b), ST_Contains(b, a), ST_Within(a, b), ST_Within(b, a),
        ST_Overlaps(a, b), ST_Overlaps(b, a)
        from (select ST_GeogFromWKB('01020000000200000000000000000000000000000000004d4000000000000000000000000000804d40') a) left_shape
        cross join (
            select 1 id, ST_GeogFromWKB('01020000000200000000000000000000000100000000004d4000000000000000000000000000804d40') b
            union all select 2, ST_GeogFromWKB('01020000000200000000000000000000000100000000004d4000000000000000000000000000004e40')
            union all select 3, ST_GeogFromWKB('01020000000200000000000000000000000000000000804c4000000000000000000100000000004d40')
            union all select 4, ST_GeogFromWKB('01020000000300000000000000000000000000000000804d4000000000000000000100000000004d4000000000000000000000000000004d40')
        ) right_shape order by id"""
    // Raw polygon boundaries remain unequal even when S2 cannot distinguish the regions.
    order_qt_geography_polygon_raw_identity """select ST_Equals(a, b), ST_Equals(b, a),
        ST_Contains(a, c), ST_Within(c, a), ST_Equals(d, e),
        ST_Contains(d, e), ST_Contains(e, d)
        from (select ST_GeogFromWKB('0103000000010000000400000000000000000000000000000000004d40000000000000f03f0000000000804d40000000000000f0bf0000000000804d4000000000000000000000000000004d40') a,
            ST_GeogFromWKB('0103000000010000000400000000000000000000000100000000004d40000000000000f03f0000000000804d40000000000000f0bf0000000000804d4000000000000000000100000000004d40') b,
            ST_GeogFromWKB('0103000000010000000400000000000000000000000000000000404d40000000000000d03f6666666666664d40000000000000d0bf6666666666664d4000000000000000000000000000404d40') c,
            ST_GeogFromWKB('0103000000010000000400000000000000000000000000000000004d4000000000000000000000000000804d40000000000000f03f0000000000804d4000000000000000000000000000004d40') d,
            ST_GeogFromWKB('0103000000010000000500000000000000000000000000000000004d4000000000000000000100000000004d4000000000000000000000000000804d40000000000000f03f0000000000804d4000000000000000000000000000004d40') e) shapes"""
    for (String function : ["ST_Contains", "ST_Within", "ST_Overlaps"]) {
        for (boolean reverse : [false, true]) {
            String left = reverse ? '0103000000010000000400000000000000000000000100000000004d40000000000000f03f0000000000804d40000000000000f0bf0000000000804d4000000000000000000100000000004d40' : '0103000000010000000400000000000000000000000000000000004d40000000000000f03f0000000000804d40000000000000f0bf0000000000804d4000000000000000000000000000004d40'
            String right = reverse ? '0103000000010000000400000000000000000000000000000000004d40000000000000f03f0000000000804d40000000000000f0bf0000000000804d4000000000000000000000000000004d40' : '0103000000010000000400000000000000000000000100000000004d40000000000000f03f0000000000804d40000000000000f0bf0000000000804d4000000000000000000100000000004d40'
            test {
                sql "select ${function}(ST_GeogFromWKB('${left}'), ST_GeogFromWKB('${right}'))"
                exception "distinct raw Polygon boundaries with the same spherical representation"
            }
        }
    }
    order_qt_geography_raw_line_contacts """select id, ST_Touches(a, b), ST_Touches(b, a)
        from (
            select 1 id, ST_GeogFromWKB('01020000000200000000000000000000000000000000804c4000000000000000000000000000004d40') a,
                ST_GeogFromWKB('01020000000200000000000000000000000100000000004d4000000000000000000000000000804d40') b
            union all select 2, ST_GeogFromWKB('01020000000200000000000000000000000000000000804c4000000000000000000000000000004d40'), ST_GeogFromWKB('01020000000200000000000000000000000000000000004d4000000000000000000000000000804d40')
            union all select 3, ST_GeogFromWKB('01020000000200000000000000000000000000000000804c4000000000000000000000000000004d40'), ST_GeogFromWKB('010200000003000000000000000000f0bf0000000000004d4000000000000000000000000000004d40000000000000f03f0000000000004d40')
            union all select 4, ST_GeogFromWKB('01020000000200000000000000008066400000000000804c4000000000008066400000000000004d40'), ST_GeogFromWKB('01020000000200000000000000008066c00000000000004d4000000000008066c00000000000804d40')
            union all select 5, ST_GeogFromWKB('0102000000020000000000000000000000000000000040564000000000000000000000000000805640'), ST_GeogFromWKB('0102000000020000000000000000805640000000000080564000000000008056400000000000405640')
            union all select 6, ST_GeogFromWKB('01020000000200000000000000000000000000000000804c4000000000000000000100000000004d40'), ST_GeogFromWKB('01020000000200000000000000000000000000000000004d4000000000000000000000000000804d40')
        ) shapes order by id"""
    // Exactly antipodal raw endpoints have no unique spherical edge; storage is still valid.
    test {
        sql "select ST_Length(ST_GeogFromWKB('0102000000020000000000000000000000000000000000000000000000008066400000000000000000'))"
        exception "does not support this Geography WKB shape"
    }
}
