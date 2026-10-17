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
    // The new typed functions continue to reject Z/M and embedded SRID metadata.
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
    test {
        sql "select ST_Contains(ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'), ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'))"
        exception "does not support this Geography shape pair"
    }
    test {
        sql "select ST_Touches(ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'), ST_GeogFromWKB('0102000000020000000000000000406540000000000000000000000000004065c00000000000000000'))"
        exception "does not support this Geography shape pair"
    }
    // Recognized default CRS case aliases retain the same calculation and write identity.
    qt_default_geography_crs_case """select
        ST_Distance(cast(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')
            as GEOGRAPHY('ogc:crs84', 'spherical')),
            cast(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')
            as GEOGRAPHY('OgC:CrS84', 'spherical'))),
        ST_AsText(cast(ST_GeogFromWKB('0101000000000000000000F03F0000000000000040')
            as GEOGRAPHY('ogc:crs84', 'spherical')))"""

    // Exercise one spatial key: two keys take a different serialized-key dispatch path.
    [geometry: "ST_GeometryFromWKBTyped", geography: "ST_GeogFromWKB"].each { kind, constructor ->
        def point1 = "0101000000000000000000F03F0000000000000040"
        def point2 = "010100000000000000000008400000000000001040"
        def point3 = "010100000000000000000014400000000000001840"
        def nullableShapes = """select number as id, ${constructor}(case number % 3
            when 0 then null when 1 then '${point1}' else '${point2}' end) as shape
            from numbers("number" = "6")"""
        "order_qt_single_group_${kind}"("""with shapes as (${nullableShapes})
            select ST_AsText(shape), count(*) from shapes group by shape""")
        "order_qt_single_distinct_${kind}"("""with shapes as (${nullableShapes})
            select ST_AsText(shape) from (select distinct shape from shapes) d""")
        // The shared key dispatcher is also used by nullable and NULL-safe equijoins.
        "qt_single_join_${kind}"("""with shapes as (${nullableShapes})
            select count(*) from shapes l join shapes r on l.shape = r.shape""")
        "qt_single_null_safe_join_${kind}"("""with shapes as (${nullableShapes})
            select count(*) from shapes l join shapes r on l.shape <=> r.shape""")

        // MIN/MAX compare stored WKB, while ANY_VALUE must keep an owning spatial state.
        "qt_single_value_aggregates_${kind}"("""with shapes as (${nullableShapes})
            select ST_AsText(min(shape)), ST_AsText(max(shape)),
                ST_AsText(any_value(shape)) in ('POINT (1 2)', 'POINT (3 4)') from shapes""")
        "qt_single_value_nulls_${kind}"("""select
            min(${constructor}(null)) is null, max(${constructor}(null)) is null,
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

}
