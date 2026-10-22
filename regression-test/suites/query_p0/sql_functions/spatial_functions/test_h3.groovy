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


suite("test_h3") {
    for (def foldOnBe in [false, true]) {
        sql "set enable_fold_constant_by_be = ${foldOnBe}"
        qt_fold_cell """select h3_lat_lng_to_cell(55.71290588,37.79506683,15),
            h3_lat_lng_to_cell(cast('55.71290588' as double),
                cast('37.79506683' as double),abs(-15))"""
        qt_fold_coordinates """select
            array_map(x -> round(x,9),h3_cell_to_lat_lng(644325524701193974)),
            array_map(p -> array_map(x -> round(x,9),p),
                h3_cell_to_boundary(644325524701193974))"""
        qt_fold_nested """select h3_lat_lng_to_cell(
            h3_cell_to_lat_lng(644325524701193974)[1],
            h3_cell_to_lat_lng(644325524701193974)[2],15),
            size(h3_cell_to_lat_lng(h3_lat_lng_to_cell(0,0,0))),
            size(h3_cell_to_boundary(h3_lat_lng_to_cell(0,0,15)))"""
        qt_fold_null """select h3_lat_lng_to_cell(cast(null as double),0,9),
            h3_lat_lng_to_cell(0,cast(null as double),9),
            h3_lat_lng_to_cell(0,0,cast(null as int)),
            h3_cell_to_lat_lng(cast(null as bigint)),
            h3_cell_to_boundary(cast(null as bigint)),
            h3_cell_to_lat_lng(h3_lat_lng_to_cell(91,0,9))"""
        qt_fold_invalid """select h3_lat_lng_to_cell(-91,0,9),
            h3_lat_lng_to_cell(0,-181,9),h3_lat_lng_to_cell(0,0,16),
            h3_cell_to_lat_lng(12345),h3_cell_to_boundary(-1),
            h3_cell_to_boundary(1509016653156329206)"""
        qt_fold_endpoints """select h3_lat_lng_to_cell(90,180,0),
            h3_lat_lng_to_cell(-90,-180,15),h3_lat_lng_to_cell(0,180,15),
            h3_lat_lng_to_cell(0,-180,15)"""
    }
    sql "set enable_fold_constant_by_be = false"
    explain {
        sql """select h3_lat_lng_to_cell(55.71290588,37.79506683,15),
            h3_cell_to_lat_lng(644325524701193974),
            h3_cell_to_boundary(644325524701193974)"""
        contains "h3_lat_lng_to_cell(55.71290588, 37.79506683, 15)"
        contains "h3_cell_to_lat_lng(644325524701193974)"
        contains "h3_cell_to_boundary(644325524701193974)"
    }
    sql "set enable_fold_constant_by_be = true"
    explain {
        sql "select h3_lat_lng_to_cell(55.71290588,37.79506683,15)"
        contains "644325524701193974"
        notContains "h3_lat_lng_to_cell(55.71290588, 37.79506683, 15)"
    }
    sql "set enable_fold_constant_by_be = false"
    qt_cell "select h3_lat_lng_to_cell(55.71290588, 37.79506683, 15)"
    qt_center """select array_map(x -> round(x, 9),
        h3_cell_to_lat_lng(644325524701193974))"""
    qt_boundary """select array_map(p -> array_map(x -> round(x, 9), p),
        h3_cell_to_boundary(644325524701193974))"""
    qt_invalid """select h3_lat_lng_to_cell(91, 0, 9),
        h3_lat_lng_to_cell(0, 181, 9), h3_lat_lng_to_cell(0, 0, -1),
        h3_lat_lng_to_cell(0, 0, 16), h3_cell_to_lat_lng(0),
        h3_cell_to_lat_lng(-1), h3_cell_to_boundary(12345)"""
    qt_null """select h3_lat_lng_to_cell(null, 0, 9),
        h3_lat_lng_to_cell(0, null, 9), h3_lat_lng_to_cell(0, 0, null),
        h3_cell_to_lat_lng(null), h3_cell_to_boundary(null)"""
    qt_non_cell """select h3_cell_to_lat_lng(1509016653156329206),
        h3_cell_to_boundary(1509016653156329206),
        h3_cell_to_lat_lng(2517822969687320233),
        h3_cell_to_boundary(2517822969687320233),
        h3_cell_to_lat_lng(9223372036854775807)"""
    qt_pentagons """select size(h3_cell_to_boundary(576636674163867647)),
        size(h3_cell_to_boundary(581109487465660415)),
        array_map(p -> array_map(x -> round(x,9),p),
            h3_cell_to_boundary(581109487465660415))"""
    sql "drop table if exists test_h3_inputs"
    sql """create table test_h3_inputs (
        id int, lat double, lng double, res int, cell bigint
    ) distributed by hash(id) buckets 1 properties("replication_num"="1")"""
    sql """insert into test_h3_inputs values
        (1,55.71290588,37.79506683,15,644325524701193974),
        (2,91,0,9,0), (3,null,0,9,null), (4,0,null,9,-1),
        (5,0,0,null,12345), (6,90,180,0,577023702256844799),
        (7,-90,-180,15,576495936675512319)"""
    order_qt_rows """select id, h3_lat_lng_to_cell(lat,lng,res),
        array_map(x -> round(x, 9), h3_cell_to_lat_lng(cell)),
        array_map(p -> array_map(x -> round(x, 9),p),h3_cell_to_boundary(cell))
        from test_h3_inputs"""
    order_qt_mixed """select id, h3_lat_lng_to_cell(lat,0,9),
        h3_lat_lng_to_cell(0,lng,res) from test_h3_inputs"""
    order_qt_array_access """select id,size(h3_cell_to_lat_lng(cell)),
        round(h3_cell_to_lat_lng(cell)[1],9),round(h3_cell_to_lat_lng(cell)[2],9),
        h3_cell_to_lat_lng(cell)[3],
        round(h3_cell_to_boundary(cell)[1][1],9),
        round(h3_cell_to_boundary(cell)[1][2],9)
        from test_h3_inputs"""
    order_qt_null_columns """select id,
        h3_lat_lng_to_cell(if(id>0,null,lat),lng,res),
        h3_cell_to_lat_lng(if(id>0,null,cell)),
        h3_cell_to_boundary(if(id>0,null,cell)) from test_h3_inputs"""
    order_qt_empty """select h3_lat_lng_to_cell(lat,lng,res),
        h3_cell_to_lat_lng(cell),h3_cell_to_boundary(cell)
        from test_h3_inputs where id < 0"""
    qt_roundtrip """select h3_lat_lng_to_cell(
        h3_cell_to_lat_lng(644325524701193974)[1],
        h3_cell_to_lat_lng(644325524701193974)[2],15)"""
    qt_many_rows """select count(*), sum(h3_lat_lng_to_cell(0,0,cast(number % 16 as int)) is null),
        sum(size(h3_cell_to_boundary(if(number % 2=0,644325524701193974,0)))),
        sum(size(h3_cell_to_lat_lng(if(number % 2=0,644325524701193974,null))))
        from numbers('number'='10000')"""

    sql "drop table if exists test_h3_locations"
    sql """create table test_h3_locations (
        id int not null, lat double not null, lng double not null, res int not null,
        cell bigint not null
    ) distributed by hash(id) buckets 3 properties("replication_num"="1")"""
    sql """insert into test_h3_locations
        select cast(number as int),
            case number % 8 when 0 then 0 when 1 then 55.71290588
                when 2 then 90 when 3 then -90 when 4 then 0
                when 5 then 0 when 6 then 37.7749 else -33.8688 end,
            case number % 8 when 0 then 0 when 1 then 37.79506683
                when 2 then 180 when 3 then -180 when 4 then 180
                when 5 then -180 when 6 then -122.4194 else 151.2093 end,
            cast(floor(number / 8) as int), 0
        from numbers('number'='128')"""
    sql """insert overwrite table test_h3_locations
        select id,lat,lng,res,h3_lat_lng_to_cell(lat,lng,res) from test_h3_locations"""
    order_qt_all_resolutions """select id,res,cell,
        size(h3_cell_to_lat_lng(cell)),size(h3_cell_to_boundary(cell)),
        h3_lat_lng_to_cell(h3_cell_to_lat_lng(cell)[1],
            h3_cell_to_lat_lng(cell)[2],res)=cell as center_roundtrip
        from test_h3_locations"""
    order_qt_location_filter """select res,count(*)
        from test_h3_locations where h3_cell_to_lat_lng(cell) is not null
            and size(h3_cell_to_boundary(cell)) between 5 and 10 group by res"""
    order_qt_cell_grouping """select res,h3_lat_lng_to_cell(lat,lng,res),count(*)
        from test_h3_locations where id % 8 in (4,5)
        group by res,h3_lat_lng_to_cell(lat,lng,res)"""
    order_qt_constant_with_columns """select id,
        h3_lat_lng_to_cell(lat,0,9),h3_lat_lng_to_cell(0,lng,9),
        h3_lat_lng_to_cell(0,0,res) from test_h3_locations where res in (0,15)"""

    sql "drop table if exists test_h3_arrays"
    sql """create table test_h3_arrays (
        id int, center array<double>, boundary array<array<double>>
    ) distributed by hash(id) buckets 1 properties("replication_num"="1")"""
    sql """insert into test_h3_arrays select id,h3_cell_to_lat_lng(cell),
        h3_cell_to_boundary(cell) from test_h3_inputs"""
    order_qt_stored_arrays """select id,size(center),size(boundary),
        array_map(x -> round(x,9),center),
        array_map(p -> array_map(x -> round(x,9),p),boundary)
        from test_h3_arrays"""
    order_qt_exploded_boundary """select id,round(vertex[1],9),round(vertex[2],9)
        from test_h3_arrays lateral view explode(boundary) points as vertex"""
    order_qt_array_composition """select id,
        size(array_filter(p -> p[1] >= 0,h3_cell_to_boundary(cell))),
        array_map(p -> size(p),h3_cell_to_boundary(cell)),
        h3_lat_lng_to_cell(center[1],center[2],15)
        from test_h3_inputs join test_h3_arrays using(id)"""

    sql "drop table if exists test_h3_batches"
    sql """create table test_h3_batches (
        id int, lat double, lng double, res int, cell bigint
    ) distributed by hash(id) buckets 3 properties("replication_num"="1")"""
    sql """insert into test_h3_batches
        select cast(n.number*7+i.id as int),i.lat,i.lng,i.res,i.cell
        from test_h3_inputs i cross join numbers('number'='5000') n"""
    order_qt_batch_nulls """select id % 7,count(*),
        count(h3_lat_lng_to_cell(lat,lng,res)),
        count(h3_cell_to_lat_lng(cell)),count(h3_cell_to_boundary(cell)),
        sum(size(h3_cell_to_lat_lng(cell))),sum(size(h3_cell_to_boundary(cell)))
        from test_h3_batches group by id % 7"""
    qt_batch_filter """select count(*) from test_h3_batches
        where h3_lat_lng_to_cell(lat,lng,res) is not null
            and h3_cell_to_boundary(cell) is not null"""
    order_qt_batch_cell_grouping """select h3_lat_lng_to_cell(lat,lng,res),count(*)
        from test_h3_batches group by h3_lat_lng_to_cell(lat,lng,res)"""
}
