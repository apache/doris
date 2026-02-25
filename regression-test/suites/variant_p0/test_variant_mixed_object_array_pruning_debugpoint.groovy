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

suite("test_variant_mixed_object_array_pruning_debugpoint", "p0, nonConcurrent") {
    def tableName = "dp_variant_mixed_object_array_pruning"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_variant_flatten_nested=false"

    try {
        try {
            GetDebugPoint().enableDebugPointForAllBEs(
                    "dp_variant_access_path_pruning_smoke",
                    [execute: 1, timeout: 3]
            )
            GetDebugPoint().disableDebugPointForAllBEs("dp_variant_access_path_pruning_smoke")
        } catch (Throwable t) {
            logger.info("skip test_variant_mixed_object_array_pruning_debugpoint, reason: ${t.toString()}")
            return
        }

        def enableAccessPathCheck = { String require, String forbid ->
            GetDebugPoint().enableDebugPointForAllBEs(
                    "VariantColumnReader.build_read_plan.access_paths",
                    [require: require, forbid: forbid, execute: 1, timeout: 60]
            )
        }

        sql "drop table if exists ${tableName}"
        sql """
            create table ${tableName} (
              id int null,
              v variant<properties("variant_max_subcolumns_count" = "0")> null
            )
            duplicate key(id)
            distributed by hash(id) buckets 1
            properties("replication_num"="1", "storage_format"="V2")
        """

        sql """insert into ${tableName} values
            (1, '{"obj":{"a":1,"b":"s","deep":{"a":{"b":100}}},"arr":[{"x":10,"y":20,"z":{"w":1}},{"x":11,"unused":1}],"nums":[1,2,3],"x":[100,101],"outer":{"k":1},"unused_root":99}'),
            (2, '{"obj":{"a":2,"deep":{"a":{"b":101},"c":1}},"arr":[{"x":10,"y":null}],"nums":[4,5],"outer":{"k":2}}'),
            (3, '[{"x":10,"y":200,"deep":{"p":{"q":1}}},{"x":12,"y":201}]'),
            (4, '{"arr":[{"x":13,"y":23}],"obj":{"a":3},"mixed":[{"m":1},2,{"m":3}],"outer":{"k":1}}'),
            (5, '[]'),
            (6, '{"arr":[{"x":1,"y":999,"inner":[{"k":"a","val":100,"unused":0},{"k":"b","val":101}]},{"x":2,"inner":[{"k":"c","val":102}],"unused2":5}],"obj":{"a":10},"outer":{"k":6},"unused_root":123}'),
            (7, '{"nest":{"l1":{"l2":{"l3":{"l4":123}}}},"arr":[{"x":7,"y":8}],"obj":{"a":7},"outer":{"k":7},"unused_root":777}')
        """
        sql "sync"

        GetDebugPoint().clearDebugPointsForAllBEs()

        enableAccessPathCheck("arr.x", "arr.y,obj.a,obj.deep.a.b,outer.k,nums,x,unused_root")
        test {
            sql """
                select id, cast(e['x'] as int) as x
                from ${tableName}
                lateral view explode(v['arr']) tmp as e
                where cast(e['x'] as int) = 10
                order by id, x
            """
            result([[1, 10], [2, 10]])
        }

        enableAccessPathCheck("arr.x,arr.y", "obj.a,obj.deep.a.b,outer.k,nums,x,unused_root")
        test {
            sql """
                select id, cast(e['x'] as int) as x, cast(e['y'] as int) as y
                from ${tableName}
                lateral view explode(v['arr']) tmp as e
                where id in (1, 2) and cast(e['x'] as int) >= 10
                order by id, x
            """
            result([[1, 10, 20], [1, 11, null], [2, 10, null]])
        }

        enableAccessPathCheck("x", "arr.x,arr.y,obj.a,obj.deep.a.b,outer.k,nums,unused_root")
        test {
            sql """
                select id, cast(vv as int) as x
                from ${tableName}
                lateral view explode(v['x']) tmp as vv
                where id = 3 and cast(vv as int) >= 11
                order by id, x
            """
            result([[3, 12]])
        }

        enableAccessPathCheck("arr.inner.val", "arr.x,arr.y,obj.a,obj.deep.a.b,outer.k,nums,x,unused_root,nest.l1.l2.l3.l4")
        test {
            sql """
                select id, cast(vv as string) as vv_str
                from ${tableName}
                lateral view explode(v['arr.inner.val']) t as vv
                where id = 6
                order by id, vv_str
            """
            result([[6, "[100, 101]"], [6, "[102]"]])
        }

        // enableAccessPathCheck("arr.x,x", "arr.y,obj.a,obj.deep.a.b,outer.k,nums,unused_root")
        // test {
        //     sql """
        //         select id, cast(e['x'] as int) as ex, cast(vv['x'] as int) as xv
        //         from ${tableName}
        //         lateral view explode(v['arr']) tmp as e
        //         lateral view explode(v) tmp2 as vv
        //         where id = 1 and cast(e['x'] as int) = 10 and cast(vv['x'] as int) >= 101
        //         order by id, ex, xv
        //     """
        //     result([[1, 10, 101]])
        // }
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
