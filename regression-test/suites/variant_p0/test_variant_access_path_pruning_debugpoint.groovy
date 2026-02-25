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

suite("test_variant_access_path_pruning_debugpoint", "p0, nonConcurrent") {
    def tableName = "dp_variant_access_path_pruning"

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
            logger.info("skip test_variant_access_path_pruning_debugpoint, reason: ${t.toString()}")
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
            (1, '{"arr":[{"x":1,"y":2,"z":{"w":5},"deep":{"a":{"b":1000},"c":{"d":2000}}},{"x":10,"y":20,"deep":{"a":{"b":1001},"c":{"d":2001}}}],"outer":{"k":1},"unused":{"u":99}}'),
            (2, '{"arr":[{"x":3,"y":4,"z":{"w":6}}],"outer":{"k":2},"unused2":1}'),
            (3, '{"arr":[{"x":10,"y":null,"deep":{"a":{"b":1002},"c":{"d":2002}}}],"outer":{"k":1}}'),
            (4, '{"level1":{"level2":[{"a":{"b":100},"c":200},{"a":{"b":101},"c":201}]},"outer":{"k":3},"other":"x"}'),
            (5, '{"arr":[{"x":7,"y":8,"deep":{"a":{"b":9999},"c":{"d":8888}},"unused3":{"p":1}},{"x":9,"y":10,"deep":{"a":{"b":7777},"c":{"d":6666}},"unused4":"x"}],"outer":{"k":4},"unused":{"u":1}}'),
            (6, '[{"x":1,"y":2,"unused":"a"},{"x":10,"y":20,"unused":"b"}]')
        """
        sql "sync"

        GetDebugPoint().clearDebugPointsForAllBEs()

        enableAccessPathCheck("arr.x", "arr.y,unused,arr.deep.a.b")
        test {
            sql """
                select id, cast(e['x'] as int) as x
                from dp_variant_access_path_pruning 
                lateral view explode(v['arr']) tmp as e
                where cast(e['x'] as int) = 10
                order by id, x
            """
            result([[1, 10], [3, 10]])
        }

        enableAccessPathCheck("arr.x,arr.y", "unused,arr.deep.a.b")
        test {
            sql """
                select id, cast(e['x'] as int) as x, cast(e['y'] as int) as y
                from ${tableName}
                lateral view explode(v['arr']) tmp as e
                where cast(e['x'] as int) = 10
                order by id, x
            """
            result([[1, 10, 20], [3, 10, null]])
        }

        enableAccessPathCheck("arr.x,outer.k", "unused,arr.y")
        test {
            sql """
                select id, cast(e['x'] as int) as x
                from ${tableName}
                lateral view explode(v['arr']) tmp as e
                where cast(v['outer']['k'] as int) = 1 and cast(e['x'] as int) = 10
                order by id, x
            """
            result([[1, 10], [3, 10]])
        }

        enableAccessPathCheck("arr.deep.a.b", "arr.deep.c.d,arr.x,arr.y,unused")
        test {
            sql """
                select id, cast(e['deep']['a']['b'] as int) as b
                from ${tableName}
                lateral view explode(v['arr']) tmp as e
                where cast(e['deep']['a']['b'] as int) >= 7000
                order by id, b
            """
            result([[5, 7777], [5, 9999]])
        }

        enableAccessPathCheck("level1.level2.a.b", "level1.level2.c,arr,unused,outer.k")
        test {
            sql """
                select id, cast(e['a']['b'] as int) as b
                from ${tableName}
                lateral view explode(v['level1']['level2']) tmp as e
                where cast(e['a']['b'] as int) >= 100
                order by id, b
            """
            result([[4, 100], [4, 101]])
        }

        enableAccessPathCheck("x", "y,unused,arr.x,outer.k,level1.level2.a.b")
        test {
            sql """
                select id, cast(vv as int) as x
                from ${tableName}
                lateral view explode(v['x']) tmp as vv
                where id = 6 and cast(vv as int) >= 10
                order by id, x
            """
            result([[6, 10]])
        }

        enableAccessPathCheck("x", "y,unused,arr.x,outer.k,level1.level2.a.b")
        test {
            sql """
                select id, cast(vv['x'] as int) as x
                from ${tableName}
                lateral view explode(v) tmp as vv
                where id = 6 and cast(vv['x'] as int) >= 10
                order by id, x
            """
            result([[6, 10]])
        }
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
