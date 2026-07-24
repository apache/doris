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

suite("bucket_shuffle_set_operation") {
    multi_sql """
        drop table if exists bucket_shuffle_set_operation1;
        create table bucket_shuffle_set_operation1(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1');
        insert into bucket_shuffle_set_operation1 values(1, 1), (2, 2), (3, 3);
        
        drop table if exists bucket_shuffle_set_operation2;
        create table bucket_shuffle_set_operation2(id int, value int) distributed by hash(id) buckets 10 properties('replication_num'='1');
        insert into bucket_shuffle_set_operation2 values(1, 1), (2, 2), (3, 3);
        
        drop table if exists bucket_shuffle_set_operation3;
        create table bucket_shuffle_set_operation3(id int, value int) distributed by hash(id) buckets 11 properties('replication_num'='1');
        insert into bucket_shuffle_set_operation3 values(1, 1), (2, 2), (3, 3);
        
        set runtime_filter_mode=off;
        """

    // make bucket shuffle set operation stable
    sql "set parallel_pipeline_task_num=5"
    // disable the bucket shuffle downgrade so the chosen shapes do not depend on the
    // backend count / parallelism of the environment running this suite
    sql "set bucket_shuffle_downgrade_ratio=0"

    def checkShapeAndResult = { String tag, String sqlStr ->
        quickTest(tag + "_shape", "explain shape plan " + sqlStr)
        quickTest(tag + "_result", sqlStr, true)
    }

    checkShapeAndResult("bucket_shuffle_union_with_all_column", """
        select *
        from (
            select * from bucket_shuffle_set_operation1
            union all
            select * from bucket_shuffle_set_operation2
        )a
        join[shuffle] (
            select *
            from bucket_shuffle_set_operation1
        )b
        on a.id=b.id""")

    checkShapeAndResult("bucket_shuffle_intersect", """
        select id from bucket_shuffle_set_operation1
        intersect
        select id from bucket_shuffle_set_operation2""")

    checkShapeAndResult("bucket_shuffle_intersect_with_all_column", """
        select * from bucket_shuffle_set_operation1
        intersect
        select * from bucket_shuffle_set_operation2""")

    checkShapeAndResult("no_bucket_shuffle_intersect", """
        select value from bucket_shuffle_set_operation1
        intersect
        select value from bucket_shuffle_set_operation2""")

    checkShapeAndResult("bucket_shuffle_to_left", """
        select id from bucket_shuffle_set_operation3
        intersect
        select id from bucket_shuffle_set_operation1
        """)

    checkShapeAndResult("bucket_shuffle_to_right", """
        select id from bucket_shuffle_set_operation1
        intersect
        select id from bucket_shuffle_set_operation3
        """)

    checkShapeAndResult("bucket_shuffle_except_1", """
        select id from bucket_shuffle_set_operation1 where id=1
        except
        select id from bucket_shuffle_set_operation2
        """)

    checkShapeAndResult("bucket_shuffle_except_2", """
        select id from bucket_shuffle_set_operation1
        except
        select id from bucket_shuffle_set_operation2 where id=1
        """)

    // The basic child of a bucket-shuffle set operation can be a join output instead of a
    // direct scan. In that shape the local exchange planned for the basic side must still
    // partition by the storage bucket function: an execution-hash local exchange would not
    // align with the bucket-distributed side and the set operation would compute wrong results.
    checkShapeAndResult("bucket_shuffle_join_as_basic_child", """
        select a.id from bucket_shuffle_set_operation1 a
        join bucket_shuffle_set_operation2 b on a.id = b.id
        intersect
        select id from bucket_shuffle_set_operation3""")

    // a set operation child can itself be a set operation whose output claims a bucket
    // distribution; the outer set operation must only treat its children as bucket-aligned
    // when they share the same storage layout
    checkShapeAndResult("bucket_shuffle_nested_set_operation", """
        select id from bucket_shuffle_set_operation3
        union all
        (select a.id from bucket_shuffle_set_operation1 a
        join bucket_shuffle_set_operation2 b on a.id = b.id
        intersect
        select id from bucket_shuffle_set_operation2)""")

    // when local shuffle is disabled entirely, every pipeline runs a single task per
    // instance so the bucket alignment holds naturally and bucket shuffle is still allowed
    sql "set enable_local_shuffle=false"
    checkShapeAndResult("bucket_shuffle_when_local_shuffle_off", """
        select id from bucket_shuffle_set_operation1
        intersect
        select id from bucket_shuffle_set_operation2""")
    sql "set enable_local_shuffle=true"

    // A shuffle join above the union pushes a hash request into the union
    // (createHashRequestAccordingToParent, the parent-hash request path). When the FE does not
    // plan the local shuffle, that request must be downgraded so the union does not choose
    // bucket shuffle, while the result stays correct.
    def unionParentHashSql = """
        select b.id from (
            select id from bucket_shuffle_set_operation1
            union all
            select id from bucket_shuffle_set_operation2
        ) u join[shuffle] bucket_shuffle_set_operation3 b on u.id = b.id
        """
    sql "set enable_local_shuffle_planner=false"
    // Golden shape: the union must stay a plain PhysicalUnion whose two children are each a
    // PhysicalDistribute[DistributionSpecHash]. That is the actual proof that the parent hash
    // request was pushed down into the union (createHashRequestAccordingToParent) and downgraded
    // to execution hash, not merely that the PhysicalUnion line lacks the [bucketShuffle] tag.
    // If that path regressed, the optimizer could instead keep an unbucketed union and add a
    // single PhysicalDistribute[DistributionSpecHash] above it for the shuffle join; the golden
    // shape below (union children are distributes, not direct scans) would catch that.
    qt_union_parent_hash_shape_when_local_shuffle_planner_off("explain shape plan " + unionParentHashSql)
    explain {
        sql "shape plan " + unionParentHashSql
        check { String e ->
            def unionIndex = e.indexOf("PhysicalUnion")
            assertTrue(unionIndex >= 0)
            // the union must not be a bucket shuffle union when the FE local shuffle planner is off
            assertFalse(e.substring(unionIndex,
                    Math.min(unionIndex + "PhysicalUnion".length() + 20, e.length())).contains("bucketShuffle"))
            // and the parent hash request must have been pushed into the union: each union child
            // arrives through a PhysicalDistribute[DistributionSpecHash], so no union child is a
            // direct scan.
            def afterUnion = e.substring(unionIndex)
            def joinProbeIndex = afterUnion.indexOf("bucket_shuffle_set_operation3")
            def unionSubtree = joinProbeIndex >= 0 ? afterUnion.substring(0, joinProbeIndex) : afterUnion
            assertEquals(2, unionSubtree.split("PhysicalDistribute\\[DistributionSpecHash\\]", -1).length - 1)
        }
    }
    order_qt_union_parent_hash_when_local_shuffle_planner_off unionParentHashSql
    sql "set enable_local_shuffle_planner=true"

    // A plain intersect/except without a parent hash request goes through
    // visitPhysicalSetOperation directly (not the parent-hash request path); with the FE local
    // shuffle planner disabled it must not choose bucket shuffle either, and the result stays
    // correct.
    sql "set enable_local_shuffle_planner=false"
    explain {
        sql "shape plan select id from bucket_shuffle_set_operation1 intersect select id from bucket_shuffle_set_operation2"
        check { String e ->
            assertFalse(e.contains("bucketShuffle"))
        }
    }
    order_qt_plain_intersect_when_local_shuffle_planner_off "select id from bucket_shuffle_set_operation1 intersect select id from bucket_shuffle_set_operation2"
    sql "set enable_local_shuffle_planner=true"

    // Set operation bucket shuffle is gated on setOperationBucketShuffleAllowed() =
    // enableLocalShufflePlanner && canUseNereidsDistributePlanner. The enable_local_shuffle_planner
    // =false cases above only exercise the first conjunct; this block guards the second one. With
    // local shuffle and the FE local-shuffle planner both enabled but the Nereids distribute planner
    // disabled, the set operation must still downgrade (no [bucketShuffle]) and keep correct results.
    sql "set enable_local_shuffle=true"
    sql "set enable_local_shuffle_planner=true"
    sql "set enable_nereids_distribute_planner=false"
    // plain intersect goes through visitPhysicalSetOperation directly
    explain {
        sql "shape plan select id from bucket_shuffle_set_operation1 intersect select id from bucket_shuffle_set_operation2"
        check { String e ->
            assertFalse(e.contains("bucketShuffle"))
        }
    }
    order_qt_plain_intersect_when_nereids_distribute_planner_off "select id from bucket_shuffle_set_operation1 intersect select id from bucket_shuffle_set_operation2"
    // the parent-hash union path (createHashRequestAccordingToParent) must downgrade too
    explain {
        sql "shape plan " + unionParentHashSql
        check { String e ->
            def unionIndex = e.indexOf("PhysicalUnion")
            assertTrue(unionIndex >= 0)
            assertFalse(e.substring(unionIndex,
                    Math.min(unionIndex + "PhysicalUnion".length() + 20, e.length())).contains("bucketShuffle"))
        }
    }
    order_qt_union_parent_hash_when_nereids_distribute_planner_off unionParentHashSql
    sql "set enable_nereids_distribute_planner=true"

    // The right child can be selected as the bucket-shuffle basic child (larger row count). The
    // set operation output must then advertise a non-specific distribution rather than a plain
    // execution hash: otherwise two such set operations with different storage layouts are
    // co-located under a join and fail bucket assignment ("Can not find tablet ... in the
    // bucket"). r1 / r2 are larger than the small tables so they become the basic child on the
    // right, and they are different tables so their bucket layouts differ.
    sql "drop table if exists bucket_shuffle_set_operation_r1"
    sql "create table bucket_shuffle_set_operation_r1(id int) distributed by hash(id) buckets 10 properties('replication_num'='1')"
    sql "insert into bucket_shuffle_set_operation_r1 select number from numbers('number'='20')"
    sql "drop table if exists bucket_shuffle_set_operation_r2"
    sql "create table bucket_shuffle_set_operation_r2(id int) distributed by hash(id) buckets 10 properties('replication_num'='1')"
    sql "insert into bucket_shuffle_set_operation_r2 select number from numbers('number'='20')"
    sql "alter table bucket_shuffle_set_operation_r1 modify column id set stats ('row_count'='1000', 'ndv'='1000', 'min_value'='0', 'max_value'='19')"
    sql "alter table bucket_shuffle_set_operation_r2 modify column id set stats ('row_count'='1000', 'ndv'='1000', 'min_value'='0', 'max_value'='19')"
    def intersectRightBasicSql = """
        select t.id from
            (select id from bucket_shuffle_set_operation1 intersect select id from bucket_shuffle_set_operation_r1) t
            join[shuffle]
            (select id from bucket_shuffle_set_operation1 intersect select id from bucket_shuffle_set_operation_r2) s
            on t.id = s.id"""
    // Prove the shuffleToRight branch (distributeToChildIndex > 0) is actually covered, under a
    // parent hash consumer. The join[shuffle] hint forces the parent to be a hash consumer of the
    // two set-operation outputs. Each INTERSECT must keep the right table (r1 / r2, the larger row
    // count) as the direct bucketed basic child while the left op1 is the enforced
    // PhysicalDistribute[DistributionSpecHash] side. Because visitPhysicalSetOperation keeps the
    // right basic child's specific storage layout, the two intersects carry different table ids
    // (r1 vs r2), so the parent re-aligns them with a bucket-shuffle join instead of co-locating
    // them (which, if the layout were erased to a layout-less execution hash, would fail bucket
    // assignment with "Can not find tablet"). If the right side stopped being selected as basic, or
    // the layout were dropped so the join co-located, these assertions fail even though the result
    // stays 1, 2, 3.
    explain {
        sql "shape plan " + intersectRightBasicSql
        check { String e ->
            assertTrue(e.contains("PhysicalIntersect[bucketShuffle]"))
            // the parent join re-aligns the two different-layout outputs (bucket-shuffle hash
            // consumer) and does not co-locate them
            assertTrue(e.contains("hashJoin[INNER_JOIN bucketShuffle]"),
                    "parent must re-align the two different-layout outputs with a bucket-shuffle join")
            assertFalse(e.contains("colocated"), "parent must not co-locate the two right-basic intersects")
            def lines = e.split("\n").findAll { it =~ /Physical|hashJoin/ }
            def depth = { String s -> (s =~ /^-*/)[0].length() }
            def parentOf = { int idx ->
                for (int k = idx - 1; k >= 0; k--) {
                    if (depth(lines[k]) < depth(lines[idx])) {
                        return lines[k]
                    }
                }
                return ""
            }
            // each INTERSECT keeps r1 / r2 (the larger, right side) as its direct bucketed basic child
            ["bucket_shuffle_set_operation_r1", "bucket_shuffle_set_operation_r2"].each { rt ->
                int i = lines.findIndexOf { it.contains("PhysicalOlapScan[" + rt + "]") }
                assertTrue(i >= 0, rt + " scan not found in shape")
                assertTrue(parentOf(i).contains("PhysicalIntersect[bucketShuffle]"),
                        rt + " must be the direct bucketed basic child of the intersect (right basic)")
            }
            // and each INTERSECT's other (left) child arrives through an enforced
            // PhysicalDistribute[DistributionSpecHash] directly under the intersect
            int enforcedLeftCount = 0
            lines.eachWithIndex { String ln, int i ->
                if (ln.contains("PhysicalDistribute[DistributionSpecHash]")
                        && parentOf(i).contains("PhysicalIntersect[bucketShuffle]")) {
                    enforcedLeftCount++
                }
            }
            assertTrue(enforcedLeftCount >= 2,
                    "each intersect must shuffle its left child through a PhysicalDistribute[DistributionSpecHash]")
        }
    }
    order_qt_intersect_right_basic_parent_hash intersectRightBasicSql

    // A non-intersect bucket-shuffle set operation (UNION ALL) whose basic / anchor child is a
    // direct bucketed scan that is bucket-pruned by an IN predicate on the distribution key, so
    // it only scans a subset of the buckets. The other child is shuffled onto the anchor's
    // storage layout and has rows in the buckets the pruned anchor does not scan. Because the
    // union is a non-intersect bucket-shuffle set operation, UnassignedScanBucketOlapTableJob
    // must fill up receiver instances for those missing buckets; without the fill-up the other
    // child's rows in the missing buckets would have no destination instance and be lost.
    //
    // The setup forces the pruned scan to be the anchor: fill_anchor has huge injected stats so
    // the IN-filtered branch still wins the largest-row-count basic-child selection, while
    // fill_spread has a mismatched bucket count (11 vs 10) so it cannot be the natural anchor and
    // must be shuffled. The bucket-shuffle join above the union supplies the parent hash request
    // that makes the union choose bucket shuffle, and the join probe fill_probe has yet another
    // bucket count (7) so the join is a real shuffle in its own fragment and does not co-locate a
    // full-bucket scan into the union fragment (which would otherwise cover the missing buckets
    // and hide the fill-up).
    sql "drop table if exists bucket_shuffle_set_operation_fill_anchor"
    sql """create table bucket_shuffle_set_operation_fill_anchor(id int)
            distributed by hash(id) buckets 10 properties('replication_num'='1')"""
    sql "insert into bucket_shuffle_set_operation_fill_anchor select number from numbers('number'='20')"
    sql "drop table if exists bucket_shuffle_set_operation_fill_spread"
    sql """create table bucket_shuffle_set_operation_fill_spread(id int)
            distributed by hash(id) buckets 11 properties('replication_num'='1')"""
    sql "insert into bucket_shuffle_set_operation_fill_spread select number from numbers('number'='20')"
    sql "drop table if exists bucket_shuffle_set_operation_fill_probe"
    sql """create table bucket_shuffle_set_operation_fill_probe(id int)
            distributed by hash(id) buckets 7 properties('replication_num'='1')"""
    sql "insert into bucket_shuffle_set_operation_fill_probe select number from numbers('number'='20')"
    sql """alter table bucket_shuffle_set_operation_fill_anchor modify column id
            set stats ('row_count'='1000000', 'ndv'='20', 'min_value'='0', 'max_value'='19')"""
    sql """alter table bucket_shuffle_set_operation_fill_spread modify column id
            set stats ('row_count'='50', 'ndv'='20', 'min_value'='0', 'max_value'='19')"""
    sql """alter table bucket_shuffle_set_operation_fill_probe modify column id
            set stats ('row_count'='30', 'ndv'='20', 'min_value'='0', 'max_value'='19')"""
    def bucketShuffleUnionFillUpSql = """
        select t.id from (
            select id from bucket_shuffle_set_operation_fill_anchor where id in (0, 2, 4, 6)
            union all
            select id from bucket_shuffle_set_operation_fill_spread
        ) t join[shuffle] bucket_shuffle_set_operation_fill_probe c on t.id = c.id"""
    explain {
        sql "shape plan " + bucketShuffleUnionFillUpSql
        check { String e ->
            assertTrue(e.contains("PhysicalUnion[bucketShuffle]"))
        }
    }
    order_qt_bucket_shuffle_union_fill_up bucketShuffleUnionFillUpSql

    // Same missing-bucket fill-up contract, but the pruned basic child exposes its storage bucket
    // key only through an equivalent slot: the union's basic child is a bucket-shuffle join output
    // that projects the join key bucket_shuffle_set_operation2.id AS k, so the storage bucket column
    // (fill_anchor.id) is hidden and only the equivalent k is visible in the set-operation output.
    // The alignment proof must resolve the bucket key through its hash equivalence set (mirroring
    // ChildrenPropertiesRegulator.canMapBucketKeysToRequire); a direct ExprId lookup would report
    // the union as not bucket-aligned, drop the BUCKET_SHUFFLE marker, and skip
    // UnassignedScanBucketOlapTableJob.fillUpInstances(), losing the shuffled side's rows in the
    // buckets the pruned basic child does not scan.
    def bucketShuffleEquivalentKeyFillUpSql = """
        select t.k from (
            select b.id as k from bucket_shuffle_set_operation_fill_anchor a
                join[shuffle] bucket_shuffle_set_operation2 b on a.id = b.id
                where a.id in (0, 2, 4, 6)
            union all
            select id from bucket_shuffle_set_operation_fill_spread
        ) t join[shuffle] bucket_shuffle_set_operation_fill_probe c on t.k = c.id"""
    explain {
        sql "shape plan " + bucketShuffleEquivalentKeyFillUpSql
        check { String e ->
            assertTrue(e.contains("PhysicalUnion[bucketShuffle]"))
        }
    }
    order_qt_bucket_shuffle_equivalent_key_fill_up bucketShuffleEquivalentKeyFillUpSql

    // Same shape but the hidden bucket key has two visible equivalent set-output columns: the join
    // chain a.id = b.id = c.id makes fill_anchor.id equivalent to both projected columns x (c.id)
    // and y (b.id), and the parent hashes the union output on the later one (y). The bucket key
    // alignment must resolve to the same output position across children (the enforced sibling is
    // shuffled by y), which is why the proof intersects each child's candidate equivalent positions
    // rather than letting the basic child pick its first visible equivalent (x) and disagree with
    // the sibling on y.
    def bucketShuffleTwoEquivalentKeysSql = """
        select t.x, t.y from (
            select c.id as x, b.id as y from bucket_shuffle_set_operation_fill_anchor a
                join[shuffle] bucket_shuffle_set_operation2 b on a.id = b.id
                join[shuffle] bucket_shuffle_set_operation3 c on a.id = c.id
                where a.id in (0, 2, 4, 6)
            union all
            select id, id from bucket_shuffle_set_operation_fill_spread
        ) t join[shuffle] bucket_shuffle_set_operation_fill_probe p on t.y = p.id"""
    explain {
        sql "shape plan " + bucketShuffleTwoEquivalentKeysSql
        check { String e ->
            assertTrue(e.contains("PhysicalUnion[bucketShuffle]"))
        }
    }
    order_qt_bucket_shuffle_two_equivalent_keys bucketShuffleTwoEquivalentKeysSql

    // The basic child candidate can be bucketed by MORE columns than the set operation distributes
    // on: a table distributed by hash(k, v) feeding an INTERSECT on only k. The (k, v) bucket key is
    // wider than the single-column requirement, so canMapBucketKeysToRequire() must reject it as the
    // bucket-shuffle basic and the set operation falls back to execution-hash shuffle. Without that
    // guard the planner hits a checkState in calAnotherSideRequiredShuffleIds and fails to plan. Two
    // such tables (different bucket counts so neither is a natural colocate basic) force the fallback
    // on both sides.
    sql "drop table if exists bucket_shuffle_set_operation_kv"
    sql """create table bucket_shuffle_set_operation_kv(k int, v int)
            distributed by hash(k, v) buckets 10 properties('replication_num'='1')"""
    sql "insert into bucket_shuffle_set_operation_kv values (1, 1), (2, 2), (3, 3)"
    sql "drop table if exists bucket_shuffle_set_operation_kv2"
    sql """create table bucket_shuffle_set_operation_kv2(k int, v int)
            distributed by hash(k, v) buckets 11 properties('replication_num'='1')"""
    sql "insert into bucket_shuffle_set_operation_kv2 values (1, 1), (2, 2), (3, 3)"
    def multiColumnBucketKeyIntersectSql = """
        select k from bucket_shuffle_set_operation_kv
        intersect
        select k from bucket_shuffle_set_operation_kv2"""
    explain {
        sql "shape plan " + multiColumnBucketKeyIntersectSql
        check { String e ->
            assertFalse(e.contains("bucketShuffle"))
        }
    }
    order_qt_multi_column_bucket_key_intersect multiColumnBucketKeyIntersectSql

    explain {
        sql """
        select id, id as id2 from (select nullable(id) as id from bucket_shuffle_set_operation1)a
        intersect
        (select id, id as id2 from bucket_shuffle_set_operation3)
        """

        check { String e ->
            def index = e.indexOf("VINTERSECT")
            e = e.substring(index)

            // extract following 6 lines of VINTERSECT:
            //
            // VINTERSECT(325)
            //  |  runtime filters: RF000[min_max] <- id[#4](-1/1/1048576), RF001[in_or_bloom] <- id[#4](-1/1/1048576)
            //  |  distribute expr lists: id2[#5]
            //  |  distribute expr lists: id2[#9]
            //  |

            def lines = e.split("\n")
            boolean checked = false
            for (int i = 1; i < Math.min(6, lines.length); ++i) {
                if (lines[i].contains("distribute expr lists")) {
                    def line = lines[i].substring(lines[i].indexOf(":") + 1)

                    // because left shuffle to right, and right only distribute 1 column(id)
                    // so we should ensure left shuffle to right only distribute by 1 column,
                    // not distribute 2 columns
                    assertTrue(line.trim().split(",").length == 1)
                    checked = true
                }
            }
            assertTrue(checked)
        }
    }

    // A bucket-shuffled UNION ALL feeding an analytic window that partitions by the union's
    // distribution key. Both branches have to be placed on local tasks by the same bucket
    // function; if one keeps its bucket placement while the other is re-partitioned by the
    // execution hash, a partition is split across tasks and the window returns per-task
    // partial counts. The cross join branch contributes two rows per id and is the larger
    // side, so it becomes the bucket-shuffle basic child and the plain scan is shuffled onto
    // it. Every id must therefore see exactly 3 rows.
    sql "drop table if exists bucket_shuffle_set_operation_win"
    sql """create table bucket_shuffle_set_operation_win(id int)
            distributed by hash(id) buckets 10 properties('replication_num'='1')"""
    sql """insert into bucket_shuffle_set_operation_win select number from numbers("number" = "40")"""
    sql "drop table if exists bucket_shuffle_set_operation_win2"
    sql """create table bucket_shuffle_set_operation_win2(id int)
            distributed by hash(id) buckets 10 properties('replication_num'='1')"""
    sql "insert into bucket_shuffle_set_operation_win2 values (1), (2)"
    // Pin the statistics. The bucket-shuffle basic child is chosen by row count, so on freshly
    // loaded tables whose statistics have not been reported yet the optimizer can pick a different
    // distribution and the case would stop exercising the bucket-shuffle path it is meant to cover.
    sql "analyze table bucket_shuffle_set_operation_win with sync"
    sql "analyze table bucket_shuffle_set_operation_win2 with sync"

    order_qt_bucket_shuffle_union_analytic_partition """
        select cnt, count(*) as rows_with_cnt
        from (
            select count(*) over (partition by id order by id) cnt
            from (
                select id from bucket_shuffle_set_operation_win
                union all
                select l.id from bucket_shuffle_set_operation_win l
                    cross join bucket_shuffle_set_operation_win2 r
            ) t
        ) x
        group by cnt"""

    // ------------------------------------------------------------------------------------------
    // Distribution matrix: union / intersect / except crossed with every distribution the
    // regulator can pick for a set operation — unaligned, execution hash, bucket shuffle with the
    // basic child on either side, and tables in a colocate group. Whichever distribution is
    // chosen, the result has to be the same, which is what each case asserts.
    //
    // Only results are checked, deliberately. Plan shapes here depend on the backend count, the
    // core count and the fuzzy session variables the pipeline injects, so a golden shape would
    // report environment differences as failures. The distribution each case is meant to exercise
    // is described in the comments and was verified while writing them.
    //
    // Which child becomes the basic one (keeps its buckets while the others are bucket-shuffled
    // onto it) follows the row count: ChildrenPropertiesRegulator picks the largest natural /
    // storage-bucketed child. The direction is therefore controlled by which side holds the larger
    // table. The analyze statements are load bearing for that: without pinned statistics the
    // branches can report the same estimated row count, the search for the largest child falls back
    // to "first child wins", and a case stops exercising the distribution it was written for
    // depending on statistics reporting timing.
    //
    // Union is a plain concatenation, so it only constrains its children when something downstream
    // depends on the distribution. The union cases therefore run under an analytic window
    // partitioned by the set operation key: the small table contributes one row per id and the
    // large one contributes two, so every id must report exactly 3. If the branches are placed by
    // different hash functions a partition splits across local tasks and the window reports 1 and
    // 2 instead. Intersect and except require hash distribution on their own, so they are checked
    // directly — misalignment makes them drop matches.
    sql "drop table if exists bucket_shuffle_set_operation_matrix_small"
    sql """create table bucket_shuffle_set_operation_matrix_small(id int, value int)
            distributed by hash(id) buckets 10 properties('replication_num'='1')"""
    sql """insert into bucket_shuffle_set_operation_matrix_small
            select number, number from numbers("number" = "40")"""

    sql "drop table if exists bucket_shuffle_set_operation_matrix_large"
    sql """create table bucket_shuffle_set_operation_matrix_large(id int, value int)
            distributed by hash(id) buckets 10 properties('replication_num'='1')"""
    sql """insert into bucket_shuffle_set_operation_matrix_large
            select number, number from numbers("number" = "40")"""
    sql """insert into bucket_shuffle_set_operation_matrix_large
            select number, number from numbers("number" = "40")"""

    // Tables in the same colocate group. A set operation does not get a colocate-specific plan:
    // the regulator still elects one basic child and bucket-shuffles the others onto it, so these
    // cases document that colocate group membership does not remove the shuffle.
    sql "drop table if exists bucket_shuffle_set_operation_co_small"
    sql """create table bucket_shuffle_set_operation_co_small(id int, value int)
            distributed by hash(id) buckets 10
            properties('replication_num'='1', 'colocate_with'='bucket_shuffle_set_operation_cg')"""
    sql """insert into bucket_shuffle_set_operation_co_small
            select number, number from numbers("number" = "40")"""
    sql "drop table if exists bucket_shuffle_set_operation_co_large"
    sql """create table bucket_shuffle_set_operation_co_large(id int, value int)
            distributed by hash(id) buckets 10
            properties('replication_num'='1', 'colocate_with'='bucket_shuffle_set_operation_cg')"""
    sql """insert into bucket_shuffle_set_operation_co_large
            select number, number from numbers("number" = "40")"""
    sql """insert into bucket_shuffle_set_operation_co_large
            select number, number from numbers("number" = "40")"""

    // more rows than matrix_small but covering only the lower half of its ids, so it can be the
    // larger (basic) side of an except while still leaving rows in the result
    sql "drop table if exists bucket_shuffle_set_operation_matrix_dense"
    sql """create table bucket_shuffle_set_operation_matrix_dense(id int, value int)
            distributed by hash(id) buckets 10 properties('replication_num'='1')"""
    4.times {
        sql """insert into bucket_shuffle_set_operation_matrix_dense
                select number, number from numbers("number" = "20")"""
    }

    sql "analyze table bucket_shuffle_set_operation_matrix_small with sync"
    sql "analyze table bucket_shuffle_set_operation_matrix_large with sync"
    sql "analyze table bucket_shuffle_set_operation_matrix_dense with sync"
    sql "analyze table bucket_shuffle_set_operation_co_small with sync"
    sql "analyze table bucket_shuffle_set_operation_co_large with sync"

    // wraps a union in the analytic window described above
    def unionUnderWindow = { String branches ->
        """select cnt, count(*) as rows_with_cnt
           from (select count(*) over (partition by id order by id) cnt from ($branches) t) x
           group by cnt"""
    }

    // --- union -------------------------------------------------------------------------------

    // nothing downstream depends on the distribution: the branches are left where they are
    order_qt_matrix_union_unaligned """
        select id from bucket_shuffle_set_operation_matrix_small
        union all
        select id from bucket_shuffle_set_operation_matrix_large"""

    // the set operation key is not the bucket key, so no child is storage bucketed and every
    // branch arrives through a global hash exchange
    order_qt_matrix_union_execution_hash unionUnderWindow("""
        select value as id from bucket_shuffle_set_operation_matrix_small
        union all
        select value as id from bucket_shuffle_set_operation_matrix_large""")

    order_qt_matrix_union_bucket_shuffle_basic_right unionUnderWindow("""
        select id from bucket_shuffle_set_operation_matrix_small
        union all
        select id from bucket_shuffle_set_operation_matrix_large""")

    order_qt_matrix_union_bucket_shuffle_basic_left unionUnderWindow("""
        select id from bucket_shuffle_set_operation_matrix_large
        union all
        select id from bucket_shuffle_set_operation_matrix_small""")

    order_qt_matrix_union_colocate_group unionUnderWindow("""
        select id from bucket_shuffle_set_operation_co_small
        union all
        select id from bucket_shuffle_set_operation_co_large""")

    // --- intersect ---------------------------------------------------------------------------

    order_qt_matrix_intersect_execution_hash """
        select value from bucket_shuffle_set_operation_matrix_small
        intersect
        select value from bucket_shuffle_set_operation_matrix_large"""

    order_qt_matrix_intersect_bucket_shuffle_basic_right """
        select id from bucket_shuffle_set_operation_matrix_small
        intersect
        select id from bucket_shuffle_set_operation_matrix_large"""

    // Same intersect with the branches written the other way round. Intersect children are
    // canonicalized, so this plans the same way as the case above: the basic child follows the row
    // count, not the position in the query.
    order_qt_matrix_intersect_bucket_shuffle_sql_order_ignored """
        select id from bucket_shuffle_set_operation_matrix_large
        intersect
        select id from bucket_shuffle_set_operation_matrix_small"""

    order_qt_matrix_intersect_colocate_group """
        select id from bucket_shuffle_set_operation_co_small
        intersect
        select id from bucket_shuffle_set_operation_co_large"""

    // --- except ------------------------------------------------------------------------------

    order_qt_matrix_except_execution_hash """
        select value from bucket_shuffle_set_operation_matrix_small
        except
        select value from bucket_shuffle_set_operation_matrix_large where value < 10"""

    // right side holds more rows, so it is the basic child and the left branch is shuffled onto it
    order_qt_matrix_except_bucket_shuffle_basic_right """
        select id from bucket_shuffle_set_operation_matrix_small
        except
        select id from bucket_shuffle_set_operation_matrix_dense"""

    // left side holds more rows, so the shuffle goes the other way
    order_qt_matrix_except_bucket_shuffle_basic_left """
        select id from bucket_shuffle_set_operation_matrix_large
        except
        select id from bucket_shuffle_set_operation_matrix_small where id < 10"""

    order_qt_matrix_except_colocate_group """
        select id from bucket_shuffle_set_operation_co_small
        except
        select id from bucket_shuffle_set_operation_co_large where id < 10"""
}
