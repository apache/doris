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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SpatialSqlSupportTest extends TestWithFeService {
    private static final String POINT_WKB = "'0101000000000000000000F03F0000000000000040'";
    private static final String SHAPES = "with shapes as (select id, st_geometryfromwkbtyped(encoded) as shape, "
            + "st_geogfromwkb(encoded) as geog, array(st_geogfromwkb(encoded)) as nested_shape from spatial_input) ";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("spatial_sql");
        createTable("create table spatial_input (id int, encoded string) duplicate key(id) "
                + "distributed by hash(id) buckets 1 properties ('replication_num'='1')");
    }

    @Test
    public void testSpatialOrderKeysAreRejected() {
        for (String query : List.of(
                "select id from shapes order by shape",
                "select shape as sort_key from shapes order by sort_key",
                "select shape from shapes order by 1 limit 2",
                "select id from shapes order by nested_shape",
                "select row_number() over (order by shape) from shapes",
                "select group_concat(cast(id as string) order by shape) from shapes")) {
            assertRejected(SHAPES + query, "ORDER BY does not support spatial keys");
        }
    }

    @Test
    public void testSpatialEqualityJoinKeysAreRejected() {
        for (String query : List.of(
                "select l.id from shapes l join shapes r on l.shape = r.shape",
                "select l.id from shapes l join shapes r on l.shape <=> r.shape",
                "select l.id from shapes l join shapes r using (shape)",
                "select l.id from shapes l, shapes r where l.shape = r.shape",
                "select l.id from shapes l join shapes r on l.nested_shape = r.nested_shape",
                "select l.shape in (select r.shape from shapes r) from shapes l")) {
            assertRejected(SHAPES + query, "comparison predicate does not support spatial arguments");
        }
    }

    @Test
    public void testOrdinarySpatialComparisonsAreRejected() {
        String point = "st_geogfromwkb(" + POINT_WKB + ")";
        for (String query : List.of(
                "select geog = " + point + " from shapes",
                "select id from shapes where shape < st_geometryfromwkbtyped(" + POINT_WKB + ")",
                "select nested_shape <=> array(" + point + ") from shapes")) {
            assertRejected(SHAPES + query, "comparison predicate does not support spatial arguments");
        }
    }

    @Test
    public void testArrayComparisonKeysRejectSpatialArguments() {
        for (String function : List.of("array_sort", "array_reverse_sort", "array_compact")) {
            assertRejected(SHAPES + "select " + function + "(nested_shape) from shapes",
                    function + " does not support");
        }
        assertRejected(SHAPES + "select array_sortby(array(id), nested_shape) from shapes",
                "array_sortby does not support");
    }

    @Test
    public void testTextAndJsonCastsCannotConstructSpatialContainers() {
        assertRejected("select cast(encoded as array<geometry>) from spatial_input", "can not cast from origin type");
        assertRejected("select cast(encoded as struct<shape:geography>) from spatial_input",
                "can not cast from origin type");
        assertRejected("select cast(cast(encoded as json) as array<geometry>) from spatial_input",
                "can not cast from origin type");
        assertRejected("select cast(cast(encoded as json) as struct<shape:geography>) from spatial_input",
                "can not cast from origin type");
    }

    @Test
    public void testOrdinaryKeysAndTypedNullStaySupported() {
        for (String query : List.of(
                "select l.id, l.shape from shapes l join shapes r on l.id = r.id order by l.id limit 2",
                "select shape, first_value(shape) over (order by id) from shapes order by st_x(shape)",
                "select group_concat(st_astext(shape) order by id) from shapes",
                "select array_sortby(array(geog), array(id)), st_x(shape) = id from shapes")) {
            PlanChecker.from(connectContext).analyze(SHAPES + query).rewrite();
        }
        PlanChecker.from(connectContext).analyze("select cast(null as array<geometry>)");
        PlanChecker.from(connectContext).analyze("select cast(encoded as array<int>) from spatial_input");
        PlanChecker.from(connectContext)
                .analyze("select cast(cast(encoded as json) as array<int>) from spatial_input");
    }

    private void assertRejected(String query, String message) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(query).rewrite(), query);
        Assertions.assertTrue(error.getMessage().contains(message), query + ": " + error.getMessage());
    }
}
