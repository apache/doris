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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SearchJoinDocumentTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("search_join_document");
        connectContext.setDatabase("search_join_document");
        // Mocked tables have no rowsets. Preserve scans so these tests exercise
        // field binding, virtual columns and translation rather than empty plans.
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE objects (id BIGINT, v VARIANT, "
                + "INDEX idx_v(v) USING INVERTED PROPERTIES('parser'='english')) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE lists (object_id BIGINT, list_id BIGINT) "
                + "DUPLICATE KEY(object_id) DISTRIBUTED BY HASH(object_id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE associations (to_id BIGINT, from_id BIGINT) "
                + "DUPLICATE KEY(to_id) DISTRIBUTED BY HASH(to_id) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
    }

    @Test
    public void testDocumentSearchAboveTwoLeftJoins() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM (SELECT id, v FROM objects) o "
                + "LEFT JOIN lists l ON o.id=l.object_id "
                + "LEFT JOIN associations a ON o.id=a.to_id "
                + "WHERE search('john', '{\"default_field\":\"v.string_17\",\"mode\":\"lucene\"}')");
    }

    @Test
    public void testDocumentMatchExistsOr() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "WITH contacts AS (SELECT id, CAST(v['string_8'] AS VARCHAR) firstname FROM objects), "
                + "members AS (SELECT object_id FROM lists) "
                + "SELECT id, firstname FROM contacts c WHERE "
                + "(firstname MATCH_ANY 'keyur patel' AND EXISTS "
                + "(SELECT 1 FROM members l WHERE l.object_id=c.id)) OR c.id>1 LIMIT 10");
    }

    @Test
    public void testSearchOrAssociation() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o LEFT JOIN lists l ON o.id=l.object_id "
                + "WHERE search('v.string_8:john') OR l.list_id=12", planner -> {
                    Assertions.assertTrue(planner.getPhysicalPlan().anyMatch(plan ->
                            plan instanceof PhysicalOlapScan && ((PhysicalOlapScan) plan).getVirtualColumns()
                                    .stream().anyMatch(column -> column.anyMatch(e -> e instanceof SearchExpression))),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testMatchResidualUsesVirtualColumn() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o FULL OUTER JOIN lists l ON o.id=l.object_id "
                + "WHERE (CAST(o.v['string_8'] AS VARCHAR) MATCH_ANY 'john' "
                + "AND CAST(o.v['string_17'] AS VARCHAR) MATCH_ALL 'smith') OR l.list_id=12", planner -> {
                    Assertions.assertTrue(planner.getPhysicalPlan().anyMatch(plan ->
                            plan instanceof PhysicalOlapScan && ((PhysicalOlapScan) plan).getVirtualColumns()
                                    .stream().anyMatch(column -> column.anyMatch(e -> e instanceof Match))),
                            planner.getPhysicalPlan().treeString());
                });
    }

    @Test
    public void testAmbiguousSearchFieldRejected() {
        Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext).analyze(
                "SELECT a.id FROM objects a JOIN objects b ON a.id=b.id WHERE search('v.string_8:john')"));
    }

    @Test
    public void testSearchWithFieldSelector() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o LEFT JOIN lists l ON o.id=l.object_id "
                + "WHERE search('v.string_8@english:john')");
    }

    @Test
    public void testSearchConstantPredicatePreservesField() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o LEFT JOIN lists l ON o.id=l.object_id "
                + "WHERE CAST(o.v['string_8'] AS VARCHAR)='john' "
                + "AND (search('v.string_8:john') OR l.list_id=12)");
    }

    @Test
    public void testQuotedAtSignIsLiteralVariantPath() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT id FROM objects WHERE search('\"v.email@work\":john')");
    }

    @Test
    public void testSearchOrMovedIntoInnerJoin() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "SELECT o.id FROM objects o JOIN lists l ON o.id=l.object_id "
                + "WHERE search('v.string_8:john') OR l.list_id=12", planner -> {
                    Assertions.assertTrue(planner.getPhysicalPlan().anyMatch(plan ->
                            plan instanceof PhysicalOlapScan && ((PhysicalOlapScan) plan).getVirtualColumns()
                                    .stream().anyMatch(column -> column.anyMatch(e -> e instanceof SearchExpression))),
                            planner.getPhysicalPlan().treeString());
                });
    }
}
