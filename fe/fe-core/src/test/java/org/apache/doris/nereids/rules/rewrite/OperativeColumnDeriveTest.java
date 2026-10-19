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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OperativeColumnDeriveTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n" + "id int not null,\n" + "name varchar(128),\n"
                + "age int,sex int)\n" + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n" + "sid int not null, \n" + "cid int not null, \n" + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n" + "properties('replication_num' = '1');");

        createTable("create table test.course (\n" + "cid int not null, \n" + "cname varchar(128), \n"
                + "teacher varchar(128))\n" + "distributed by hash(cid) buckets 10\n"
                + "properties('replication_num' = '1');");

        connectContext.setDatabase("test");
    }

    @Test
    public void testProject() {
        PlanChecker.from(connectContext)
                .analyze("select sid+1, grade as x from score")
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalProject(
                        logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 1
                                && scan.getOperativeSlots().get(0).getName().equals("sid"))
                ));
    }

    @Test
    public void testUnionAll() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score union all select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalUnion(
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().isEmpty())
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().isEmpty())
                        )
                ));
    }

    @Test
    public void testUnionAllBackPropagate() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score where cid > 0 union all select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalUnion(
                        logicalProject(
                                logicalFilter(
                                        logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 1
                                                    && scan.getOperativeSlots().get(0).getName().equals("cid")
                                        ))
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 1
                                            && scan.getOperativeSlots().get(0).getName().equals("age"))
                        )
                ));
    }

    @Test
    public void testPhysicalResultSinkDoesNotMarkOutputAsOperativeSlots() {
        Plan analyzedPlan = PlanChecker.from(connectContext)
                .analyze("select * from score")
                .getCascadesContext()
                .getRewritePlan();
        LogicalOlapScan scan = analyzedPlan.<LogicalOlapScan>collect(LogicalOlapScan.class::isInstance)
                .iterator()
                .next();

        List<NamedExpression> outputExprs = new ArrayList<>(scan.getOutput());
        PhysicalResultSink<Plan> sink = new PhysicalResultSink<>(outputExprs, Optional.empty(),
                scan.getLogicalProperties(), scan);
        Plan rewritten = new OperativeColumnDerive().rewriteRoot(sink, null);
        LogicalOlapScan rewrittenScan = rewritten.<LogicalOlapScan>collect(LogicalOlapScan.class::isInstance)
                .iterator()
                .next();

        Assertions.assertTrue(rewrittenScan.getOperativeSlots().isEmpty());
        // the empty list is the derivation result and not the initial value, so the statistics
        // derivation must not fall back to reading the column stats of all table columns
        Assertions.assertTrue(rewrittenScan.isOperativeSlotsDerived());
    }

    /**
     * UNION DISTINCT deduplicates on all of its outputs, so the children of the union have to read
     * them even though nothing above the union demands them: the demand is created by the union
     * itself and must not wait for {@code BuildAggForUnion}, which runs after the set operation
     * rules that already consume the statistics.
     */
    @Test
    public void testUnionDistinct() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score "
                        + "union select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalUnion(
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 2)
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 2)
                        )
                ));
    }

    /**
     * INTERSECT deduplicates on all of its outputs as well, so the same rule as for a UNION DISTINCT
     * applies to it.
     */
    @Test
    public void testIntersectDistinct() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score "
                        + "intersect select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalIntersect(
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 2)
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 2)
                        )
                ));
    }

    /**
     * EXCEPT deduplicates on all of its outputs as well, so the same rule as for a UNION DISTINCT
     * applies to it.
     */
    @Test
    public void testExceptDistinct() {
        PlanChecker.from(connectContext)
                .analyze("select a, b from (select sid as a, cid as b from score "
                        + "except select id as a, age as b from student) T")
                .applyTopDown(new MergeProjectable())
                .customRewrite(new OperativeColumnDerive())
                .matches(logicalExcept(
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 2)
                        ),
                        logicalProject(
                                logicalOlapScan().when(scan -> scan.getOperativeSlots().size() == 2)
                        )
                ));
    }

    /**
     * The producer of a materialized CTE only forwards the slots its consumers read, e.g. through a
     * projection of a bare slot, and creates no demand by itself. Without seeding the demand of the
     * consumers the derivation marks the whole output of the producer as not operative and the
     * statistics of the column the consumers use are never requested.
     */
    @Test
    public void testMaterializedCteProducerKeepsTheKeysItsConsumersRead() {
        CascadesContext cascadesContext = PlanChecker.from(connectContext)
                .analyze("with c as (select sid from score) "
                        + "select sid from c intersect select sid from c")
                .getCascadesContext();
        LogicalCTEAnchor<?, ?> cteAnchor = cascadesContext.getRewritePlan()
                .<LogicalCTEAnchor<?, ?>>collectToList(LogicalCTEAnchor.class::isInstance).get(0);

        // without the demand of the consumers the producer remains without operative slots
        Plan derivedWithoutDemand = cteAnchor.child(0).accept(
                new OperativeColumnDerive(), new OperativeColumnDerive.DeriveContext());
        Assertions.assertTrue(derivedWithoutDemand.<LogicalOlapScan>collectToList(
                LogicalOlapScan.class::isInstance).get(0).getOperativeSlots().isEmpty());

        // the intersect deduplicates on `sid` of both consumers, so the consumers demand it
        OperativeColumnDerive.DeriveContext consumerDemand = new OperativeColumnDerive.DeriveContext();
        cteAnchor.child(1).accept(new OperativeColumnDerive(), consumerDemand);
        OperativeColumnDerive.DeriveContext deriveContext = new OperativeColumnDerive.DeriveContext();
        RewriteCteChildren.seedProducerOperativeSlots(cteAnchor, cascadesContext, consumerDemand, deriveContext);
        Plan derivedProducer = cteAnchor.child(0).accept(new OperativeColumnDerive(), deriveContext);
        List<LogicalOlapScan> producerScans = derivedProducer.<LogicalOlapScan>collectToList(
                LogicalOlapScan.class::isInstance);
        Assertions.assertEquals(1, producerScans.size());
        Assertions.assertEquals(ImmutableList.of("sid"), producerScans.get(0).getOperativeSlots().stream()
                .map(NamedExpression::getName)
                .collect(java.util.stream.Collectors.toList()));
    }

    /**
     * The seeded demand is the demand of the estimation, not the whole output of the CTE: a
     * materialized CTE that is only forwarded to the client, without anything above it that
     * estimates its columns, demands nothing from the producer. Here the two consumers are combined
     * with UNION ALL, which neither deduplicates nor uses the keys, so the producer keeps no
     * operative slot and its statistics are not requested.
     */
    @Test
    public void testMaterializedCteProducerWithoutDemandStaysEmpty() {
        CascadesContext cascadesContext = PlanChecker.from(connectContext)
                .analyze("with c as (select sid from score) "
                        + "select sid from c union all select sid from c")
                .getCascadesContext();
        LogicalCTEAnchor<?, ?> cteAnchor = cascadesContext.getRewritePlan()
                .<LogicalCTEAnchor<?, ?>>collectToList(LogicalCTEAnchor.class::isInstance).get(0);

        OperativeColumnDerive.DeriveContext consumerDemand = new OperativeColumnDerive.DeriveContext();
        cteAnchor.child(1).accept(new OperativeColumnDerive(), consumerDemand);
        Assertions.assertTrue(consumerDemand.operativeSlotIds.isEmpty());

        OperativeColumnDerive.DeriveContext deriveContext = new OperativeColumnDerive.DeriveContext();
        RewriteCteChildren.seedProducerOperativeSlots(cteAnchor, cascadesContext, consumerDemand, deriveContext);
        Assertions.assertTrue(deriveContext.operativeSlotIds.isEmpty());
        Plan derivedProducer = cteAnchor.child(0).accept(new OperativeColumnDerive(), deriveContext);
        Assertions.assertTrue(derivedProducer.<LogicalOlapScan>collectToList(
                LogicalOlapScan.class::isInstance).get(0).getOperativeSlots().isEmpty());
    }

    /**
     * The slot of the producer that a consumer keeps can be replaced more than once while the
     * statement is rewritten. The seeded demand has to follow the whole replacement chain, so that
     * it names the slot that is current in the plan that is derived and not an intermediate one that
     * no longer exists: when {@code a} was replaced by {@code b} and {@code b} by {@code c}, the
     * consumer still references {@code a}, and the demand has to be seeded with {@code c}.
     */
    @Test
    public void testProducerOperativeSlotsResolveThroughTheWholeReplacementChain() {
        CascadesContext cascadesContext = PlanChecker.from(connectContext)
                .analyze("with c as (select sid from score) "
                        + "select sid from c intersect select sid from c")
                .getCascadesContext();
        LogicalCTEAnchor<?, ?> cteAnchor = cascadesContext.getRewritePlan()
                .<LogicalCTEAnchor<?, ?>>collectToList(LogicalCTEAnchor.class::isInstance).get(0);
        int producerSlotId = cteAnchor.child(0).<LogicalOlapScan>collectToList(
                LogicalOlapScan.class::isInstance).get(0).getOutput().get(0).getExprId().asInt();

        // as if the rewrite of the producer renamed the slot twice: a -> b -> c, as a rule publishes it
        int replacedOnce = producerSlotId + 1000;
        int replacedTwice = producerSlotId + 2000;
        Map<ExprId, ExprId> renamed = new HashMap<>();
        renamed.put(new ExprId(producerSlotId), new ExprId(replacedOnce));
        cascadesContext.getStatementContext().addProducerExprIdRemap(cteAnchor.getCteId(), renamed);
        Map<ExprId, ExprId> renamedAgain = new HashMap<>();
        renamedAgain.put(new ExprId(replacedOnce), new ExprId(replacedTwice));
        cascadesContext.getStatementContext().addProducerExprIdRemap(cteAnchor.getCteId(), renamedAgain);

        OperativeColumnDerive.DeriveContext consumerDemand = new OperativeColumnDerive.DeriveContext();
        cteAnchor.child(1).accept(new OperativeColumnDerive(), consumerDemand);
        OperativeColumnDerive.DeriveContext deriveContext = new OperativeColumnDerive.DeriveContext();
        RewriteCteChildren.seedProducerOperativeSlots(cteAnchor, cascadesContext, consumerDemand, deriveContext);

        Assertions.assertTrue(deriveContext.operativeSlotIds.contains(replacedTwice),
                "the seeded demand has to name the last slot of the chain");
        Assertions.assertFalse(deriveContext.operativeSlotIds.contains(replacedOnce),
                "the seeded demand must not name an intermediate slot of the chain");
        Assertions.assertFalse(deriveContext.operativeSlotIds.contains(producerSlotId),
                "the seeded demand must not name the slot that was already replaced");
    }
}
