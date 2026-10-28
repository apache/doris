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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.Expr;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Guards how {@link PluginDrivenScanNode} keeps CAST predicates in Doris for a connector that cannot
 * evaluate them, such as Paimon.
 *
 * <p>The neutral expression converter drops CASTs, so a pushed {@code CAST(code AS INT) = 5} would reach
 * the connector as {@code code = 5} and could prune files that hold {@code '05'} or {@code ' 5'}, losing
 * rows. A connector that returns false from {@code supportsCastPredicatePushdown} therefore never receives
 * a conjunct with a CAST anywhere in its tree; the conjunct stays in Doris and is evaluated after the
 * scan. branch-4.x fixed the same row loss inside its Paimon predicate converter (#67933); master enforces
 * it in {@code buildRemainingFilter}, which had no direct test.</p>
 *
 * <p>The conjuncts come from planning real queries, so the shapes are the ones Nereids produces. The
 * filter is then built through the real private method on a {@code CALLS_REAL_METHODS} node with only the
 * connector metadata injected.</p>
 */
public class PluginDrivenScanNodeCastPredicateTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("db1");
        createTables("create table db1.cast_predicates (id int, code string, amount decimal(10, 2))"
                + " distributed by hash(id) properties('replication_num' = '1')");
    }

    @ParameterizedTest
    @ValueSource(strings = {"cast(code as int) = 5", "5 = cast(code as int)", "cast(code as int) > 5",
            "cast(code as int) in (5, 6)", "cast(code as int) not in (5, 6)",
            "cast(code as int) is null", "cast(code as int) is not null",
            "cast(code as int) = 5 or id = 1", "cast(amount as decimal(10, 1)) = 1.2"})
    public void castConjunctsStayInDoris(String predicate) throws Exception {
        List<Expr> conjuncts = planConjuncts(predicate);
        // Precondition: the planner keeps the CAST as a CastExpr node, the shape the rule looks for.
        Assertions.assertTrue(conjuncts.stream().anyMatch(PluginDrivenScanNode::containsCastExpr), predicate);
        List<Expr> before = Expr.cloneList(conjuncts);

        PluginDrivenScanNode node = nodeWith(conjuncts, false);
        Optional<ConnectorExpression> filter = Deencapsulation.invoke(node, "buildRemainingFilter");

        Assertions.assertFalse(filter.isPresent(), predicate);
        // Nothing was sent to the connector, so the source LIMIT must stay off ...
        Assertions.assertEquals(Collections.emptyList(),
                Deencapsulation.getField(node, "filteredToOriginalIndex"), predicate);
        // ... and every conjunct must remain for Doris to evaluate.
        Assertions.assertEquals(before, conjuncts, predicate);
    }

    @Test
    public void independentPlainConjunctIsStillPushed() throws Exception {
        List<Expr> conjuncts = planConjuncts("cast(code as int) = 5 and id = 1");
        Assertions.assertEquals(2, conjuncts.size());
        int plainIndex = PluginDrivenScanNode.containsCastExpr(conjuncts.get(0)) ? 1 : 0;
        Assertions.assertFalse(PluginDrivenScanNode.containsCastExpr(conjuncts.get(plainIndex)));
        List<Expr> before = Expr.cloneList(conjuncts);

        PluginDrivenScanNode node = nodeWith(conjuncts, false);
        Optional<ConnectorExpression> filter = Deencapsulation.invoke(node, "buildRemainingFilter");

        Assertions.assertTrue(filter.isPresent());
        Assertions.assertTrue(filter.get() instanceof ConnectorComparison, filter.get().toString());
        ConnectorComparison comparison = (ConnectorComparison) filter.get();
        Assertions.assertEquals("id", ((ConnectorColumnRef) comparison.getLeft()).getColumnName());
        Assertions.assertEquals(Collections.singletonList(plainIndex),
                Deencapsulation.getField(node, "filteredToOriginalIndex"));
        Assertions.assertEquals(before, conjuncts);
    }

    @ParameterizedTest
    @ValueSource(strings = {"code = '5'", "id > 1", "id in (1, 2)"})
    public void plainConjunctsArePushedWhole(String predicate) throws Exception {
        List<Expr> conjuncts = planConjuncts(predicate);
        Assertions.assertFalse(conjuncts.stream().anyMatch(PluginDrivenScanNode::containsCastExpr), predicate);

        PluginDrivenScanNode node = nodeWith(conjuncts, false);
        Optional<ConnectorExpression> filter = Deencapsulation.invoke(node, "buildRemainingFilter");

        Assertions.assertTrue(filter.isPresent(), predicate);
        // Every conjunct went to the connector, so no index remapping is needed.
        Assertions.assertNull(Deencapsulation.getField(node, "filteredToOriginalIndex"), predicate);
    }

    @Test
    public void castConjunctIsPushedWhenTheConnectorAcceptsCasts() throws Exception {
        List<Expr> conjuncts = planConjuncts("cast(code as int) = 5");

        PluginDrivenScanNode node = nodeWith(conjuncts, true);
        Optional<ConnectorExpression> filter = Deencapsulation.invoke(node, "buildRemainingFilter");

        // The same conjunct is pushed once the connector accepts CASTs, so the rule above is the switch.
        Assertions.assertTrue(filter.isPresent());
        Assertions.assertNull(Deencapsulation.getField(node, "filteredToOriginalIndex"));
    }

    private List<Expr> planConjuncts(String predicate) throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext,
                "select * from db1.cast_predicates where " + predicate);
        executor.execute();
        return executor.planner().getScanNodes().get(0).getConjuncts();
    }

    /**
     * A node whose connector declares {@code supportsCast} and whose conjuncts are {@code conjuncts}.
     * {@code cachedMetadata} is injected directly so the per-statement metadata funnel stays out of the test.
     */
    private static PluginDrivenScanNode nodeWith(List<Expr> conjuncts, boolean supportsCast) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        Mockito.when(metadata.supportsCastPredicatePushdown(Mockito.any())).thenReturn(supportsCast);
        Deencapsulation.setField(node, "cachedMetadata", metadata);
        Deencapsulation.setField(node, "conjuncts", conjuncts);
        return node;
    }
}
