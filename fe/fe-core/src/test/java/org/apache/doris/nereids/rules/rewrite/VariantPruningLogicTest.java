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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class VariantPruningLogicTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_variant_pruning_logic");
        useDatabase("test_variant_pruning_logic");
        createTable("create table variant_msg_tbl(\n"
                + "  id int,\n"
                + "  msg variant\n"
                + ") properties ('replication_num'='1')");
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enablePruneNestedColumns = true;
    }

    @Test
    public void testMatchOnDotVariantSubColumnUsesSlotRefInScanPredicate() throws Exception {
        String sql = "select id from variant_msg_tbl "
                + "where cast(msg.trace_id as string) match_phrase_prefix 'abc'";
        List<OlapScanNode> olapScanNodes = collectOlapScanNodes(sql);
        Assertions.assertEquals(1, olapScanNodes.size());

        List<MatchPredicate> matchPredicates = new ArrayList<>();
        Expr.collectList(olapScanNodes.get(0).getConjuncts(), MatchPredicate.class, matchPredicates);
        Assertions.assertEquals(1, matchPredicates.size());

        Expr leftWithoutCast = matchPredicates.get(0).getChildWithoutCast(0);
        Assertions.assertInstanceOf(SlotRef.class, leftWithoutCast, matchPredicates.get(0).toString());
        SlotRef leftSlot = (SlotRef) leftWithoutCast;
        Assertions.assertEquals(ImmutableList.of("trace_id"), leftSlot.getDesc().getSubColLables());
    }

    private List<OlapScanNode> collectOlapScanNodes(String sql) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) executeNereidsSql(sql).planner();
        List<OlapScanNode> olapScanNodes = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            olapScanNodes.addAll(fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance));
        }
        return olapScanNodes;
    }
}
