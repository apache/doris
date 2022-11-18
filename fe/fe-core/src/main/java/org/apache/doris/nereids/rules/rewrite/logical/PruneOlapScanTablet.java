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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.PartitionColumnFilter;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import java.util.List;
import java.util.Map;

/**
 * prune bucket
 */
public class PruneOlapScanTablet extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan())
                .thenApply(ctx -> {
                    LogicalFilter<LogicalOlapScan> filter = ctx.root;
                    LogicalOlapScan olapScan = filter.child();
                    Expression predicate = filter.getPredicates();
                    OlapTable table = olapScan.getTable();
                    List<Expression> expressions = ExpressionUtils.extractConjunction(predicate);
                    return null;
                }).toRule(RuleType.OLAP_SCAN_TABLET_PRUNE);
    }

    private Map<String, PartitionColumnFilter> comuteColumnFilter(List<Expression> exprs, LogicalOlapScan olapScan) {
        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        for (Expression expr : exprs) {
            PartitionColumnFilter filter = new PartitionColumnFilter();

        }
        return filters;
    }

    private Range<PartitionKey> getRange(Expression expr) {
        if (expr instanceof EqualTo) {

        } else if (expr instanceof LessThan) {

        } else if (expr instanceof GreaterThan) {

        } else if (expr instanceof LessThanEqual) {

        } else if (expr instanceof GreaterThanEqual) {

        } else if (expr instanceof InPredicate) {

        } else if (expr instanceof Not) {

        }
    }

    private boolean checkExpression(Expression expr) {
        if (expr instanceof ComparisonPredicate) {

        }
    }
}
