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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * add default order by for limit
 * select * from t limit 10 offset 5
 * to: select * from t order by {keys} limit 10 offset 5
 */
public class AddDefaultOrderByForLimit implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.ADD_DEFAULT_ORDER_BY_FOR_LIMIT.build(
                        logicalLimit(logicalFilter(logicalOlapScan()))
                                .then(limit -> limit.withChildren(ImmutableList.of(
                                        new LogicalSort<>(buildOrderKeys(limit.child().child()), limit.child()))))),
                RuleType.ADD_DEFAULT_ORDER_BY_FOR_LIMIT.build(
                        logicalLimit(logicalOlapScan())
                                .then(limit -> limit.withChildren(ImmutableList.of(
                                        new LogicalSort<>(buildOrderKeys(limit.child()), limit.child())))))
        );
    }

    private List<OrderKey> buildOrderKeys(LogicalOlapScan scan) {
        List<OrderKey> orderKeys = Lists.newArrayList();
        List<Slot> outputs = scan.getOutput();
        OlapTable table = scan.getTable();
        int n = table.getKeysNum();
        for (int i = 0; i < n; ++i) {
            orderKeys.add(new OrderKey(outputs.get(i), true, true));
        }
        return orderKeys;
    }
}
