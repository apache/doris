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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.stream.Collectors;

/**
 the sort node will create new slots for order by keys if the order by keys is not in the output
 so need create a project above sort node to prune the unnecessary order by keys
 */
public class PruneSortColumns extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalSort()
                .when(sort -> !sort.isOrderKeysPruned() && !sort.getOutputSet()
                        .containsAll(sort.getOrderKeys().stream()
                                .map(orderKey -> orderKey.getExpr()).collect(Collectors.toSet())))
                .then(sort -> {
                    return new LogicalProject(sort.getOutput(), ImmutableList.of(), false,
                            sort.withOrderKeysPruned(true));
                }).toRule(RuleType.COLUMN_PRUNE_SORT);
    }
}
