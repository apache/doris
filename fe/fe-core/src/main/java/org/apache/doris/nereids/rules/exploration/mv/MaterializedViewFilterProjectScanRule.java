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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * MaterializedViewFilterProjectScanRule
 */
public class MaterializedViewFilterProjectScanRule extends AbstractMaterializedViewScanRule {

    public static final MaterializedViewFilterProjectScanRule INSTANCE = new MaterializedViewFilterProjectScanRule();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalFilter(logicalProject(logicalOlapScan())).thenApplyMultiNoThrow(ctx -> {
                    LogicalFilter<LogicalProject<LogicalOlapScan>> root = ctx.root;
                    return rewrite(root, ctx.cascadesContext);
                }).toRule(RuleType.MATERIALIZED_VIEW_FILTER_PROJECT_SCAN));
    }
}
