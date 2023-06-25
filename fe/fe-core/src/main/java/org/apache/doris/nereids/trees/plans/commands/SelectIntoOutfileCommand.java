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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Map;
import java.util.Objects;

/**
 * query into outfile command
 */
public class SelectIntoOutfileCommand extends Command implements ForwardWithSync, Explainable {
    private final LogicalPlan logicalQuery;
    private final String format;
    private final Map<String, String> properties;

    public SelectIntoOutfileCommand(LogicalPlan logicalQuery, String format, Map<String, String> properties) {
        super(PlanType.SELECT_INTO_OUTFILE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery);
        this.format = format != null ? format : "csv";
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSelectIntoOutfileCommand(this, context);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) throws Exception {
        return logicalQuery;
    }
}
