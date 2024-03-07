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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.hadoop.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * add constraint command
 */
public class ShowConstraintsCommand extends Command implements NoForward {

    public static final Logger LOG = LogManager.getLogger(ShowConstraintsCommand.class);
    private final List<String> nameParts;

    /**
     * constructor
     */
    public ShowConstraintsCommand(List<String> nameParts) {
        super(PlanType.SHOW_CONSTRAINTS_COMMAND);
        this.nameParts = nameParts;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        TableIf tableIf = RelationUtil.getDbAndTable(
                RelationUtil.getQualifierName(ctx, nameParts), ctx.getEnv()).value();
        List<List<String>> res = tableIf.getConstraintsMap().entrySet().stream()
                        .map(e -> Lists.newArrayList(e.getKey(),
                                e.getValue().getType().getName(),
                                e.getValue().toString()))
                    .collect(Collectors.toList());
        executor.handleShowConstraintStmt(res);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowConstraintsCommand(this, context);
    }
}
