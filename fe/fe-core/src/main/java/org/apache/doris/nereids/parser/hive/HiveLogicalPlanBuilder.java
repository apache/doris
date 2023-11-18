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

package org.apache.doris.nereids.parser.hive;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.LogicalPlanBuilder;
import org.apache.doris.nereids.parser.ParserUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import org.apache.commons.lang3.StringUtils;

/**
 * Build a logical plan tree with unbounded nodes.
 */
public class HiveLogicalPlanBuilder extends LogicalPlanBuilder {

    public static final String DEFAULT_TABLE_ALIAS = "_";

    @Override
    public LogicalPlan visitAliasedQuery(DorisParser.AliasedQueryContext ctx) {
        LogicalPlan plan = withTableAlias(visitQuery(ctx.query()), ctx.tableAlias());
        for (DorisParser.LateralViewContext lateralViewContext : ctx.lateralView()) {
            plan = withGenerate(plan, lateralViewContext);
        }
        return plan;
    }

    private LogicalPlan withTableAlias(LogicalPlan plan, DorisParser.TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        return ParserUtils.withOrigin(ctx.strictIdentifier(), () -> {
            String alias = StringUtils.isEmpty(ctx.strictIdentifier().getText())
                        ? ctx.strictIdentifier().getText() : DEFAULT_TABLE_ALIAS;
            if (null != ctx.identifierList()) {
                throw new ParseException("Do not implemented", ctx);
            }
            return new LogicalSubQueryAlias<>(alias, plan);
        });
    }
}
