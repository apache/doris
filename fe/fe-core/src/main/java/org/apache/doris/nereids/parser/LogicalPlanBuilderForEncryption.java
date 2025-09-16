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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.trees.plans.commands.info.SetVarOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**LogicalPlanBuilderForEncryption*/
public class LogicalPlanBuilderForEncryption extends LogicalPlanBuilder {
    private final Map<Pair<Integer, Integer>, String> indexInSqlToString;

    public LogicalPlanBuilderForEncryption(Map<Integer, ParserRuleContext> selectHintMap,
                                           Map<Pair<Integer, Integer>, String> indexInSqlToString) {
        super(selectHintMap);
        this.indexInSqlToString = Objects.requireNonNull(indexInSqlToString, "indexInSqlToString is null");
    }

    // select into outfile clause
    @Override
    public LogicalPlan visitStatementDefault(DorisParser.StatementDefaultContext ctx) {
        if (ctx.outFileClause() != null && ctx.outFileClause().propertyClause() != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.outFileClause().propertyClause();
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitStatementDefault(ctx);
    }

    // export into outfile clause
    @Override
    public BrokerDesc visitWithRemoteStorageSystem(DorisParser.WithRemoteStorageSystemContext ctx) {
        Map<String, String> properties = visitPropertyItemList(ctx.brokerProperties);
        encryptProperty(properties, ctx.brokerProperties.start.getStartIndex(),
                ctx.brokerProperties.stop.getStopIndex());
        return super.visitWithRemoteStorageSystem(ctx);
    }

    // load into outfile clause
    @Override
    public LogicalPlan visitLoad(DorisParser.LoadContext ctx) {
        if (ctx.withRemoteStorageSystem() != null) {
            Map<String, String> properties =
                    new HashMap<>(visitPropertyItemList(ctx.withRemoteStorageSystem().brokerProperties));
            encryptProperty(properties, ctx.withRemoteStorageSystem().brokerProperties.start.getStartIndex(),
                    ctx.withRemoteStorageSystem().brokerProperties.stop.getStopIndex());
        }
        return super.visitLoad(ctx);
    }

    // set password clause
    @Override
    public SetVarOp visitSetPassword(DorisParser.SetPasswordContext ctx) {
        encryptPassword(ctx.pwd.getStartIndex(), ctx.pwd.getStopIndex());
        return super.visitSetPassword(ctx);
    }

    // set ldap password clause
    @Override
    public SetVarOp visitSetLdapAdminPassword(DorisParser.SetLdapAdminPasswordContext ctx) {
        encryptPassword(ctx.pwd.getStartIndex(), ctx.pwd.getStopIndex());
        return super.visitSetLdapAdminPassword(ctx);
    }

    // create catalog clause
    @Override
    public LogicalPlan visitCreateCatalog(DorisParser.CreateCatalogContext ctx) {
        if (ctx.propertyClause() != null) {
            DorisParser.PropertyClauseContext context = ctx.propertyClause();
            encryptProperty(visitPropertyClause(context), context.fileProperties.start.getStartIndex(),
                    context.fileProperties.stop.getStopIndex());
        }
        return super.visitCreateCatalog(ctx);
    }

    // create table clause
    @Override
    public LogicalPlan visitCreateTable(DorisParser.CreateTableContext ctx) {
        // property or ext property
        if (ctx.propertyClause() != null) {
            List<DorisParser.PropertyClauseContext> propertyClauseContexts = ctx.propertyClause();
            for (DorisParser.PropertyClauseContext propertyClauseContext : propertyClauseContexts) {
                if (propertyClauseContext != null) {
                    encryptProperty(visitPropertyClause(propertyClauseContext),
                            propertyClauseContext.fileProperties.start.getStartIndex(),
                            propertyClauseContext.fileProperties.stop.getStopIndex());
                }
            }
        }
        return super.visitCreateTable(ctx);
    }

    // select from tvf
    @Override
    public LogicalPlan visitTableValuedFunction(DorisParser.TableValuedFunctionContext ctx) {
        DorisParser.PropertyItemListContext properties = ctx.properties;
        if (properties != null) {
            encryptProperty(visitPropertyItemList(properties), properties.start.getStartIndex(),
                    properties.stop.getStopIndex());
        }
        return super.visitTableValuedFunction(ctx);
    }

    private void encryptProperty(Map<String, String> properties, int start, int stop) {
        if (MapUtils.isNotEmpty(properties)) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, "=",
                    true, false, true);
            indexInSqlToString.put(Pair.of(start, stop), printableMap.toString());
        }
    }

    private void encryptPassword(int start, int stop) {
        indexInSqlToString.put(Pair.of(start, stop), "'*XXX'");
    }
}
