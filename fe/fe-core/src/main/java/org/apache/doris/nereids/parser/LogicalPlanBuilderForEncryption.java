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
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.InsertTableContext;
import org.apache.doris.nereids.DorisParser.JobFromToClauseContext;
import org.apache.doris.nereids.DorisParser.SupportedDmlStatementContext;
import org.apache.doris.nereids.trees.plans.commands.info.SetVarOp;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**LogicalPlanBuilderForEncryption*/
public class LogicalPlanBuilderForEncryption extends LogicalPlanBuilder {
    private static final Set<String> STAGE_ADDITIONAL_SENSITIVE_KEYS = new HashSet<>();
    private static final Set<String> ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS = new HashSet<>();

    static {
        STAGE_ADDITIONAL_SENSITIVE_KEYS.add("ak");
        STAGE_ADDITIONAL_SENSITIVE_KEYS.add("sk");

        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("sasl.jaas.config");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("property.sasl.jaas.config");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("aws.access_key");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("property.aws.access_key");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("aws.secret_key");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("property.aws.secret_key");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("aws.session_key");
        ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS.add("property.aws.session_key");
    }

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

    // grant user identity clause
    @Override
    public UserDesc visitGrantUserIdentify(DorisParser.GrantUserIdentifyContext ctx) {
        if (ctx.pwd != null) {
            encryptPassword(ctx.pwd.getStartIndex(), ctx.pwd.getStopIndex());
        }
        return super.visitGrantUserIdentify(ctx);
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

    @Override
    public LogicalPlan visitCreateRoutineLoad(DorisParser.CreateRoutineLoadContext ctx) {
        if (ctx.propertyClause() != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.propertyClause();
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        if (ctx.customProperties != null) {
            encryptProperty(visitPropertyItemList(ctx.customProperties),
                    ctx.customProperties.start.getStartIndex(),
                    ctx.customProperties.stop.getStopIndex(),
                    ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS);
        }
        return super.visitCreateRoutineLoad(ctx);
    }

    // create repository clause (CREATE [READ ONLY] REPOSITORY ... WITH <backend> ... PROPERTIES(...))
    @Override
    public LogicalPlan visitCreateRepository(DorisParser.CreateRepositoryContext ctx) {
        if (ctx.storageBackend() != null && ctx.storageBackend().properties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.storageBackend().properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitCreateRepository(ctx);
    }

    // alter repository clause (ALTER REPOSITORY ... PROPERTIES(...))
    @Override
    public LogicalPlan visitAlterRepository(DorisParser.AlterRepositoryContext ctx) {
        if (ctx.propertyClause() != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.propertyClause();
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitAlterRepository(ctx);
    }

    @Override
    public LogicalPlan visitAlterResource(DorisParser.AlterResourceContext ctx) {
        if (ctx.propertyClause() != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.propertyClause();
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitAlterResource(ctx);
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

    @Override
    public LogicalPlan visitCreateDatabase(DorisParser.CreateDatabaseContext ctx) {
        if (ctx.propertyClause() != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.propertyClause();
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitCreateDatabase(ctx);
    }

    @Override
    public LogicalPlan visitCreateStage(DorisParser.CreateStageContext ctx) {
        if (ctx.properties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex(),
                    STAGE_ADDITIONAL_SENSITIVE_KEYS);
        }
        return super.visitCreateStage(ctx);
    }

    // create storage vault clause
    @Override
    public LogicalPlan visitCreateStorageVault(DorisParser.CreateStorageVaultContext ctx) {
        if (ctx.properties != null && ctx.properties.fileProperties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitCreateStorageVault(ctx);
    }

    // create authentication integration clause
    @Override
    public LogicalPlan visitCreateAuthenticationIntegration(DorisParser.CreateAuthenticationIntegrationContext ctx) {
        if (ctx.properties != null && ctx.properties.fileProperties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitCreateAuthenticationIntegration(ctx);
    }

    // alter storage vault clause
    @Override
    public LogicalPlan visitAlterStorageVault(DorisParser.AlterStorageVaultContext ctx) {
        if (ctx.properties != null && ctx.properties.fileProperties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitAlterStorageVault(ctx);
    }

    @Override
    public LogicalPlan visitAlterRoutineLoad(DorisParser.AlterRoutineLoadContext ctx) {
        if (ctx.properties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        if (ctx.propertyItemList() != null) {
            encryptProperty(visitPropertyItemList(ctx.propertyItemList()),
                    ctx.propertyItemList().start.getStartIndex(),
                    ctx.propertyItemList().stop.getStopIndex(),
                    ROUTINE_LOAD_ADDITIONAL_SENSITIVE_KEYS);
        }
        return super.visitAlterRoutineLoad(ctx);
    }

    // alter authentication integration properties clause
    @Override
    public LogicalPlan visitAlterAuthenticationIntegrationProperties(
            DorisParser.AlterAuthenticationIntegrationPropertiesContext ctx) {
        if (ctx.properties != null && ctx.properties.fileProperties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitAlterAuthenticationIntegrationProperties(ctx);
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

    @Override
    public LogicalPlan visitCreateResource(DorisParser.CreateResourceContext ctx) {
        if (ctx.properties != null) {
            DorisParser.PropertyClauseContext propertyClauseContext = ctx.properties;
            encryptProperty(visitPropertyClause(propertyClauseContext),
                    propertyClauseContext.fileProperties.start.getStartIndex(),
                    propertyClauseContext.fileProperties.stop.getStopIndex());
        }
        return super.visitCreateResource(ctx);
    }

    // create job select tvf
    @Override
    public LogicalPlan visitCreateScheduledJob(DorisParser.CreateScheduledJobContext ctx) {
        if (ctx.supportedDmlStatement() != null) {
            SupportedDmlStatementContext supportedDmlStatementContext = ctx.supportedDmlStatement();
            visitInsertTable((InsertTableContext) supportedDmlStatementContext);
        } else if (ctx.jobFromToClause() != null) {
            JobFromToClauseContext jobFromToClauseContext = ctx.jobFromToClause();
            encryptProperty(visitPropertyItemList(jobFromToClauseContext.sourceProperties),
                    jobFromToClauseContext.sourceProperties.start.getStartIndex(),
                    jobFromToClauseContext.sourceProperties.stop.getStopIndex());

        }
        return super.visitCreateScheduledJob(ctx);
    }

    // alter job select tvf
    @Override
    public LogicalPlan visitAlterJob(DorisParser.AlterJobContext ctx) {
        SupportedDmlStatementContext supportedDmlStatementContext = ctx.supportedDmlStatement();
        if (ctx.supportedDmlStatement() != null) {
            visitInsertTable((InsertTableContext) supportedDmlStatementContext);
        } else if (ctx.jobFromToClause() != null) {
            JobFromToClauseContext jobFromToClauseContext = ctx.jobFromToClause();
            encryptProperty(visitPropertyItemList(jobFromToClauseContext.sourceProperties),
                    jobFromToClauseContext.sourceProperties.start.getStartIndex(),
                    jobFromToClauseContext.sourceProperties.stop.getStopIndex());

        }
        return super.visitAlterJob(ctx);
    }

    private void encryptProperty(Map<String, String> properties, int start, int stop) {
        encryptProperty(properties, start, stop, null);
    }

    private void encryptProperty(Map<String, String> properties, int start, int stop,
            Set<String> additionalSensitiveKeys) {
        if (MapUtils.isNotEmpty(properties)) {
            DatasourcePrintableMap<String, String> printableMap = new DatasourcePrintableMap<>(properties, "=",
                    true, false, true);
            printableMap.setAdditionalSensitiveKeys(additionalSensitiveKeys);
            indexInSqlToString.put(Pair.of(start, stop), printableMap.toString());
        }
    }

    private void encryptPassword(int start, int stop) {
        indexInSqlToString.put(Pair.of(start, stop), "'*XXX'");
    }
}
