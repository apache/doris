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

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * create table command
 */
@Developing
public class CreateTableCommand extends Command implements ForwardWithSync {
    public static final Logger LOG = LogManager.getLogger(CreateTableCommand.class);

    private final Optional<LogicalPlan> ctasQuery;
    private final CreateTableInfo createTableInfo;

    public CreateTableCommand(Optional<LogicalPlan> ctasQuery, CreateTableInfo createTableInfo) {
        super(PlanType.CREATE_TABLE_COMMAND);
        this.ctasQuery = ctasQuery;
        this.createTableInfo = Objects.requireNonNull(createTableInfo, "require CreateTableInfo object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        if (!ctasQuery.isPresent()) {
            createTableInfo.validate(ctx);
            CreateTableStmt createTableStmt = createTableInfo.translateToLegacyStmt();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Nereids start to execute the create table command, query id: {}, tableName: {}",
                        ctx.queryId(), createTableInfo.getTableName());
            }
            try {
                Env.getCurrentEnv().createTable(createTableStmt);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e.getCause());
            }
            return;
        }
        LogicalPlan query = ctasQuery.get();
        List<String> ctasCols = createTableInfo.getCtasColumns();
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        // must disable constant folding by be, because be constant folding may return wrong type
        ctx.getSessionVariable().disableConstantFoldingByBEOnce();
        Plan plan = planner.plan(new UnboundResultSink<>(query), PhysicalProperties.ANY, ExplainLevel.NONE);
        if (ctasCols == null) {
            // we should analyze the plan firstly to get the columns' name.
            ctasCols = plan.getOutput().stream().map(NamedExpression::getName).collect(Collectors.toList());
        }
        List<Slot> slots = plan.getOutput();
        if (slots.size() != ctasCols.size()) {
            throw new AnalysisException("ctas column size is not equal to the query's");
        }
        ImmutableList.Builder<ColumnDefinition> columnsOfQuery = ImmutableList.builder();
        for (int i = 0; i < slots.size(); i++) {
            Slot s = slots.get(i);
            DataType dataType = s.getDataType().conversion();
            if (i == 0 && dataType.isStringType()) {
                dataType = VarcharType.createVarcharType(ScalarType.MAX_VARCHAR_LENGTH);
            } else {
                dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                        NullType.class, TinyIntType.INSTANCE);
                dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                        DecimalV2Type.class, DecimalV2Type.SYSTEM_DEFAULT);
                if (s.isColumnFromTable()) {
                    if ((!((SlotReference) s).getTable().isPresent()
                            || !((SlotReference) s).getTable().get().isManagedTable())) {
                        if (createTableInfo.getPartitionTableInfo().inIdentifierPartitions(s.getName())
                                || (createTableInfo.getDistribution() != null
                                && createTableInfo.getDistribution().inDistributionColumns(s.getName()))) {
                            // String type can not be used in partition/distributed column
                            // so we replace it to varchar
                            dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                                    StringType.class, VarcharType.MAX_VARCHAR_TYPE);
                        } else {
                            dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                                    CharacterType.class, StringType.INSTANCE);
                        }
                    }
                } else {
                    if (ctx.getSessionVariable().useMaxLengthOfVarcharInCtas) {
                        dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                                VarcharType.class, VarcharType.MAX_VARCHAR_TYPE);
                        dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                                CharType.class, VarcharType.MAX_VARCHAR_TYPE);
                    }
                }
            }
            // if the column is an expression, we set it to nullable, otherwise according to the nullable of the slot.
            columnsOfQuery.add(new ColumnDefinition(s.getName(), dataType, !s.isColumnFromTable() || s.nullable()));
        }
        List<String> qualifierTableName = RelationUtil.getQualifierName(ctx, createTableInfo.getTableNameParts());
        createTableInfo.validateCreateTableAsSelect(qualifierTableName, columnsOfQuery.build(), ctx);
        CreateTableStmt createTableStmt = createTableInfo.translateToLegacyStmt();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Nereids start to execute the ctas command, query id: {}, tableName: {}",
                    ctx.queryId(), createTableInfo.getTableName());
        }
        try {
            if (Env.getCurrentEnv().createTable(createTableStmt)) {
                return;
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }

        query = UnboundTableSinkCreator.createUnboundTableSink(createTableInfo.getTableNameParts(),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), query);
        try {
            if (!FeConstants.runningUnitTest) {
                new InsertIntoTableCommand(query, Optional.empty(), Optional.empty(), Optional.empty()).run(
                        ctx, executor);
            }
            if (ctx.getState().getStateType() == MysqlStateType.ERR) {
                handleFallbackFailedCtas(ctx);
            }
        } catch (Exception e) {
            handleFallbackFailedCtas(ctx);
            throw new AnalysisException("Failed to execute CTAS Reason: " + e.getMessage(), e);
        }
    }

    void handleFallbackFailedCtas(ConnectContext ctx) {
        try {
            Env.getCurrentEnv().dropTable(new DropTableStmt(false,
                    new TableName(createTableInfo.getCtlName(),
                            createTableInfo.getDbName(), createTableInfo.getTableName()), true));
        } catch (Exception e) {
            // TODO: refactor it with normal error process.
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, e.getMessage());
        }
    }

    public boolean isCtasCommand() {
        return ctasQuery.isPresent();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableCommand(this, context);
    }

    // for test
    public CreateTableInfo getCreateTableInfo() {
        return createTableInfo;
    }
}
