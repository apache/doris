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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.RelationUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * bind an unbound logicalOlapTableSink represent the target table of an insert command
 */
public class BindInsertTargetTable extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return unboundOlapTableSink()
                .thenApply(ctx -> {
                    UnboundOlapTableSink<?> sink = ctx.root;
                    Pair<Database, OlapTable> pair = bind(ctx.cascadesContext, sink);
                    Database database = pair.first;
                    OlapTable table = pair.second;

                    LogicalPlan child = ((LogicalPlan) sink.child());

                    LogicalOlapTableSink<?> boundSink = new LogicalOlapTableSink<>(
                            database,
                            table,
                            bindTargetColumns(table, sink.getColNames()),
                            bindPartitionIds(table, sink.getPartitions()),
                            sink.child());

                    // we need to insert all the columns of the target table although some columns are not mentions.
                    // so we add a projects to supply the default value.

                    if (boundSink.getCols().size() != child.getOutput().size()) {
                        throw new AnalysisException("insert into cols should be corresponding to the query output");
                    }

                    Map<Column, NamedExpression> columnToOutput = Maps.newHashMap();
                    for (int i = 0; i < boundSink.getCols().size(); ++i) {
                        columnToOutput.put(boundSink.getCols().get(i), child.getOutput().get(i));
                    }

                    List<NamedExpression> newOutput = Lists.newArrayList();
                    for (Column column : boundSink.getTargetTable().getFullSchema()) {
                        if (columnToOutput.containsKey(column)) {
                            newOutput.add(columnToOutput.get(column));
                        } else if (column.getDefaultValue() == null) {
                            newOutput.add(new Alias(
                                    new NullLiteral(DataType.fromCatalogType(column.getType())),
                                    column.getName()
                            ));
                        } else {
                            newOutput.add(new Alias(Literal.of(column.getDefaultValue())
                                    .checkedCastTo(DataType.fromCatalogType(column.getType())), column.getName()));
                        }
                    }

                    LogicalProject<?> project = ProjectCollector.INSTANCE.collect(sink);
                    newOutput.addAll(project.getOutputs());

                    return boundSink.withChildren(new LogicalProject<>(newOutput, boundSink.child()));

                }).toRule(RuleType.BINDING_INSERT_TARGET_TABLE);
    }

    private Pair<Database, OlapTable> bind(CascadesContext cascadesContext, UnboundOlapTableSink<? extends Plan> sink) {
        List<String> tableQualifier = RelationUtil.getQualifierName(cascadesContext.getConnectContext(),
                sink.getNameParts());
        Pair<DatabaseIf, TableIf> pair = RelationUtil.getDbAndTable(tableQualifier,
                cascadesContext.getConnectContext().getEnv());
        if (!(pair.second instanceof OlapTable)) {
            throw new AnalysisException("the target table of insert into is not an OLAP table");
        }
        return Pair.of(((Database) pair.first), (OlapTable) pair.second);
    }

    private List<Long> bindPartitionIds(OlapTable table, List<String> partitions) {
        return partitions == null
                ? null
                : partitions.stream().map(pn -> {
                    Partition partition = table.getPartition(pn);
                    if (partition == null) {
                        throw new AnalysisException(String.format("partition %s is not found in table %s",
                                pn, table.getName()));
                    }
                    return partition.getId();
                }).collect(Collectors.toList());
    }

    private List<Column> bindTargetColumns(OlapTable table, List<String> colsName) {
        return colsName == null
                ? table.getFullSchema().stream().filter(Column::isVisible).collect(Collectors.toList())
                : colsName.stream().map(cn -> {
                    Column column = table.getColumn(cn);
                    if (column == null) {
                        throw new AnalysisException(String.format("column %s is not found in table %s",
                                cn, table.getName()));
                    }
                    return column;
                }).collect(Collectors.toList());
    }

    private static class ProjectCollector extends DefaultPlanVisitor<LogicalPlan, Void> {
        public static final ProjectCollector INSTANCE = new ProjectCollector();
        private LogicalProject firstProject = null;

        public LogicalProject collect(Plan plan) {
            firstProject = null;
            plan.accept(this, null);
            return firstProject;
        }

        @Override
        public LogicalPlan visitLogicalProject(LogicalProject<?> project, Void unused) {
            if (firstProject == null) {
                firstProject = project;
            }
            return project;
        }
    }
}
