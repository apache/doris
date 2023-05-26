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
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
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
        return unboundOlapTableSink(logicalProject())
                .thenApply(ctx -> {
                    UnboundOlapTableSink<LogicalProject<Plan>> sink = ctx.root;
                    LogicalProject<Plan> project = sink.child();
                    Pair<Database, OlapTable> pair = bind(ctx.cascadesContext, sink);
                    Database database = pair.first;
                    OlapTable table = pair.second;

                    List<NamedExpression> newProjects = Lists.newArrayList(project.child().getOutput());
                    newProjects.addAll(project.getProjects());
                    LogicalProject<?> newProject = new LogicalProject<>(newProjects, project.child());

                    LogicalOlapTableSink<?> boundSink = new LogicalOlapTableSink<>(
                            database,
                            table,
                            bindTargetColumns(table, sink.getColNames()),
                            bindPartitionIds(table, sink.getPartitions()),
                            newProject);

                    // we need to insert all the columns of the target table although some columns are not mentions.
                    // so we add a projects to supply the default value.

                    if (boundSink.getCols().size() != project.getOutputs().size()) {
                        throw new AnalysisException("insert into cols should be corresponding to the query output");
                    }

                    Map<Column, NamedExpression> columnToOutput = Maps.newHashMap();
                    for (int i = 0; i < boundSink.getCols().size(); ++i) {
                        // DataType lhs = DataType.fromCatalogType(boundSink.getCols().get(i).getType());
                        // DataType rhs = project.getOutput().get(i).getDataType();
                        columnToOutput.put(boundSink.getCols().get(i), project.getOutput().get(i));
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
                            newOutput.add(new Alias(
                                    new StringLiteral(column.getDefaultValue()),
                                    column.getName()
                            ));
                        }
                    }
                    newOutput.addAll(project.child().getOutput());
                    return new LogicalProject<>(newOutput, boundSink);

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
}
