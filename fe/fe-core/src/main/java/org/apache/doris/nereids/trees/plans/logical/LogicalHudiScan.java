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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hudi.source.COWIncrementalRelation;
import org.apache.doris.datasource.hudi.source.EmptyIncrementalRelation;
import org.apache.doris.datasource.hudi.source.IncrementalRelation;
import org.apache.doris.datasource.hudi.source.MORIncrementalRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableSet;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Hudi scan for Hudi table
 */
public class LogicalHudiScan extends LogicalFileScan {
    private static final Logger LOG = LogManager.getLogger(LogicalHudiScan.class);

    // for hudi incremental read
    private final Optional<TableScanParams> scanParams;
    private final Optional<IncrementalRelation> incrementalRelation;

    /**
     * Constructor for LogicalHudiScan.
     */
    protected LogicalHudiScan(RelationId id, ExternalTable table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            SelectedPartitions selectedPartitions, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams, Optional<IncrementalRelation> incrementalRelation) {
        super(id, table, qualifier, groupExpression, logicalProperties,
                selectedPartitions, tableSample, tableSnapshot);
        Objects.requireNonNull(scanParams, "scanParams should not null");
        Objects.requireNonNull(incrementalRelation, "incrementalRelation should not null");
        this.scanParams = scanParams;
        this.incrementalRelation = incrementalRelation;
    }

    public LogicalHudiScan(RelationId id, ExternalTable table, List<String> qualifier,
            Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot) {
        this(id, table, qualifier, Optional.empty(), Optional.empty(),
                ((HMSExternalTable) table).initHudiSelectedPartitions(tableSnapshot), tableSample, tableSnapshot,
                Optional.empty(), Optional.empty());
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams;
    }

    public Optional<IncrementalRelation> getIncrementalRelation() {
        return incrementalRelation;
    }

    /**
     * replace incremental params as AND expression
     * incr('beginTime'='20240308110257169', 'endTime'='20240308110677278') =>
     * _hoodie_commit_time >= 20240308110257169 and _hoodie_commit_time <= '20240308110677278'
     */
    public Set<Expression> generateIncrementalExpression(List<Slot> slots) {
        if (!incrementalRelation.isPresent()) {
            return Collections.emptySet();
        }
        SlotReference timeField = null;
        for (Slot slot : slots) {
            if ("_hoodie_commit_time".equals(slot.getName())) {
                timeField = (SlotReference) slot;
                break;
            }
        }
        if (timeField == null) {
            return Collections.emptySet();
        }
        StringLiteral upperValue = new StringLiteral(incrementalRelation.get().getEndTs());
        StringLiteral lowerValue = new StringLiteral(incrementalRelation.get().getStartTs());
        ComparisonPredicate less = new LessThanEqual(timeField, upperValue);
        ComparisonPredicate great = incrementalRelation.get().isIncludeStartTime()
                ? new GreaterThanEqual(timeField, lowerValue)
                : new GreaterThan(timeField, lowerValue);
        return ImmutableSet.of(great, less);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalHudiScan",
                "qualified", qualifiedName(),
                "output", getOutput()
        );
    }

    @Override
    public LogicalHudiScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalHudiScan(relationId, (ExternalTable) table, qualifier, groupExpression,
                Optional.of(getLogicalProperties()), selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalHudiScan(relationId, (ExternalTable) table, qualifier,
                groupExpression, logicalProperties, selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    public LogicalHudiScan withSelectedPartitions(SelectedPartitions selectedPartitions) {
        return new LogicalHudiScan(relationId, (ExternalTable) table, qualifier, Optional.empty(),
                Optional.of(getLogicalProperties()), selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    @Override
    public LogicalHudiScan withRelationId(RelationId relationId) {
        return new LogicalHudiScan(relationId, (ExternalTable) table, qualifier, Optional.empty(),
                Optional.empty(), selectedPartitions, tableSample, tableSnapshot,
                scanParams, incrementalRelation);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalHudiScan(this, context);
    }

    /**
     * Set scan params for incremental read
     *
     * @param table should be hudi table
     * @param scanParams including incremental read params
     */
    public LogicalHudiScan withScanParams(HMSExternalTable table, TableScanParams scanParams) {
        Optional<IncrementalRelation> newIncrementalRelation = Optional.empty();
        Optional<TableScanParams> newScanParams = Optional.empty();
        if (scanParams != null && scanParams.incrementalRead()) {
            Map<String, String> optParams = table.getHadoopProperties();
            if (scanParams.getParams().containsKey("beginTime")) {
                optParams.put("hoodie.datasource.read.begin.instanttime", scanParams.getParams().get("beginTime"));
            }
            if (scanParams.getParams().containsKey("endTime")) {
                optParams.put("hoodie.datasource.read.end.instanttime", scanParams.getParams().get("endTime"));
            }
            scanParams.getParams().forEach((k, v) -> {
                if (k.startsWith("hoodie.")) {
                    optParams.put(k, v);
                }
            });
            HoodieTableMetaClient hudiClient = table.getHudiClient();
            try {
                boolean isCowOrRoTable = table.isHoodieCowTable();
                if (isCowOrRoTable) {
                    Map<String, String> serd = table.getRemoteTable().getSd().getSerdeInfo().getParameters();
                    if ("true".equals(serd.get("hoodie.query.as.ro.table"))
                            && table.getRemoteTable().getTableName().endsWith("_ro")) {
                        // Incremental read RO table as RT table, I don't know why?
                        isCowOrRoTable = false;
                        LOG.warn("Execute incremental read on RO table: {}", table.getFullQualifiers());
                    }
                }
                if (hudiClient.getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
                    newIncrementalRelation = Optional.of(new EmptyIncrementalRelation(optParams));
                } else if (isCowOrRoTable) {
                    newIncrementalRelation = Optional.of(new COWIncrementalRelation(
                            optParams, HiveMetaStoreClientHelper.getConfiguration(table), hudiClient));
                } else {
                    newIncrementalRelation = Optional.of(new MORIncrementalRelation(
                            optParams, HiveMetaStoreClientHelper.getConfiguration(table), hudiClient));
                }
            } catch (Exception e) {
                throw new AnalysisException(
                        "Failed to create incremental relation for table: " + table.getFullQualifiers(), e);
            }
        }
        newScanParams = Optional.ofNullable(scanParams);
        return new LogicalHudiScan(relationId, table, qualifier, Optional.empty(),
                Optional.empty(), selectedPartitions, tableSample, tableSnapshot,
                newScanParams, newIncrementalRelation);
    }
}
