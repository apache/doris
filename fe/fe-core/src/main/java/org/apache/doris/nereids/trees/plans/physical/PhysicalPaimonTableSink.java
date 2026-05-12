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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecPaimonBucketShuffle;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.PaimonBucketId;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** physical paimon sink */
public class PhysicalPaimonTableSink<CHILD_TYPE extends Plan> extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private static final Logger LOG = LogManager.getLogger(PhysicalPaimonTableSink.class);

    /**
     * constructor
     */
    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                   PaimonExternalTable targetTable,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   Optional<GroupExpression> groupExpression,
                                   LogicalProperties logicalProperties,
                                   CHILD_TYPE child) {
        this(database, targetTable, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.GATHER, null, child);
    }

    /**
     * constructor
     */
    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                   PaimonExternalTable targetTable,
                                   List<Column> cols,
                                   List<NamedExpression> outputExprs,
                                   Optional<GroupExpression> groupExpression,
                                   LogicalProperties logicalProperties,
                                   PhysicalProperties physicalProperties,
                                   Statistics statistics,
                                   CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PAIMON_TABLE_SINK, database, targetTable, cols, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable,
                cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPaimonTableSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                                                 Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable, cols, outputExprs,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, (PaimonExternalTable) targetTable, cols, outputExprs,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, child());
    }

    /**
     * Decide required physical properties for upstream plan.
     *
     * Prefer bucket shuffle for bucket tables when FE can resolve the bucket
     * layout; otherwise fall back to partition shuffle or random partitioning.
     */
    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getSessionVariable().enablePaimonDistributedBucketShuffle) {
            try {
                org.apache.paimon.table.Table paimonTable =
                        ((PaimonExternalTable) targetTable).getPaimonTable(
                                MvccUtil.getSnapshotFromContext((PaimonExternalTable) targetTable));
                if (paimonTable instanceof org.apache.paimon.table.FileStoreTable) {
                    org.apache.paimon.schema.TableSchema schema =
                            ((org.apache.paimon.table.FileStoreTable) paimonTable).schema();
                    int bucketNum = schema.numBuckets();
                    // If bucketNum > 0, it is a bucket table.
                    // We must use bucket shuffle or gather (pass to one).
                    // If we can't do bucket shuffle (e.g. key mismatch), we fallback to GATHER for safety.
                    if (bucketNum > 0) {
                        List<String> bucketKeys = schema.bucketKeys();
                        if (bucketKeys.isEmpty()) {
                            bucketKeys = schema.fieldNames();
                        }
                        List<Expression> args = new ArrayList<>();
                        for (String key : bucketKeys) {
                            Slot slot = null;
                            for (Slot s : child().getOutput()) {
                                if (key.equalsIgnoreCase(s.getName())) {
                                    slot = s;
                                    break;
                                }
                            }
                            if (slot == null) {
                                // Can't find bucket key in output, fallback to GATHER to avoid data corruption
                                return PhysicalProperties.GATHER;
                            }
                            args.add(slot);
                        }
                        args.add(new IntegerLiteral(bucketNum));
                        List<Expression> partitionExprs = new ArrayList<>();
                        partitionExprs.add(new PaimonBucketId(
                                args.get(0), args.subList(1, args.size()).toArray(new Expression[0])));
                        DistributionSpecPaimonBucketShuffle shuffle =
                                new DistributionSpecPaimonBucketShuffle(partitionExprs);
                        return new PhysicalProperties(shuffle);
                    }
                }
            } catch (Exception e) {
                LOG.warn("paimon: failed to access schema for table={}.{}: {}",
                        ((PaimonExternalTable) targetTable).getDbName(), targetTable.getName(), e.getMessage());
                return PhysicalProperties.SINK_RANDOM_PARTITIONED;
            }
        }
        Set<String> partitionNames;
        try {
            partitionNames = ((PaimonExternalTable) targetTable)
                    .getPartitionColumnNames(MvccUtil.getSnapshotFromContext((PaimonExternalTable) targetTable));
        } catch (Exception e) {
            LOG.warn("paimon: failed to get partition names for table={}.{}: {}",
                    ((PaimonExternalTable) targetTable).getDbName(), targetTable.getName(), e.getMessage());
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }
        if (!partitionNames.isEmpty()) {
            List<Expression> partitionExprs = new ArrayList<>();
            for (Slot slot : child().getOutput()) {
                if (partitionNames.contains(slot.getName().toLowerCase())) {
                    partitionExprs.add(slot);
                }
            }
            if (partitionExprs.isEmpty()) {
                LOG.warn("paimon: partition keys not found in child output for table={}.{}; fallback RANDOM",
                        ((PaimonExternalTable) targetTable).getDbName(), targetTable.getName());
                return PhysicalProperties.SINK_RANDOM_PARTITIONED;
            }
            DistributionSpecPaimonBucketShuffle shuffleInfo =
                    new DistributionSpecPaimonBucketShuffle(partitionExprs);
            return new PhysicalProperties(shuffleInfo);
        }
        return PhysicalProperties.SINK_RANDOM_PARTITIONED;
    }
}
