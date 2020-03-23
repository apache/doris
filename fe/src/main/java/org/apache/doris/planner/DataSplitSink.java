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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TAggregationType;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TDataSplitSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionKey;
import org.apache.doris.thrift.TPartitionRange;
import org.apache.doris.thrift.TRangePartition;
import org.apache.doris.thrift.TRollupSchema;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

// This class used to split data read from file to batch
@Deprecated
public class DataSplitSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(DataSplitSink.class);

    private final OlapTable targetTable;

    private List<Expr> partitionExprs;
    private List<EtlRangePartitionInfo> partitions;
    private Map<String, RollupSchema> rollups;

    public DataSplitSink(OlapTable targetTable, TupleDescriptor desc) throws AnalysisException {
        this.targetTable = targetTable;
        Map<String, Expr> slotByCol = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < desc.getSlots().size(); ++i) {
            Column col = targetTable.getBaseSchema().get(i);
            SlotDescriptor slotDesc = desc.getSlots().get(i);
            SlotRef slotRef = new SlotRef(slotDesc);
            if (slotDesc.getType().getPrimitiveType() == PrimitiveType.CHAR) {
                slotRef.setType(Type.CHAR);
            }
            slotByCol.put(col.getName(), slotRef);
        }
        rollups = RollupSchema.createRollup(targetTable, slotByCol);

        partitionExprs = Lists.newArrayList();
        partitions = EtlRangePartitionInfo.createParts(targetTable, slotByCol, null, partitionExprs);
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "DPP SINK\n");
        strBuilder.append(prefix + "  TABLE NAME: " + targetTable.getName() + "\n");
        strBuilder.append(prefix + "  PARTITIONS: " + Joiner.on(", ").join(
                Lists.transform(partitions, new Function<EtlRangePartitionInfo, Long>() {
                    @Override
                    public Long apply(EtlRangePartitionInfo etlRangePartitionInfo) {
                        return etlRangePartitionInfo.getId();
                    }
                })) + "\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSplitSink tSplitSink = new TDataSplitSink(Expr.treesToThrift(partitionExprs),
                EtlRangePartitionInfo.listToThrift(partitions),
                RollupSchema.mapToThrift(rollups));

        TDataSink tDataSink = new TDataSink(TDataSinkType.DATA_SPLIT_SINK);
        tDataSink.setSplit_sink(tSplitSink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return new DataPartition(partitionExprs, partitions);
    }

    // Class used to describe rollup schema
    public static class RollupSchema {
        private final KeysType keysType;
        private final List<Expr> keys;
        private final List<Expr> values;
        private final List<AggregateType> valueOps;

        public RollupSchema(KeysType keysType, List<Expr> keys, List<Expr> values, List<AggregateType> valueOps) {
            this.keysType = keysType;
            this.keys = keys;
            this.values = values;
            this.valueOps = valueOps;
        }

        public TRollupSchema toThrift() {
            TRollupSchema tRollupSchema = new TRollupSchema();

            tRollupSchema.setKeys_type(keysType.name());
            tRollupSchema.setKeys(Expr.treesToThrift(keys));
            tRollupSchema.setValues(Expr.treesToThrift(values));
            tRollupSchema.setValue_ops(Lists.transform(valueOps,
                new Function<AggregateType, TAggregationType>() {
                    @Override
                    public TAggregationType apply(AggregateType aggregateType) {
                        return aggregateType.toThrift();
                    }
                })
            );


            return tRollupSchema;
        }

        public static Map<String, TRollupSchema> mapToThrift(Map<String, RollupSchema> rollups) {
            return Maps.transformValues(rollups, new Function<RollupSchema, TRollupSchema>() {
                @Override
                public TRollupSchema apply(RollupSchema rollupSchema) {
                    return rollupSchema.toThrift();
                }
            });
        }

        // Create one rollup schema
        private static RollupSchema createRollupSchema(
                KeysType keysType, List<Column> columns, Map<String, Expr> exprByCol) {
            List<Expr> keys = Lists.newArrayList();
            List<Expr> values = Lists.newArrayList();
            List<AggregateType> valueOps = Lists.newArrayList();

            for (Column column : columns) {
                Expr expr = exprByCol.get(column.getName());
                // NOTE: use CHAR instead of Varchar, because two types is different in storage
                if (column.getDataType() == PrimitiveType.CHAR) {
                    expr.setType(Type.CHAR);
                }
                if (column.isKey()) {
                    // Key column
                    keys.add(expr);
                } else {
                    values.add(expr);
                    valueOps.add(column.getAggregationType());
                }
            }

            return new RollupSchema(keysType, keys, values, valueOps);
        }

        public static Map<String, RollupSchema> createRollup(OlapTable tbl, Map<String, Expr> exprByCol) {
            Map<String, RollupSchema> rollups = Maps.newHashMap();
            // create rollup schema
            // add schema hash to job load info
            for (Map.Entry<Long, Integer> entry : tbl.getIndexIdToSchemaHash().entrySet()) {
                long indexId = entry.getKey();
                rollups.put(String.valueOf(indexId),
                        createRollupSchema(tbl.getKeysType(), tbl.getSchemaByIndexId(indexId), exprByCol));
            }
            return rollups;
        }
    }

    // Partition key used to split data into different partitions.
    public static class EtlPartitionKey {
        public static final EtlPartitionKey POS_INFINITE = new EtlPartitionKey((short) 1);
        public static final EtlPartitionKey NEG_INFINITE = new EtlPartitionKey((short) -1);

        // when sign is less than 0 means this key is negative infinite
        // when sign is greater than 0 means this key is positive infinite
        // when sign is 0 means this key represent by item of 'type' and 'key'
        private final short sign;
        private final PrimitiveType type;
        // Memory content in c language for build-in type.
        // and utf-8 string for others.
        private final String key;

        private EtlPartitionKey(short sign) {
            this.sign = sign;
            this.type = null;
            this.key = null;
        }

        public EtlPartitionKey(PrimitiveType type, String key) {
            this.sign = 0;
            this.type = type;
            this.key = key;
        }

        public static EtlPartitionKey getPosInfinite() {
            return POS_INFINITE;
        }

        public static EtlPartitionKey getNegInfinite() {
            return NEG_INFINITE;
        }

        public TPartitionKey toThrift() {
            TPartitionKey tKey = new TPartitionKey(sign);
            if (sign == 0) {
                tKey.setType(type.toThrift());
                tKey.setKey(key);
            }

            return tKey;
        }
    }

    // One range of a partition
    public static class EtlPartitionRange {
        private final EtlPartitionKey startKey;
        private final EtlPartitionKey endKey;
        private final boolean includeStartKey;
        private final boolean includeEndKey;

        public static EtlPartitionRange closed(EtlPartitionKey startKey, EtlPartitionKey endKey) {
            return new EtlPartitionRange(startKey, true, endKey, true);
        }

        public static EtlPartitionRange closedOpen(EtlPartitionKey startKey, EtlPartitionKey endKey) {
            return new EtlPartitionRange(startKey, true, endKey, false);
        }

        public static EtlPartitionRange all() {
            return closed(EtlPartitionKey.getNegInfinite(), EtlPartitionKey.getPosInfinite());
        }

        private EtlPartitionRange(EtlPartitionKey startKey, boolean includeStartKey,
                                  EtlPartitionKey endKey, boolean includeEndKey) {
            this.startKey = startKey;
            this.includeStartKey = includeStartKey;
            this.endKey = endKey;
            this.includeEndKey = includeEndKey;
        }

        public TPartitionRange toThrift() {
            return new TPartitionRange(startKey.toThrift(), endKey.toThrift(),
                    includeStartKey, includeEndKey);
        }
    }

    // This partition under table
    public static class EtlRangePartitionInfo {
        private long id;
        private EtlPartitionRange range;
        private List<Expr> distributeExprs;
        private int distributeBuckets;

        public EtlRangePartitionInfo(long id, EtlPartitionRange range, List<Expr> distributeExprs,
                int distributeBuckets) {
            this.id = id;
            this.range = range;
            this.distributeExprs = distributeExprs;
            this.distributeBuckets = distributeBuckets;
        }

        public long getId() {
            return id;
        }

        public TRangePartition toThrift(boolean containDist) {
            TRangePartition tRangePartition = new TRangePartition(id, range.toThrift());
            if (containDist) {
                tRangePartition.setDistributed_exprs(Expr.treesToThrift(distributeExprs));
                tRangePartition.setDistribute_bucket(distributeBuckets);
            }
            return tRangePartition;
        }

        public static List<TRangePartition> listToThrift(List<EtlRangePartitionInfo> partitionInfos) {
            return Lists.transform(partitionInfos, new Function<EtlRangePartitionInfo, TRangePartition>() {
                @Override
                public TRangePartition apply(EtlRangePartitionInfo partitionInfo) {
                    return partitionInfo.toThrift(true);
                }
            });
        }

        public static List<TRangePartition> listToNonDistThrift(List<EtlRangePartitionInfo> partitionInfos) {
            return Lists.transform(partitionInfos, new Function<EtlRangePartitionInfo, TRangePartition>() {
                @Override
                public TRangePartition apply(EtlRangePartitionInfo partitionInfo) {
                    return partitionInfo.toThrift(false);
                }
            });
        }

        private static EtlPartitionRange createPartitionRange(
                RangePartitionInfo rangePartitionInfo, long partitionId) throws AnalysisException {
            EtlPartitionRange partitionRange = null;
            // only one partition column
            Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
            PrimitiveType columnType = partitionColumn.getDataType();

            Range<PartitionKey> range = rangePartitionInfo.getRange(partitionId);
            // start key
            EtlPartitionKey startPartitionKey = null;
            PartitionKey startKey = range.lowerEndpoint();
            String startKeyStr = startKey.getKeys().get(0).getStringValue();
            if (startKey.isMinValue()) {
                startPartitionKey = EtlPartitionKey.NEG_INFINITE;
            } else {
                startPartitionKey = new EtlPartitionKey(columnType, startKeyStr);
            }

            // end key
            EtlPartitionKey endPartitionKey = null;
            PartitionKey endKey = range.upperEndpoint();
            String endKeyStr = endKey.getKeys().get(0).getStringValue();
            if (endKey.isMaxValue()) {
                endPartitionKey = EtlPartitionKey.POS_INFINITE;
            } else {
                endPartitionKey = new EtlPartitionKey(columnType, endKeyStr);
            }

            // create partition range
            partitionRange = EtlPartitionRange.closedOpen(startPartitionKey, endPartitionKey);
            return partitionRange;
        }

        public static List<EtlRangePartitionInfo> createParts(
                OlapTable tbl, Map<String, Expr> exprByCol, Set<Long> targetPartitionIds,
                List<Expr> partitionExprs) throws AnalysisException {
            List<EtlRangePartitionInfo> parts = Lists.newArrayList();
            PartitionInfo partInfo = tbl.getPartitionInfo();
            if (partInfo.getType() == PartitionType.RANGE) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partInfo;
                for (Column col : rangePartitionInfo.getPartitionColumns()) {
                    partitionExprs.add(exprByCol.get(col.getName()));
                }
                for (Partition part : tbl.getAllPartitions()) {
                    if (targetPartitionIds != null && !targetPartitionIds.contains(part.getId())) {
                        continue;
                    }
                    DistributionInfo distInfo = part.getDistributionInfo();
                    parts.add(new EtlRangePartitionInfo(
                            part.getId(),
                            createPartitionRange(rangePartitionInfo, part.getId()),
                            DistributionInfo.toDistExpr(tbl, distInfo, exprByCol),
                            distInfo.getBucketNum()));
                }
            } else if (partInfo.getType() == PartitionType.UNPARTITIONED) {
                Preconditions.checkState(tbl.getPartitions().size() == 1, tbl.getId());
                for (Partition part : tbl.getPartitions()) {
                    DistributionInfo distInfo = part.getDistributionInfo();
                    parts.add(new EtlRangePartitionInfo(
                            part.getId(),
                            EtlPartitionRange.all(),
                            DistributionInfo.toDistExpr(tbl, distInfo, exprByCol),
                            distInfo.getBucketNum()));
                }
            } else {
                throw new AnalysisException("Unsupported partition type: " + partInfo.getType());
            }
            return parts;
        }

    }
}
