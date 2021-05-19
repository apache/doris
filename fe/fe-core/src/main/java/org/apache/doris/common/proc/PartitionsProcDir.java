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

package org.apache.doris.common.proc;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LimitElement;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.TimeUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/*
 * SHOW PROC /dbs/dbId/tableId/partitions, or
 * SHOW PROC /dbs/dbId/tableId/temp_partitions
 * show [temp] partitions' detail info within a table
 */
public class PartitionsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionId").add("PartitionName")
            .add("VisibleVersion").add("VisibleVersionTime").add("VisibleVersionHash")
            .add("State").add("PartitionKey").add("Range").add("DistributionKey")
            .add("Buckets").add("ReplicationNum").add("StorageMedium").add("CooldownTime")
            .add("LastConsistencyCheckTime")
            .add("DataSize")
            .add("IsInMemory")
            .build();

    private Database db;
    private OlapTable olapTable;
    private boolean isTempPartition = false;

    public PartitionsProcDir(Database db, OlapTable olapTable, boolean isTempPartition) {
        this.db = db;
        this.olapTable = olapTable;
        this.isTempPartition = isTempPartition;
    }

    public boolean filter(String columnName, Comparable element, Map<String, Expr> filterMap) throws AnalysisException {
        if (filterMap == null) {
            return true;
        }
        Expr subExpr = filterMap.get(columnName.toLowerCase());
        if (subExpr == null) {
            return true;
        }
        if (subExpr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (subExpr.getChild(1) instanceof StringLiteral && binaryPredicate.getOp() == BinaryPredicate.Operator.EQ) {
                return ((StringLiteral) subExpr.getChild(1)).getValue().equals(element);
            }
            long leftVal;
            long rightVal;
            if (subExpr.getChild(1) instanceof DateLiteral) {
                leftVal = (new DateLiteral((String) element, Type.DATETIME)).getLongValue();
                rightVal = ((DateLiteral) subExpr.getChild(1)).getLongValue();
            } else {
                leftVal = Long.parseLong(element.toString());
                rightVal = ((IntLiteral)subExpr.getChild(1)).getLongValue();
            }
            switch (binaryPredicate.getOp()) {
                case EQ:
                case EQ_FOR_NULL:
                    return leftVal == rightVal;
                case GE:
                    return leftVal >= rightVal;
                case GT:
                    return leftVal > rightVal;
                case LE:
                    return leftVal <= rightVal;
                case LT:
                    return leftVal < rightVal;
                case NE:
                    return leftVal != rightVal;
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        } else {
            return like((String)element, ((StringLiteral) subExpr.getChild(1)).getValue());
        }
        return true;
    }

    public boolean like(String str, String expr) {
        expr = expr.toLowerCase();
        expr = expr.replace(".", "\\.");
        expr = expr.replace("?", ".");
        expr = expr.replace("%", ".*");
        str = str.toLowerCase();
        return str.matches(expr);
    }

    public ProcResult fetchResultByFilter(Map<String, Expr> filterMap, List<OrderByPair> orderByPairs, LimitElement limitElement) throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        List<List<Comparable>> filterPartitionInfos;
        //where
        if (filterMap == null || filterMap.isEmpty()) {
            filterPartitionInfos = partitionInfos;
        } else {
            filterPartitionInfos = Lists.newArrayList();
            for (List<Comparable> partitionInfo : partitionInfos) {
                if (partitionInfo.size() != TITLE_NAMES.size()) {
                    throw new AnalysisException("PartitionInfos.size() " + partitionInfos.size()
                        + " not equal TITLE_NAMES.size() " + TITLE_NAMES.size());
                }
                boolean isNeed = true;
                for (int i = 0; i < partitionInfo.size(); i++) {
                    isNeed = filter(TITLE_NAMES.get(i), partitionInfo.get(i), filterMap);
                    if (!isNeed) {
                        break;
                    }
                }

                if (isNeed) {
                    filterPartitionInfos.add(partitionInfo);
                }
            }
        }

        // order by
        if (orderByPairs != null) {
            ListComparator<List<Comparable>> comparator;
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
            filterPartitionInfos.sort(comparator);
        }

        //limit
        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > filterPartitionInfos.size()) {
                endIndex = filterPartitionInfos.size();
            }
            filterPartitionInfos = filterPartitionInfos.subList(beginIndex,endIndex);
        }

        return getBasicProcResult(filterPartitionInfos);
    }

    public BaseProcResult getBasicProcResult(List<List<Comparable>> partitionInfos) {
        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> info : partitionInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    private List<List<Comparable>> getPartitionInfos() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkState(olapTable.getType() == TableType.OLAP);

        // get info
        List<List<Comparable>> partitionInfos = new ArrayList<List<Comparable>>();
        olapTable.readLock();
        try {
            List<Long> partitionIds;
            PartitionInfo tblPartitionInfo = olapTable.getPartitionInfo();

            // for range partitions, we return partitions in ascending range order by default.
            // this is to be consistent with the behaviour before 0.12
            if (tblPartitionInfo.getType() == PartitionType.RANGE || tblPartitionInfo.getType() == PartitionType.LIST) {
                partitionIds = tblPartitionInfo.getSortedItemMap(isTempPartition).stream()
                        .map(Map.Entry::getKey).collect(Collectors.toList());
            } else {
                Collection<Partition> partitions = isTempPartition ? olapTable.getTempPartitions() : olapTable.getPartitions();
                partitionIds = partitions.stream().map(Partition::getId).collect(Collectors.toList());
            }

            Joiner joiner = Joiner.on(", ");
            for (Long partitionId : partitionIds) {
                Partition partition = olapTable.getPartition(partitionId);

                List<Comparable> partitionInfo = new ArrayList<Comparable>();
                String partitionName = partition.getName();
                partitionInfo.add(partitionId);
                partitionInfo.add(partitionName);
                partitionInfo.add(partition.getVisibleVersion());
                partitionInfo.add(TimeUtils.longToTimeString(partition.getVisibleVersionTime()));
                partitionInfo.add(partition.getVisibleVersionHash());
                partitionInfo.add(partition.getState());

                if (tblPartitionInfo.getType() == PartitionType.RANGE
                        || tblPartitionInfo.getType() == PartitionType.LIST) {
                    List<Column> partitionColumns = tblPartitionInfo.getPartitionColumns();
                    List<String> colNames = new ArrayList<>();
                    for (Column column : partitionColumns) {
                        colNames.add(column.getName());
                    }
                    partitionInfo.add(joiner.join(colNames));
                    partitionInfo.add(tblPartitionInfo.getItem(partitionId).getItems().toString());
                } else {
                    partitionInfo.add("");
                    partitionInfo.add("");
                }

                // distribution
                DistributionInfo distributionInfo = partition.getDistributionInfo();
                if (distributionInfo.getType() == DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> distributionColumns = hashDistributionInfo.getDistributionColumns();
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < distributionColumns.size(); i++) {
                        if (i != 0) {
                            sb.append(", ");
                        }
                        sb.append(distributionColumns.get(i).getName());
                    }
                    partitionInfo.add(sb.toString());
                } else {
                    partitionInfo.add("ALL KEY");
                }

                partitionInfo.add(distributionInfo.getBucketNum());

                partitionInfo.add(tblPartitionInfo.getReplicaAllocation(partitionId).toCreateStmt());

                DataProperty dataProperty = tblPartitionInfo.getDataProperty(partitionId);
                partitionInfo.add(dataProperty.getStorageMedium().name());
                partitionInfo.add(TimeUtils.longToTimeString(dataProperty.getCooldownTimeMs()));

                partitionInfo.add(TimeUtils.longToTimeString(partition.getLastCheckTime()));

                long dataSize = partition.getDataSize();
                Pair<Double, String> sizePair = DebugUtil.getByteUint(dataSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(sizePair.first) + " "
                        + sizePair.second;
                partitionInfo.add(readableSize);
                partitionInfo.add(tblPartitionInfo.getIsInMemory(partitionId));

                partitionInfos.add(partitionInfo);
            }
        } finally {
            olapTable.readUnlock();
        }
        return partitionInfos;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<List<Comparable>> partitionInfos = getPartitionInfos();
        return getBasicProcResult(partitionInfos);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String partitionIdStr) throws AnalysisException {
        long partitionId = -1L;
        try {
            partitionId = Long.valueOf(partitionIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid partition id format: " + partitionIdStr);
        }

        olapTable.readLock();
        try {
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                throw new AnalysisException("Partition[" + partitionId + "] does not exist");
            }

            return new IndicesProcDir(db, olapTable, partition);
        } finally {
            olapTable.readUnlock();
        }
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (int i = 0; i < TITLE_NAMES.size(); ++i) {
            if (TITLE_NAMES.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        return -1;
    }
}
