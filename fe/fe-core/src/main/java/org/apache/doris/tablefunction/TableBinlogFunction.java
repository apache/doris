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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RowBinlogTableWrapper;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The implementation of table valued function `binlog`.
 *
 * This TVF is debug-only, used to read binlog<row> data from a table.
 */
public class TableBinlogFunction extends TableValuedFunctionIf {
    public static final String NAME = "binlog";

    private static final String DB = "db";
    private static final String TABLE = "table";
    private static final String PARTITION = "partition";
    private static final String TABLET = "tablet";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(DB, TABLE, PARTITION, TABLET);

    private final String dbName;
    private final String tableName;
    private final PartitionNamesInfo partitionNamesInfo;
    private final Set<Long> specifiedTabletIds;
    private final OlapTable originTable;
    private final RowBinlogTableWrapper rowBinlogTableWrapper;

    public TableBinlogFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (Map.Entry<String, String> e : params.entrySet()) {
            String key = StringUtils.lowerCase(e.getKey());
            if (!PROPERTIES_SET.contains(key)) {
                throw new AnalysisException("'" + e.getKey() + "' is invalid property");
            }
            validParams.put(key, e.getValue());
        }

        this.tableName = StringUtils.trimToEmpty(validParams.get(TABLE));
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("'table' is required for binlog<row>");
        }

        String db = Strings.nullToEmpty(validParams.get(DB)).trim();
        if (db.isEmpty()) {
            ConnectContext ctx = ConnectContext.get();
            if (ctx != null) {
                db = Strings.nullToEmpty(ctx.getDatabase()).trim();
            }
        }
        if (db.isEmpty()) {
            throw new AnalysisException("'db' is required for binlog<row>");
        }
        this.dbName = db;

        this.partitionNamesInfo = parsePartitionNamesInfo(validParams.get(PARTITION));
        this.specifiedTabletIds = parseTabletIds(validParams.get(TABLET));

        DatabaseIf<?> dbIf;
        TableIf tableIf;
        try {
            dbIf = Env.getCurrentEnv().getInternalCatalog().getDbOrMetaException(dbName);
            tableIf = dbIf.getTableOrMetaException(tableName, TableType.OLAP);
        } catch (MetaNotFoundException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (tableIf.getType() != TableType.OLAP) {
            throw new AnalysisException("binlog<row> only supports OLAP table, table=" + tableName);
        }

        this.originTable = (OlapTable) tableIf;
        originTable.readLock();
        try {
            if (!originTable.needRowBinlog()) {
                throw new AnalysisException("binlog<row> is not enabled for table=" + originTable.getName());
            }
            this.rowBinlogTableWrapper = new RowBinlogTableWrapper(originTable);
        } finally {
            originTable.readUnlock();
        }
    }

    @Override
    public String getTableName() {
        return "BinlogTableFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        originTable.readLock();
        try {
            return originTable.getRowBinlogMeta().getSchema(true);
        } finally {
            originTable.readUnlock();
        }
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        // Replace tvf FunctionGenTable with the binlog<row> OlapTable wrapper.
        desc.setTable(rowBinlogTableWrapper);
        OlapScanNode olapScanNode = new OlapScanNode(id, desc, "OlapScanNode",
                ScanContext.builder().clusterName(sv.resolveCloudClusterName()).build());
        olapScanNode.setSelectedIndexInfo(rowBinlogTableWrapper.getBaseIndexId(), true, "binlog<row> read");
        if (specifiedTabletIds != null && !specifiedTabletIds.isEmpty()) {
            olapScanNode.setSpecifiedTabletIds(specifiedTabletIds);
        }
        // Resolve partition names to IDs, same pattern as Nereids PhysicalPlanTranslator.
        if (partitionNamesInfo != null && !partitionNamesInfo.getPartitionNames().isEmpty()) {
            List<Long> partitionIds = Lists.newArrayList();
            originTable.readLock();
            try {
                for (String partName : partitionNamesInfo.getPartitionNames()) {
                    Partition partition = originTable.getPartition(partName);
                    if (partition == null) {
                        throw new IllegalStateException("Partition not found: " + partName);
                    }
                    partitionIds.add(partition.getId());
                }
            } finally {
                originTable.readUnlock();
            }
            olapScanNode.setSelectedPartitionIds(partitionIds);
        } else {
            try {
                olapScanNode.computePartitionInfo();
            } catch (AnalysisException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
        return olapScanNode;
    }

    private static PartitionNamesInfo parsePartitionNamesInfo(String partitions) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitions)) {
            return null;
        }
        List<String> partitionNames = Lists.newArrayList(
                partitions.split(",", -1)).stream().map(String::trim).filter(s -> !s.isEmpty()).collect(
                Collectors.toList());
        if (partitionNames.isEmpty()) {
            throw new AnalysisException("Invalid partition names: " + partitions);
        }
        return new PartitionNamesInfo(false, partitionNames);
    }

    private static Set<Long> parseTabletIds(String tabletIds) throws AnalysisException {
        if (Strings.isNullOrEmpty(tabletIds)) {
            return null;
        }
        try {
            Set<Long> tabletIdSet = Lists.newArrayList(tabletIds.split(",", -1)).stream().map(String::trim).filter(
                    s -> !s.isEmpty()).map(Long::parseLong).collect(Collectors.toSet());
            if (tabletIdSet.isEmpty()) {
                throw new AnalysisException("Invalid tablet ids: " + tabletIds);
            }
            return tabletIdSet;
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet ids: " + tabletIds);
        }
    }
}
