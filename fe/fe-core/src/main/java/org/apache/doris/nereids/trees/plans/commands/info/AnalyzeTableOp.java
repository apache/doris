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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * AnalyzeTableOp
 */
public class AnalyzeTableOp extends AnalyzeOp {
    private final TableNameInfo tableNameInfo;
    private List<String> columnNames;
    private PartitionNamesInfo partitionNames;

    // after analyzed
    private long catalogId;
    private long dbId;
    private TableIf table;

    /**
     * AnalyzeTableOp
     */
    public AnalyzeTableOp(TableNameInfo tableNameInfo,
                          PartitionNamesInfo partitionNames,
                          List<String> columnNames,
                          AnalyzeProperties properties) {
        super(properties, PlanType.ANALYZE_TABLE);
        this.tableNameInfo = tableNameInfo;
        this.partitionNames = partitionNames;
        this.columnNames = columnNames;
        this.analyzeProperties = properties;
    }

    /**
     * AnalyzeTableOp
     */
    public AnalyzeTableOp(AnalyzeProperties analyzeProperties, TableNameInfo tableNameInfo,
                          List<String> columnNames, long dbId, TableIf table) throws AnalysisException {
        super(analyzeProperties, PlanType.ANALYZE_TABLE);
        this.tableNameInfo = tableNameInfo;
        this.columnNames = columnNames;
        this.dbId = dbId;
        this.table = table;
        String catalogName = tableNameInfo.getCtl();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(catalogName);
        this.catalogId = catalog.getId();
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public long getDbId() {
        return dbId;
    }

    public TableIf getTable() {
        return table;
    }

    public TableNameInfo getTblName() {
        return tableNameInfo;
    }

    public Set<String> getColumnNames() {
        if (columnNames == null) {
            return null;
        }
        return new HashSet<>(columnNames);
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public void setCatalogId(long catalogId) {
        this.catalogId = catalogId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public void setTable(TableIf table) {
        this.table = table;
    }

    /**
     * getPartitionNames
     */
    public Set<String> getPartitionNames() {
        if (partitionNames == null || partitionNames.getPartitionNames() == null || partitionNames.isStar()) {
            return Collections.emptySet();
        }
        Set<String> partitions = Sets.newHashSet();
        partitions.addAll(partitionNames.getPartitionNames());
        return partitions;
    }

    /**
     * isStarPartition
     * @return for OLAP table, only in overwrite situation, overwrite auto detect partition
     *         for External table, all partitions.
     */
    public boolean isStarPartition() {
        if (partitionNames == null) {
            return false;
        }
        return partitionNames.isStar();
    }

    public long getPartitionCount() {
        if (partitionNames == null) {
            return 0;
        }
        return partitionNames.getCount();
    }

    public boolean isPartitionOnly() {
        return partitionNames != null;
    }

    /**
     * isSamplingPartition
     */
    public boolean isSamplingPartition() {
        if (!(table instanceof HMSExternalTable) || partitionNames != null) {
            return false;
        }
        int partNum = ConnectContext.get().getSessionVariable().getExternalTableAnalyzePartNum();
        if (partNum == -1 || partitionNames != null) {
            return false;
        }
        return table instanceof HMSExternalTable && table.getPartitionNames().size() > partNum;
    }

    public long getCatalogId() {
        return catalogId;
    }
}
