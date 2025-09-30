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
// This file is modified copy of TableRef

package org.apache.doris.info;

import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * table ref info
 * baseTableRef
    : multipartIdentifier optScanParams? tableSnapshot? specifiedPartition?
        tabletList? tableAlias sample? relationHint?
    currently  tableNameInfo(multipartIdentifier) and specifiedPartition API exposed.
    if you need write one and use.
 */
public class TableRefInfo {
    protected JoinOperator joinOp;
    protected String tableAlias;
    protected boolean isMark;
    protected TableNameInfo tableNameInfo;
    private final TableScanParams scanParams;
    private final TableSnapshot tableSnapShot;
    private final PartitionNamesInfo partitionNamesInfo;
    private final List<Long> sampleTabletIds;
    private final TableSample tableSample;
    private final List<String> commonHints;

    /**
     * constructor
     */
    public TableRefInfo(TableNameInfo tableNameInfo, String tableAlias) {
        this(tableNameInfo, null, tableAlias);
    }

    public TableRefInfo(TableNameInfo tableNameInfo, PartitionNamesInfo partitionNamesInfo, String tableAlias) {
        this(tableNameInfo, partitionNamesInfo, tableAlias, new ArrayList<>());
    }

    public TableRefInfo(TableNameInfo tableNameInfo, PartitionNamesInfo partitionNamesInfo,
                        String tableAlias, List<String> relationHints) {
        this(tableNameInfo, partitionNamesInfo, new ArrayList<>(), tableAlias, null, relationHints);
    }

    public TableRefInfo(TableNameInfo tableNameInfo, PartitionNamesInfo partitionNamesInfo,
                        List<Long> tabletIdList, String tableAlias,
                        TableSample tableSample, List<String> relationHints) {
        this(tableNameInfo, null, partitionNamesInfo, tabletIdList, tableAlias, tableSample, relationHints);
    }

    public TableRefInfo(TableNameInfo tableNameInfo, TableSnapshot tableSnapShot,
                        PartitionNamesInfo partitionNamesInfo,
                        List<Long> tabletIdList, String tableAlias,
                        TableSample tableSample, List<String> relationHints) {
        this(tableNameInfo, null, tableSnapShot, partitionNamesInfo,
                tabletIdList, tableAlias, tableSample, relationHints);
    }

    /**
     * TableRefInfo
     */
    public TableRefInfo(TableNameInfo tableNameInfo, TableScanParams scanParams,
                        TableSnapshot tableSnapShot, PartitionNamesInfo partitionNamesInfo,
                        List<Long> sampleTabletIds, String tableAlias,
                        TableSample tableSample, List<String> commonHints) {
        Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        this.tableNameInfo = tableNameInfo;
        this.scanParams = scanParams;
        this.tableSnapShot = tableSnapShot;
        this.partitionNamesInfo = partitionNamesInfo;
        this.sampleTabletIds = sampleTabletIds;
        this.tableAlias = tableAlias;
        this.tableSample = tableSample;
        this.commonHints = commonHints;
    }

    protected TableRefInfo(TableRefInfo other) {
        tableNameInfo = other.tableNameInfo;
        tableAlias = other.tableAlias;
        isMark = other.isMark;
        partitionNamesInfo = other.partitionNamesInfo;
        tableSnapShot = (other.tableSnapShot != null) ? new TableSnapshot(other.tableSnapShot) : null;
        scanParams = other.scanParams;
        tableSample = other.tableSample;
        commonHints = other.commonHints;
        sampleTabletIds = other.sampleTabletIds;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public PartitionNamesInfo getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public void analyze(ConnectContext ctx) throws UserException {
        tableNameInfo.analyze(ctx);
        if (partitionNamesInfo != null) {
            partitionNamesInfo.validate();
        }
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public boolean hasAlias() {
        return tableAlias != null;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    public void setJoinOp(JoinOperator joinOp) {
        this.joinOp = joinOp;
    }

    public boolean isMark() {
        return isMark;
    }

    public void setMark(TupleDescriptor markTuple) {
        this.isMark = markTuple != null;
    }

    public TableScanParams getScanParams() {
        return scanParams;
    }

    public TableSnapshot getTableSnapShot() {
        return tableSnapShot;
    }

    public List<Long> getSampleTabletIds() {
        return sampleTabletIds;
    }

    public TableSample getTableSample() {
        return tableSample;
    }

    public List<String> getCommonHints() {
        return commonHints;
    }

    @Override
    public TableRefInfo clone() {
        return new TableRefInfo(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableNameInfo);
        if (partitionNamesInfo != null) {
            sb.append(partitionNamesInfo.toSql());
        }
        if (tableAlias != null) {
            sb.append(" AS ").append(tableAlias);
        }
        return sb.toString();
    }
}
