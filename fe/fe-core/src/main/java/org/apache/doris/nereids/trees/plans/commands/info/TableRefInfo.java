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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

/**
 * table ref info
 * baseTableRef
    : multipartIdentifier optScanParams? tableSnapshot? specifiedPartition?
        tabletList? tableAlias sample? relationHint?
    currently  tableNameInfo(multipartIdentifier) and specifiedPartition API exposed.
    if you need write one and use.
 */
public class TableRefInfo {
    private final TableNameInfo tableNameInfo;
    private final TableScanParams scanParams;
    private final TableSnapshot tableSnapShot;
    private final PartitionNamesInfo partitionNamesInfo;
    private final List<Long> tabletIdList;
    private final String tableAlias;
    private final TableSample tableSample;
    private final List<String> relationHints;

    /**
     * constructor
     */
    public TableRefInfo(TableNameInfo tableNameInfo, TableScanParams scanParams,
                        TableSnapshot tableSnapShot, PartitionNamesInfo partitionNamesInfo,
                        List<Long> tabletIdList, String tableAlias,
                        TableSample tableSample, List<String> relationHints) {
        this.tableNameInfo = tableNameInfo;
        this.scanParams = scanParams;
        this.tableSnapShot = tableSnapShot;
        this.partitionNamesInfo = partitionNamesInfo;
        this.tabletIdList = tabletIdList;
        this.tableAlias = tableAlias;
        this.tableSample = tableSample;
        this.relationHints = relationHints;
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
            partitionNamesInfo.validate(ctx);
        }
    }
}
