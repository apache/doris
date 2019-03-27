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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import java.util.List;

public class ColocateTableUtils {

    static Table getColocateTable(Database db, String tableName) {
        Table parentTable;
        db.readLock();
        try {
            parentTable = db.getTable(tableName);
        } finally {
            db.readUnlock();
        }
        return parentTable;
    }

    static void checkTableExist(Table colocateTable, String colocateTableName) throws DdlException {
        if (colocateTable == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_NO_EXIT, colocateTableName);
        }
    }

    static void checkTableType(Table colocateTable) throws DdlException {
        if (colocateTable.type != (Table.TableType.OLAP)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_OLAP_TABLE, colocateTable.getName());
        }
    }

    static void checkBucketNum(OlapTable parentTable, DistributionInfo childDistributionInfo) throws DdlException {
        int parentBucketNum = parentTable.getDefaultDistributionInfo().getBucketNum();
        if (parentBucketNum != childDistributionInfo.getBucketNum()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_SAME_BUCKNUM, parentBucketNum);
        }
    }

    static void checkBucketNum(DistributionInfo oldDistributionInfo, DistributionInfo newDistributionInfo)
            throws DdlException {
        int oldBucketNum = oldDistributionInfo.getBucketNum();
        if (oldBucketNum != newDistributionInfo.getBucketNum()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_SAME_BUCKNUM, oldBucketNum);
        }
    }

    static void checkReplicationNum(OlapTable parentTable, PartitionInfo childPartitionInfo) throws DdlException {
        short childReplicationNum = childPartitionInfo.idToReplicationNum.entrySet().iterator().next().getValue();
        checkReplicationNum(parentTable.getPartitionInfo(), childReplicationNum);
    }

    static void checkReplicationNum(PartitionInfo rangePartitionInfo, short childReplicationNum) throws DdlException {
        short oldReplicationNum = rangePartitionInfo.idToReplicationNum.entrySet().iterator().next().getValue();
        checkReplicationNum(oldReplicationNum, childReplicationNum);
    }

    private static void checkReplicationNum(short oldReplicationNum, short newReplicationNum) throws DdlException {
        if (oldReplicationNum != newReplicationNum) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_MUST_SAME_REPLICAT_NUM, oldReplicationNum);
        }
    }

    static void checkDistributionColumnSizeAndType(OlapTable parentTable, DistributionInfo childDistributionInfo)
            throws DdlException {
        HashDistributionInfo parentDistribution = (HashDistributionInfo) (parentTable).getDefaultDistributionInfo();
        List<Column> parentColumns = parentDistribution.getDistributionColumns();
        List<Column> childColumns = ((HashDistributionInfo) childDistributionInfo).getDistributionColumns();

        int parentColumnSize = parentColumns.size();
        if (parentColumnSize != childColumns.size()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_SAME_DISTRIBUTED_COLUMNS_SIZE,
                    parentColumnSize);
        }

        for (int i = 0; i < parentColumnSize; i++) {
            Type parentColumnType = parentColumns.get(i).getType();
            if (!parentColumnType.equals(childColumns.get(i).getType())) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COLOCATE_TABLE_SAME_DISTRIBUTED_COLUMNS_TYPE,
                        childColumns.get(i).getName(), parentColumnType);
            }
        }
    }

}
