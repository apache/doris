// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * author: wuyunfeng
 * date: 18/1/5 10:58
 * project: palo2
 */
public class TransPartitionProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("PartitionId")
            .add("PartitionName")
            .add("CommittedVersion")
            .add("CommittedVersionHash")
            .add("State")
            .build();

    private long tid;
    private Database db;
    private OlapTable olapTable;

    public TransPartitionProcNode(long tid, Database db, OlapTable olapTable) {
        this.tid = tid;
        this.db = db;
        this.olapTable = olapTable;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkState(olapTable.getType() == Table.TableType.OLAP);
        Catalog catalog = Catalog.getInstance();
        GlobalTransactionMgr transactionMgr = catalog.getCurrentGlobalTransactionMgr();
        List<List<Comparable>> partitionInfos = transactionMgr.getPartitionTransInfo(tid, db, olapTable);
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
}
