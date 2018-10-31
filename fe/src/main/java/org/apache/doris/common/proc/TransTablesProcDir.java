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
import org.apache.doris.common.util.ListComparator;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * author: wuyunfeng
 * date: 18/1/5 19:15
 * project: palo2
 */
public class TransTablesProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TableId")
            .add("TableName")
            .add("PartitionNum")
            .add("State")
            .build();

    private Database db;
    private long tid;

    public TransTablesProcDir(Database db, long tid) {
        this.db = db;
        this.tid = tid;
    }


    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tableIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        if (Strings.isNullOrEmpty(tableIdStr)) {
            throw new AnalysisException("TableIdStr is null");
        }

        long tableId = -1L;
        try {
            tableId = Long.valueOf(tableIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid table id format: " + tableIdStr);
        }

        Table table = null;
        db.readLock();
        try {
            table = db.getTable(tableId);
        } finally {
            db.readUnlock();
        }
        if (table == null) {
            throw new AnalysisException("Table[" + tableId + "] does not exist");
        }

        return new TransPartitionProcNode(tid, db, (OlapTable) table);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);

        // get info
        GlobalTransactionMgr transactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<List<Comparable>> tableInfos = transactionMgr.getTableTransInfo(tid, db);
        // sort by table id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(tableInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (List<Comparable> info : tableInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }
}
