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

package com.baidu.palo.common.proc;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.transaction.GlobalTransactionMgr;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.util.ListComparator;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * author: wuyunfeng
 * date: 18/1/5 10:43
 * project: palo2
 */
public class TransProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TransactionId")
            .add("Label")
            .add("Coordinator")
            .add("TransactionStatus")
            .add("LoadJobSourceType")
            .add("PrepareTime")
            .add("CommitTime")
            .add("FinishTime")
            .add("Reason")
            .add("ErrorReplicasCount")
            .build();
    private long dbId;

    public TransProcDir(long dbId) {
        this.dbId = dbId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        Catalog catalog = Catalog.getInstance();
        GlobalTransactionMgr transactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<List<Comparable>> infos = transactionMgr.getDbTransInfo(dbId);
        // order by transactionId, asc
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(infos, comparator);
        for (List<Comparable> info : infos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tidStr) throws AnalysisException {
        if (tidStr == null || Strings.isNullOrEmpty(tidStr)) {
            throw new AnalysisException("Table id is null");
        }

        long tid = -1L;
        try {
            tid = Long.valueOf(tidStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid transaction id format: " + tid);
        }
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new AnalysisException("Database[" + dbId + "] does not exist.");
        }

        return new TransTablesProcDir(db, tid);
    }
}
