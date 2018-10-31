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
import com.google.common.base.Preconditions;
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
public class TransDbProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId")
            .add("DbName")
            .build();
    private Catalog catalog;

    public TransDbProcDir(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(catalog);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        GlobalTransactionMgr transactionMgr = catalog.getCurrentGlobalTransactionMgr();
        List<List<Comparable>> infos = transactionMgr.getDbInfo();
        // order by dbId, asc
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
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        if (catalog == null || Strings.isNullOrEmpty(dbIdStr)) {
            throw new AnalysisException("Db id is null");
        }
        long dbId = -1L;
        try {
            dbId = Long.valueOf(dbIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }

        Database db = catalog.getDb(dbId);
        if (db == null) {
            throw new AnalysisException("Database[" + dbId + "] does not exist.");
        }

        return new TransProcDir(dbId);
    }
}
