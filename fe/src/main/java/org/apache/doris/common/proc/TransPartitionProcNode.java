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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.transaction.GlobalTransactionMgr;

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
            .add("CommittedVersion")
            .add("CommittedVersionHash")
            .build();

    private long tid;
    private long tableId;

    public TransPartitionProcNode(long tid, long tableId) {
        this.tid = tid;
        this.tableId = tableId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        GlobalTransactionMgr transactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<List<Comparable>> partitionInfos = transactionMgr.getPartitionTransInfo(tid, tableId);
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
