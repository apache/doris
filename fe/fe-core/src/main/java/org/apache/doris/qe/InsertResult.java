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

package org.apache.doris.qe;

import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Lists;

import java.util.List;

// Save the result of last insert operation.
// So that user can view it by executing SHOW LAST INSERT.
public class InsertResult {
    public long txnId;
    public String label;
    public String db;
    public String tbl;
    public TransactionStatus txnStatus;
    public long loadedRows;
    public long filteredRows;

    public InsertResult(long txnId, String label, String db, String tbl, TransactionStatus txnStatus,
                        long loadedRows, long filteredRows) {
        this.txnId = txnId;
        this.label = label;
        this.db = db;
        this.tbl = tbl;
        this.txnStatus = txnStatus;
        this.loadedRows = loadedRows;
        this.filteredRows = filteredRows;
    }

    public void updateResult(TransactionStatus txnStatus, long loadedRows, int filteredRows) {
        this.txnStatus = txnStatus;
        this.loadedRows += loadedRows;
        this.filteredRows += filteredRows;
    }

    public List<String> toRow() {
        List<String> row = Lists.newArrayList();
        row.add(String.valueOf(txnId));
        row.add(label);
        row.add(db);
        row.add(tbl);
        row.add(txnStatus.name());
        row.add(String.valueOf(loadedRows));
        row.add(String.valueOf(filteredRows));
        return row;
    }

}
