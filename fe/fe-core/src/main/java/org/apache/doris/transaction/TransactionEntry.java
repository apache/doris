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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.proto.InternalService;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TTxnParams;

import java.util.ArrayList;
import java.util.List;

public class TransactionEntry {

    private String label = "";
    private Database db;
    private Table table;
    private Backend backend;
    private TTxnParams txnConf;
    private List<InternalService.PDataRow> dataToSend = new ArrayList<>();
    private long rowsInTransaction = 0;

    public TransactionEntry() {
    }

    public TransactionEntry(TTxnParams txnConf, Database db, Table table) {
        this.txnConf = txnConf;
        this.db = db;
        this.table = table;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Database getDb() {
        return db;
    }

    public void setDb(Database db) {
        this.db = db;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public TTxnParams getTxnConf() {
        return txnConf;
    }

    public void setTxnConf(TTxnParams txnConf) {
        this.txnConf = txnConf;
    }

    public boolean isTxnModel() {
        return txnConf != null && txnConf.isNeedTxn();
    }

    public boolean isTxnIniting() {
        return isTxnModel() && txnConf.getTxnId() == -1;
    }

    public boolean isTxnBegin() {
        return isTxnModel() && txnConf.getTxnId() != -1;
    }

    public List<InternalService.PDataRow> getDataToSend() {
        return dataToSend;
    }

    public void setDataToSend(List<InternalService.PDataRow> dataToSend) {
        this.dataToSend = dataToSend;
    }

    public void clearDataToSend() {
        dataToSend.clear();
    }

    public long getRowsInTransaction() {
        return rowsInTransaction;
    }

    public void setRowsInTransaction(long rowsInTransaction) {
        this.rowsInTransaction = rowsInTransaction;
    }
}
