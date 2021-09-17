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

package org.apache.doris.load.sync;

import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class SyncChannel {
    private static final Logger LOG = LogManager.getLogger(SyncChannel.class);

    protected long id;
    protected long jobId;
    protected Database db;
    protected OlapTable tbl;
    protected List<String> columns;
    protected PartitionNames partitionNames;
    protected String targetTable;
    protected String srcDataBase;
    protected String srcTable;
    protected SyncChannelCallback callback;

    public SyncChannel(long id, SyncJob syncJob, Database db, OlapTable table, List<String> columns, String srcDataBase, String srcTable) {
        this.id = id;
        this.jobId = syncJob.getId();
        this.db = db;
        this.tbl = table;
        this.columns = columns;
        this.targetTable = table.getName().toLowerCase();
        this.srcDataBase = srcDataBase.toLowerCase();
        this.srcTable = srcTable.toLowerCase();
    }

    public void beginTxn(long batchId) throws UserException, TException, TimeoutException,
            InterruptedException, ExecutionException {
    }

    public void abortTxn(String reason) throws TException, TimeoutException,
            InterruptedException, ExecutionException {
    }

    public void commitTxn() throws TException, TimeoutException,
            InterruptedException, ExecutionException {
    }

    public void initTxn(long timeoutSecond) {
    }

    public String getInfo() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(srcDataBase).append(".").append(srcTable);
        stringBuilder.append("->");
        stringBuilder.append(targetTable);
        return stringBuilder.toString();
    }

    public long getId() {
        return id;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public String getSrcDataBase() {
        return srcDataBase;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setCallback(SyncChannelCallback callback) {
        this.callback = callback;
    }

    public void setPartitions(PartitionNames partitionNames) {
        this.partitionNames = partitionNames;
    }
}