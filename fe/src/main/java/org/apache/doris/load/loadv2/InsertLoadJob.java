/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The class is performed to record the finished info of insert load job.
 * It is created after txn is visible which belongs to insert load job.
 * The state of insert load job is always finished, so it will never be scheduled by JobScheduler.
 */
public class InsertLoadJob extends LoadJob {

    private long tableId;

    // only for log replay
    public InsertLoadJob() {
        super();
        this.jobType = EtlJobType.INSERT;
    }

    public InsertLoadJob(String label, long dbId, long tableId, long createTimestamp) {
        super(dbId, label);
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.finishTimestamp = System.currentTimeMillis();
        this.state = JobState.FINISHED;
        this.progress = 100;
        this.jobType = EtlJobType.INSERT;
        this.timeoutSecond = Config.insert_load_default_timeout_second;
    }

    @Override
    protected Set<String> getTableNames() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        database.readLock();
        try {
            Table table = database.getTable(tableId);
            if (table == null) {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            }
            return new HashSet<>(Arrays.asList(table.getName()));
        } finally {
            database.readUnlock();
        }
    }

    @Override
    void executeJob() {
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(tableId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tableId = in.readLong();
    }
}
