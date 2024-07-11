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

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.AutoIncrementIdUpdateLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AutoIncrementGenerator implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(AutoIncrementGenerator.class);

    // _MIN_BATCH_SIZE = 4064 in load task
    private static final long BATCH_ID_INTERVAL = 500000;

    @SerializedName(value = "dbId")
    private Long dbId;
    @SerializedName(value = "tableId")
    private Long tableId;
    @SerializedName(value = "columnId")
    private Long columnId;
    private long nextId;
    @SerializedName(value = "batchEndId")
    private long batchEndId;

    private EditLog editLog;

    public AutoIncrementGenerator() {
    }

    public AutoIncrementGenerator(long dbId, long tableId, long columnId, long nextId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnId = columnId;
        this.nextId = nextId;
        this.batchEndId = -1;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public synchronized void applyChange(long columnId, long batchNextId) {
        if (this.columnId == columnId && batchEndId < batchNextId) {
            LOG.info("[auto-inc] AutoIncrementGenerator applyChange, db_id={}, table_id={}, column_id={}, "
                    + "batchNextId={}", dbId, tableId, columnId, batchNextId);
            nextId = batchNextId;
            batchEndId = batchNextId;
        }
    }

    public synchronized Pair<Long, Long> getAutoIncrementRange(long columnId,
            long length, long lowerBound) throws UserException {
        LOG.info("[auto-inc] getAutoIncrementRange request, db_id={}, table_id={}, column_id={}, length={}", dbId,
                tableId, columnId, length);
        if (this.columnId != columnId) {
            throw new UserException("column dosen't exist, columnId=" + columnId);
        }
        long startId = nextId;
        long endId = startId + length;
        if (endId > batchEndId) {
            Preconditions.checkState(editLog != null);
            long newBatchEndId = (endId / BATCH_ID_INTERVAL + 1) * BATCH_ID_INTERVAL;
            AutoIncrementIdUpdateLog info = new AutoIncrementIdUpdateLog(dbId, tableId, columnId, newBatchEndId);
            editLog.logUpdateAutoIncrementId(info);
            batchEndId = newBatchEndId;
            LOG.info("[auto-inc] update batchEndId to {}, db_id={}, table_id={}, column_id={}",
                    newBatchEndId, dbId, tableId, columnId);
        }
        nextId = endId;
        LOG.info("[auto-inc] getAutoIncrementRange result, db_id={}, table_id={}, column_id={}, start={}, length:{}",
                dbId, tableId, columnId, startId, length);
        return Pair.of(startId, length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AutoIncrementGenerator read(DataInput in) throws IOException {
        AutoIncrementGenerator res = GsonUtils.GSON.fromJson(Text.readString(in), AutoIncrementGenerator.class);
        LOG.info("[auto-inc] read AutoIncrementGenerator db_id={}, table_id={}, column_id={}, nextId={}, "
                + "batchEndId={}", res.dbId, res.tableId, res.columnId, res.nextId, res.batchEndId);
        return res;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        nextId = batchEndId;
        LOG.info("[auto-inc] AutoIncrementGenerator set nextId to batchEndId={}", batchEndId);
    }

}
