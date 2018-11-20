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

import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.EditLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// This new Id generator is just same as TransactionIdGenerator.
// But we can't just use TransactionIdGenerator to replace the old catalog's 'nextId' for compatibility reason.
// cause they are using different edit log operation type.
public class CatalogIdGenerator implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogIdGenerator.class);

    private static final int BATCH_ID_INTERVAL = 1000;

    private long nextId;
    // has to set it to an invalid value, then it will be logged when id is firstly increment
    private long batchEndId = -1;

    private EditLog editLog;

    public CatalogIdGenerator(long initValue) {
        nextId = initValue;
    }

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    // performance is more quickly
    public synchronized long getNextId() {
        if (nextId < batchEndId) {
            ++nextId;
            LOG.debug("get next id: " + nextId);
            return nextId;
        } else {
            batchEndId = batchEndId + BATCH_ID_INTERVAL;
            editLog.logSaveNextId(batchEndId);
            ++nextId;
            return nextId;
        }
    }

    public synchronized void setId(long id) {
        if (id > batchEndId) {
            batchEndId = id;
            nextId = id;
        }
    }

    // just for checkpoint, so no need to synchronize
    public long getBatchEndId() {
        return batchEndId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(batchEndId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        long endId = in.readLong();
        batchEndId = endId;
        // maybe a little rough
        nextId = batchEndId;
    }

}
