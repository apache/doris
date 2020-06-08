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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.doris.persist.EditLog;

public class TransactionIdGenerator {

    public static final long NEXT_ID_INIT_VALUE = 1;
    private static final int BATCH_ID_INTERVAL = 1000;
    
    private long nextId = NEXT_ID_INIT_VALUE;
    // has to set it to an invalid value, then it will be logged when id is firstly increment
    private long batchEndId = 0;
    
    private EditLog editLog;
    
    public TransactionIdGenerator() {
    }
    
    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }
    
    // performance is more quickly
    public synchronized long getNextTransactionId() {
        if (nextId < batchEndId) {
            ++ nextId;
            return nextId;
        } else {
            batchEndId = batchEndId + BATCH_ID_INTERVAL;
            editLog.logSaveTransactionId(batchEndId);
            ++ nextId;
            return nextId;
        }
    }

    public synchronized void initTransactionId(long id) {
        if (id > batchEndId) {
            batchEndId = id;
            nextId = id;
        }
    }
    
    // this two function used to read snapshot or write snapshot
    public void write(DataOutput out) throws IOException {
        out.writeLong(batchEndId);
    }
    public void readFields(DataInput in) throws IOException {
        batchEndId = in.readLong();
        // maybe a little rough
        nextId = batchEndId;
    }
}
