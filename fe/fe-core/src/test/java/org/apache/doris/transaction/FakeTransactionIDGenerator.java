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

import org.apache.doris.persist.EditLog;

import mockit.Mock;
import mockit.MockUp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class FakeTransactionIDGenerator extends MockUp<TransactionIdGenerator> {

    private long currentId = 1000L;

    @Mock
    public void $init() { // CHECKSTYLE IGNORE THIS LINE
        // do nothing
    }

    @Mock
    public void setEditLog(EditLog editLog) {
        // do nothing
    }

    @Mock
    public synchronized long getNextTransactionId() {
        System.out.println("getNextTransactionId is called");
        return currentId++;
    }

    @Mock
    public void write(DataOutput out) throws IOException {
        // do nothing
    }

    @Mock
    public void readFields(DataInput in) throws IOException {
        // do nothing
    }

    public void setCurrentId(long newId) {
        this.currentId = newId;
    }
}
