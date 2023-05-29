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

package org.apache.doris.sdk;

import org.apache.doris.sdk.serialization.RowBatch;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DorisValueReader {

    private final DorisClient dorisClient;
    private final TScanNextBatchParams nextBatchParams;
    private final Lock clientLock = new ReentrantLock();
    private final AtomicBoolean eos = new AtomicBoolean(false);
    private long offset = 0;
    private RowBatch rowBatch;

    public DorisValueReader(Map<String, String> requiredParams) throws IOException, TException {
        this.dorisClient = new DorisClient(requiredParams);
        this.dorisClient.openScanner();
        this.nextBatchParams = new TScanNextBatchParams();
        this.nextBatchParams.setContextId(dorisClient.getContextId());
        initDorisValueReader();
    }

    private void initDorisValueReader() {
        Thread asyncThread = new Thread(new Runnable() {
            @Override
            public void run() {
                clientLock.lock();
                try {
                    while (!eos.get()) {
                        nextBatchParams.setOffset(offset);
                        TScanBatchResult nextResult = dorisClient.getNext(nextBatchParams);
                        eos.set(nextResult.isEos());
                        if (!eos.get()) {
                            RowBatch rowBatch = new RowBatch(nextResult, dorisClient.getSchema()).readArrow();
                            offset += rowBatch.getReadRowCount();
                            rowBatch.close();
                        }
                    }
                } catch (TException e) {
                    throw new RuntimeException(e);
                } finally {
                    clientLock.unlock();
                }
            }
        });
        asyncThread.start();
    }

    public boolean hasNext() throws TException {
        clientLock.lock();
        boolean hasNext = false;
        try {
            // Arrow data was acquired synchronously during the iterative process
            if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                if (rowBatch != null) {
                    offset += rowBatch.getReadRowCount();
                    rowBatch.close();
                }
                nextBatchParams.setOffset(offset);
                TScanBatchResult nextResult = dorisClient.getNext(nextBatchParams);
                eos.set(nextResult.isEos());
                if (!eos.get()) {
                    rowBatch = new RowBatch(nextResult, dorisClient.getSchema()).readArrow();
                }
            }
            hasNext = !eos.get();
        } finally {
            clientLock.unlock();
        }
        return hasNext;
    }

    public Object getNext() throws TException {
        if (!hasNext()) {
            throw new RuntimeException("Currently no data to read.");
        }
        return rowBatch.next();
    }

    public void close() throws TException {
        dorisClient.close();
    }
}
