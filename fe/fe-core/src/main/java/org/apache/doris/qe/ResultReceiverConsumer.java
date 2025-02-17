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

import org.apache.doris.common.Status;

import com.google.common.collect.Lists;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ResultReceiverConsumer {
    class ReceiverContext {
        public ReceiverContext(ResultReceiver receiver, int offset) {
            this.receiver = receiver;
            this.offset = offset;
        }

        public void createFuture() {
            future = executor.submit(() -> {
                readyOffsets.offer(offset);
                RowBatch rowBatch = receiver.getNext(status);
                return rowBatch;

            });
        }

        ResultReceiver receiver;
        Status status = new Status();
        Future<RowBatch> future;
        final int offset;
    }

    private final ExecutorService executor;
    private List<ReceiverContext> contexts = Lists.newArrayList();
    boolean futureInitialized = false;

    BlockingQueue<Integer> readyOffsets;
    int finishedReceivers = 0;

    public ResultReceiverConsumer(List<ResultReceiver> resultReceivers) {
        for (int i = 0; i < resultReceivers.size(); i++) {
            ReceiverContext context = new ReceiverContext(resultReceivers.get(i), i);
            contexts.add(context);
        }
        this.executor = Executors.newFixedThreadPool(resultReceivers.size());
        readyOffsets = new ArrayBlockingQueue<>(resultReceivers.size());
    }

    public RowBatch getNext(Status status) throws TException, InterruptedException, ExecutionException {
        if (!futureInitialized) {
            futureInitialized = true;
            for (ReceiverContext context : contexts) {
                context.createFuture();
            }
        }

        ReceiverContext context = contexts.get(readyOffsets.take());
        RowBatch rowBatch = context.future.get();
        if (!context.status.ok()) {
            status.updateStatus(context.status.getErrorCode(), context.status.getErrorMsg());
            return rowBatch;
        }
        if (rowBatch.isEos()) {
            finishedReceivers++;
            if (finishedReceivers != contexts.size()) {
                rowBatch.setEos(false);
            }
        } else {
            context.createFuture();
        }

        return rowBatch;
    }
}
