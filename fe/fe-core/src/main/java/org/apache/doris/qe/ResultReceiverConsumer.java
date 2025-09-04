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

import org.apache.doris.common.QueryTimeoutException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

public class ResultReceiverConsumer {
    class ReceiverContext {
        public ReceiverContext(ResultReceiver receiver, int offset) {
            this.receiver = receiver;
            this.offset = offset;
        }

        public void createFuture() {
            if (errMsg != null) {
                return;
            }
            try {
                receiver.createFuture(new FutureCallback<InternalService.PFetchDataResult>() {
                    @Override
                    public void onSuccess(InternalService.PFetchDataResult result) {
                        readyOffsets.offer(offset);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        readyOffsets.offer(offset);
                    }
                });
            } catch (RpcException e) {
                setErrMsg(e.getMessage());
                readyOffsets.offer(offset);
            }
        }

        ResultReceiver receiver;
        final int offset;
    }

    private List<ReceiverContext> contexts = Lists.newArrayList();
    private boolean futureInitialized = false;
    private String errMsg;
    private final long timeoutTs;

    void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    BlockingQueue<Integer> readyOffsets;
    int finishedReceivers = 0;

    public ResultReceiverConsumer(List<ResultReceiver> resultReceivers, long timeoutDeadline) {
        for (int i = 0; i < resultReceivers.size(); i++) {
            ReceiverContext context = new ReceiverContext(resultReceivers.get(i), i);
            contexts.add(context);
        }
        this.readyOffsets = new ArrayBlockingQueue<>(resultReceivers.size());
        timeoutTs = timeoutDeadline;
    }

    public boolean isEos() {
        return finishedReceivers == contexts.size();
    }

    public RowBatch getNext(Status status) throws TException, InterruptedException, ExecutionException, UserException {
        if (!futureInitialized) {
            futureInitialized = true;
            for (ReceiverContext context : contexts) {
                context.createFuture();
            }
        }

        Integer offset = readyOffsets.poll(timeoutTs - System.currentTimeMillis(),
                java.util.concurrent.TimeUnit.MILLISECONDS);
        if (offset == null) {
            throw new QueryTimeoutException();
        }
        if (errMsg != null) {
            throw new UserException(errMsg);
        }

        ReceiverContext context = contexts.get(offset);
        RowBatch rowBatch = context.receiver.getNext(status);
        if (!status.ok() || rowBatch == null) {
            return rowBatch;
        }
        if (rowBatch.isEos()) {
            finishedReceivers++;
            rowBatch.setEos(isEos());
        } else {
            context.createFuture();
        }

        return rowBatch;
    }

    public synchronized void cancel(Status reason) {
        for (ReceiverContext context : contexts) {
            context.receiver.cancel(reason);
        }
    }
}
