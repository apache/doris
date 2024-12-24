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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.rpc.TCustomProtocolFactory;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResultReceiver {
    private static final Logger LOG = LogManager.getLogger(ResultReceiver.class);
    private boolean isDone = false;
    // runStatus represents the running status of the ResultReceiver.
    // If it is not "OK," it indicates cancel.
    private Status runStatus = new Status();
    private long packetIdx = 0;
    private long timeoutTs = 0;
    private TNetworkAddress address;
    private Types.PUniqueId queryId;
    private Types.PUniqueId finstId;
    private Long backendId;
    private Thread currentThread;
    private Future<InternalService.PFetchDataResult> fetchDataAsyncFuture = null;
    public String cancelReason = "";

    int maxMsgSizeOfResultReceiver;

    private void setRunStatus(Status status) {
        runStatus.updateStatus(status.getErrorCode(), status.getErrorMsg());
    }

    private boolean isCancel() {
        return !runStatus.ok();
    }

    public ResultReceiver(TUniqueId queryId, TUniqueId tid, Long backendId, TNetworkAddress address, long timeoutTs,
            int maxMsgSizeOfResultReceiver) {
        this.queryId = Types.PUniqueId.newBuilder().setHi(queryId.hi).setLo(queryId.lo).build();
        this.finstId = Types.PUniqueId.newBuilder().setHi(tid.hi).setLo(tid.lo).build();
        this.backendId = backendId;
        this.address = address;
        this.timeoutTs = timeoutTs;
        this.maxMsgSizeOfResultReceiver = maxMsgSizeOfResultReceiver;
    }

    public RowBatch getNext(Status status) throws TException {
        if (isDone) {
            return null;
        }
        final RowBatch rowBatch = new RowBatch();
        try {
            while (!isDone && !isCancel()) {
                InternalService.PFetchDataRequest request = InternalService.PFetchDataRequest.newBuilder()
                        .setFinstId(finstId)
                        .setRespInAttachment(false)
                        .build();

                currentThread = Thread.currentThread();
                fetchDataAsyncFuture = BackendServiceProxy.getInstance().fetchDataAsync(address, request);
                InternalService.PFetchDataResult pResult = null;

                while (pResult == null) {
                    long currentTs = System.currentTimeMillis();
                    if (currentTs >= timeoutTs) {
                        throw new TimeoutException("query timeout, query id = " + DebugUtil.printId(this.queryId));
                    }
                    try {
                        pResult = fetchDataAsyncFuture.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                    } catch (CancellationException e) {
                        LOG.warn("Future of ResultReceiver of query {} is cancelled", DebugUtil.printId(this.queryId));
                        if (!isCancel()) {
                            LOG.warn("ResultReceiver is not set to cancelled state, this should not happen");
                        } else {
                            status.updateStatus(TStatusCode.CANCELLED, this.cancelReason);
                            return null;
                        }
                    } catch (TimeoutException e) {
                        LOG.warn("Query {} get result timeout, get result duration {} ms",
                                DebugUtil.printId(this.queryId), (timeoutTs - currentTs) / 1000);
                        setRunStatus(Status.TIMEOUT);
                        status.updateStatus(TStatusCode.TIMEOUT, "Query timeout");
                        updateCancelReason("Query timeout");
                        return null;
                    } catch (InterruptedException e) {
                        // continue to get result
                        LOG.warn("Future of ResultReceiver of query {} got interrupted Exception",
                                DebugUtil.printId(this.queryId), e);
                        if (isCancel()) {
                            status.updateStatus(TStatusCode.CANCELLED, "cancelled");
                            return null;
                        }
                    }
                }

                Status resultStatus = new Status(pResult.getStatus());
                if (resultStatus.getErrorCode() != TStatusCode.OK) {
                    status.updateStatus(resultStatus.getErrorCode(), resultStatus.getErrorMsg());
                    return null;
                }

                rowBatch.setQueryStatistics(pResult.getQueryStatistics());

                if (packetIdx != pResult.getPacketSeq()) {
                    LOG.warn("finistId={}, receive packet failed, expect={}, receive={}",
                            DebugUtil.printId(finstId), packetIdx, pResult.getPacketSeq());
                    status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, "receive error packet");
                    return null;
                }

                packetIdx++;
                isDone = pResult.getEos();

                if (pResult.hasEmptyBatch() && pResult.getEmptyBatch()) {
                    LOG.info("finistId={}, get first empty rowbatch", DebugUtil.printId(finstId));
                    rowBatch.setEos(false);
                    return rowBatch;
                } else if (pResult.hasRowBatch() && pResult.getRowBatch().size() > 0) {
                    byte[] serialResult = pResult.getRowBatch().toByteArray();
                    TResultBatch resultBatch = new TResultBatch();
                    TDeserializer deserializer = new TDeserializer(
                            new TCustomProtocolFactory(this.maxMsgSizeOfResultReceiver));
                    try {
                        deserializer.deserialize(resultBatch, serialResult);
                    } catch (TException e) {
                        if (e.getMessage().contains("MaxMessageSize reached")) {
                            throw new TException(
                                    "MaxMessageSize reached, try increase max_msg_size_of_result_receiver");
                        } else {
                            throw e;
                        }
                    }

                    rowBatch.setBatch(resultBatch);
                    rowBatch.setEos(pResult.getEos());
                    return rowBatch;
                }
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", DebugUtil.printId(finstId), e);
            status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
            SimpleScheduler.addToBlacklist(backendId, e.getMessage());
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception, finstId={}", DebugUtil.printId(finstId), e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.updateStatus(TStatusCode.TIMEOUT, e.getMessage());
            } else {
                status.updateStatus(TStatusCode.THRIFT_RPC_ERROR, e.getMessage());
                // Shutdown maybe called by other request, should ignore this case.
                if (!e.getMessage().contains("shutdown")) {
                    SimpleScheduler.addToBlacklist(backendId, e.getMessage());
                }
            }
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, finstId={}", DebugUtil.printId(finstId), e);
            status.updateStatus(TStatusCode.TIMEOUT, "Query timeout");
        } finally {
            synchronized (this) {
                currentThread = null;
            }
        }

        if ((isCancel())) {
            status.updateStatus(runStatus.getErrorCode(), runStatus.getErrorMsg());
        }
        return rowBatch;
    }

    private void updateCancelReason(String reason) {
        if (this.cancelReason.isEmpty()) {
            this.cancelReason = reason;
        } else {
            LOG.warn("Query {} already has cancel reason: {}, new reason {} will be ignored",
                    DebugUtil.printId(queryId), cancelReason, reason);
        }
    }

    public void cancel(Types.PPlanFragmentCancelReason reason, String cancelMessage) {
        if (reason == Types.PPlanFragmentCancelReason.TIMEOUT) {
            setRunStatus(Status.TIMEOUT);
        } else {
            setRunStatus(Status.CANCELLED);
        }

        updateCancelReason(cancelMessage);
        synchronized (this) {
            if (currentThread != null) {
                // TODO(cmy): we cannot interrupt this thread, or we may throw
                // java.nio.channels.ClosedByInterruptException when we call
                // MysqlChannel.realNetSend -> SocketChannelImpl.write
                // And user will lost connection to Palo
                // currentThread.interrupt();
            }
            if (fetchDataAsyncFuture != null) {
                if (fetchDataAsyncFuture.cancel(true)) {
                    LOG.info("ResultReceiver of query {} is cancelled", DebugUtil.printId(queryId));
                } else {
                    LOG.warn("ResultReceiver of query {} cancel failed, typically means the future is finished",
                            DebugUtil.printId(queryId));
                }
            }
        }
    }
}
