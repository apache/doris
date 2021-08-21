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
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ResultReceiver {
    private static final Logger LOG = LogManager.getLogger(ResultReceiver.class);
    private boolean isDone    = false;
    private boolean isCancel  = false;
    private long packetIdx = 0;
    private long timeoutTs = 0;
    private TNetworkAddress address;
    private Types.PUniqueId finstId;
    private Long backendId;
    private Thread currentThread;

    public ResultReceiver(TUniqueId tid, Long backendId, TNetworkAddress address, int timeoutMs) {
        this.finstId = Types.PUniqueId.newBuilder().setHi(tid.hi).setLo(tid.lo).build();
        this.backendId = backendId;
        this.address = address;
        this.timeoutTs = System.currentTimeMillis() + timeoutMs;
    }

    public RowBatch getNext(Status status) throws TException {
        if (isDone) {
            return null;
        }
        final RowBatch rowBatch = new RowBatch();
        try {
            while (!isDone && !isCancel) {
                InternalService.PFetchDataRequest request = InternalService.PFetchDataRequest.newBuilder()
                        .setFinstId(finstId)
                        .setRespInAttachment(false)
                        .build();
                
                currentThread = Thread.currentThread();
                Future<InternalService.PFetchDataResult> future = BackendServiceProxy.getInstance().fetchDataAsync(address, request);
                InternalService.PFetchDataResult pResult = null;
                while (pResult == null) {
                    long currentTs = System.currentTimeMillis();
                    if (currentTs >= timeoutTs) {
                        throw new TimeoutException("query timeout");
                    }
                    try {
                        pResult = future.get(timeoutTs - currentTs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // continue to get result
                        LOG.info("future get interrupted Exception");
                        if (isCancel) {
                            status.setStatus(Status.CANCELLED);
                            return null;
                        }
                    }
                }
                TStatusCode code = TStatusCode.findByValue(pResult.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    status.setPstatus(pResult.getStatus());
                    return null;
                } 
 
                rowBatch.setQueryStatistics(pResult.getQueryStatistics());

                if (packetIdx != pResult.getPacketSeq()) {
                    LOG.warn("receive packet failed, expect={}, receive={}", packetIdx, pResult.getPacketSeq());
                    status.setRpcStatus("receive error packet");
                    return null;
                }
    
                packetIdx++;
                isDone = pResult.getEos();

                if (pResult.hasEmptyBatch() && pResult.getEmptyBatch()) {
                    LOG.info("get first empty rowbatch");
                    rowBatch.setEos(false);
                    return rowBatch;
                } else if (pResult.hasRowBatch() && pResult.getRowBatch().size() > 0) {
                    byte[] serialResult = pResult.getRowBatch().toByteArray();
                    TResultBatch resultBatch = new TResultBatch();
                    TDeserializer deserializer = new TDeserializer();
                    deserializer.deserialize(resultBatch, serialResult);
                    rowBatch.setBatch(resultBatch);
                    rowBatch.setEos(pResult.getEos());
                    return rowBatch;
                }
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", finstId, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.addToBlacklist(backendId, e.getMessage());
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception, finstId={}", finstId, e);
            if (e.getMessage().contains("time out")) {
                // if timeout, we set error code to TIMEOUT, and it will not retry querying.
                status.setStatus(new Status(TStatusCode.TIMEOUT, e.getMessage()));
            } else {
                status.setRpcStatus(e.getMessage());
                SimpleScheduler.addToBlacklist(backendId, e.getMessage());
            }
        } catch (TimeoutException e) {
            LOG.warn("fetch result timeout, finstId={}", finstId, e);
            status.setStatus("query timeout");
        } finally {
            synchronized (this) {
                currentThread = null;
            }
        }
        
        if (isCancel) {
            status.setStatus(Status.CANCELLED);
        }
        return rowBatch;
    }

    public void cancel() {
        isCancel = true;
        synchronized (this) {
            if (currentThread != null) {
                // TODO(cmy): we cannot interrupt this thread, or we may throw
                // java.nio.channels.ClosedByInterruptException when we call
                // MysqlChannel.realNetSend -> SocketChannelImpl.write
                // And user will lost connection to Palo
                // currentThread.interrupt();
            }
        }
    }
}
