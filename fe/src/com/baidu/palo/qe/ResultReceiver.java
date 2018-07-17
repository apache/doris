// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.qe;

import com.baidu.palo.common.Status;
import com.baidu.palo.rpc.BackendServiceProxy;
import com.baidu.palo.rpc.PFetchDataRequest;
import com.baidu.palo.rpc.PFetchDataResult;
import com.baidu.palo.rpc.PUniqueId;
import com.baidu.palo.rpc.RpcException;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TResultBatch;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TUniqueId;

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
    private PUniqueId finstId;
    private Long backendId;
    private Thread currentThread;

    public ResultReceiver(TUniqueId tid, Long backendId, TNetworkAddress address, int timeoutMs) {
        this.finstId = new PUniqueId(tid);
        this.backendId = backendId;
        this.address = address;
        this.timeoutTs = System.currentTimeMillis() + timeoutMs;
    }

    public TResultBatch getNext(Status status) throws TException {
        if (isDone) {
            return null;
        }

        try {
            while (!isDone && !isCancel) {
                PFetchDataRequest request = new PFetchDataRequest(finstId);

                currentThread = Thread.currentThread();
                Future<PFetchDataResult> future = BackendServiceProxy.getInstance().fetchDataAsync(address, request);
                PFetchDataResult pResult = null;
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
                TStatusCode code = TStatusCode.findByValue(pResult.status.code);
                if (code != TStatusCode.OK) {
                    status.setPstatus(pResult.status);
                    return null;
                }

                if (packetIdx != pResult.packetSeq) {
                    LOG.warn("receive packet failed, expect={}, receive={}", packetIdx, pResult.packetSeq);
                    status.setRpcStatus("receive error packet");
                    return null;
                }

                packetIdx++;
                isDone = pResult.eos;

                byte[] serialResult = request.getSerializedResult();
                if (serialResult != null && serialResult.length > 0) {
                    TResultBatch resultBatch = new TResultBatch();
                    TDeserializer deserializer = new TDeserializer();
                    deserializer.deserialize(resultBatch, serialResult);
                    return resultBatch;
                }
            }
        } catch (RpcException e) {
            LOG.warn("fetch result rpc exception, finstId={}", finstId, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.updateBlacklistBackends(backendId);
        } catch (ExecutionException e) {
            LOG.warn("fetch result execution exception, finstId={}", finstId, e);
            status.setRpcStatus(e.getMessage());
            SimpleScheduler.updateBlacklistBackends(backendId);
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
        return null;
    }

    public void cancel() {
        isCancel = true;
        synchronized (this) {
            if (currentThread != null) {
                currentThread.interrupt();
            }

        }
    }
}
