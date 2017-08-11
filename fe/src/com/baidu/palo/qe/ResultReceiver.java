// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.qe;

import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.Status;
import com.baidu.palo.thrift.BackendService;
import com.baidu.palo.thrift.PaloInternalServiceVersion;
import com.baidu.palo.thrift.TFetchDataParams;
import com.baidu.palo.thrift.TFetchDataResult;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TPlanFragmentDestination;
import com.baidu.palo.thrift.TResultBatch;
import com.baidu.palo.thrift.TStatusCode;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ResultReceiver {
    private static final Logger LOG = LogManager.getLogger(ResultReceiver.class);
    private boolean isDone    = false;
    private boolean isCancel  = false;
    private int     packetIdx = 0;
    private int              timeoutMs;
    private TNetworkAddress  rootFragmentAddress;
    private TFetchDataResult thriftResult;
    private TFetchDataParams thriftParams;
    private Long backendID;

    public ResultReceiver(TPlanFragmentDestination resultSource, 
            Long backendID, int timeoutMs) {
        this.timeoutMs = timeoutMs;
        thriftParams = new TFetchDataParams();
        thriftParams.setProtocol_version(PaloInternalServiceVersion.V1);
        thriftParams.setFragment_instance_id(resultSource.fragment_instance_id);
        rootFragmentAddress =
                new TNetworkAddress(resultSource.server.hostname, resultSource.server.port);
        this.backendID = backendID;
    }

    public TResultBatch getNext(Status status) {
        if (isDone) {
            return null;
        }
        
        try {
            while (!isDone && !isCancel) {
                getNextFromRpc();
                // check packet num
                if (packetIdx != thriftResult.packet_num) {
                    status.setStatus("receive packet failed, expect " + packetIdx 
                            + " but recept is " + thriftResult.packet_num);
                    return null;
                }
    
                packetIdx++;
                isDone = thriftResult.eos;
                if (thriftResult.result_batch.rows.size() > 0) {
                    return thriftResult.result_batch;
                }
            }
        } catch (org.apache.thrift.transport.TTransportException e)  {
            if (e.getType() == org.apache.thrift.transport.TTransportException.TIMED_OUT) {
                // not set rpcError, for it dosn't retry
                status.setStatus(e.getMessage());
            } else {
                status.setRpcStatus(e.getMessage());
            }
        } catch (Exception e) {
            status.setStatus(e.getMessage());
        }
        
        if (isCancel) {
            status.setStatus(Status.CANCELLED);
        }
        return null;
    }

    public void cancel() {
        isCancel = true;
    }

    void getNextFromRpc() throws Exception {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        boolean isReturnToPool = false;

        try {
            // there is no need to retry for read socket
            address = new TNetworkAddress(rootFragmentAddress.hostname,
                    rootFragmentAddress.port);
            client = ClientPool.backendPool.borrowObject(address, timeoutMs);
            thriftResult = client.fetch_data(thriftParams);
            isReturnToPool = true;
        } catch (org.apache.thrift.transport.TTransportException e) {
            boolean ok = ClientPool.backendPool.reopen(client, timeoutMs);
            if (!ok) {
                String errMsg = "reopen rpc error, address=" + address;
                LOG.warn(errMsg);
                SimpleScheduler.updateBlacklistBackends(this.backendID);
                throw new InternalException(errMsg);
            }
            if (e.getType() == org.apache.thrift.transport.TTransportException.TIMED_OUT) {
                throw e;
            } else {
                thriftResult = client.fetch_data(thriftParams);
                isReturnToPool = true;
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
        
        if (thriftResult != null) {
            if (!thriftResult.getStatus().getStatus_code().equals(TStatusCode.OK)) {
                String errMsg = thriftResult.getStatus().getError_msgs().get(0);
                LOG.warn("root fragment {} return error, errMsg = {}", address, errMsg);
                throw new InternalException(errMsg);
            }
        }
    }
}
