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

import org.apache.doris.common.ClientPool;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.thrift.TWaitingTxnStatusResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class MasterTxnExecutor {
    private static final Logger LOG = LogManager.getLogger(MasterTxnExecutor.class);

    private int waitTimeoutMs;
    // the total time of thrift connectTime add readTime and writeTime
    private int thriftTimeoutMs;
    private ConnectContext ctx;

    public MasterTxnExecutor(ConnectContext ctx) {
        this.ctx = ctx;
        this.waitTimeoutMs = ctx.getExecTimeout() * 1000;
        this.thriftTimeoutMs = ctx.getExecTimeout() * 1000;
    }

    private TNetworkAddress getMasterAddress() throws TException {
        ctx.getEnv().checkReadyOrThrowTException();
        String masterHost = ctx.getEnv().getMasterHost();
        int masterRpcPort = ctx.getEnv().getMasterRpcPort();
        return new TNetworkAddress(masterHost, masterRpcPort);
    }

    private FrontendService.Client getClient(TNetworkAddress thriftAddress) throws TException {
        try {
            return ClientPool.frontendPool.borrowObject(thriftAddress, thriftTimeoutMs);
        } catch (Exception e) {
            // may throw NullPointerException. add err msg
            throw new TException("Failed to get master client.", e);
        }
    }

    // Send request to Master
    public TLoadTxnBeginResult beginTxn(TLoadTxnBeginRequest request) throws TException {
        TNetworkAddress thriftAddress = getMasterAddress();

        FrontendService.Client client = getClient(thriftAddress);

        LOG.info("Send begin transaction {} to Master {}", ctx.getStmtId(), thriftAddress);

        boolean isReturnToPool = false;
        try {
            TLoadTxnBeginResult result = client.loadTxnBegin(request);
            isReturnToPool = true;
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                throw new TException("begin txn failed.");
            }
            return result;
        } catch (TTransportException e) {
            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw e;
            }
            if (e.getType() == TTransportException.TIMED_OUT) {
                throw e;
            } else {
                TLoadTxnBeginResult result = client.loadTxnBegin(request);
                isReturnToPool = true;
                return result;
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
    }

    public TWaitingTxnStatusResult getWaitingTxnStatus(TWaitingTxnStatusRequest request) throws TException {
        TNetworkAddress thriftAddress = getMasterAddress();

        FrontendService.Client client = getClient(thriftAddress);

        LOG.info("Send waiting transaction status stmtId={}, txnId={} to Master {}", ctx.getStmtId(),
                request.getTxnId(), thriftAddress);

        boolean isReturnToPool = false;
        try {
            TWaitingTxnStatusResult result = client.waitingTxnStatus(request);
            isReturnToPool = true;
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                throw new TException(
                        "get txn status (id=" + request.getTxnId() + ") failed, status code: " + result.getStatus()
                                .getStatusCode() + ", msg: "
                                + result.getStatus().getErrorMsgs() + ".");
            }
            return result;
        } catch (TTransportException e) {
            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw e;
            }
            if (e.getType() == TTransportException.TIMED_OUT) {
                throw e;
            } else {
                TWaitingTxnStatusResult result = client.waitingTxnStatus(request);
                if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                    throw new TException("commit failed.");
                }
                isReturnToPool = true;
                return result;
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
    }
}
