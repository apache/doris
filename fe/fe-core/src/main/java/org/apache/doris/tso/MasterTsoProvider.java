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

package org.apache.doris.tso;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetCurrentTsoResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

public final class MasterTsoProvider {
    private MasterTsoProvider() {
    }

    public static long getCurrentTso(ConnectContext context) {
        Env env = context.getEnv();
        if (env.isMaster()) {
            return requirePositive(env.getTSOService().getTSO());
        }

        TNetworkAddress address = new TNetworkAddress(env.getMasterHost(), env.getMasterRpcPort());
        FrontendService.Client client = null;
        boolean reusable = false;
        try {
            int timeoutMs = context.getExecTimeoutS() * 1000;
            client = ClientPool.frontendPool.borrowObject(address, timeoutMs);
            TGetCurrentTsoResult result = client.getCurrentTso();
            reusable = true;
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                throw new AnalysisException("Failed to get current TSO from Master FE: "
                        + result.getStatus().getErrorMsgs());
            }
            if (!result.isSetTso()) {
                throw new AnalysisException("Master FE did not return a current TSO");
            }
            return requirePositive(result.getTso());
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new AnalysisException("Failed to get current TSO from Master FE: " + e.getMessage(), e);
        } finally {
            if (client != null) {
                if (reusable) {
                    ClientPool.frontendPool.returnObject(address, client);
                } else {
                    ClientPool.frontendPool.invalidateObject(address, client);
                }
            }
        }
    }

    private static long requirePositive(long tso) {
        if (tso <= 0) {
            throw new AnalysisException("Master FE returned a non-positive TSO");
        }
        return tso;
    }
}
