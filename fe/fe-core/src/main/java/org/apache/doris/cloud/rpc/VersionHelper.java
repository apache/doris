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

package org.apache.doris.cloud.rpc;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class VersionHelper {
    private static final Logger LOG = LogManager.getLogger(VersionHelper.class);

    // Call get_version() from meta service, and save the elapsed to summary profile.
    public static Cloud.GetVersionResponse getVersionFromMeta(Cloud.GetVersionRequest req)
            throws RpcException {
        long startAt = System.nanoTime();
        boolean isTableVersion = req.getIsTableVersion();
        try {
            return getVisibleVersion(req);
        } finally {
            SummaryProfile profile = getSummaryProfile();
            if (profile != null) {
                long elapsed = System.nanoTime() - startAt;
                if (isTableVersion) {
                    profile.addGetTableVersionTime(elapsed);
                } else {
                    profile.addGetPartitionVersionTime(elapsed);
                }
            }
        }
    }

    public static Cloud.GetVersionResponse getVisibleVersion(Cloud.GetVersionRequest request) throws RpcException {
        int tryTimes = 0;
        while (tryTimes++ < Config.metaServiceRpcRetryTimes()) {
            Cloud.GetVersionResponse resp = getVisibleVersionInternal(request,
                    Config.default_get_version_from_ms_timeout_second * 1000);
            if (resp != null) {
                if (resp.hasStatus() && (resp.getStatus().getCode() == Cloud.MetaServiceCode.OK
                        || resp.getStatus().getCode() == Cloud.MetaServiceCode.VERSION_NOT_FOUND)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get version from meta service, code: {}", resp.getStatus().getCode());
                    }
                    return resp;
                }

                LOG.warn("get version from meta service failed, status: {}, retry time: {}",
                        resp.getStatus(), tryTimes);
            }
            // sleep random millis, retry rpc failed
            if (tryTimes > Config.metaServiceRpcRetryTimes() / 2) {
                sleepSeveralMs(500, 1000);
            } else {
                sleepSeveralMs(20, 200);
            }
        }

        LOG.warn("get version from meta service failed after retry {} times", tryTimes);
        throw new RpcException("get version from meta service", "failed after retry n times");
    }

    public static Cloud.GetVersionResponse getVisibleVersionInternal(Cloud.GetVersionRequest request, int timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Cloud.GetVersionResponse resp = null;
        try {
            Future<Cloud.GetVersionResponse> future = MetaServiceProxy.getInstance().getVisibleVersionAsync(request);

            while (resp == null) {
                try {
                    resp = future.get(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("get version from meta service: future get interrupted exception");
                }
            }
        } catch (RpcException | ExecutionException | TimeoutException | RuntimeException e) {
            LOG.warn("get version from meta service failed, exception: ", e);
        }
        return resp;
    }

    private static void sleepSeveralMs(int lowerMs, int upperMs) {
        // sleep random millis [lowerMs, upperMs] ms
        try {
            Thread.sleep(lowerMs + (int) (Math.random() * (upperMs - lowerMs)));
        } catch (InterruptedException e) {
            LOG.warn("get snapshot from meta service: sleep get interrupted exception");
        }
    }

    private static SummaryProfile getSummaryProfile() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null) {
            StmtExecutor executor = ctx.getExecutor();
            if (executor != null) {
                return executor.getSummaryProfile();
            }
        }
        return null;
    }

}
