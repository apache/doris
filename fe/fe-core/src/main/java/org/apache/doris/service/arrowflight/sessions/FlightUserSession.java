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
// This file is copied from

package org.apache.doris.service.arrowflight.sessions;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TResultSinkType;

/**
 * Object holding UserSesssion
 */
public class FlightUserSession {
    private final String username;
    private final long issuedAt;
    private final long expiresAt;
    private final ConnectContext connectContext;

    public FlightUserSession()  {
        this.username = "";
        this.issuedAt = 0;
        this.expiresAt = 0;
        this.connectContext = new ConnectContext();
    }

    public FlightUserSession(String username, long issuedAt, long expiresAt)  {
        this.username = username;
        this.issuedAt = issuedAt;
        this.expiresAt = expiresAt;
        this.connectContext = buildConnectContext();
    }

    public String getUsername() {
        return username;
    }

    public long getIssuedAt() {
        return issuedAt;
    }

    public long getExpiresAt() {
        return expiresAt;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setAuthResult(final UserIdentity userIdentity, final String remoteIp) {
        connectContext.setQualifiedUser(userIdentity.getQualifiedUser());
        connectContext.setCurrentUserIdentity(userIdentity);
        connectContext.setRemoteIP(remoteIp);
    }

    public static ConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        // sessionVariable.internalSession = true;
        sessionVariable.setEnablePipelineEngine(false); // TODO
        sessionVariable.setEnablePipelineXEngine(false); // TODO
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setQualifiedUser(UserIdentity.UNKNOWN.getQualifiedUser());
        connectContext.setCurrentUserIdentity(UserIdentity.UNKNOWN);
        connectContext.setStartTime();
        connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER); // TODO
        connectContext.setResultSinkType(TResultSinkType.ARROW_FLIGHT_PROTOCAL);
        return connectContext;
    }
}
