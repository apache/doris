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
import org.apache.doris.service.ExecuteEnv;

/**
 * Manages Flight User Session ConnectContext.
 */
public interface FlightSessionsManager {

    /**
     * Resolves an existing ConnectContext for the given peerIdentity.
     * <p>
     *
     * @param peerIdentity identity after authorization
     * @return The ConnectContext or null if no sessionId is given.
     */
    ConnectContext getConnectContext(String peerIdentity);

    /**
     * Creates a ConnectContext object and store it in the local cache, assuming that peerIdentity was already
     * validated.
     *
     * @param peerIdentity identity after authorization
     */
    ConnectContext createConnectContext(String peerIdentity);

    static ConnectContext buildConnectContext(String peerIdentity, UserIdentity userIdentity, String remoteIP) {
        ConnectContext connectContext = new FlightSqlConnectContext(peerIdentity);
        connectContext.setEnv(Env.getCurrentEnv());
        connectContext.setStartTime();
        connectContext.setQualifiedUser(userIdentity.getQualifiedUser());
        connectContext.setCurrentUserIdentity(userIdentity);
        connectContext.setRemoteIP(remoteIP);
        connectContext.setUserQueryTimeout(
                connectContext.getEnv().getAuth().getQueryTimeout(connectContext.getQualifiedUser()));
        connectContext.setUserInsertTimeout(
                connectContext.getEnv().getAuth().getInsertTimeout(connectContext.getQualifiedUser()));

        connectContext.setConnectScheduler(ExecuteEnv.getInstance().getScheduler());
        return connectContext;
    }
}
