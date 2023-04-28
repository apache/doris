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

package org.apache.doris.analysis;

import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * Used for load refactor, as an adapter for original load stmt, will proxy to insert stmt or original load stmt, chosen
 * by configuration
 */
public class UnifiedLoadStmt extends DdlStmt {

    private final StatementBase proxyStmt;

    public UnifiedLoadStmt(LabelName label, List<DataDescription> dataDescriptions,
            BrokerDesc brokerDesc, String cluster, Map<String, String> properties, String comment, LoadType loadType) {
        final ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().isEnableUnifiedLoad()) {
            switch (loadType) {
                case BROKER_LOAD:
                    proxyStmt = new BrokerLoadStmt(label, dataDescriptions, brokerDesc, properties, comment);
                    break;
                case MYSQL_LOAD:
                default:
                    throw new IllegalStateException("does not support load type: " + loadType);
            }
        } else {
            proxyStmt = new LoadStmt(label, dataDescriptions, brokerDesc, cluster, properties, comment);
        }
    }

    public void init() {
        Preconditions.checkNotNull(proxyStmt, "impossible state, proxy stmt should be not null");
        proxyStmt.setOrigStmt(getOrigStmt());
        proxyStmt.setUserInfo(getUserInfo());
    }

    public StatementBase getProxyStmt() {
        return proxyStmt;
    }
}
