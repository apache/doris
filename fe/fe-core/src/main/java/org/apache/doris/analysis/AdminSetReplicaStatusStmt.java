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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

/*
 *  admin set replicas status properties ("key" = "val", ..);
 *  Required:
 *      "tablet_id" = "10010",
 *      "backend_id" = "10001"
 *      "status" = "drop"/"bad"/"ok"
 */
public class AdminSetReplicaStatusStmt extends DdlStmt implements NotFallbackInParser {

    public static final String TABLET_ID = "tablet_id";
    public static final String BACKEND_ID = "backend_id";
    public static final String STATUS = "status";

    private Map<String, String> properties;
    private long tabletId = -1;
    private long backendId = -1;
    private ReplicaStatus status;

    public AdminSetReplicaStatusStmt(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        checkProperties();
    }

    private void checkProperties() throws AnalysisException {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            if (key.equalsIgnoreCase(TABLET_ID)) {
                try {
                    tabletId = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid tablet id format: " + val);
                }
            } else if (key.equalsIgnoreCase(BACKEND_ID)) {
                try {
                    backendId = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid backend id format: " + val);
                }
            } else if (key.equalsIgnoreCase(STATUS)) {
                status = ReplicaStatus.valueOf(val.toUpperCase());
                if (status != ReplicaStatus.BAD && status != ReplicaStatus.OK && status != ReplicaStatus.DROP) {
                    throw new AnalysisException("Do not support setting replica status as " + val);
                }
            } else {
                throw new AnalysisException("Unknown property: " + key);
            }
        }

        if (tabletId == -1 || backendId == -1 || status == null) {
            throw new AnalysisException("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        }
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public ReplicaStatus getStatus() {
        return status;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
