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
 *      "backend_id" = "10001",
 *  Optional:
 *      "version" = "100",
 *      "last_success_version" = "100",
 *      "last_failed_version" = "-1",
 */
public class AdminSetReplicaVersionStmt extends DdlStmt implements NotFallbackInParser {

    public static final String TABLET_ID = "tablet_id";
    public static final String BACKEND_ID = "backend_id";
    public static final String VERSION = "version";
    public static final String LAST_SUCCESS_VERSION = "last_success_version";
    public static final String LAST_FAILED_VERSION = "last_failed_version";

    private Map<String, String> properties;
    private long tabletId = -1;
    private long backendId = -1;
    private Long version = null;
    private Long lastSuccessVersion = null;
    private Long lastFailedVersion = null;

    public AdminSetReplicaVersionStmt(Map<String, String> properties) {
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
            } else if (key.equalsIgnoreCase(VERSION)) {
                try {
                    version = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid version format: " + val);
                }
                if (version <= 0) {
                    throw new AnalysisException("Required version > 0");
                }
            } else if (key.equalsIgnoreCase(LAST_SUCCESS_VERSION)) {
                try {
                    lastSuccessVersion = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid last success version format: " + val);
                }
                if (lastSuccessVersion <= 0) {
                    throw new AnalysisException("Required last success version > 0");
                }
            } else if (key.equalsIgnoreCase(LAST_FAILED_VERSION)) {
                try {
                    lastFailedVersion = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid last failed version format: " + val);
                }
                if (lastFailedVersion <= 0 && lastFailedVersion != -1) {
                    throw new AnalysisException("Required last failed version > 0 or == -1");
                }
            } else {
                throw new AnalysisException("Unknown property: " + key);
            }
        }

        if (tabletId == -1 || backendId == -1
                || (version == null && lastSuccessVersion == null && lastFailedVersion == null)) {
            throw new AnalysisException("Should add following properties: TABLET_ID, BACKEND_ID, "
                    + "VERSION, LAST_SUCCESS_VERSION, LAST_FAILED_VERSION");
        }
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public Long getVersion() {
        return version;
    }

    public Long getLastSuccessVersion() {
        return lastSuccessVersion;
    }

    public Long getLastFailedVersion() {
        return lastFailedVersion;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
