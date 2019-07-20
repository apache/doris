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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/*
 *  admin set replicas status properties ("key" = "val", ..);
 *  Required:
 *      "tablet_id" = "1,2,3,4,5,...",
 *      "backend_id" = "10001"
 *      "status" = "bad"
 */
public class AdminSetReplicaStatusStmt extends DdlStmt {

    public static final String TABLET_ID = "tablet_id";
    public static final String BACKEND_ID = "backend_id";
    public static final String STATUS = "status";
    public static final String STATUS_BAD = "bad";

    private Map<String, String> properties;
    private List<Long> tabletIds = Lists.newArrayList();
    private long backendId = -1;
    private String status = null;

    public AdminSetReplicaStatusStmt(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
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
                    String[] ids = val.replace(" ", "").split(",");
                    for (String idStr : ids) {
                        tabletIds.add(Long.valueOf(idStr));
                    }
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
                status = val.toLowerCase();
                if (!status.equals(STATUS_BAD)) {
                    throw new AnalysisException("Do not support setting replica status as " + val);
                }
            } else {
                throw new AnalysisException("Unknown property: " + key);
            }
        }

        if (tabletIds.isEmpty() || backendId == -1 || status == null) {
            throw new AnalysisException("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        }
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public long getBackendId() {
        return backendId;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
