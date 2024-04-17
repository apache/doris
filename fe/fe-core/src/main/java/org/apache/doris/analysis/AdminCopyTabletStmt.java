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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;

// ADMIN COPY TABLET 10110 PROPERTIES('version' = '1000', backend_id = '10001');
public class AdminCopyTabletStmt extends ShowStmt {
    public static final String PROP_VERSION = "version";
    public static final String PROP_BACKEND_ID = "backend_id";
    public static final String PROP_EXPIRATION = "expiration_minutes";
    private static final long DEFAULT_EXPIRATION_MINUTES = 60;
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("TabletId")
            .add("BackendId").add("Ip").add("Path").add("ExpirationMinutes").add("CreateTableStmt").build();

    private long tabletId;
    private Map<String, String> properties = Maps.newHashMap();
    private long version = -1;
    private long backendId = -1;
    private long expirationMinutes = DEFAULT_EXPIRATION_MINUTES;    // default 60min

    public AdminCopyTabletStmt(long tabletId, Map<String, String> properties) {
        this.tabletId = tabletId;
        this.properties = properties;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getVersion() {
        return version;
    }

    public long getBackendId() {
        return backendId;
    }

    public long getExpirationMinutes() {
        return expirationMinutes;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (properties == null) {
            return;
        }
        try {
            Iterator<Map.Entry<String, String>> iter = properties.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                if (entry.getKey().equalsIgnoreCase(PROP_VERSION)) {
                    version = Long.valueOf(entry.getValue());
                    iter.remove();
                    continue;
                } else if (entry.getKey().equalsIgnoreCase(PROP_BACKEND_ID)) {
                    backendId = Long.valueOf(entry.getValue());
                    iter.remove();
                    continue;
                } else if (entry.getKey().equalsIgnoreCase(PROP_EXPIRATION)) {
                    expirationMinutes = Long.valueOf(entry.getValue());
                    expirationMinutes = Math.min(DEFAULT_EXPIRATION_MINUTES, expirationMinutes);
                    iter.remove();
                    continue;
                }
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid property: " + e.getMessage());
        }

        if (!properties.isEmpty()) {
            throw new AnalysisException("Unknown property: " + properties);
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createStringType()));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

