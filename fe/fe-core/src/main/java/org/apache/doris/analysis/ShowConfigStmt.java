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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.system.NodeType;

import com.google.common.collect.ImmutableList;

// show frontend config;
public class ShowConfigStmt extends ShowStmt implements NotFallbackInParser {
    public static final ImmutableList<String> FE_TITLE_NAMES = new ImmutableList.Builder<String>().add("Key").add(
            "Value").add("Type").add("IsMutable").add("MasterOnly").add("Comment").build();

    public static final ImmutableList<String> BE_TITLE_NAMES = new ImmutableList.Builder<String>().add("BackendId")
            .add("Host").add("Key").add("Value").add("Type").add("IsMutable").build();

    private NodeType type;

    private String pattern;

    private long backendId;

    private boolean isShowSingleBackend;

    public ShowConfigStmt(NodeType type, String pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public ShowConfigStmt(NodeType type, String pattern, long backendId) {
        this.type = type;
        this.pattern = pattern;
        this.backendId = backendId;
        this.isShowSingleBackend = true;
    }

    public NodeType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    public long getBackendId() {
        return backendId;
    }

    public boolean isShowSingleBackend() {
        return isShowSingleBackend;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (type == NodeType.FRONTEND) {
            for (String title : FE_TITLE_NAMES) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        } else {
            for (String title : BE_TITLE_NAMES) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        // no need forward to master for backend config
        if (type == NodeType.BACKEND) {
            return RedirectStatus.NO_FORWARD;
        }
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
