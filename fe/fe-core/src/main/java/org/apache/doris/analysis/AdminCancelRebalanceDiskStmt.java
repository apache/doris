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
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdminCancelRebalanceDiskStmt extends DdlStmt {
    private List<Backend> backends = Lists.newArrayList();

    public AdminCancelRebalanceDiskStmt(List<String> backends) {
        ImmutableMap<Long, Backend> backendsInfo = Env.getCurrentSystemInfo().getIdToBackend();
        Map<String, Long> backendsID = new HashMap<String, Long>();
        for (Backend backend : backendsInfo.values()) {
            backendsID.put(NetUtils.getHostPortInAccessibleFormat(backend.getHost(), backend.getHeartbeatPort()),
                    backend.getId());
        }
        if (backends == null) {
            for (Backend backend : backendsInfo.values()) {
                this.backends.add(backend);
            }
        } else {
            for (String backend : backends) {
                if (backendsID.get(backend) != null) {
                    this.backends.add(backendsInfo.get(backendsID.get(backend)));
                    backendsID.remove(backend); // avoid repetition
                }
            }
        }
    }

    public List<Backend> getBackends() {
        return backends;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
