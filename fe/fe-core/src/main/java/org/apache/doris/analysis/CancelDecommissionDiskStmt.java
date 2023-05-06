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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Preconditions;

import java.util.List;

public class CancelDecommissionDiskStmt extends CancelAlterSystemStmt {

    protected String hostPort;

    protected HostInfo hostInfo;

    protected List<String> rootPaths;

    protected CancelDecommissionDiskStmt(String hostPort, List<String> rootPaths) {
        this.hostPort = hostPort;
        this.rootPaths = rootPaths;
        this.hostInfo = null;
    }

    public HostInfo getHostInfo() {
        return hostInfo;
    }

    public List<String> getRootPaths() {
        return rootPaths;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        hostInfo = SystemInfoService.getIpHostAndPort(hostPort, true);
        Preconditions.checkState(hostInfo != null);
        Preconditions.checkState(!rootPaths.isEmpty());
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CANCEL DECOMMISSION BACKEND DISK ");
        sb.append("\"").append(hostPort).append("\" ");
        for (int i = 0; i < rootPaths.size(); i++) {
            sb.append("\"").append(rootPaths.get(i)).append("\"");
            if (i != rootPaths.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
