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
import org.apache.doris.common.Pair;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;

import java.util.LinkedList;
import java.util.List;

public class CancelAlterSystemStmt extends CancelStmt {

    protected List<String> hostPorts;
    private List<Pair<String, Integer>> hostPortPairs;

    public CancelAlterSystemStmt(List<String> hostPorts) {
        this.hostPorts = hostPorts;
        this.hostPortPairs = new LinkedList<Pair<String, Integer>>();
    }

    public List<Pair<String, Integer>> getHostPortPairs() {
        return hostPortPairs;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        for (String hostPort : hostPorts) {
            Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort);
            this.hostPortPairs.add(pair);
        }

        Preconditions.checkState(!this.hostPortPairs.isEmpty());
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CANCEL DECOMMISSION BACKEND ");
        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }
}
