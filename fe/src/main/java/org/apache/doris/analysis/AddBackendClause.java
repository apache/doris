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

import com.google.common.base.Strings;

import java.util.List;

public class AddBackendClause extends BackendClause {

    // be in free state is not owned by any cluster
    protected boolean isFree;
    // cluster that backend will be added to 
    protected String destCluster;

    public AddBackendClause(List<String> hostPorts) {
        super(hostPorts);
        this.isFree = true;
        this.destCluster = "";
    }
   
    public AddBackendClause(List<String> hostPorts, boolean isFree) {
        super(hostPorts);
        this.isFree = isFree;
        this.destCluster = "";
    }

    public AddBackendClause(List<String> hostPorts, String destCluster) {
        super(hostPorts);
        this.isFree = false;
        this.destCluster = destCluster;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        if (isFree) {
            sb.append("FREE ");
        }
        sb.append("BACKEND ");

        if (!Strings.isNullOrEmpty(destCluster)) {
            sb.append("to").append(destCluster);
        }

        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    public boolean isFree() {
        return this.isFree;
    } 

    public String getDestCluster() {
        return destCluster;
    }

}
