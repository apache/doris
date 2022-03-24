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
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.resource.Tag;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class AddBackendClause extends BackendClause {
    // be in free state is not owned by any cluster
    protected boolean isFree;
    // cluster that backend will be added to 
    protected String destCluster;
    protected Map<String, String> properties = Maps.newHashMap();
    private Tag tag;

    public AddBackendClause(List<String> hostPorts) {
        super(hostPorts);
        this.isFree = true;
        this.destCluster = "";
    }

    public AddBackendClause(List<String> hostPorts, boolean isFree, Map<String, String> properties) {
        super(hostPorts);
        this.isFree = isFree;
        this.destCluster = "";
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
    }

    public AddBackendClause(List<String> hostPorts, String destCluster) {
        super(hostPorts);
        this.isFree = false;
        this.destCluster = destCluster;
    }

    public Tag getTag() {
        return tag;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        super.analyze(analyzer);
        tag = PropertyAnalyzer.analyzeBackendTagProperties(properties, Tag.DEFAULT_BACKEND_TAG);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
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

