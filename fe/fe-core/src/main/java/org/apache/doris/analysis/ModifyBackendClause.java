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

import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class ModifyBackendClause extends BackendClause {
    protected Map<String, String> properties = Maps.newHashMap();
    protected Map<String, String> analyzedProperties = Maps.newHashMap();
    private Tag tag = null;
    private Boolean isQueryDisabled = null;
    private Boolean isLoadDisabled = null;

    public ModifyBackendClause(List<String> hostPorts, Map<String, String> properties) {
        super(hostPorts);
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        super.analyze(analyzer);
        tag = PropertyAnalyzer.analyzeBackendTagProperties(properties, null);
        isQueryDisabled = PropertyAnalyzer.analyzeBackendDisableProperties(properties,
                PropertyAnalyzer.PROPERTIES_DISABLE_QUERY, null);
        isLoadDisabled = PropertyAnalyzer.analyzeBackendDisableProperties(properties,
                PropertyAnalyzer.PROPERTIES_DISABLE_LOAD, null);
        if (tag != null) {
            analyzedProperties.put(tag.type, tag.value);
        }
        if (isQueryDisabled != null) {
            analyzedProperties.put(PropertyAnalyzer.PROPERTIES_DISABLE_QUERY, String.valueOf(isQueryDisabled));
        }
        if (isLoadDisabled != null) {
            analyzedProperties.put(PropertyAnalyzer.PROPERTIES_DISABLE_LOAD, String.valueOf(isLoadDisabled));
        }
        if (!properties.isEmpty()) {
            throw new AnalysisException("unknown properties setting for key ("
                    + StringUtils.join(properties.keySet(), ",") + ")");
        }
    }

    public Tag getTag() {
        return tag;
    }

    public Boolean isQueryDisabled() {
        return isQueryDisabled;
    }

    public Boolean isLoadDisabled() {
        return isLoadDisabled;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY BACKEND ");
        for (int i = 0; i < hostPorts.size(); i++) {
            sb.append("\"").append(hostPorts.get(i)).append("\"");
            if (i != hostPorts.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(" SET (");
        for (String key : analyzedProperties.keySet()) {
            sb.append("\"").append(key).append("\"=\"");
            sb.append(analyzedProperties.get(analyzedProperties.get(key))).append("\",");
        }
        if (!analyzedProperties.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(")");
        return sb.toString();
    }
}
