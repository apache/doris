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
import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.resource.Tag;

import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class ModifyBackendClause extends BackendClause {
    protected Map<String, String> properties;
    protected Map<String, String> analyzedProperties = Maps.newHashMap();
    @Getter
    private Map<String, String> tagMap = null;
    private Boolean isQueryDisabled = null;
    private Boolean isLoadDisabled = null;

    public ModifyBackendClause(List<String> hostPorts, Map<String, String> properties) {
        super(hostPorts);
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        super.analyze(analyzer);
        tagMap = PropertyAnalyzer.analyzeBackendTagsProperties(properties, null);
        isQueryDisabled = PropertyAnalyzer.analyzeBackendDisableProperties(properties,
                PropertyAnalyzer.PROPERTIES_DISABLE_QUERY, null);
        isLoadDisabled = PropertyAnalyzer.analyzeBackendDisableProperties(properties,
                PropertyAnalyzer.PROPERTIES_DISABLE_LOAD, null);
        if (!tagMap.isEmpty()) {
            if (!tagMap.containsKey(Tag.TYPE_LOCATION)) {
                throw new AnalysisException(NEED_LOCATION_TAG_MSG);
            }
            if (!Config.enable_multi_tags && tagMap.size() > 1) {
                throw new AnalysisException(MUTLI_TAG_DISABLED_MSG);
            }
            // TODO:
            //  here we can add some privilege check so that only authorized user can modify specified type of tag.
            //  For example, only root user can set tag with type 'computation'
            for (Map.Entry<String, String> entry : tagMap.entrySet()) {
                analyzedProperties.put("tag." + entry.getKey(), entry.getValue());
            }
        }
        if (isQueryDisabled != null) {
            analyzedProperties.put(PropertyAnalyzer.PROPERTIES_DISABLE_QUERY, String.valueOf(isQueryDisabled));
        }
        if (isLoadDisabled != null) {
            analyzedProperties.put(PropertyAnalyzer.PROPERTIES_DISABLE_LOAD, String.valueOf(isLoadDisabled));
        }
        if (!properties.isEmpty()) {
            throw new AnalysisException(
                    "unknown properties setting for key (" + StringUtils.join(properties.keySet(), ",") + ")");
        }
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
        for (int i = 0; i < params.size(); i++) {
            sb.append("\"").append(params.get(i)).append("\"");
            if (i != params.size() - 1) {
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
