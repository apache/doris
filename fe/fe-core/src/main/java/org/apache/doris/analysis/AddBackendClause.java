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

import java.util.List;
import java.util.Map;

public class AddBackendClause extends BackendClause {
    protected Map<String, String> properties = Maps.newHashMap();
    @Getter
    private Map<String, String> tagMap;

    public AddBackendClause(List<String> hostPorts) {
        super(hostPorts);
    }

    public AddBackendClause(List<String> hostPorts, Map<String, String> properties) {
        super(hostPorts);
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        super.analyze(analyzer);
        tagMap = PropertyAnalyzer.analyzeBackendTagsProperties(properties, Tag.DEFAULT_BACKEND_TAG);
        if (!tagMap.containsKey(Tag.TYPE_LOCATION)) {
            throw new AnalysisException(NEED_LOCATION_TAG_MSG);
        }
        if (!Config.enable_multi_tags && tagMap.size() > 1) {
            throw new AnalysisException(MUTLI_TAG_DISABLED_MSG);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        sb.append("BACKEND ");
        for (int i = 0; i < params.size(); i++) {
            sb.append("\"").append(params.get(i)).append("\"");
            if (i != params.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
