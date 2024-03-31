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

import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CopyIntoProperties extends CopyProperties {

    private static final ImmutableSet<String> DATA_DESC_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(LINE_DELIMITER)
            .add(PARAM_STRIP_OUTER_ARRAY)
            .add(PARAM_FUZZY_PARSE)
            .add(PARAM_NUM_AS_STRING)
            .add(PARAM_JSONPATHS)
            .add(PARAM_JSONROOT)
            .build();

    private static final ImmutableSet<String> EXEC_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(STRICT_MODE).add(LOAD_PARALLELISM)
            .build();

    private static final ImmutableSet<String> COPY_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(TYPE).add(COMPRESSION).add(COLUMN_SEPARATOR).add(SIZE_LIMIT).add(ON_ERROR).add(ASYNC).add(FORCE)
            .addAll(DATA_DESC_PROPERTIES).addAll(EXEC_PROPERTIES).add(USE_DELETE_SIGN).build();

    public CopyIntoProperties(Map<String, String> properties) {
        super(properties, "");
    }

    public void analyze() throws AnalysisException {
        analyzeTypeAndCompression();
        analyzeSizeLimit();
        analyzeOnError();
        analyzeAsync();
        analyzeStrictMode();
        analyzeLoadParallelism();
        analyzeForce();
        analyzeUseDeleteSign();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!COPY_PROPERTIES.contains(entry.getKey())) {
                throw new AnalysisException("Property '" + entry.getKey() + "' is invalid");
            }
        }
    }

    public Map<String, String> getDataDescriptionProperties() {
        return getKeysProperties(DATA_DESC_PROPERTIES);
    }

    public Map<String, String> getExecProperties() {
        Map<String, String> results = getKeysProperties(EXEC_PROPERTIES);
        results.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, String.valueOf(getMaxFilterRatio()));
        return results;
    }

    private Map<String, String> getKeysProperties(ImmutableSet<String> keys) {
        Map<String, String> result = new HashMap<>();
        for (String property : keys) {
            if (properties.containsKey(property)) {
                result.put(removeFilePrefix(property), properties.get(property));
            }
        }
        return result;
    }

    protected void mergeProperties(StageProperties stageProperties) {
        Map<String, String> properties = stageProperties.getDefaultPropertiesWithoutPrefix();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (!this.properties.containsKey(entry.getKey())) {
                this.properties.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
