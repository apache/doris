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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * HMS resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "hive"
 * PROPERTIES
 * (
 * "type" = "hms",
 * "hive.metastore.uris" = "thrift://172.21.0.44:7004"
 * );
 */
public class HMSResource extends Resource {
    // required
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HMSResource(String name) {
        super(name, ResourceType.HMS);
        properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        List<String> requiredFields = Collections.singletonList(HIVE_METASTORE_URIS);
        for (String field : requiredFields) {
            if (!properties.containsKey(field)) {
                throw new AnalysisException("Missing [" + field + "] in properties.");
            }
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        List<String> requiredFields = Collections.singletonList(HIVE_METASTORE_URIS);
        for (String field : requiredFields) {
            if (!properties.containsKey(field)) {
                throw new DdlException("Missing [" + field + "] in properties.");
            }
        }
        this.properties = properties;
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
