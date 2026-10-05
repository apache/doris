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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Minimal persistence stub for ES resource.
 *
 * <p>This class exists solely for backward compatibility with older metadata images
 * that contain serialized EsResource entries. All functional ES logic has been moved to
 * {@link org.apache.doris.datasource.es.EsProperties} and
 * {@link org.apache.doris.datasource.es.EsExternalCatalog}.</p>
 *
 * <p>Creating new ES resources is no longer supported. Users should use ES Catalog instead.</p>
 */
@Deprecated
public class EsResource extends Resource {

    @SerializedName("properties")
    private Map<String, String> properties;

    public EsResource() {
        super();
    }

    public EsResource(String name) {
        super(name, Resource.ResourceType.ES);
        properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        throw new DdlException("ES resource is no longer supported. Please use ES Catalog instead.");
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> properties) throws DdlException {
        throw new DdlException("ES resource is no longer supported. Please use ES Catalog instead.");
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }
}
