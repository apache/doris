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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * CatalogProperty to store the properties for catalog.
 */
@Data
public class CatalogProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogProperty.class);

    @SerializedName(value = "resource")
    private String resource;
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private volatile Resource catalogResource = null;

    public CatalogProperty(String resource, Map<String, String> properties) {
        this.resource = resource;
        this.properties = properties;
    }

    private Resource catalogResource() {
        if (catalogResource == null) {
            synchronized (this) {
                if (catalogResource == null) {
                    catalogResource = Env.getCurrentEnv().getResourceMgr().getResource(resource);
                }
            }
        }
        return catalogResource;
    }

    public String getOrDefault(String key, String defaultVal) {
        if (resource == null) {
            return properties.getOrDefault(key, defaultVal);
        } else {
            return catalogResource().getCopiedProperties().getOrDefault(key, defaultVal);
        }
    }

    public Map<String, String> getProperties() {
        if (resource == null) {
            return new HashMap<>(properties);
        } else {
            return catalogResource().getCopiedProperties();
        }
    }

    public void modifyCatalogProps(Map<String, String> props) {
        if (resource == null) {
            properties.putAll(props);
        } else {
            LOG.error("Please change the resource {} properties directly", resource);
        }
    }

    public Map<String, String> getS3HadoopProperties() {
        if (resource == null) {
            return S3Resource.getS3HadoopProperties(properties);
        } else {
            return S3Resource.getS3HadoopProperties(catalogResource().getCopiedProperties());
        }
    }

    public Map<String, String> getHadoopProperties() {
        Map<String, String> hadoopProperties = getProperties();
        hadoopProperties.putAll(getS3HadoopProperties());
        return hadoopProperties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CatalogProperty read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CatalogProperty.class);
    }
}
