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

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * CatalogProperty to store the properties for catalog.
 */
@Data
public class CatalogProperty implements Writable {
    @SerializedName(value = "properties")
    private Map<String, String> properties = Maps.newHashMap();

    public String getOrDefault(String key, String defaultVal) {
        return properties.getOrDefault(key, defaultVal);
    }

    // todo: remove and use HdfsResource
    public Map<String, String> getDfsProperties() {
        Map<String, String> dfsProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(HdfsResource.HADOOP_FS_PREFIX)
                    || entry.getKey().equals(HdfsResource.HADOOP_USER_NAME)) {
                // todo: still missing properties like hadoop.xxx
                dfsProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return dfsProperties;
    }

    // todo: remove and use S3Resource
    public Map<String, String> getS3Properties() {
        Map<String, String> s3Properties = Maps.newHashMap();
        if (properties.containsKey(S3Resource.S3_ACCESS_KEY)) {
            s3Properties.put("fs.s3a.access.key", properties.get(S3Resource.S3_ACCESS_KEY));
            s3Properties.put(S3Resource.S3_ACCESS_KEY, properties.get(S3Resource.S3_ACCESS_KEY));
        }
        if (properties.containsKey(S3Resource.S3_SECRET_KEY)) {
            s3Properties.put("fs.s3a.secret.key", properties.get(S3Resource.S3_SECRET_KEY));
            s3Properties.put(S3Resource.S3_SECRET_KEY, properties.get(S3Resource.S3_SECRET_KEY));
        }
        if (properties.containsKey(S3Resource.S3_ENDPOINT)) {
            s3Properties.put("fs.s3a.endpoint", properties.get(S3Resource.S3_ENDPOINT));
            s3Properties.put(S3Resource.S3_ENDPOINT, properties.get(S3Resource.S3_ENDPOINT));
        }
        if (properties.containsKey(S3Resource.S3_REGION)) {
            s3Properties.put("fs.s3a.endpoint.region", properties.get(S3Resource.S3_REGION));
            s3Properties.put(S3Resource.S3_REGION, properties.get(S3Resource.S3_REGION));
        }
        if (properties.containsKey(S3Resource.S3_MAX_CONNECTIONS)) {
            s3Properties.put("fs.s3a.connection.maximum", properties.get(S3Resource.S3_MAX_CONNECTIONS));
            s3Properties.put(S3Resource.S3_MAX_CONNECTIONS, properties.get(S3Resource.S3_MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Resource.S3_REQUEST_TIMEOUT_MS)) {
            s3Properties.put("fs.s3a.connection.request.timeout", properties.get(S3Resource.S3_REQUEST_TIMEOUT_MS));
            s3Properties.put(S3Resource.S3_REQUEST_TIMEOUT_MS, properties.get(S3Resource.S3_REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Resource.S3_CONNECTION_TIMEOUT_MS)) {
            s3Properties.put("fs.s3a.connection.timeout", properties.get(S3Resource.S3_CONNECTION_TIMEOUT_MS));
            s3Properties.put(S3Resource.S3_CONNECTION_TIMEOUT_MS, properties.get(S3Resource.S3_CONNECTION_TIMEOUT_MS));
        }
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        s3Properties.put("fs.s3a.attempts.maximum", "2");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(S3Resource.S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
        return s3Properties;
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
