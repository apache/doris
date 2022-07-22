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

import org.apache.doris.catalog.HiveTable;
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
 * DataSourceProperty to store the properties for datasource.
 */
@Data
public class DataSourceProperty implements Writable {
    @SerializedName(value = "properties")
    private Map<String, String> properties = Maps.newHashMap();

    public String getOrDefault(String key, String defaultVal) {
        return properties.getOrDefault(key, defaultVal);
    }

    public Map<String, String> getDfsProperties() {
        Map<String, String> dfsProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(HiveTable.HIVE_HDFS_PREFIX)) {
                dfsProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return dfsProperties;
    }

    public Map<String, String> getS3Properties() {
        Map<String, String> s3Properties = Maps.newHashMap();
        if (properties.containsKey(HiveTable.S3_AK)) {
            s3Properties.put("fs.s3a.access.key", properties.get(HiveTable.S3_AK));
            s3Properties.put(HiveTable.S3_AK, properties.get(HiveTable.S3_AK));
        }
        if (properties.containsKey(HiveTable.S3_SK)) {
            s3Properties.put("fs.s3a.secret.key", properties.get(HiveTable.S3_SK));
            s3Properties.put(HiveTable.S3_SK, properties.get(HiveTable.S3_SK));
        }
        if (properties.containsKey(HiveTable.S3_ENDPOINT)) {
            s3Properties.put("fs.s3a.endpoint", properties.get(HiveTable.S3_ENDPOINT));
            s3Properties.put(HiveTable.S3_ENDPOINT, properties.get(HiveTable.S3_ENDPOINT));
        }
        if (properties.containsKey(HiveTable.AWS_REGION)) {
            s3Properties.put("fs.s3a.endpoint.region", properties.get(HiveTable.AWS_REGION));
            s3Properties.put(HiveTable.AWS_REGION, properties.get(HiveTable.AWS_REGION));
        }
        if (properties.containsKey(HiveTable.AWS_MAX_CONN_SIZE)) {
            s3Properties.put("fs.s3a.connection.maximum", properties.get(HiveTable.AWS_MAX_CONN_SIZE));
            s3Properties.put(HiveTable.AWS_MAX_CONN_SIZE, properties.get(HiveTable.AWS_MAX_CONN_SIZE));
        }
        if (properties.containsKey(HiveTable.AWS_REQUEST_TIMEOUT_MS)) {
            s3Properties.put("fs.s3a.connection.request.timeout", properties.get(HiveTable.AWS_REQUEST_TIMEOUT_MS));
            s3Properties.put(HiveTable.AWS_REQUEST_TIMEOUT_MS, properties.get(HiveTable.AWS_REQUEST_TIMEOUT_MS));
        }
        if (properties.containsKey(HiveTable.AWS_CONN_TIMEOUT_MS)) {
            s3Properties.put("fs.s3a.connection.timeout", properties.get(HiveTable.AWS_CONN_TIMEOUT_MS));
            s3Properties.put(HiveTable.AWS_CONN_TIMEOUT_MS, properties.get(HiveTable.AWS_CONN_TIMEOUT_MS));
        }
        s3Properties.put("fs.s3.impl.disable.cache", "true");
        s3Properties.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        s3Properties.put("fs.s3a.attempts.maximum", "2");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(HiveTable.S3_FS_PREFIX)) {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
        return s3Properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DataSourceProperty read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DataSourceProperty.class);
    }
}
