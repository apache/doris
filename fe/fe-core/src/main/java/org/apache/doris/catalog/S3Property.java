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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class S3Property extends RemoteStorageProperty implements Writable {

    public static final String S3_ENDPOINT = "s3_endpoint";
    public static final String S3_REGION = "s3_region";
    public static final String S3_ROOT_PATH = "s3_root_path";
    public static final String S3_ACCESS_KEY = "s3_access_key";
    public static final String S3_SECRET_KEY = "s3_secret_key";
    public static final String S3_MAX_CONNECTIONS = "s3_max_connections";
    public static final String S3_REQUEST_TIMEOUT_MS = "s3_request_timeout_ms";
    public static final String S3_CONNECTION_TIMEOUT_MS = "s3_connection_timeout_ms";
    public static final String DEFAULT_S3_MAX_CONNECTIONS = "50";
    public static final String DEFAULT_S3_REQUEST_TIMEOUT_MS = "3000";
    public static final String DEFAULT_S3_CONNECTION_TIMEOUT_MS = "1000";

    @SerializedName(value = "endPoint")
    private String endPoint;
    @SerializedName(value = "region")
    private String region;
    @SerializedName(value = "rootPath")
    private String rootPath;
    @SerializedName(value = "accessKey")
    private String accessKey;
    @SerializedName(value = "secretKey")
    private String secretKey;
    @SerializedName(value = "maxConnections")
    private long maxConnections;
    @SerializedName(value = "requestTimeoutMs")
    private long requestTimeoutMs;
    @SerializedName(value = "connectionTimeoutMs")
    private long connectionTimeoutMs;

    public S3Property(Map<String, String> properties) {
        this.endPoint = properties.get(S3_ENDPOINT);
        this.region = properties.get(S3_REGION);
        this.rootPath = properties.get(S3_ROOT_PATH);
        this.accessKey = properties.get(S3_ACCESS_KEY);
        this.secretKey = properties.get(S3_SECRET_KEY);
        this.maxConnections = Long.parseLong(properties.getOrDefault(S3_MAX_CONNECTIONS, DEFAULT_S3_MAX_CONNECTIONS));
        this.requestTimeoutMs = Long.parseLong(
                properties.getOrDefault(S3_REQUEST_TIMEOUT_MS, DEFAULT_S3_REQUEST_TIMEOUT_MS));
        this.connectionTimeoutMs = Long.parseLong(
                properties.getOrDefault(S3_CONNECTION_TIMEOUT_MS, DEFAULT_S3_CONNECTION_TIMEOUT_MS));
    }

    @Override
    public RemoteStorageType getStorageType() {
        return RemoteStorageType.S3;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static S3Property read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, S3Property.class);
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(S3_ENDPOINT, endPoint);
        properties.put(S3_REGION, region);
        properties.put(S3_ROOT_PATH, rootPath);
        properties.put(S3_ACCESS_KEY, accessKey);
        properties.put(S3_SECRET_KEY, secretKey);
        properties.put(S3_MAX_CONNECTIONS, String.valueOf(maxConnections));
        properties.put(S3_REQUEST_TIMEOUT_MS, String.valueOf(requestTimeoutMs));
        properties.put(S3_CONNECTION_TIMEOUT_MS, String.valueOf(connectionTimeoutMs));

        return properties;
    }
}
