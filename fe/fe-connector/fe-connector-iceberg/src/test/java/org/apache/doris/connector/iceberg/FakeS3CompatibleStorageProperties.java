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

package org.apache.doris.connector.iceberg;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;

import java.util.Collections;
import java.util.Map;

/**
 * Hand-written {@link S3CompatibleFileSystemProperties} test double (no Mockito) for the iceberg
 * S3FileIO assembly tests. Mirrors the real fe-filesystem S3-compatible providers' relevant surface:
 * a configurable {@code providerName} ("S3"/"OSS"/"COS"/"OBS" — only "S3" is the generic-AWS analog of
 * legacy {@code S3Properties}, which is what gates the assume-role block) and the typed S3 getters the
 * connector reads. All getters default to {@code ""}; tests set only what they assert on.
 */
final class FakeS3CompatibleStorageProperties implements S3CompatibleFileSystemProperties {

    private final String providerName;
    private String endpoint = "";
    private String region = "";
    private String accessKey = "";
    private String secretKey = "";
    private String sessionToken = "";
    private String roleArn = "";
    private String externalId = "";
    private String usePathStyle = "";

    FakeS3CompatibleStorageProperties(String providerName) {
        this.providerName = providerName;
    }

    FakeS3CompatibleStorageProperties endpoint(String v) {
        this.endpoint = v;
        return this;
    }

    FakeS3CompatibleStorageProperties region(String v) {
        this.region = v;
        return this;
    }

    FakeS3CompatibleStorageProperties accessKey(String v) {
        this.accessKey = v;
        return this;
    }

    FakeS3CompatibleStorageProperties secretKey(String v) {
        this.secretKey = v;
        return this;
    }

    FakeS3CompatibleStorageProperties sessionToken(String v) {
        this.sessionToken = v;
        return this;
    }

    FakeS3CompatibleStorageProperties roleArn(String v) {
        this.roleArn = v;
        return this;
    }

    FakeS3CompatibleStorageProperties externalId(String v) {
        this.externalId = v;
        return this;
    }

    FakeS3CompatibleStorageProperties usePathStyle(String v) {
        this.usePathStyle = v;
        return this;
    }

    @Override
    public String providerName() {
        return providerName;
    }

    @Override
    public StorageKind kind() {
        return StorageKind.OBJECT_STORAGE;
    }

    @Override
    public FileSystemType type() {
        return FileSystemType.S3;
    }

    @Override
    public Map<String, String> rawProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> matchedProperties() {
        return Collections.emptyMap();
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public String getRegion() {
        return region;
    }

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public String getSecretKey() {
        return secretKey;
    }

    @Override
    public String getSessionToken() {
        return sessionToken;
    }

    @Override
    public String getRoleArn() {
        return roleArn;
    }

    @Override
    public String getExternalId() {
        return externalId;
    }

    @Override
    public String getBucket() {
        return "";
    }

    @Override
    public String getRootPath() {
        return "";
    }

    @Override
    public String getMaxConnections() {
        return "";
    }

    @Override
    public String getRequestTimeoutMs() {
        return "";
    }

    @Override
    public String getConnectionTimeoutMs() {
        return "";
    }

    @Override
    public String getUsePathStyle() {
        return usePathStyle;
    }
}
