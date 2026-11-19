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

import org.apache.doris.connector.iceberg.auth.GcsVendedCredentials;
import org.apache.doris.filesystem.gcs.GcpAuth;
import org.apache.doris.filesystem.gcs.GcsFileSystemProperties;
import org.apache.doris.filesystem.gcs.auth.GcpCredential;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/** Supplies Iceberg S3FileIO clients configured with native GCP bearer authentication. */
public class GcpS3FileIOAwsClientFactory implements S3FileIOAwsClientFactory {
    private static final long serialVersionUID = 1L;

    private Map<String, String> properties;

    public GcpS3FileIOAwsClientFactory() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.properties = withVendedCredentials(properties);
    }

    @Override
    public S3Client s3() {
        try {
            return GcpAuth.createS3Client(gcsProperties(), credentials());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public S3AsyncClient s3Async() {
        try {
            return GcpAuth.createS3AsyncClient(gcsProperties(), credentials());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Table-scoped server credentials override the catalog's ambient GCP identity. */
    public static Map<String, String> withVendedCredentials(Map<String, String> properties) {
        Map<String, String> result = new HashMap<>(properties);
        if (properties.containsKey(GcpCredential.ICEBERG_OAUTH2_TOKEN)) {
            result.put(GcpCredential.OAUTH2_TOKEN, properties.get(GcpCredential.ICEBERG_OAUTH2_TOKEN));
            result.remove(GcpCredential.OAUTH2_TOKEN_EXPIRES_AT);
            if (properties.containsKey(GcpCredential.ICEBERG_OAUTH2_TOKEN_EXPIRES_AT)) {
                result.put(GcpCredential.OAUTH2_TOKEN_EXPIRES_AT,
                        properties.get(GcpCredential.ICEBERG_OAUTH2_TOKEN_EXPIRES_AT));
            }
            result.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, "ACCESS_TOKEN");
            result.remove(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT);
        }
        return result;
    }

    private GoogleCredentials credentials() throws IOException {
        GcpCredential credential = gcsProperties().getAuth().getNativeCredential().orElseThrow();
        if (properties.containsKey(GcpCredential.OAUTH2_TOKEN) && GcsVendedCredentials.isRefreshEnabled(properties)) {
            return GcsVendedCredentials.createCredentials(new AccessToken(credential.getAccessToken(),
                    new Date(credential.getExpiresAtMillis())), properties);
        }
        return GcpAuth.createCredentials(credential);
    }

    private GcsFileSystemProperties gcsProperties() {
        if (properties == null) {
            throw new IllegalStateException("GCP Iceberg client factory has not been initialized");
        }
        GcsFileSystemProperties storageProperties = GcsFileSystemProperties.of(properties);
        if (storageProperties.getAuth().getNativeCredential().isEmpty()) {
            throw new IllegalArgumentException("Native GCP Iceberg authentication requires GCS credentials");
        }
        return storageProperties;
    }
}
