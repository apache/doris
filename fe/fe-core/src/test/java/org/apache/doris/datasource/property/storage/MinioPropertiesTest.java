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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class MinioPropertiesTest {

    private Map<String, String> origProps;

    @BeforeEach
    public void setup() {
        origProps = new HashMap<>();
    }

    @Test
    public void testValidMinioConfiguration() throws UserException {
        origProps.put("s3.endpoint", "http://localhost:9000");
        origProps.put("s3.access_key", "minioAccessKey");
        origProps.put("s3.secret_key", "minioSecretKey");

        MinioProperties minioProperties = (MinioProperties) StorageProperties.createPrimary(origProps);

        Assertions.assertEquals("http://localhost:9000", minioProperties.getEndpoint());
        Assertions.assertEquals("minioAccessKey", minioProperties.getAccessKey());
        Assertions.assertEquals("minioSecretKey", minioProperties.getSecretKey());
        Assertions.assertEquals("us-east-1", minioProperties.getRegion());
        origProps.remove("s3.endpoint");
        origProps.put("uri", "http://localhost:9000/test/");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Property minio.endpoint is required", () -> StorageProperties.createPrimary(origProps));
        origProps.put("s3.endpoint", "http://localhost:9000");
        ExceptionChecker.expectThrowsNoException(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testGuessIsMeWithMinio() {
        origProps.put("s3.access_key", "minioAccessKey");
        Assertions.assertTrue(MinioProperties.guessIsMe(origProps));
    }

    @Test
    public void testGuessIsMeWithFsS3aEndpoint() {
        origProps.put("fs.s3a.endpoint", "http://ozone-s3g:9878");
        Assertions.assertTrue(MinioProperties.guessIsMe(origProps));
    }

    @Test
    public void testMissingAccessKey() {
        origProps.put("s3.endpoint", "http://localhost:9000");
        origProps.put("s3.secret_key", "minioSecretKey");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Both the access key and the secret key must be set.",
                () -> StorageProperties.createPrimary(origProps));
        origProps.remove("s3.secret_key");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testMissingSecretKey() {
        origProps.put("s3.endpoint", "http://localhost:9000");
        origProps.put("s3.access_key", "minioAccessKey");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Both the access key and the secret key must be set.",
                () -> StorageProperties.createPrimary(origProps));
        origProps.remove("s3.access_key");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testEndpoint() {
        origProps.put("s3.endpoint", "not-a-valid-url");
        origProps.put("s3.access_key", "a");
        origProps.put("s3.secret_key", "b");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
        origProps.put("s3.endpoint", "http://localhost:9000");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testBackendConfigProperties() {
        origProps.put("s3.endpoint", "http://localhost:9000");
        origProps.put("s3.access_key", "minioAccessKey");
        origProps.put("s3.secret_key", "minioSecretKey");

        MinioProperties minioProperties = (MinioProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProps = minioProperties.getBackendConfigProperties();

        Assertions.assertEquals("http://localhost:9000", backendProps.get("AWS_ENDPOINT"));
        Assertions.assertEquals("minioAccessKey", backendProps.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("minioSecretKey", backendProps.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("us-east-1", backendProps.get("AWS_REGION"));
    }

    @Test
    public void testFsS3aAliasProperties() {
        origProps.put("fs.s3a.endpoint", "http://ozone-s3g:9878");
        origProps.put("fs.s3a.endpoint.region", "us-east-1");
        origProps.put("fs.s3a.access.key", "hadoop");
        origProps.put("fs.s3a.secret.key", "hadoop");
        origProps.put("fs.s3a.path.style.access", "true");
        origProps.put("fs.s3a.connection.maximum", "77");
        origProps.put("fs.s3a.connection.request.timeout", "4321");
        origProps.put("fs.s3a.connection.timeout", "1234");

        MinioProperties minioProperties = (MinioProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("http://ozone-s3g:9878", minioProperties.getEndpoint());
        Assertions.assertEquals("us-east-1", minioProperties.getRegion());
        Assertions.assertEquals("hadoop", minioProperties.getAccessKey());
        Assertions.assertEquals("hadoop", minioProperties.getSecretKey());
        Assertions.assertEquals("true", minioProperties.getUsePathStyle());
        Assertions.assertEquals("77", minioProperties.getMaxConnections());
        Assertions.assertEquals("4321", minioProperties.getRequestTimeoutS());
        Assertions.assertEquals("1234", minioProperties.getConnectionTimeoutS());

        Map<String, String> backendProps = minioProperties.getBackendConfigProperties();
        Assertions.assertEquals("http://ozone-s3g:9878", backendProps.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east-1", backendProps.get("AWS_REGION"));
        Assertions.assertEquals("hadoop", backendProps.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("hadoop", backendProps.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("true", backendProps.get("use_path_style"));
        Assertions.assertEquals("77", backendProps.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("4321", backendProps.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("1234", backendProps.get("AWS_CONNECTION_TIMEOUT_MS"));
    }
}
