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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.properties.FileSystemProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class S3FileSystemProviderTest {

    private final S3FileSystemProvider provider = new S3FileSystemProvider();

    @Test
    void supports_acceptsRoleBasedS3Configuration() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");
        props.put("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/snapshot-role");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsConfiguredCredentialsProviderType() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");
        props.put("s3.credentials_provider_type", "ENV");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_rejectsConfigurationWithoutCredentialsOrRole() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");

        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_acceptsLegacyConvertedMapWithoutExplicitCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("_STORAGE_TYPE_", "S3");
        props.put("AWS_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
        props.put("AWS_REGION", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsExplicitS3ProviderWithoutCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "S3");
        props.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        props.put("s3.region", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_acceptsExplicitS3SupportWithoutCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("fs.s3.support", "true");
        props.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        props.put("s3.region", "us-west-2");

        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void bind_returnsValidatedS3FileSystemProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://minio.local");
        props.put("s3.region", "us-west-2");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");

        FileSystemProperties bound = provider.bind(props);

        Assertions.assertInstanceOf(S3FileSystemProperties.class, bound);
        Assertions.assertEquals("https://minio.local", ((S3FileSystemProperties) bound).getEndpoint());
    }

    @Test
    void create_usesTypedS3FileSystemProperties() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://minio.local");
        props.put("s3.region", "us-west-2");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");

        FileSystem fileSystem = provider.create(provider.bind(props));

        Assertions.assertInstanceOf(S3FileSystem.class, fileSystem);
        S3FileSystem s3 = (S3FileSystem) fileSystem;
        Assertions.assertTrue(s3.properties().isPresent());
        Assertions.assertEquals("https://minio.local", s3.properties().orElseThrow().getEndpoint());
    }
}
