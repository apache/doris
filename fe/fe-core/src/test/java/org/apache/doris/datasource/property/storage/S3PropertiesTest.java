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
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

public class S3PropertiesTest {
    private Map<String, String> origProps;

    private static String secretKey = "";
    private static String accessKey = "";
    private static String hdfsPath = "";

    @Mocked
    StsClient mockStsClient;

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testS3Properties() {
        origProps.put("s3.endpoint", "https://cos.example.com");
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");
        origProps.put("s3.region", "us-west-1");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Invalid endpoint: https://cos.example.com", () -> StorageProperties.createAll(origProps));
        origProps = new HashMap<>();
        origProps.put("s3.endpoint", "s3-fips.dualstack.us-east-2.amazonaws.com");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-east-2", s3Properties.getRegion());
        Assertions.assertEquals("s3-fips.dualstack.us-east-2.amazonaws.com", s3Properties.getEndpoint());

        origProps = new HashMap<>();
        origProps.put("s3.endpoint", "s3-fips.dualstack.us-east-2.amazonaws.com");
        origProps.put("s3.access_key", "myS3AccessKey");
        ExceptionChecker.expectThrowsWithMsg(StoragePropertiesException.class,
                "Please set s3.access_key and s3.secret_key", () -> StorageProperties.createAll(origProps));
        origProps.put("s3.secret_key", "myS3SecretKey");
        ExceptionChecker.expectThrowsNoException(() -> StorageProperties.createAll(origProps));
    }

    @Test
    public void testEndpointPattern() throws UserException {
        /*
         * region:
         * us-east-2
         * endpoint:
         * s3.us-east-2.amazonaws.com
         * s3.dualstack.us-east-2.amazonaws.com
         * s3-fips.dualstack.us-east-2.amazonaws.com
         * s3-fips.us-east-2.amazonaws.com
         * */
        String endpoint = "s3.us-east-2.amazonaws.com";
        origProps.put("s3.endpoint", endpoint);
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-east-2", s3Properties.getRegion());
        origProps.put("s3.endpoint", "s3.dualstack.us-east-2.amazonaws.com");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-east-2", s3Properties.getRegion());
        origProps.put("s3.endpoint", "s3-fips.dualstack.us-east-2.amazonaws.com");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-east-2", s3Properties.getRegion());
        origProps.put("s3.endpoint", "s3-fips.us-east-2.amazonaws.com");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-east-2", s3Properties.getRegion());

        origProps.put("glue.endpoint", "glue.us-wast-2.amazonaws.com");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-east-2", s3Properties.getRegion());
        origProps.remove("s3.endpoint");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-wast-2", s3Properties.getRegion());
        origProps.put("glue.endpoint", "glue-fips.us-west-2.amazonaws.com");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());
        origProps.put("glue.endpoint", "glue.us-gov-west-1.amazonaws.com");
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-gov-west-1", s3Properties.getRegion());
    }

    @Test
    public void testToNativeS3Configuration() throws UserException {
        origProps.put("s3.endpoint", "https://cos.example.com");
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");
        origProps.put("s3.region", "us-west-1");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        origProps.put("use_path_style", "true");
        origProps.put("s3.connection.maximum", "88");
        origProps.put("s3.connection.timeout", "6000");
        origProps.put("test_non_storage_param", "6000");

        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Invalid endpoint: https://cos.example.com", () -> {
                    StorageProperties.createAll(origProps).get(1);
                });
        origProps.put("s3.endpoint", "s3.us-west-1.amazonaws.com");
        S3Properties s3Properties = (S3Properties) StorageProperties.createAll(origProps).get(0);
        Map<String, String> s3Props = s3Properties.getBackendConfigProperties();
        Map<String, String> s3Config = s3Properties.getMatchedProperties();
        Assertions.assertTrue(!s3Config.containsKey("test_non_storage_param"));

        origProps.forEach((k, v) -> {
            if (!k.equals("test_non_storage_param") && !k.equals(StorageProperties.FS_S3_SUPPORT)) {
                Assertions.assertEquals(v, s3Config.get(k));
            }
        });
        // Validate the S3 properties
        Assertions.assertEquals("s3.us-west-1.amazonaws.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-1", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myS3AccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("myS3SecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("6000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        origProps.remove("use_path_style");
        origProps.remove("s3.connection.maximum");
        origProps.remove("s3.connection.timeout");
        s3Props = s3Properties.getBackendConfigProperties();

        Assertions.assertEquals("true", s3Props.get("use_path_style"));
        Assertions.assertEquals("88", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("6000", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
    }


    @Test
    public void testGetRegion() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("s3.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        origProps.put("s3.access_key", "myCOSAccessKey");
        origProps.put("s3.secret_key", "myCOSSecretKey");
        origProps.put("s3.region", "cn-hangzhou");
        OSSProperties ossProperties = (OSSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("cn-hangzhou", ossProperties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", ossProperties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", ossProperties.getSecretKey());
        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", ossProperties.getEndpoint());
        origProps = new HashMap<>();
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.access_key", "myCOSAccessKey");
        origProps.put("s3.secret_key", "myCOSSecretKey");
        origProps.put("s3.region", "us-west-2");
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", s3Properties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", s3Properties.getSecretKey());
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", s3Properties.getEndpoint());

    }

    @Test
    public void testGetRegionWithDefault() throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("uri", "https://example-bucket.s3.us-west-2.amazonaws.com/path/to/file.txt");
        origProps.put("s3.access_key", "myCOSAccessKey");
        origProps.put("s3.secret_key", "myCOSSecretKey");
        origProps.put("s3.region", "us-west-2");
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());
        Assertions.assertEquals("myCOSAccessKey", s3Properties.getAccessKey());
        Assertions.assertEquals("myCOSSecretKey", s3Properties.getSecretKey());
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", s3Properties.getEndpoint());
        Map<String, String> s3EndpointProps = new HashMap<>();
        s3EndpointProps.put("oss.access_key", "myCOSAccessKey");
        s3EndpointProps.put("oss.secret_key", "myCOSSecretKey");
        s3EndpointProps.put("oss.region", "cn-hangzhou");
        origProps.put("uri", "s3://examplebucket-1250000000/test/file.txt");
        // not support
        ExceptionChecker.expectThrowsNoException(() -> StorageProperties.createPrimary(s3EndpointProps));
    }

    @Test
    public void testS3IamRoleWithExternalId() throws UserException {
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.role_arn", "arn:aws:iam::123456789012:role/MyTestRole");
        origProps.put("s3.external_id", "external-123");

        S3Properties s3Props = (S3Properties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProperties = s3Props.getBackendConfigProperties();

        Assertions.assertEquals("arn:aws:iam::123456789012:role/MyTestRole", backendProperties.get("AWS_ROLE_ARN"));
        Assertions.assertEquals("external-123", backendProperties.get("AWS_EXTERNAL_ID"));
    }

    @Test
    public void testGetAwsCredentialsProviderWithIamRoleAndExternalId(@Mocked StsClientBuilder mockBuilder,
            @Mocked StsClient mockStsClient, @Mocked InstanceProfileCredentialsProvider mockInstanceCreds) {

        new Expectations() {
            {
                StsClient.builder();
                result = mockBuilder;
                mockBuilder.credentialsProvider((AwsCredentialsProvider) any);
                result = mockBuilder;
                mockBuilder.build();
                result = mockStsClient;
                InstanceProfileCredentialsProvider.create();
                result = mockInstanceCreds;
            }
        };

        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.role_arn", "arn:aws:iam::123456789012:role/MyTestRole");
        origProps.put("s3.external_id", "external-123");
        origProps.put("s3.region", "us-west-2");
        S3Properties s3Props = (S3Properties) StorageProperties.createPrimary(origProps);
        AwsCredentialsProvider provider = s3Props.getAwsCredentialsProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertTrue(provider instanceof StsAssumeRoleCredentialsProvider);
    }

    @Test
    public void testGetAwsCredentialsProviderWithAccessKeyAndSecretKey() throws UserException {
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.access_key", "myAccessKey");
        origProps.put("s3.secret_key", "mySecretKey");
        origProps.put("s3.region", "us-west-2");
        S3Properties s3Props = (S3Properties) StorageProperties.createPrimary(origProps);
        AwsCredentialsProvider provider = s3Props.getAwsCredentialsProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertTrue(provider instanceof StaticCredentialsProvider);
        origProps.put("s3.session_token", "mySessionToken");
        s3Props = (S3Properties) StorageProperties.createPrimary(origProps);
        provider = s3Props.getAwsCredentialsProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertTrue(provider instanceof StaticCredentialsProvider);
        origProps.put("s3.role_arn", "arn:aws:iam::123456789012:role/MyTestRole");
        origProps.put("s3.external_id", "external-123");
        s3Props = (S3Properties) StorageProperties.createPrimary(origProps);
        provider = s3Props.getAwsCredentialsProvider();
        Assertions.assertNotNull(provider);
        Assertions.assertTrue(provider instanceof StaticCredentialsProvider);
    }

    @Test
    public void testS3ExpressEndpointPattern() throws UserException {
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");

        // S3 Express Control Endpoint (Regional)
        String endpointControl = "s3express-control.us-west-2.amazonaws.com";
        origProps.put("s3.endpoint", endpointControl);
        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());

        // S3 Express Zonal Endpoint
        String endpointZonal = "s3express-usw2-az1.us-west-2.amazonaws.com";
        origProps.put("s3.endpoint", endpointZonal);
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("us-west-2", s3Properties.getRegion());

        // Test with https scheme
        String endpointWithScheme = "https://s3express-control.eu-central-1.amazonaws.com";
        origProps.put("s3.endpoint", endpointWithScheme);
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("eu-central-1", s3Properties.getRegion());

        // Test with path
        String endpointWithPath = "https://s3express-control.eu-central-1.amazonaws.com/path/to/obj";
        origProps.put("s3.endpoint", endpointWithPath);
        s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("eu-central-1", s3Properties.getRegion());
    }

    @Test
    public void testInvalidEndpoint() {
        origProps.put("s3.access_key", "myS3AccessKey");
        origProps.put("s3.secret_key", "myS3SecretKey");

        // Fails because it contains 'amazonaws.com' but doesn't match the strict S3 endpoint pattern (missing region).
        String invalidEndpoint1 = "s3.amazonaws.com";
        origProps.put("s3.endpoint", invalidEndpoint1);
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Invalid endpoint: s3.amazonaws.com", () -> StorageProperties.createPrimary(origProps));

        // Fails because it contains 'amazonaws.com' but doesn't match the strict S3 endpoint pattern (invalid subdomain).
        String invalidEndpoint2 = "my-s3-service.amazonaws.com";
        origProps.put("s3.endpoint", invalidEndpoint2);
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Invalid endpoint: my-s3-service.amazonaws.com",
                () -> StorageProperties.createPrimary(origProps));

        // Fails because it contains 'amazonaws.com' but doesn't match the strict S3 endpoint pattern (invalid TLD).
        String invalidEndpoint3 = "http://s3.us-west-2.amazonaws.com.cn";
        origProps.put("s3.endpoint", invalidEndpoint3);
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Invalid endpoint: http://s3.us-west-2.amazonaws.com.cn",
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testGetBackendConfigPropertiesWithRuntimeCredentials() throws UserException {
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.access_key", "base-access-key");
        origProps.put("s3.secret_key", "base-secret-key");
        origProps.put("s3.region", "us-west-2");

        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);

        // Test with runtime credentials (vended credentials)
        Map<String, String> runtimeCredentials = new HashMap<>();
        runtimeCredentials.put("AWS_ACCESS_KEY", "vended-access-key");
        runtimeCredentials.put("AWS_SECRET_KEY", "vended-secret-key");
        runtimeCredentials.put("AWS_TOKEN", "vended-session-token");

        Map<String, String> result = s3Properties.getBackendConfigProperties(runtimeCredentials);

        // Runtime credentials should override base properties
        Assertions.assertEquals("vended-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("vended-secret-key", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("vended-session-token", result.get("AWS_TOKEN"));

        // Base properties not overridden should still be present
        Assertions.assertEquals("us-west-2", result.get("AWS_REGION"));
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", result.get("AWS_ENDPOINT"));
    }

    @Test
    public void testGetBackendConfigPropertiesWithNullRuntimeCredentials() throws UserException {
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.access_key", "base-access-key");
        origProps.put("s3.secret_key", "base-secret-key");
        origProps.put("s3.region", "us-west-2");

        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);

        Map<String, String> result = s3Properties.getBackendConfigProperties(null);
        Map<String, String> baseResult = s3Properties.getBackendConfigProperties();

        // Should be identical to base properties when runtime credentials are null
        Assertions.assertEquals(baseResult.size(), result.size());
        Assertions.assertEquals("base-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("base-secret-key", result.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("us-west-2", result.get("AWS_REGION"));
    }

    @Test
    public void testGetBackendConfigPropertiesWithEmptyRuntimeCredentials() throws UserException {
        origProps.put("s3.endpoint", "s3.us-west-2.amazonaws.com");
        origProps.put("s3.access_key", "base-access-key");
        origProps.put("s3.secret_key", "base-secret-key");

        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(origProps);

        Map<String, String> emptyCredentials = new HashMap<>();
        Map<String, String> result = s3Properties.getBackendConfigProperties(emptyCredentials);
        Map<String, String> baseResult = s3Properties.getBackendConfigProperties();

        // Should be identical to base properties when runtime credentials are empty
        Assertions.assertEquals(baseResult.size(), result.size());
        Assertions.assertEquals("base-access-key", result.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("base-secret-key", result.get("AWS_SECRET_KEY"));
    }

    @Test
    public void testS3PropertiesFromIcebergRest() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("iceberg.rest.access-key-id", "aaa");
        props.put("iceberg.rest.secret-access-key", "bbb");
        props.put("iceberg.rest.signing-region", "ap-east-1");

        S3Properties s3Properties = (S3Properties) StorageProperties.createPrimary(props);
        Assertions.assertEquals("ap-east-1", s3Properties.region);
        Assertions.assertEquals("https://s3.ap-east-1.amazonaws.com", s3Properties.endpoint);
        Assertions.assertEquals("aaa", s3Properties.accessKey);
        Assertions.assertEquals("bbb", s3Properties.secretKey);
    }
}
