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

package org.apache.doris.load.routineload.kafka;

import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for AWS MSK IAM authentication configuration in Routine Load.
 */
public class KafkaAwsMskIamAuthTest {

    private Map<String, String> dataSourceProperties;

    @Before
    public void setUp() {
        dataSourceProperties = new HashMap<>();
        dataSourceProperties.put("kafka_broker_list", "b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098");
        dataSourceProperties.put("kafka_topic", "test_topic");
    }

    @Test
    public void testValidAwsMskIamConfig() throws UserException {
        // Test with OAUTHBEARER mechanism (correct way for AWS MSK IAM)
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyMskRole");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("SASL_SSL", props.getCustomKafkaProperties().get("security.protocol"));
        Assert.assertEquals("OAUTHBEARER", props.getCustomKafkaProperties().get("sasl.mechanism"));
        Assert.assertEquals("us-east-1", props.getCustomKafkaProperties().get("aws.region"));
    }

    @Test
    public void testValidOAuthBearerConfig() throws UserException {
        // Test with OAUTHBEARER mechanism (alternative approach)
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("SASL_SSL", props.getCustomKafkaProperties().get("security.protocol"));
        Assert.assertEquals("OAUTHBEARER", props.getCustomKafkaProperties().get("sasl.mechanism"));
    }

    @Test
    public void testMissingSecurityProtocol() {
        // Test missing security.protocol when using AWS MSK IAM
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for missing security.protocol");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("security.protocol"));
            Assert.assertTrue(e.getMessage().contains("SASL_SSL"));
        }
    }

    @Test
    public void testInvalidSecurityProtocol() {
        // Test invalid security.protocol value
        dataSourceProperties.put("property.security.protocol", "PLAINTEXT");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for invalid security.protocol");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("SASL_SSL"));
        }
    }

    @Test
    public void testMissingSaslMechanism() {
        // Test missing sasl.mechanism when using SASL_SSL with AWS
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.aws.region", "us-east-1");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for missing sasl.mechanism");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("sasl.mechanism"));
            Assert.assertTrue(e.getMessage().contains("OAUTHBEARER"));
        }
    }

    @Test
    public void testInvalidSaslMechanism() {
        // Test invalid sasl.mechanism value when using AWS MSK IAM
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "PLAIN");
        dataSourceProperties.put("property.aws.region", "us-east-1");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for invalid sasl.mechanism with AWS config");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("OAUTHBEARER"));
        }
    }

    @Test
    public void testMissingRegionWithRoleArn() {
        // Test with role ARN but missing region - should fail
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyMskRole");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for missing region with AWS properties");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("aws.region"));
            Assert.assertTrue(e.getMessage().contains("required"));
        }
    }

    @Test
    public void testCompleteAwsIamConfigWithRoleArn() throws UserException {
        // Test complete configuration with IAM role ARN
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyMskRole");
        dataSourceProperties.put("property.aws.profile.name", "default");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Map<String, String> customProps = props.getCustomKafkaProperties();
        Assert.assertEquals("SASL_SSL", customProps.get("security.protocol"));
        Assert.assertEquals("OAUTHBEARER", customProps.get("sasl.mechanism"));
        Assert.assertEquals("us-east-1", customProps.get("aws.region"));
        Assert.assertEquals("arn:aws:iam::123456789012:role/MyMskRole", customProps.get("aws.msk.iam.role.arn"));
        Assert.assertEquals("default", customProps.get("aws.profile.name"));
    }

    @Test
    public void testScramSha256Config() throws UserException {
        // Test SCRAM-SHA-256 configuration (non-IAM SASL)
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "SCRAM-SHA-256");
        dataSourceProperties.put("property.sasl.username", "myuser");
        dataSourceProperties.put("property.sasl.password", "mypassword");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("SASL_SSL", props.getCustomKafkaProperties().get("security.protocol"));
        Assert.assertEquals("SCRAM-SHA-256", props.getCustomKafkaProperties().get("sasl.mechanism"));
    }

    @Test
    public void testPlaintextConfigWithoutSasl() throws UserException {
        // Test plain PLAINTEXT configuration (no SASL)
        dataSourceProperties.put("property.security.protocol", "PLAINTEXT");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("PLAINTEXT", props.getCustomKafkaProperties().get("security.protocol"));
    }

    @Test
    public void testSslConfigWithoutSasl() throws UserException {
        // Test SSL configuration without SASL
        dataSourceProperties.put("property.security.protocol", "SSL");
        dataSourceProperties.put("property.ssl.ca.location", "/path/to/ca-cert");
        dataSourceProperties.put("property.ssl.certificate.location", "/path/to/client-cert");
        dataSourceProperties.put("property.ssl.key.location", "/path/to/client-key");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("SSL", props.getCustomKafkaProperties().get("security.protocol"));
    }

    @Test
    public void testPublicAccessWithExplicitCredentials() throws UserException {
        // Test public MSK access (broker contains "-public.") with explicit credentials
        dataSourceProperties.put("kafka_broker_list", "b-1-public.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9198");
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.access.key", "AKIAIOSFODNN7EXAMPLE");
        dataSourceProperties.put("property.aws.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("us-east-1", props.getCustomKafkaProperties().get("aws.region"));
        Assert.assertEquals("AKIAIOSFODNN7EXAMPLE", props.getCustomKafkaProperties().get("aws.access.key"));
    }

    @Test
    public void testPublicAccessWithoutCredentials() {
        // Test public MSK access without credentials - should fail
        dataSourceProperties.put("kafka_broker_list", "b-1-public.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9198");
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for public access without credentials");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("public"));
            Assert.assertTrue(e.getMessage().contains("credentials"));
        }
    }

    @Test
    public void testInternalAccessWithInstanceProfile() throws UserException {
        // Test internal MSK access with instance profile (credentials_provider)
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.credentials.provider", "INSTANCE_PROFILE");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("INSTANCE_PROFILE", props.getCustomKafkaProperties().get("aws.credentials.provider"));
    }

    @Test
    public void testInternalAccessWithProfile() throws UserException {
        // Test internal MSK access with AWS profile
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.profile.name", "default");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("default", props.getCustomKafkaProperties().get("aws.profile.name"));
    }

    @Test
    public void testCrossAccountAccessWithRoleArnAndCredentials() throws UserException {
        // Test cross-account access: Account B's credentials + Account A's role ARN
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.access.key", "AKIAIOSFODNN7EXAMPLE");
        dataSourceProperties.put("property.aws.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::111111111111:role/AccountAMskRole");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Map<String, String> customProps = props.getCustomKafkaProperties();
        Assert.assertEquals("AKIAIOSFODNN7EXAMPLE", customProps.get("aws.access.key"));
        Assert.assertEquals("arn:aws:iam::111111111111:role/AccountAMskRole", customProps.get("aws.msk.iam.role.arn"));
    }

    @Test
    public void testMissingAccessKeyWithSecretKey() {
        // Test with only secret key but missing access key - should fail
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for missing access key");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("aws.access.key"));
            Assert.assertTrue(e.getMessage().contains("aws.secret.key"));
            Assert.assertTrue(e.getMessage().contains("together"));
        }
    }

    @Test
    public void testMissingSecretKeyWithAccessKey() {
        // Test with only access key but missing secret key - should fail
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.access.key", "AKIAIOSFODNN7EXAMPLE");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for missing secret key");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("aws.access.key"));
            Assert.assertTrue(e.getMessage().contains("aws.secret.key"));
            Assert.assertTrue(e.getMessage().contains("together"));
        }
    }

    @Test
    public void testPublicAccessWithProfile() throws UserException {
        // Test public access with AWS profile (allowed but warned)
        dataSourceProperties.put("kafka_broker_list", "b-1-public.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9198");
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.profile.name", "default");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        // Should succeed but a warning should be logged
        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("default", props.getCustomKafkaProperties().get("aws.profile.name"));
    }

    @Test
    public void testPublicAccessWithCredentialsProvider() throws UserException {
        // Test public access with credentials provider (allowed but warned)
        dataSourceProperties.put("kafka_broker_list", "b-1-public.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9198");
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.credentials.provider", "ENV");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        // Should succeed but a warning should be logged
        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("ENV", props.getCustomKafkaProperties().get("aws.credentials.provider"));
    }

    @Test
    public void testMultipleCredentialsSources() throws UserException {
        // Test with multiple credentials sources - all should be allowed
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "OAUTHBEARER");
        dataSourceProperties.put("property.aws.region", "us-east-1");
        dataSourceProperties.put("property.aws.access.key", "AKIAIOSFODNN7EXAMPLE");
        dataSourceProperties.put("property.aws.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyRole");
        dataSourceProperties.put("property.aws.profile.name", "default");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        // All properties should be preserved (BE will use them in priority order)
        Map<String, String> customProps = props.getCustomKafkaProperties();
        Assert.assertEquals("AKIAIOSFODNN7EXAMPLE", customProps.get("aws.access.key"));
        Assert.assertEquals("arn:aws:iam::123456789012:role/MyRole", customProps.get("aws.msk.iam.role.arn"));
        Assert.assertEquals("default", customProps.get("aws.profile.name"));
    }
}
