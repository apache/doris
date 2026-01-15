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

import org.apache.doris.common.AnalysisException;
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
        // Test with AWS_MSK_IAM mechanism
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "AWS_MSK_IAM");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyMskRole");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Assert.assertNotNull(props.getCustomKafkaProperties());
        Assert.assertEquals("SASL_SSL", props.getCustomKafkaProperties().get("security.protocol"));
        Assert.assertEquals("AWS_MSK_IAM", props.getCustomKafkaProperties().get("sasl.mechanism"));
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
        dataSourceProperties.put("property.sasl.mechanism", "AWS_MSK_IAM");

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
        dataSourceProperties.put("property.sasl.mechanism", "AWS_MSK_IAM");

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
        // Test missing sasl.mechanism when using SASL_SSL
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for missing sasl.mechanism");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("sasl.mechanism"));
        }
    }

    @Test
    public void testInvalidSaslMechanism() {
        // Test invalid sasl.mechanism value when using AWS MSK IAM
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "PLAIN");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyMskRole");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");

        try {
            props.analyze();
            Assert.fail("Should throw AnalysisException for invalid sasl.mechanism with AWS role");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("AWS_MSK_IAM") || e.getMessage().contains("OAUTHBEARER"));
        }
    }

    @Test
    public void testCompleteAwsIamConfigWithRoleArn() throws UserException {
        // Test complete configuration with IAM role ARN
        dataSourceProperties.put("property.security.protocol", "SASL_SSL");
        dataSourceProperties.put("property.sasl.mechanism", "AWS_MSK_IAM");
        dataSourceProperties.put("property.aws.msk.iam.role.arn", "arn:aws:iam::123456789012:role/MyMskRole");
        dataSourceProperties.put("property.aws.profile.name", "default");

        KafkaDataSourceProperties props = new KafkaDataSourceProperties(dataSourceProperties);
        props.setTimezone("UTC");
        props.analyze();

        Map<String, String> customProps = props.getCustomKafkaProperties();
        Assert.assertEquals("SASL_SSL", customProps.get("security.protocol"));
        Assert.assertEquals("AWS_MSK_IAM", customProps.get("sasl.mechanism"));
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
}
