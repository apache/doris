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

package org.apache.doris.datasource.property.metastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AWSGlueMetaStoreBasePropertiesTest {
    private Map<String, String> baseValidProps() {
        Map<String, String> props = new HashMap<>();
        props.put("glue.access_key", "ak");
        props.put("glue.secret_key", "sk");
        props.put("glue.endpoint", "glue.us-east-1.amazonaws.com");
        return props;
    }

    @Test
    void testValidPropertiesWithRegionFromEndpoint() {
        Map<String, String> props = baseValidProps();
        // no region set -> should be extracted from endpoint
        AWSGlueMetaStoreBaseProperties glueProps = AWSGlueMetaStoreBaseProperties.of(props);
        Assertions.assertEquals("ak", glueProps.glueAccessKey);
        Assertions.assertEquals("sk", glueProps.glueSecretKey);
        Assertions.assertEquals("us-east-1", glueProps.glueRegion);
    }

    @Test
    void testValidPropertiesWithExplicitRegion() {
        Map<String, String> props = baseValidProps();
        props.put("glue.region", "ap-southeast-1");
        AWSGlueMetaStoreBaseProperties glueProps = AWSGlueMetaStoreBaseProperties.of(props);
        Assertions.assertEquals("ap-southeast-1", glueProps.glueRegion);
    }

    @Test
    void testMissingAccessKeyThrows() {
        Map<String, String> props = baseValidProps();
        props.remove("glue.access_key");
        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AWSGlueMetaStoreBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("glue.access_key"));
    }

    @Test
    void testMissingSecretKeyThrows() {
        Map<String, String> props = baseValidProps();
        props.remove("glue.secret_key");

        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AWSGlueMetaStoreBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("glue.access_key and glue.secret_key must be set together"));
    }

    @Test
    void testMissingEndpointThrows() {
        Map<String, String> props = baseValidProps();
        props.remove("glue.endpoint");

        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AWSGlueMetaStoreBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("At least one of glue.endpoint or glue.region must be set"));
    }

    @Test
    void testInvalidEndpointThrows() {
        Map<String, String> props = baseValidProps();
        props.put("glue.endpoint", "http://invalid-endpoint.com");

        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AWSGlueMetaStoreBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("Invalid AWS Glue endpoint"));
    }

    @Test
    void testExtractRegionFailsWhenPatternMatchesButNoRegion() {
        Map<String, String> props = baseValidProps();
        props.put("glue.endpoint", "glue..amazonaws.com"); // malformed

        IllegalArgumentException ex = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> AWSGlueMetaStoreBaseProperties.of(props)
        );
        Assertions.assertTrue(ex.getMessage().contains("Invalid AWS Glue endpoint"));
    }

    @Test
    void testIamRole() {
        Map<String, String> props = baseValidProps();
        props.remove("glue.access_key");
        props.remove("glue.secret_key");
        props.put("glue.role_arn", "arn:aws:iam::1001:role/doris-glue-role");
        AWSGlueMetaStoreBaseProperties glueProps = AWSGlueMetaStoreBaseProperties.of(props);
        Assertions.assertEquals("arn:aws:iam::1001:role/doris-glue-role", glueProps.glueIAMRole);
    }
}
