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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Phase A5 golden tests freezing the current fe-core S3Properties behaviour.
 * Expected values are the oracle for the future StorageAdapter facade; do NOT
 * "fix" them to look nicer — they must match what production currently emits.
 */
public class S3PropertiesParityTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "myAk");
        props.put("s3.secret_key", "mySk");
        return props;
    }

    @Test
    public void testIdentity() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Assertions.assertTrue(sp instanceof S3Properties);
        Assertions.assertEquals(StorageProperties.Type.S3, sp.getType());
        Assertions.assertEquals("S3", sp.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("s3", "s3a", "s3n"), ((S3Properties) sp).schemas());
    }

    @Test
    public void testBasicAkSkBackendMapExact() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        // Region is auto-extracted from the standard AWS endpoint.
        // S3Properties always emits AWS_CREDENTIALS_PROVIDER_TYPE with the mode name
        // (default DEFAULT), even when ak/sk are present — unlike its siblings.
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com",
                "AWS_REGION", "us-east-1",
                "AWS_ACCESS_KEY", "myAk",
                "AWS_SECRET_KEY", "mySk",
                "AWS_MAX_CONNECTIONS", "50",
                "AWS_REQUEST_TIMEOUT_MS", "3000",
                "AWS_CONNECTION_TIMEOUT_MS", "1000",
                "use_path_style", "false",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testSessionTokenAddsAwsToken() {
        Map<String, String> props = baseProps();
        props.put("s3.session_token", "tok123");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Assertions.assertEquals("tok123", sp.getBackendConfigProperties().get("AWS_TOKEN"));
        Assertions.assertEquals("tok123", sp.getHadoopStorageConfig().get("fs.s3a.session.token"));
    }

    @Test
    public void testBasicAkSkHadoopConfig() {
        StorageProperties sp = StorageProperties.createPrimary(baseProps());
        Configuration conf = sp.getHadoopStorageConfig();
        ParityAsserts.assertConfContains(conf, ParityAsserts.map(
                "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "fs.s3a.endpoint", "https://s3.us-east-1.amazonaws.com",
                "fs.s3a.endpoint.region", "us-east-1",
                "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "fs.s3a.access.key", "myAk",
                "fs.s3a.secret.key", "mySk",
                "fs.s3a.connection.maximum", "50",
                "fs.s3a.connection.request.timeout", "3000",
                "fs.s3a.connection.timeout", "1000",
                "fs.s3a.path.style.access", "false",
                // ensureDisableCache over schemas {s3, s3a, s3n}
                "fs.s3.impl.disable.cache", "true",
                "fs.s3a.impl.disable.cache", "true",
                "fs.s3n.impl.disable.cache", "true"
        ));
        // 2.4-2: fe-core builds `new Configuration()` which loads core-default.xml.
        Assertions.assertNotNull(conf.get("io.file.buffer.size"));
    }

    @Test
    public void testUserFsPassthroughAndUserDisableCacheOverride() {
        Map<String, String> props = baseProps();
        props.put("fs.custom.flag", "v1");
        props.put("fs.s3a.impl.disable.cache", "false"); // user explicit value wins
        StorageProperties sp = StorageProperties.createPrimary(props);
        Configuration conf = sp.getHadoopStorageConfig();
        Assertions.assertEquals("v1", conf.get("fs.custom.flag"));
        Assertions.assertEquals("false", conf.get("fs.s3a.impl.disable.cache"));
        Assertions.assertEquals("true", conf.get("fs.s3.impl.disable.cache"));
    }

    @Test
    public void testAssumeRoleBackendMapExact() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        props.put("s3.role_arn", "arn:aws:iam::123456789012:role/doris");
        props.put("s3.external_id", "ext-1");
        StorageProperties sp = StorageProperties.createPrimary(props);
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com",
                "AWS_REGION", "us-east-1",
                "AWS_ACCESS_KEY", "",
                "AWS_SECRET_KEY", "",
                "AWS_MAX_CONNECTIONS", "50",
                "AWS_REQUEST_TIMEOUT_MS", "3000",
                "AWS_CONNECTION_TIMEOUT_MS", "1000",
                "use_path_style", "false",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT",
                "AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/doris",
                "AWS_EXTERNAL_ID", "ext-1"
        ), sp.getBackendConfigProperties());
    }

    @Test
    public void testAssumeRoleHadoopConfigOnlyWhenAkBlank() {
        // 2.4-3: fe-core only emits assumed-role keys when accessKey is blank.
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        props.put("s3.role_arn", "arn:aws:iam::123456789012:role/doris");
        props.put("s3.external_id", "ext-1");
        StorageProperties sp = StorageProperties.createPrimary(props);
        Configuration conf = sp.getHadoopStorageConfig();
        Assertions.assertEquals("arn:aws:iam::123456789012:role/doris", conf.get("fs.s3a.assumed.role.arn"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
                conf.get("fs.s3a.aws.credentials.provider"));
        Assertions.assertEquals("ext-1", conf.get("fs.s3a.assumed.role.external.id"));
        // Config.aws_credentials_provider_version defaults to v2:
        // the chain never includes Anonymous for assumed-role (includeAnonymousInDefault=false).
        String chain = conf.get("fs.s3a.assumed.role.credentials.provider");
        Assertions.assertNotNull(chain);
        Assertions.assertTrue(chain.startsWith(
                "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider"), chain);
        Assertions.assertFalse(chain.contains("AnonymousCredentialsProvider"), chain);

        // With accessKey present, role keys must NOT be emitted by fe-core.
        Map<String, String> akProps = baseProps();
        akProps.put("s3.role_arn", "arn:aws:iam::123456789012:role/doris");
        StorageProperties spAk = StorageProperties.createPrimary(akProps);
        ParityAsserts.assertConfLacks(spAk.getHadoopStorageConfig(), "fs.s3a.assumed.role.arn");
        // Measured 2026-07-22: fs.s3a.assumed.role.credentials.provider is never null
        // because core-default.xml ships SimpleAWSCredentialsProvider as its default —
        // freeze that fe-core leaves the hadoop default untouched here.
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                spAk.getHadoopStorageConfig().get("fs.s3a.assumed.role.credentials.provider"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                spAk.getHadoopStorageConfig().get("fs.s3a.aws.credentials.provider"));
        // But the backend map still carries AWS_ROLE_ARN unconditionally.
        Assertions.assertEquals("arn:aws:iam::123456789012:role/doris",
                spAk.getBackendConfigProperties().get("AWS_ROLE_ARN"));
    }

    @Test
    public void testAnonymousBackendMapExact() {
        // 2.4-5: anonymous S3 still emits empty-string AK/SK keys, and (S3-specific)
        // AWS_CREDENTIALS_PROVIDER_TYPE stays "DEFAULT", not "ANONYMOUS".
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        StorageProperties sp = StorageProperties.createPrimary(props);
        ParityAsserts.assertExactMap(ParityAsserts.map(
                "AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com",
                "AWS_REGION", "us-east-1",
                "AWS_ACCESS_KEY", "",
                "AWS_SECRET_KEY", "",
                "AWS_MAX_CONNECTIONS", "50",
                "AWS_REQUEST_TIMEOUT_MS", "3000",
                "AWS_CONNECTION_TIMEOUT_MS", "1000",
                "use_path_style", "false",
                "AWS_CREDENTIALS_PROVIDER_TYPE", "DEFAULT"
        ), sp.getBackendConfigProperties());
        // v2 anonymous-capable default chain on the hadoop side.
        String chain = sp.getHadoopStorageConfig().get("fs.s3a.aws.credentials.provider");
        Assertions.assertNotNull(chain);
        Assertions.assertTrue(chain.contains("AnonymousCredentialsProvider"), chain);
        ParityAsserts.assertConfLacks(sp.getHadoopStorageConfig(), "fs.s3a.access.key");
    }

    @Test
    public void testRegionMissingThrows() {
        // 2.4-4: fe-core throws when region can neither be read nor derived.
        Map<String, String> props = new HashMap<>();
        props.put("fs.s3.support", "true");
        props.put("s3.endpoint", "http://192.168.0.1:9000");
        props.put("s3.access_key", "myAk");
        props.put("s3.secret_key", "mySk");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(props));
        Assertions.assertTrue(e.getMessage().startsWith("Region is not set"), e.getMessage());
    }

    @Test
    public void testOnlyAkWithoutSkThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        props.put("s3.access_key", "myAk");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(props));
        Assertions.assertTrue(e.getMessage().contains("Both the access key and the secret key"), e.getMessage());
    }
}
