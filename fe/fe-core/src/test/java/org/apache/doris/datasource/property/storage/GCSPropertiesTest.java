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

import org.apache.doris.cloud.proto.Cloud.CredProviderTypePB;
import org.apache.doris.cloud.proto.Cloud.GcpCredentialPB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.storage.auth.GcpCredential;
import org.apache.doris.datasource.property.storage.auth.GcpCredentialProviderType;
import org.apache.doris.fs.obj.S3ObjStorage;
import org.apache.doris.thrift.TCredProviderType;
import org.apache.doris.thrift.TGcpCredentialProviderType;
import org.apache.doris.thrift.TObjStorageType;
import org.apache.doris.thrift.TS3StorageParam;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class GCSPropertiesTest {

    private Map<String, String> origProps;

    @BeforeEach
    public void setUp() {
        origProps = new HashMap<>();
    }

    @Test
    public void testGuessIsMeByEndpoint() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        origProps.clear();
        origProps.put("s3.endpoint", "https://storage.googleapis.com");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        origProps.clear();
        origProps.put("endpoint", "https://my.custom.endpoint.com");
        Assertions.assertFalse(GCSProperties.guessIsMe(origProps));
        origProps.put("gs.endpoint", "https://my.custom.endpoint.com");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
    }

    @Test
    public void testGuessIsMeByNativeAuthenticationAndLocation() {
        origProps.put(StorageProperties.FS_PROVIDER_KEY, "gcp");
        origProps.put(S3Properties.REGION, "us-east1");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        Assertions.assertInstanceOf(GCSProperties.class, StorageProperties.createPrimary(origProps));

        origProps.clear();
        origProps.put(S3Properties.REGION, "us-east1");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        Assertions.assertFalse(S3Properties.guessIsMe(origProps));
        Assertions.assertInstanceOf(GCSProperties.class, StorageProperties.createPrimary(origProps));

        origProps.clear();
        origProps.put("uri", "gs://load-and-tvf-bucket/path/file.parquet");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        Assertions.assertInstanceOf(GCSProperties.class, StorageProperties.createPrimary(origProps));

        origProps.clear();
        origProps.put("warehouse", "GS://external-catalog-bucket/warehouse");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        Assertions.assertTrue(GCSProperties.guessIsMe(origProps));
        Assertions.assertInstanceOf(GCSProperties.class, StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testDefaultValues() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.access_key", "myAccessKey");
        origProps.put("gs.secret_key", "mySecretKey");
        GCSProperties gcsProperties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("https://storage.googleapis.com", gcsProperties.getEndpoint());
        Assertions.assertEquals("us-east1", gcsProperties.getRegion()); // default
        Assertions.assertEquals("myAccessKey", gcsProperties.getAccessKey());
        Assertions.assertEquals("mySecretKey", gcsProperties.getSecretKey());
        Assertions.assertEquals("false", gcsProperties.getUsePathStyle());
    }

    @Test
    public void testOverrideRegionAndPathStyle() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.access_key", "myAccessKey");
        origProps.put("gs.secret_key", "mySecretKey");
        origProps.put("gs.use_path_style", "true");

        GCSProperties gcsProperties = (GCSProperties) StorageProperties.createPrimary(origProps);
        gcsProperties.setRegion("asia-northeast1");

        Assertions.assertEquals("asia-northeast1", gcsProperties.getRegion());
        Assertions.assertEquals("true", gcsProperties.getUsePathStyle());
    }

    @Test
    public void testGenerateBackendS3Configuration() {
        origProps.put("gs.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.access_key", "myAccessKey");
        origProps.put("gs.secret_key", "mySecretKey");
        origProps.put("gs.connection.maximum", "200");
        origProps.put("gs.connection.request.timeout", "999");
        origProps.put("gs.connection.timeout", "888");
        origProps.put("gs.use_path_style", "true");

        GCSProperties gcsProperties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> s3Props = gcsProperties.generateBackendS3Configuration();

        Assertions.assertEquals("https://storage.googleapis.com", s3Props.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east1", s3Props.get("AWS_REGION"));
        Assertions.assertEquals("myAccessKey", s3Props.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("mySecretKey", s3Props.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("200", s3Props.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("999", s3Props.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("888", s3Props.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", s3Props.get("use_path_style"));
    }

    @Test
    public void testGCSAwsCredentialsProvider() throws Exception {
        Map<String, String> gcsProps = new HashMap<>();
        gcsProps.put("fs.gcs.support", "true");
        gcsProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "ANONYMOUS");
        GCSProperties gcsStorageProperties = (GCSProperties) StorageProperties.createPrimary(gcsProps);
        Assertions.assertEquals(AnonymousCredentialsProvider.class, gcsStorageProperties.getAwsCredentialsProvider().getClass());
        gcsProps.remove(GCSProperties.CREDENTIAL_PROVIDER_TYPE);
        gcsProps.put("gs.access_key", "myAccessKey");
        gcsProps.put("gs.secret_key", "mySecretKey");
        gcsStorageProperties = (GCSProperties) StorageProperties.createPrimary(gcsProps);
        Assertions.assertEquals(StaticCredentialsProvider.class, gcsStorageProperties.getAwsCredentialsProvider().getClass());
    }

    @Test
    public void testS3DisableHadoopCache() {
        Map<String, String> props = Maps.newHashMap();
        props.put("fs.gcs.support", "true");
        GCSProperties s3Properties = (GCSProperties) StorageProperties.createPrimary(props);
        // No longer disabled by default: the patched FileSystem keys its cache by doris.fs.cache.key.
        Assertions.assertNull(s3Properties.hadoopStorageConfig.get("fs.gs.impl.disable.cache"));
        props.put("fs.gs.impl.disable.cache", "true");
        s3Properties = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertTrue(s3Properties.hadoopStorageConfig.getBoolean("fs.gs.impl.disable.cache", false));
        props.put("fs.gs.impl.disable.cache", "false");
        s3Properties = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertFalse(s3Properties.hadoopStorageConfig.getBoolean("fs.gs.impl.disable.cache", false));
        props.put("fs.gs.impl.disable.cache", "null");
        s3Properties = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertFalse(s3Properties.hadoopStorageConfig.getBoolean("fs.gs.impl.disable.cache", false));
    }

    @Test
    public void testNativeHadoopAuthenticationConfiguration() throws Exception {
        for (String provider : new String[] {"default", "compute_engine"}) {
            for (String account : new String[] {"", "target@test-project.iam.gserviceaccount.com"}) {
                Map<String, String> props = Maps.newHashMap();
                props.put("provider", "GCP");
                props.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, provider);
                props.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT, account);
                GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
                Configuration conf = storage.hadoopStorageConfig;
                Assertions.assertEquals(GoogleHadoopFileSystem.class.getName(), conf.get("fs.gs.impl"));
                Assertions.assertEquals("default".equals(provider) ? "APPLICATION_DEFAULT" : "COMPUTE_ENGINE",
                        conf.get("fs.gs.auth.type"));
                Assertions.assertEquals(account, conf.get("fs.gs.auth.impersonation.service.account"));
                // Resolve and initialize the real connector without requesting live GCP credentials.
                conf.setBoolean("fs.gs.lazy.init.enable", true);
                try (FileSystem fs = FileSystem.newInstance(URI.create("gs://test-bucket/warehouse"), conf)) {
                    Assertions.assertInstanceOf(GoogleHadoopFileSystem.class, fs);
                    Assertions.assertEquals("gs", fs.getScheme());
                }
                Assertions.assertEquals(storage.getFsCacheFingerprint(), conf.get(StorageProperties.FS_CACHE_KEY_PROPERTY));
            }
        }
    }

    @Test
    public void testHmacHadoopAuthenticationIsUnchanged() {
        origProps.put(StorageProperties.FS_GCS_SUPPORT, "true");
        origProps.put("gs.access_key", "test-ak");
        origProps.put("gs.secret_key", "test-sk");
        GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem",
                storage.hadoopStorageConfig.get("fs.gs.impl"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                storage.hadoopStorageConfig.get(Constants.AWS_CREDENTIALS_PROVIDER));
    }

    @Test
    public void testNativeGcpDefaultCredentials() {
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "default");

        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertTrue(properties.hasGcpCredential());
        Assertions.assertEquals(GcpCredentialProviderType.DEFAULT,
                properties.getGcpCredentialProviderType());
        Assertions.assertEquals("DEFAULT", properties.getBackendConfigProperties()
                .get(GCSProperties.CREDENTIAL_PROVIDER_TYPE));
        Assertions.assertFalse(properties.getBackendConfigProperties()
                .containsKey("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    public void testNativeGcpCredentialProviderTypeNormalization() {
        Map<String, GcpCredentialProviderType> cases = new HashMap<>();
        cases.put("DEFAULT", GcpCredentialProviderType.DEFAULT);
        cases.put("default", GcpCredentialProviderType.DEFAULT);
        cases.put("DeFaUlT", GcpCredentialProviderType.DEFAULT);
        cases.put(" COMPUTE_ENGINE ", GcpCredentialProviderType.COMPUTE_ENGINE);

        cases.forEach((value, expected) -> {
            origProps.clear();
            origProps.put(StorageProperties.FS_PROVIDER_KEY, "GCP");
            origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, value);

            GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
            Assertions.assertEquals(expected, properties.getGcpCredentialProviderType(), value);
            Assertions.assertEquals(expected.name(), properties.getBackendConfigProperties()
                    .get(GCSProperties.CREDENTIAL_PROVIDER_TYPE), value);
        });
    }

    @Test
    public void testNativeGcpRejectsInvalidCredentialProviderTypes() {
        for (String value : new String[] {"ADC", "", "unknown"}) {
            origProps.clear();
            origProps.put(StorageProperties.FS_PROVIDER_KEY, "GCP");
            origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, value);

            IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StorageProperties.createPrimary(origProps), value);
            Assertions.assertTrue(exception.getMessage().contains(GCSProperties.CREDENTIAL_PROVIDER_TYPE), value);
            Assertions.assertTrue(exception.getMessage().contains("DEFAULT"), value);
            Assertions.assertTrue(exception.getMessage().contains("COMPUTE_ENGINE"), value);
        }
    }

    @Test
    public void testNativeGcpDefaultsImpersonationToDefault() {
        origProps.put(StorageProperties.FS_PROVIDER_KEY, "GCP");
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "target@my-project.iam.gserviceaccount.com");

        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(GcpCredentialProviderType.DEFAULT,
                properties.getGcpCredentialProviderType());
        ObjectStoreInfoPB obj = S3Properties.getObjStoreInfoPB(origProps).build();
        Assertions.assertTrue(obj.getCredential().getGcpCredential().hasCredentialProviderType());
        Assertions.assertEquals(GcpCredentialPB.CredentialProviderType.DEFAULT,
                obj.getCredential().getGcpCredential().getCredentialProviderType());
        Assertions.assertEquals(TGcpCredentialProviderType.DEFAULT,
                S3Properties.getS3TStorageParam(origProps).getCredential()
                        .getGcpCredential().getCredentialProviderType());
    }

    @Test
    public void testNativeGcpEmptyTargetDefaultsToDefault() {
        origProps.put(StorageProperties.FS_PROVIDER_KEY, "GCP");
        Assertions.assertTrue(S3Properties.getObjStoreInfoPB(origProps).hasCredential());
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT, "");
        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertTrue(properties.hasGcpCredential());
        Assertions.assertEquals(GcpCredentialProviderType.DEFAULT, properties.getGcpCredentialProviderType());
        Assertions.assertEquals("", properties.getGcpCredential().getImpersonationServiceAccount());
        Assertions.assertEquals(TGcpCredentialProviderType.DEFAULT,
                S3Properties.getS3TStorageParam(origProps).getCredential()
                        .getGcpCredential().getCredentialProviderType());
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT, " ");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testNativeGcpComputeEngineAndImpersonation() {
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "target@my-project.iam.gserviceaccount.com");

        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(GcpCredentialProviderType.COMPUTE_ENGINE,
                properties.getGcpCredentialProviderType());
        Assertions.assertEquals("target@my-project.iam.gserviceaccount.com",
                properties.getBackendConfigProperties()
                        .get(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT));
    }

    @Test
    public void testNativeGcpDefaultAndImpersonation() {
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "target@my-project.iam.gserviceaccount.com");

        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(GcpCredentialProviderType.DEFAULT,
                properties.getGcpCredentialProviderType());
        Assertions.assertEquals("target@my-project.iam.gserviceaccount.com",
                properties.getBackendConfigProperties()
                        .get(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT));
    }

    @Test
    public void testNativeGcpDefaultServiceAccountEmails() {
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");

        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "123456789-compute@developer.gserviceaccount.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));

        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "my-project@appspot.gserviceaccount.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testNativeGcpComputeEngineCredentials() {
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");

        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(GcpCredentialProviderType.COMPUTE_ENGINE,
                properties.getGcpCredentialProviderType());
        Assertions.assertEquals("COMPUTE_ENGINE", properties.getBackendConfigProperties()
                .get(GCSProperties.CREDENTIAL_PROVIDER_TYPE));
        Assertions.assertFalse(properties.getBackendConfigProperties()
                .containsKey(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT));
    }

    @Test
    public void testNativeGcpCloudAndThriftMapping() {
        origProps.put(S3Properties.ENDPOINT, "https://storage.googleapis.com");
        origProps.put(S3Properties.REGION, "us-east1");
        origProps.put(S3Properties.BUCKET, "bucket");
        origProps.put(S3Properties.ROOT_PATH, "root");
        origProps.put(S3Properties.ACCESS_KEY, "");
        origProps.put(S3Properties.SECRET_KEY, "");
        origProps.put(StorageProperties.FS_PROVIDER_KEY, "GCP");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "target@my-project.iam.gserviceaccount.com");

        ObjectStoreInfoPB objectStoreInfo = S3Properties.getObjStoreInfoPB(origProps).build();
        Assertions.assertTrue(objectStoreInfo.hasCredential());
        Assertions.assertTrue(objectStoreInfo.getCredential().hasGcpCredential());
        Assertions.assertEquals(GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE,
                objectStoreInfo.getCredential().getGcpCredential().getCredentialProviderType());
        Assertions.assertEquals("target@my-project.iam.gserviceaccount.com",
                objectStoreInfo.getCredential().getGcpCredential().getImpersonationServiceAccount());
        TS3StorageParam thrift = S3Properties.getS3TStorageParam(origProps);
        Assertions.assertEquals(TObjStorageType.GCP, thrift.getProvider());
        Assertions.assertTrue(thrift.getCredential().isSetGcpCredential());
        Assertions.assertEquals(TGcpCredentialProviderType.COMPUTE_ENGINE,
                thrift.getCredential().getGcpCredential().getCredentialProviderType());
        Assertions.assertEquals("target@my-project.iam.gserviceaccount.com",
                thrift.getCredential().getGcpCredential().getImpersonationServiceAccount());

        origProps.remove(GCSProperties.CREDENTIAL_PROVIDER_TYPE);
        origProps.remove(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT);
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE.toUpperCase(), "default");
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT.toUpperCase(),
                "uppercase@my-project.iam.gserviceaccount.com");
        objectStoreInfo = S3Properties.getObjStoreInfoPB(origProps).build();
        Assertions.assertEquals(GcpCredentialPB.CredentialProviderType.DEFAULT,
                objectStoreInfo.getCredential().getGcpCredential().getCredentialProviderType());
        Assertions.assertEquals("uppercase@my-project.iam.gserviceaccount.com",
                objectStoreInfo.getCredential().getGcpCredential().getImpersonationServiceAccount());
    }

    @Test
    public void testNativeGcpValidation() {
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        origProps.put("gs.access_key", "ak");
        origProps.put("gs.secret_key", "sk");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(origProps));

        origProps.clear();
        origProps.put("fs.gcs.support", "true");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        origProps.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                "target@my-project.iam.gserviceaccount.com");
        Assertions.assertDoesNotThrow(() -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testNativeGcpRejectsAwsAuthenticationAndProviderConflicts() {
        String[] conflictingKeys = {
                S3Properties.ACCESS_KEY,
                S3Properties.SECRET_KEY,
                S3Properties.SESSION_TOKEN,
                S3Properties.Env.ACCESS_KEY,
                S3Properties.Env.SECRET_KEY,
                S3Properties.Env.TOKEN,
                S3Properties.ROLE_ARN,
                S3Properties.EXTERNAL_ID,
                S3Properties.CREDENTIALS_PROVIDER_TYPE,
                S3Properties.Env.ROLE_ARN,
                S3Properties.Env.EXTERNAL_ID,
                S3Properties.Env.CREDENTIALS_PROVIDER_TYPE
        };
        for (String key : conflictingKeys) {
            origProps.clear();
            origProps.put("fs.gcs.support", "true");
            origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
            origProps.put(key, "value");
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StorageProperties.createPrimary(origProps), key);
        }

        for (String provider : new String[] {"S3", "AWS"}) {
            origProps.clear();
            origProps.put(S3Properties.REGION, "us-east1");
            origProps.put(StorageProperties.FS_PROVIDER_KEY, provider);
            origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StorageProperties.createPrimary(origProps), provider);
        }

        origProps.clear();
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        origProps.put(S3Properties.REGION, "us-east-1");
        origProps.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testAwsCredentialProviderTypeDoesNotSelectGcp() {
        origProps.put(S3Properties.ENDPOINT, "s3.us-west-2.amazonaws.com");
        origProps.put(S3Properties.REGION, "us-west-2");
        origProps.put(S3Properties.CREDENTIALS_PROVIDER_TYPE, "INSTANCE_PROFILE");

        Assertions.assertFalse(GCSProperties.guessIsMe(origProps));
        Assertions.assertInstanceOf(S3Properties.class, StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testGsPrefixedConnectionProperties() {
        origProps.put("provider", "gcp");
        origProps.put("gs.endpoint", "https://custom.example.com");
        origProps.put("gs.access_key", "access");
        origProps.put("gs.secret_key", "secret");
        origProps.put("gs.session_token", "token");
        origProps.put("gs.connection.maximum", "123");
        origProps.put("gs.connection.request.timeout", "456");
        origProps.put("gs.connection.timeout", "789");
        origProps.put("gs.use_path_style", "true");
        origProps.put("gs.force_parsing_by_standard_uri", "true");
        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backend = properties.getBackendConfigProperties();
        Assertions.assertEquals("https://custom.example.com", backend.get("AWS_ENDPOINT"));
        Assertions.assertEquals("access", backend.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("secret", backend.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("token", backend.get("AWS_TOKEN"));
        Assertions.assertEquals("123", backend.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("456", backend.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("789", backend.get("AWS_CONNECTION_TIMEOUT_MS"));
        Assertions.assertEquals("true", backend.get("use_path_style"));
        Assertions.assertEquals("true", properties.getForceParsingByStandardUrl());

        origProps.remove("provider");
        Assertions.assertInstanceOf(GCSProperties.class, StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testGsPrefixPrecedenceAndLegacyAliases() {
        origProps.put("s3.endpoint", "https://storage.googleapis.com");
        origProps.put("gs.credential_provider_type", "DEFAULT");
        GCSProperties legacy = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals(GcpCredentialProviderType.DEFAULT, legacy.getGcpCredentialProviderType());

        origProps.put("gs.endpoint", "https://custom.example.com");
        origProps.put("gs.credential_provider_type", "COMPUTE_ENGINE");
        origProps.put("gs.impersonation_service_account", "target@project.iam.gserviceaccount.com");
        GCSProperties properties = (GCSProperties) StorageProperties.createPrimary(origProps);
        Assertions.assertEquals("https://custom.example.com", properties.getEndpoint());
        Assertions.assertEquals(GcpCredentialProviderType.COMPUTE_ENGINE, properties.getGcpCredentialProviderType());
        Map<String, String> backend = properties.getBackendConfigProperties();
        Assertions.assertEquals("COMPUTE_ENGINE", backend.get("gs.credential_provider_type"));
        Assertions.assertEquals("target@project.iam.gserviceaccount.com",
                backend.get("gs.impersonation_service_account"));

        origProps.put("gs.credential_provider_type", "");
        Assertions.assertThrows(IllegalArgumentException.class, () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testGcpCredentialConflictsBeforeAliasConversion() {
        for (String key : new String[] {"gs.access_key", "gs.secret_key", "gs.session_token",
                "s3.access_key", "s3.secret_key", "s3.session_token"}) {
            origProps.clear();
            origProps.put("provider", "GCP");
            origProps.put("gs.credential_provider_type", "DEFAULT");
            origProps.put(key, "conflicting-credential");
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StorageProperties.createPrimary(origProps), key);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> S3Properties.getObjStoreInfoPB(origProps), key);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> S3Properties.getS3TStorageParam(origProps), key);
        }
    }

    @Test
    public void testOmittedAuthenticationUsesAdcAcrossStorageSelectors() {
        Map<String, String> selectors = new HashMap<>();
        selectors.put("provider", "GCP");
        selectors.put("fs.gcs.support", "true");
        selectors.put("gs.endpoint", "https://storage.googleapis.com");
        selectors.put("s3.endpoint", "https://storage.googleapis.com");
        selectors.put("uri", "gs://test-bucket/data.parquet");
        selectors.put("warehouse", "gs://test-bucket/warehouse");
        selectors.forEach((key, value) -> {
            Map<String, String> props = new HashMap<>();
            props.put(key, value);
            GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
            Assertions.assertTrue(storage.hasGcpCredential(), key);
            Assertions.assertEquals(GcpCredentialProviderType.DEFAULT, storage.getGcpCredentialProviderType(), key);
            Assertions.assertEquals("DEFAULT", storage.getCredentialProviderType(), key);
            Assertions.assertEquals("", storage.getGcpCredential().getImpersonationServiceAccount(), key);
            Assertions.assertEquals("APPLICATION_DEFAULT", storage.hadoopStorageConfig.get("fs.gs.auth.type"), key);
            Assertions.assertEquals(GoogleHadoopFileSystem.class.getName(),
                    storage.hadoopStorageConfig.get("fs.gs.impl"), key);
            Map<String, String> backend = storage.getBackendConfigProperties();
            Assertions.assertEquals("DEFAULT", backend.get(GCSProperties.CREDENTIAL_PROVIDER_TYPE), key);
            Assertions.assertFalse(backend.containsKey("AWS_CREDENTIALS_PROVIDER_TYPE"), key);
            ObjectStoreInfoPB objectStoreInfo = S3Properties.getObjStoreInfoPB(props).build();
            Assertions.assertEquals(ObjectStoreInfoPB.Provider.GCP, objectStoreInfo.getProvider(), key);
            Assertions.assertEquals(GcpCredentialPB.CredentialProviderType.DEFAULT,
                    objectStoreInfo.getCredential().getGcpCredential().getCredentialProviderType(), key);
            TS3StorageParam thrift = S3Properties.getS3TStorageParam(props);
            Assertions.assertEquals(TObjStorageType.GCP, thrift.getProvider(), key);
            Assertions.assertTrue(thrift.isSetCredential(), key);
            Assertions.assertEquals(TGcpCredentialProviderType.DEFAULT,
                    thrift.getCredential().getGcpCredential().getCredentialProviderType(), key);
        });
    }

    @Test
    public void testExplicitAnonymousUsesUnsignedFeHadoopAndBackendClients() {
        for (String key : new String[] {GCSProperties.CREDENTIAL_PROVIDER_TYPE,
                GCSProperties.CREDENTIAL_PROVIDER_TYPE.toUpperCase(),
                S3Properties.CREDENTIALS_PROVIDER_TYPE, S3Properties.Env.CREDENTIALS_PROVIDER_TYPE}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", "GCP");
            props.put(key, " anonymous ");
            GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
            Assertions.assertFalse(storage.hasGcpCredential(), key);
            Assertions.assertEquals("ANONYMOUS", storage.getCredentialProviderType(), key);
            Assertions.assertInstanceOf(AnonymousCredentialsProvider.class, storage.getAwsCredentialsProvider(), key);
            Assertions.assertEquals("org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
                    storage.hadoopStorageConfig.get(Constants.AWS_CREDENTIALS_PROVIDER), key);
            Map<String, String> backend = storage.getBackendConfigProperties();
            Assertions.assertEquals("ANONYMOUS", backend.get("AWS_CREDENTIALS_PROVIDER_TYPE"), key);
            Assertions.assertFalse(backend.containsKey(GCSProperties.CREDENTIAL_PROVIDER_TYPE), key);
            TS3StorageParam thrift = S3Properties.getS3TStorageParam(props);
            Assertions.assertFalse(thrift.isSetCredential(), key);
            Assertions.assertEquals(TCredProviderType.ANONYMOUS, thrift.getCredProviderType(), key);
            // Building the real client must not try to discover ADC or contact the metadata server.
            try (software.amazon.awssdk.services.s3.S3Client client = storage.createS3Client()) {
                Assertions.assertNotNull(client);
            }
        }
    }

    @Test
    public void testGcsClientsApplyConfiguredEndpointScheme() throws Exception {
        String originalScheme = Config.s3_client_http_scheme;
        try {
            Config.s3_client_http_scheme = "https";
            for (String providerType : new String[] {"COMPUTE_ENGINE", "ANONYMOUS"}) {
                for (String endpoint : new String[] {"127.0.0.1:9000", "http://127.0.0.1:9000"}) {
                    Map<String, String> props = new HashMap<>();
                    props.put("provider", "GCP");
                    props.put(GCSProperties.ENDPOINT, endpoint);
                    props.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, providerType);
                    GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
                    URI expected = URI.create(endpoint.startsWith("http://") ? endpoint : "https://" + endpoint);
                    try (S3Client client = storage.createS3Client();
                            S3ObjStorage objStorage = new S3ObjStorage(storage)) {
                        Assertions.assertEquals(expected, client.serviceClientConfiguration().endpointOverride().get());
                        Assertions.assertEquals(expected,
                                objStorage.getClient().serviceClientConfiguration().endpointOverride().get());
                    }
                }
            }
        } finally {
            Config.s3_client_http_scheme = originalScheme;
        }
    }

    @Test
    public void testAnonymousAliasesDoNotReuseAdcFileSystem() throws Exception {
        Map<String, String> base = new HashMap<>();
        base.put("provider", "GCP");
        GCSProperties adc = (GCSProperties) StorageProperties.createPrimary(base);
        String anonymousFingerprint = null;
        for (String key : new String[] {GCSProperties.CREDENTIAL_PROVIDER_TYPE,
                GCSProperties.CREDENTIAL_PROVIDER_TYPE.toUpperCase(Locale.ROOT),
                S3Properties.CREDENTIALS_PROVIDER_TYPE, S3Properties.Env.CREDENTIALS_PROVIDER_TYPE}) {
            Map<String, String> props = new HashMap<>(base);
            props.put(key, " anonymous ");
            GCSProperties anonymous = (GCSProperties) StorageProperties.createPrimary(props);
            Assertions.assertNotEquals(adc.getFsCacheFingerprint(), anonymous.getFsCacheFingerprint(), key);
            if (anonymousFingerprint == null) {
                anonymousFingerprint = anonymous.getFsCacheFingerprint();
            }
            Assertions.assertEquals(anonymousFingerprint, anonymous.getFsCacheFingerprint(), key);
            Assertions.assertEquals(anonymousFingerprint,
                    anonymous.getBackendConfigProperties().get(StorageProperties.FS_CACHE_KEY_PROPERTY), key);

            for (boolean anonymousFirst : new boolean[] {false, true}) {
                Configuration firstConf = cacheOnlyConfiguration(anonymousFirst ? anonymous : adc);
                Configuration secondConf = cacheOnlyConfiguration(anonymousFirst ? adc : anonymous);
                URI uri = URI.create("gs://cache-test-" + UUID.randomUUID() + "/warehouse");
                try (FileSystem first = FileSystem.get(uri, firstConf);
                        FileSystem second = FileSystem.get(uri, secondConf)) {
                    Assertions.assertNotSame(first, second, key);
                    Assertions.assertSame(first, FileSystem.get(uri, firstConf), key);
                    Assertions.assertSame(second, FileSystem.get(uri, secondConf), key);
                }
            }
        }
    }

    private Configuration cacheOnlyConfiguration(GCSProperties storage) {
        // Exercise the real Hadoop cache with the emitted credential key, without GCS I/O or ADC discovery.
        Configuration conf = new Configuration(false);
        conf.set("fs.gs.impl", RawLocalFileSystem.class.getName());
        conf.set(StorageProperties.FS_CACHE_KEY_PROPERTY,
                storage.hadoopStorageConfig.get(StorageProperties.FS_CACHE_KEY_PROPERTY));
        return conf;
    }

    @Test
    public void testNativeCredentialCacheIdentityUsesResolvedProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "GCP");
        String adcFingerprint = StorageProperties.createPrimary(props).getFsCacheFingerprint();
        for (String key : new String[] {GCSProperties.CREDENTIAL_PROVIDER_TYPE,
                GCSProperties.CREDENTIAL_PROVIDER_TYPE.toUpperCase(Locale.ROOT)}) {
            props.put(key, " default ");
            Assertions.assertEquals(adcFingerprint, StorageProperties.createPrimary(props).getFsCacheFingerprint());
            props.put(key, " compute_engine ");
            Assertions.assertNotEquals(adcFingerprint, StorageProperties.createPrimary(props).getFsCacheFingerprint());
            props.remove(key);
        }

        String targetFingerprint = null;
        for (String key : new String[] {GCSProperties.IMPERSONATION_SERVICE_ACCOUNT,
                GCSProperties.IMPERSONATION_SERVICE_ACCOUNT.toUpperCase(Locale.ROOT)}) {
            props.put(key, "target@test-project.iam.gserviceaccount.com");
            String fingerprint = StorageProperties.createPrimary(props).getFsCacheFingerprint();
            Assertions.assertNotEquals(adcFingerprint, fingerprint);
            if (targetFingerprint == null) {
                targetFingerprint = fingerprint;
            }
            Assertions.assertEquals(targetFingerprint, fingerprint);
            props.put(key, "other@test-project.iam.gserviceaccount.com");
            Assertions.assertNotEquals(targetFingerprint, StorageProperties.createPrimary(props).getFsCacheFingerprint());
            props.remove(key);
        }
    }

    @Test
    public void testAnonymousRejectsConflictingCredentials() {
        for (String key : new String[] {"gs.access_key", "gs.secret_key", "gs.session_token",
                GCSProperties.IMPERSONATION_SERVICE_ACCOUNT, S3Properties.ROLE_ARN}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", "GCP");
            props.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "ANONYMOUS");
            props.put(key, "target@test.iam.gserviceaccount.com");
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> StorageProperties.createPrimary(props), key);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> S3Properties.getS3TStorageParam(props), key);
        }
    }

    @Test
    public void testAnonymousVaultRemainsUnsupported() {
        Map<String, String> selectors = new HashMap<>();
        selectors.put("provider", "GCP");
        selectors.put("fs.gcs.support", "true");
        selectors.put("gs.endpoint", "https://storage.googleapis.com");
        selectors.put("s3.endpoint", "https://storage.googleapis.com");
        selectors.put("uri", "gs://test-bucket/data.parquet");
        selectors.put("warehouse", "gs://test-bucket/warehouse");
        selectors.forEach((selector, value) -> {
            for (String key : new String[] {GCSProperties.CREDENTIAL_PROVIDER_TYPE,
                    GCSProperties.CREDENTIAL_PROVIDER_TYPE.toUpperCase(Locale.ROOT),
                    S3Properties.CREDENTIALS_PROVIDER_TYPE, S3Properties.Env.CREDENTIALS_PROVIDER_TYPE}) {
                Map<String, String> props = new HashMap<>();
                props.put(selector, value);
                props.put(key, " anonymous ");
                IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                        () -> S3Properties.getObjStoreInfoPB(props), selector + ": " + key);
                Assertions.assertTrue(error.getMessage().contains("storage vaults"));
                // Vault validation must not reject the same anonymous configuration for FE/BE reads.
                GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
                Assertions.assertFalse(storage.hasGcpCredential());
                Assertions.assertEquals(TCredProviderType.ANONYMOUS,
                        S3Properties.getS3TStorageParam(props).getCredProviderType());
            }
        });
    }

    @Test
    public void testGcpVaultAnonymousValidationDoesNotChangeAwsMapping() {
        for (String key : new String[] {S3Properties.CREDENTIALS_PROVIDER_TYPE,
                S3Properties.Env.CREDENTIALS_PROVIDER_TYPE}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", "S3");
            props.put(key, "ANONYMOUS");
            Assertions.assertEquals(CredProviderTypePB.ANONYMOUS,
                    S3Properties.getObjStoreInfoPB(props).getCredProviderType());
            Assertions.assertEquals(TCredProviderType.ANONYMOUS,
                    S3Properties.getS3TStorageParam(props).getCredProviderType());
        }
    }

    @Test
    public void testDefaultGcpCredentialsDoNotOverrideHmacAliasesOrOtherProviders() {
        for (String[] keys : new String[][] {{"gs.access_key", "gs.secret_key"},
                {"s3.access_key", "s3.secret_key"}, {"AWS_ACCESS_KEY", "AWS_SECRET_KEY"},
                {"access_key", "secret_key"}, {"ACCESS_KEY", "SECRET_KEY"}}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", "GCP");
            props.put(keys[0], "hmac-ak");
            props.put(keys[1], "hmac-sk");
            GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
            Assertions.assertFalse(storage.hasGcpCredential(), keys[0]);
            Assertions.assertEquals("", storage.getCredentialProviderType(), keys[0]);
            Assertions.assertInstanceOf(StaticCredentialsProvider.class, storage.getAwsCredentialsProvider());
            Assertions.assertFalse(S3Properties.getObjStoreInfoPB(props).hasCredential());
            Assertions.assertFalse(S3Properties.getS3TStorageParam(props).isSetCredential());
        }
        for (String provider : new String[] {"S3", "AZURE", "OSS"}) {
            Map<String, String> props = new HashMap<>();
            props.put("provider", provider);
            Assertions.assertFalse(S3Properties.getObjStoreInfoPB(props).hasCredential(), provider);
            Assertions.assertFalse(S3Properties.getS3TStorageParam(props).isSetCredential(), provider);
        }
        Assertions.assertFalse(S3Properties.getObjStoreInfoPB(new HashMap<>()).hasCredential());
        Assertions.assertFalse(S3Properties.getS3TStorageParam(new HashMap<>()).isSetCredential());
    }

    @Test
    public void testLegacyS3ProviderPreservesGcsHmacAndAnonymousAccess() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "S3");
        props.put("s3.endpoint", "https://storage.googleapis.com");
        props.put("s3.access_key", "hmac-ak");
        props.put("s3.secret_key", "hmac-sk");
        GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertFalse(storage.hasGcpCredential());
        Assertions.assertEquals("", storage.getCredentialProviderType());
        Assertions.assertEquals("hmac-ak", storage.getAwsCredentialsProvider().resolveCredentials().accessKeyId());
        Assertions.assertEquals("hmac-sk", storage.getAwsCredentialsProvider().resolveCredentials().secretAccessKey());
        Assertions.assertEquals("hmac-ak", storage.getBackendConfigProperties().get("AWS_ACCESS_KEY"));
        Assertions.assertFalse(S3Properties.getObjStoreInfoPB(props).hasCredential());
        TS3StorageParam thrift = S3Properties.getS3TStorageParam(props);
        Assertions.assertFalse(thrift.isSetCredential());
        Assertions.assertEquals(TObjStorageType.AWS, thrift.getProvider());

        props.remove("s3.access_key");
        props.remove("s3.secret_key");
        props.put(S3Properties.CREDENTIALS_PROVIDER_TYPE, "ANONYMOUS");
        storage = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertTrue(storage.isAnonymous());
        Assertions.assertInstanceOf(AnonymousCredentialsProvider.class, storage.getAwsCredentialsProvider());
        Assertions.assertEquals(TCredProviderType.ANONYMOUS,
                S3Properties.getS3TStorageParam(props).getCredProviderType());
    }

    @Test
    public void testLegacyS3OmittedAuthenticationUsesUnsignedClients() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "S3");
        props.put("s3.endpoint", "https://storage.googleapis.com");
        GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertTrue(storage.isAnonymous());
        Assertions.assertFalse(storage.hasGcpCredential());
        Assertions.assertEquals("ANONYMOUS", storage.getCredentialProviderType());
        Assertions.assertInstanceOf(AnonymousCredentialsProvider.class, storage.getAwsCredentialsProvider());
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem",
                storage.hadoopStorageConfig.get("fs.gs.impl"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
                storage.hadoopStorageConfig.get(Constants.AWS_CREDENTIALS_PROVIDER));
        Assertions.assertEquals("ANONYMOUS", storage.getBackendConfigProperties().get("AWS_CREDENTIALS_PROVIDER_TYPE"));
        TS3StorageParam thrift = S3Properties.getS3TStorageParam(props);
        Assertions.assertEquals(TCredProviderType.ANONYMOUS, thrift.getCredProviderType());
        Assertions.assertFalse(thrift.isSetCredential());
        Assertions.assertEquals(TObjStorageType.AWS, thrift.getProvider());
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> S3Properties.getObjStoreInfoPB(props));
        Assertions.assertTrue(error.getMessage().contains("storage vaults"));
    }

    @Test
    public void testResolvedAuthenticationIsSharedByClientsAndFingerprint() {
        Map<String, String> props = new HashMap<>();
        props.put("provider", "GCP");
        props.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE.toUpperCase(Locale.ROOT), " compute_engine ");
        props.put(GCSProperties.IMPERSONATION_SERVICE_ACCOUNT, "target@test.iam.gserviceaccount.com");
        GCSProperties storage = (GCSProperties) StorageProperties.createPrimary(props);
        GcpCredential credential = storage.getGcpCredential();
        String fingerprint = storage.getFsCacheFingerprint();

        // Changing the caller's map must not change the identity of an already initialized instance.
        props.clear();
        props.put("provider", "GCP");
        props.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "ANONYMOUS");
        Assertions.assertTrue(storage.hasGcpCredential());
        Assertions.assertSame(credential, storage.getGcpCredential());
        Assertions.assertThrows(IllegalStateException.class, storage::getAwsCredentialsProvider);
        Assertions.assertEquals("COMPUTE_ENGINE", storage.getCredentialProviderType());
        Assertions.assertEquals("COMPUTE_ENGINE", storage.getHadoopStorageConfig().get("fs.gs.auth.type"));
        Assertions.assertEquals("target@test.iam.gserviceaccount.com",
                storage.getHadoopStorageConfig().get("fs.gs.auth.impersonation.service.account"));
        Assertions.assertEquals("COMPUTE_ENGINE",
                storage.getBackendConfigProperties().get(GCSProperties.CREDENTIAL_PROVIDER_TYPE));
        Assertions.assertEquals(fingerprint, storage.getFsCacheFingerprint());

        GCSProperties anonymous = (GCSProperties) StorageProperties.createPrimary(props);
        Assertions.assertFalse(anonymous.hasGcpCredential());
        Assertions.assertNotEquals(fingerprint, anonymous.getFsCacheFingerprint());
    }

    @Test
    public void testLegacyAnonymousRejectsCredentialConflictsAcrossPaths() {
        for (String anonymousKey : new String[] {S3Properties.CREDENTIALS_PROVIDER_TYPE,
                S3Properties.Env.CREDENTIALS_PROVIDER_TYPE}) {
            for (String conflictKey : new String[] {"gs.access_key", "gs.secret_key", "gs.session_token",
                    S3Properties.ROLE_ARN, S3Properties.EXTERNAL_ID}) {
                Map<String, String> props = new HashMap<>();
                props.put("provider", "GCP");
                props.put(anonymousKey, " anonymous ");
                props.put(conflictKey, "conflicting-value");
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> StorageProperties.createPrimary(props), conflictKey);
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> S3Properties.getS3TStorageParam(props), conflictKey);
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> S3Properties.getObjStoreInfoPB(props), conflictKey);
            }
        }
    }

}
