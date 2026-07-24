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

package org.apache.doris.datasource.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Phase B3 golden comparison: for every input scenario of the A5 parity matrix, the new
 * {@link StorageAdapter} facade must produce EXACTLY what the legacy typed
 * {@code StorageProperties} classes (the oracle, still present until Phase D) produce —
 * backend map key-for-key, the complete Hadoop Configuration key-for-key, storage name,
 * type and schemas. Do not weaken these assertions: they are the deletion gate for the
 * legacy package.
 */
public class StorageAdapterParityTest {

    // ---------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Full-parity assertion of one adapter against one legacy oracle instance. */
    private static void assertParity(StorageProperties oracle, StorageAdapter adapter) throws Exception {
        Assertions.assertEquals(oracle.getType().name(), adapter.getType().name(), "type");
        Assertions.assertEquals(oracle.getStorageName(), adapter.getStorageName(), "storageName");
        Assertions.assertEquals(oracleSchemas(oracle), adapter.schemas(), "schemas");
        Assertions.assertEquals(new TreeMap<>(oracle.getBackendConfigProperties()),
                new TreeMap<>(adapter.getBackendConfigProperties()), "backend map");
        assertConfEquals(oracle.getHadoopStorageConfig(), adapter.getHadoopStorageConfig());
    }

    private static void assertBothMatch(Map<String, String> props) throws Exception {
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        assertParity(oracle, adapter);
    }

    /** schemas() is protected on the legacy class; read it reflectively for the comparison. */
    @SuppressWarnings("unchecked")
    private static Set<String> oracleSchemas(StorageProperties oracle) throws Exception {
        Class<?> clazz = oracle.getClass();
        while (clazz != null) {
            try {
                Method method = clazz.getDeclaredMethod("schemas");
                method.setAccessible(true);
                return (Set<String>) method.invoke(oracle);
            } catch (NoSuchMethodException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new IllegalStateException("no schemas() on " + oracle.getClass());
    }

    /** The ENTIRE Configuration (hadoop defaults included) must be identical, key for key. */
    private static void assertConfEquals(Configuration expected, Configuration actual) {
        if (expected == null || actual == null) {
            Assertions.assertEquals(expected, actual, "one hadoop config is null, the other is not");
            return;
        }
        Assertions.assertEquals(dump(expected), dump(actual), "hadoop configuration");
    }

    private static Map<String, String> dump(Configuration conf) {
        Map<String, String> m = new TreeMap<>();
        conf.forEach(entry -> m.put(entry.getKey(), entry.getValue()));
        return m;
    }

    // ---------------------------------------------------------------
    // S3 (ledger 2.4-3/4/5 scenarios)
    // ---------------------------------------------------------------

    @Test
    public void testS3BasicAkSk() throws Exception {
        assertBothMatch(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk"));
    }

    @Test
    public void testS3SessionToken() throws Exception {
        assertBothMatch(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk",
                "s3.session_token", "tok123"));
    }

    @Test
    public void testS3UserFsPassthroughAndUserDisableCacheOverride() throws Exception {
        assertBothMatch(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk",
                "fs.custom.flag", "v1",
                "fs.s3a.impl.disable.cache", "false"));
    }

    @Test
    public void testS3AssumeRoleWithoutAk() throws Exception {
        assertBothMatch(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris",
                "s3.external_id", "ext-1"));
    }

    @Test
    public void testS3AssumeRoleWithAkPresent() throws Exception {
        // 2.4-3: with static AK the role keys stay in the backend map but must NOT reach
        // the hadoop config.
        assertBothMatch(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk",
                "s3.role_arn", "arn:aws:iam::123456789012:role/doris"));
    }

    @Test
    public void testS3Anonymous() throws Exception {
        // 2.4-5: empty-string AK/SK keys must be present; provider type stays DEFAULT for S3.
        assertBothMatch(map("s3.endpoint", "https://s3.us-east-1.amazonaws.com"));
    }

    @Test
    public void testS3ExplicitCredentialsProviderType() throws Exception {
        assertBothMatch(map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.credentials_provider_type", "INSTANCE_PROFILE"));
    }

    @Test
    public void testS3RegionMissingThrowsOnBothSides() {
        Map<String, String> props = map(
                "fs.s3.support", "true",
                "s3.endpoint", "http://192.168.0.1:9000",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk");
        IllegalArgumentException oracleEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(new HashMap<>(props)));
        IllegalArgumentException adapterEx = Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageAdapter.of(new HashMap<>(props)));
        Assertions.assertTrue(oracleEx.getMessage().startsWith("Region is not set"), oracleEx.getMessage());
        Assertions.assertTrue(adapterEx.getMessage().startsWith("Region is not set"), adapterEx.getMessage());
    }

    @Test
    public void testS3AkWithoutSkThrowsOnBothSides() {
        Map<String, String> props = map(
                "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.access_key", "myAk");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageProperties.createPrimary(new HashMap<>(props)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StorageAdapter.of(new HashMap<>(props)));
    }

    // ---------------------------------------------------------------
    // S3-compatible dialects
    // ---------------------------------------------------------------

    @Test
    public void testOssBasic() throws Exception {
        assertBothMatch(map(
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "myAk",
                "oss.secret_key", "mySk"));
    }

    @Test
    public void testOssAnonymousEmitsAnonymousProviderType() throws Exception {
        // 2.4-5: dialects (unlike S3) emit AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS when anon.
        assertBothMatch(map(
                "fs.oss.support", "true",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com"));
    }

    @Test
    public void testOssNonStandardEndpointRewrittenToInternal() throws Exception {
        assertBothMatch(map(
                "fs.oss.support", "true",
                "s3.endpoint", "s3.cn-hangzhou.aliyuncs.com",
                "s3.region", "cn-hangzhou",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk"));
    }

    @Test
    public void testObsBasic() throws Exception {
        assertBothMatch(map(
                "obs.endpoint", "obs.cn-north-4.myhuaweicloud.com",
                "obs.access_key", "myAk",
                "obs.secret_key", "mySk"));
    }

    @Test
    public void testCosBasic() throws Exception {
        assertBothMatch(map(
                "cos.endpoint", "cos.ap-guangzhou.myqcloud.com",
                "cos.access_key", "myAk",
                "cos.secret_key", "mySk"));
    }

    @Test
    public void testGcsBasic() throws Exception {
        assertBothMatch(map(
                "gs.endpoint", "https://storage.googleapis.com",
                "gs.access_key", "myAk",
                "gs.secret_key", "mySk"));
    }

    @Test
    public void testGcsAnonymous() throws Exception {
        // fe-core GCSProperties sets no anonymous s3a provider; the SPI extra is dropped
        // by the facade.
        assertBothMatch(map(
                "fs.gcs.support", "true",
                "gs.endpoint", "https://storage.googleapis.com"));
    }

    @Test
    public void testMinioBasic() throws Exception {
        assertBothMatch(map(
                "minio.endpoint", "http://127.0.0.1:9000",
                "minio.access_key", "myAk",
                "minio.secret_key", "mySk"));
    }

    @Test
    public void testOzoneExplicit() throws Exception {
        assertBothMatch(map(
                "fs.ozone.support", "true",
                "ozone.endpoint", "http://127.0.0.1:9878",
                "ozone.access_key", "myAk",
                "ozone.secret_key", "mySk"));
    }

    // ---------------------------------------------------------------
    // HDFS family
    // ---------------------------------------------------------------

    @Test
    public void testHdfsSimpleAuth() throws Exception {
        Map<String, String> props = map(
                "fs.hdfs.support", "true",
                "uri", "hdfs://nameservice1/path/f.orc",
                "hadoop.username", "hadoop");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        assertParity(oracle, adapter);
        Assertions.assertFalse(adapter.isKerberos());
        Assertions.assertNotNull(adapter.getExecutionAuthenticator());
    }

    @Test
    public void testHdfsKerberos() throws Exception {
        Map<String, String> props = map(
                "fs.hdfs.support", "true",
                "uri", "hdfs://nameservice1/path/f.orc",
                "hdfs.authentication.type", "kerberos",
                "hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM",
                "hadoop.kerberos.keytab", "/etc/doris/doris.keytab");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        assertParity(oracle, adapter);
        Assertions.assertTrue(adapter.isKerberos());
    }

    @Test
    public void testHdfsHaConfigCopiedToBackend() throws Exception {
        assertBothMatch(map(
                "fs.hdfs.support", "true",
                "fs.defaultFS", "hdfs://ns1",
                "dfs.nameservices", "ns1",
                "dfs.ha.namenodes.ns1", "nn1,nn2",
                "dfs.namenode.rpc-address.ns1.nn1", "host1:8020",
                "dfs.namenode.rpc-address.ns1.nn2", "host2:8020",
                "dfs.client.failover.proxy.provider.ns1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"));
    }

    @Test
    public void testJfsUriRidesHdfsIdentity() throws Exception {
        // fe-core rode jfs:// on HdfsProperties: the adapter must report HDFS type/name/schemas
        // and the two auth keys HdfsProperties always emitted.
        assertBothMatch(map("uri", "jfs://myjfs/warehouse/t"));
    }

    @Test
    public void testJfsKerberosDelta() throws Exception {
        assertBothMatch(map(
                "uri", "jfs://myjfs/warehouse/t",
                "hdfs.authentication.type", "kerberos",
                "hadoop.kerberos.principal", "doris/_HOST@EXAMPLE.COM",
                "hadoop.kerberos.keytab", "/etc/doris/doris.keytab"));
    }

    @Test
    public void testOssHdfsBasic() throws Exception {
        assertBothMatch(map(
                "oss.hdfs.endpoint", "cn-hangzhou.oss-dls.aliyuncs.com",
                "oss.hdfs.access_key", "myAk",
                "oss.hdfs.secret_key", "mySk",
                "uri", "oss://mybucket/warehouse/t"));
    }

    @Test
    public void testOssHdfsDeprecatedFlagAndDlfEndpoint() throws Exception {
        assertBothMatch(map(
                "oss.hdfs.enabled", "true",
                "oss.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "oss.access_key", "myAk",
                "oss.secret_key", "mySk",
                "oss.region", "cn-hangzhou"));
    }

    // ---------------------------------------------------------------
    // Azure (ledger 2.4-7)
    // ---------------------------------------------------------------

    @Test
    public void testAzureSharedKey() throws Exception {
        assertBothMatch(map(
                "provider", "azure",
                "azure.account_name", "myacct",
                "azure.account_key", "mykey",
                "container", "mycontainer"));
    }

    @Test
    public void testAzureOauth2BackendDumpsWholeConf() throws Exception {
        assertBothMatch(map(
                "provider", "azure",
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "myacct.dfs.core.windows.net",
                "azure.oauth2_client_id", "cid",
                "azure.oauth2_client_secret", "csec",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tid/oauth2/token",
                "iceberg.catalog.type", "rest",
                "fs.custom.flag", "v1"));
    }

    @Test
    public void testAzureOauth2OutsideIcebergRestThrowsOnBothSides() {
        Map<String, String> props = map(
                "provider", "azure",
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "myacct.dfs.core.windows.net",
                "azure.oauth2_client_id", "cid",
                "azure.oauth2_client_secret", "csec",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tid/oauth2/token");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> StorageProperties.createPrimary(new HashMap<>(props)));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> StorageAdapter.of(new HashMap<>(props)));
    }

    // ---------------------------------------------------------------
    // Broker / Local / HTTP
    // ---------------------------------------------------------------

    @Test
    public void testBrokerIdentityAndParams() throws Exception {
        Map<String, String> props = map(
                "broker.name", "hdfs_broker",
                "broker.username", "u1",
                "broker.password", "p1",
                "unrelated.key", "x");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        assertParity(oracle, adapter);
        BrokerProperties brokerOracle = (BrokerProperties) oracle;
        Assertions.assertEquals(brokerOracle.getBrokerName(), adapter.getBrokerName());
        Assertions.assertEquals(new TreeMap<>(brokerOracle.getBrokerParams()),
                new TreeMap<>(adapter.getBrokerParams()));
    }

    @Test
    public void testLocalIdentity() throws Exception {
        Map<String, String> props = map("file_path", "/tmp/data.csv");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        assertParity(oracle, adapter);
        Assertions.assertEquals(oracle.validateAndGetUri(props), adapter.validateAndGetUri(props));
    }

    @Test
    public void testHttpIdentity() throws Exception {
        Map<String, String> props = map(
                "fs.http.support", "true",
                "uri", "https://example.com/data.csv",
                "http.header.Authorization", "Bearer tok");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        assertParity(oracle, adapter);
        Assertions.assertEquals(oracle.validateAndGetUri(props), adapter.validateAndGetUri(props));
    }

    // ---------------------------------------------------------------
    // URI validation/normalization (facade-owned for S3 family / Azure)
    // ---------------------------------------------------------------

    @Test
    public void testS3UriMethodsMatchOracle() throws Exception {
        Map<String, String> props = map(
                "s3.endpoint", "s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1",
                "s3.access_key", "myAk",
                "s3.secret_key", "mySk",
                "uri", "cos://my-bucket/my/key.csv");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        Assertions.assertEquals(oracle.validateAndGetUri(props), adapter.validateAndGetUri(props));
        for (String uri : new String[] {
                "s3://bucket/key",
                "oss://bucket/dir/key",
                "cos://bucket/key",
                "gs://bucket/key",
                "s3a://bucket/key",
                // AWS allows dots in bucket names; a non-aliyun dotted authority must NOT be
                // mistaken for an OSS bucket-domain and rewritten.
                "s3://my.bucket.name/dir/key",
                "https://s3.us-east-1.amazonaws.com/my-bucket/my-key"}) {
            Assertions.assertEquals(oracle.validateAndNormalizeUri(uri),
                    adapter.validateAndNormalizeUri(uri), "uri: " + uri);
        }
        // Blank path and missing uri throw the same unchecked exception on both sides.
        Assertions.assertThrows(StoragePropertiesException.class, () -> oracle.validateAndNormalizeUri(""));
        Assertions.assertThrows(StoragePropertiesException.class, () -> adapter.validateAndNormalizeUri(""));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> oracle.validateAndGetUri(map("k", "v")));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> adapter.validateAndGetUri(map("k", "v")));
    }

    /**
     * Full URI matrix of external_table_p0.refactor_storage_param.test_s3_tvf_s3_storage (OSS
     * leg): virtual-hosted bucket-domain uris under object-store schemes ({@code
     * s3://bucket.endpoint/key}, {@code oss://bucket.endpoint/key}) crossed with every endpoint
     * alias spelling, use_path_style and force_parsing_by_standard_uri. The legacy oracle is
     * OSSProperties.validateAndNormalizeUri (rewriteOssBucketIfNecessary + S3PropertyUtils);
     * the facade must be byte-identical for every cell.
     */
    @Test
    public void testOssVirtualHostedUriMatrixMatchesOracle() throws Exception {
        final String bucket = "doris-regression-hk";
        final String endpoint = "oss-cn-hongkong-internal.aliyuncs.com";
        final String key = "outfile_different_s3/exp_x_0.parquet";
        final String normalized = "s3://" + bucket + "/" + key;

        String[] uris = new String[] {
                "s3://" + bucket + "." + endpoint + "/" + key,   // regression cell (CI)
                "oss://" + bucket + "." + endpoint + "/" + key,  // regression cell (CI)
                "http://" + bucket + "." + endpoint + "/" + key,
                "https://" + bucket + "." + endpoint + "/" + key,
                "http://" + endpoint + "/" + bucket + "/" + key,
                "s3://" + bucket + "/" + key,
                "cos://" + bucket + "/" + key,
        };
        // Cells that must normalize to s3://bucket/key regardless of the flag combination:
        // the bucket-domain rewrite is uri-only and the s3:// fast path ignores the flags.
        Set<String> flagIndependent = Set.of(uris[0], uris[1], uris[5], uris[6]);

        for (String endpointKey : new String[] {"s3.endpoint", "oss.endpoint", "AWS_ENDPOINT", null}) {
            for (String usePathStyle : new String[] {"false", "true"}) {
                for (String forceParsing : new String[] {"false", "true"}) {
                    Map<String, String> props = map(
                            "s3.access_key", "myAk",
                            "s3.secret_key", "mySk",
                            "s3.region", "cn-hongkong",
                            "use_path_style", usePathStyle,
                            "force_parsing_by_standard_uri", forceParsing);
                    if (endpointKey != null) {
                        props.put(endpointKey, endpoint);
                    } else {
                        // endpoint omitted (groovy passes an empty endpoint key): OSS is bound
                        // and the endpoint derived from the uri property / region instead.
                        props.put("uri", uris[0]);
                    }
                    StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
                    StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
                    for (String uri : uris) {
                        String cell = "endpointKey=" + endpointKey + " use_path_style=" + usePathStyle
                                + " force=" + forceParsing + " uri=" + uri;
                        String oracleOut = oracle.validateAndNormalizeUri(uri);
                        Assertions.assertEquals(oracleOut, adapter.validateAndNormalizeUri(uri), cell);
                        if (flagIndependent.contains(uri)) {
                            Assertions.assertEquals(normalized, oracleOut, "oracle self-check " + cell);
                        }
                    }
                    // http(s) virtual-hosted uris normalize to s3://bucket/key when path style
                    // is off (standard-uri parse splits the first host label off).
                    if ("false".equals(usePathStyle)) {
                        Assertions.assertEquals(normalized, adapter.validateAndNormalizeUri(uris[2]));
                        Assertions.assertEquals(normalized, adapter.validateAndNormalizeUri(uris[3]));
                    } else {
                        // path-style parse takes the first path segment as the bucket.
                        Assertions.assertEquals(normalized, adapter.validateAndNormalizeUri(uris[4]));
                    }
                }
            }
        }

        // oss-dls (OSS-HDFS) locations were excluded from the OSS binding in legacy code and
        // must not be rewritten by the facade either.
        Assertions.assertEquals("s3://b.cn-hangzhou.oss-dls.aliyuncs.com/p",
                StorageUriUtils.validateAndNormalizeS3Uri(
                        "oss://b.cn-hangzhou.oss-dls.aliyuncs.com/p", "false", "false"));
    }

    @Test
    public void testAzureUriMethodsMatchOracle() throws Exception {
        Map<String, String> props = map(
                "provider", "azure",
                "azure.endpoint", "https://myacct.blob.core.windows.net",
                "azure.account_name", "myacct",
                "azure.account_key", "myKey");
        StorageProperties oracle = StorageProperties.createPrimary(new HashMap<>(props));
        StorageAdapter adapter = StorageAdapter.of(new HashMap<>(props));
        for (String uri : new String[] {
                "wasbs://container@myacct.blob.core.windows.net/dir/file.txt",
                "abfss://container@myacct.dfs.core.windows.net/dir/file.txt",
                "abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/a.parquet",
                "https://myacct.blob.core.windows.net/container/dir/file.txt",
                "s3://container/dir/file.txt"}) {
            Assertions.assertEquals(oracle.validateAndNormalizeUri(uri),
                    adapter.validateAndNormalizeUri(uri), "uri: " + uri);
        }
        Map<String, String> loadProps = map("uri", "wasbs://c@myacct.blob.core.windows.net/f");
        Assertions.assertEquals(oracle.validateAndGetUri(loadProps), adapter.validateAndGetUri(loadProps));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> oracle.validateAndNormalizeUri("ftp://x/y"));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> adapter.validateAndNormalizeUri("ftp://x/y"));
    }

    // ---------------------------------------------------------------
    // Routing: ofAll vs createAll, guess kill-switch, empty map
    // ---------------------------------------------------------------

    @Test
    public void testOfAllAmbiguousDoubleHitMatchesCreateAll() throws Exception {
        // aliyuncs endpoint guesses OSS, s3.region guesses S3, plus default HDFS at index 0.
        Map<String, String> props = map(
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "myAk",
                "oss.secret_key", "mySk",
                "s3.region", "cn-hangzhou");
        assertOfAllMatchesCreateAll(props);
    }

    @Test
    public void testOfAllExplicitOssSupportSuppressesGuessAndFallback() throws Exception {
        Map<String, String> props = map(
                "fs.oss.support", "true",
                "oss.endpoint", "oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "myAk",
                "oss.secret_key", "mySk",
                "s3.region", "cn-hangzhou");
        assertOfAllMatchesCreateAll(props);
    }

    @Test
    public void testEmptyMapPrimaryThrowsAndAllYieldsHdfsFallback() throws Exception {
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> StorageProperties.createPrimary(new HashMap<>()));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> StorageAdapter.of(new HashMap<>()));
        assertOfAllMatchesCreateAll(new HashMap<>());
    }

    private static void assertOfAllMatchesCreateAll(Map<String, String> props) throws Exception {
        List<StorageProperties> oracles;
        try {
            oracles = StorageProperties.createAll(new HashMap<>(props));
        } catch (UserException e) {
            throw new IllegalStateException(e);
        }
        List<StorageAdapter> adapters = StorageAdapter.ofAll(new HashMap<>(props));
        Assertions.assertEquals(oracles.size(), adapters.size(), "createAll/ofAll size");
        for (int i = 0; i < oracles.size(); i++) {
            assertParity(oracles.get(i), adapters.get(i));
        }
    }
}
