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

package org.apache.doris.connector;

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * FIX-URI-NORMALIZE fe-core bridge test: pins that
 * {@link DefaultConnectorContext#normalizeStorageUri} rewrites a connector-supplied storage URI to
 * BE's canonical {@code s3://} scheme using the catalog's storage properties (the same
 * {@code LocationPath} normalization legacy {@code PaimonScanNode} applies via the 2-arg
 * {@code LocationPath.of(path, storagePropertiesMap)}). The paimon connector cannot import that
 * machinery, so this hook is its only access; without it a native ORC/Parquet read on an
 * OSS/COS/OBS warehouse reaches BE with an un-openable {@code oss://} path (data file fails, or a
 * deletion vector is silently dropped). FAILS before the fix (the method is a no-op default
 * returning the raw URI).
 */
public class DefaultConnectorContextNormalizeUriTest {

    private static final Supplier<ExecutionAuthenticator> NOOP_AUTH =
            () -> new ExecutionAuthenticator() {};

    /** A context whose storage-props supplier yields a real OSS storage-properties map, built with
     *  the same {@code StorageProperties.createAll} machinery a real OSS catalog uses. */
    private static DefaultConnectorContext ossContext() throws Exception {
        Map<String, String> oss = new HashMap<>();
        oss.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        oss.put("oss.access_key", "ak");
        oss.put("oss.secret_key", "sk");
        List<StorageProperties> all = StorageProperties.createAll(oss);
        Map<StorageProperties.Type, StorageProperties> map = all.stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity(), (a, b) -> a));
        return new DefaultConnectorContext("c", 1L, NOOP_AUTH, () -> map);
    }

    @Test
    public void normalizesOssSchemeToS3() throws Exception {
        // WHY: BE's scheme-dispatched S3 file factory only recognizes s3://; legacy LocationPath.of
        // rewrites oss:// (and cos/obs/s3a) -> s3://. This hook is the connector's ONLY access to that
        // normalization (it must not import LocationPath). MUTATION: returning the raw oss:// path
        // (the no-op SPI default) -> red.
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                ossContext().normalizeStorageUri("oss://bkt/warehouse/db/t/part-0.parquet"));
    }

    @Test
    public void s3SchemeIsUnchanged() throws Exception {
        // WHY: an already-canonical s3:// path must pass through unchanged (idempotent fast path).
        // MUTATION: mangling the s3:// path -> red.
        Assertions.assertEquals("s3://bkt/warehouse/f.parquet",
                ossContext().normalizeStorageUri("s3://bkt/warehouse/f.parquet"));
    }

    @Test
    public void nullOrBlankIsReturnedUnchanged() throws Exception {
        // WHY: defensive short-circuit before touching the storage-props supplier -> no NPE on a
        // null/blank path. MUTATION: NPE, or fabricating output from nothing -> red.
        Assertions.assertNull(ossContext().normalizeStorageUri(null));
        Assertions.assertEquals("", ossContext().normalizeStorageUri(""));
    }

    @Test
    public void failsLoudWhenNoStoragePropertiesForScheme() {
        // WHY: a context with no storage-properties map must FAIL LOUD on a real path rather than
        // silently shipping the raw oss:// to BE (which would corrupt reads). Mirrors legacy
        // LocationPath.of(path, {}) throwing StoragePropertiesException. The ctors that do not wire a
        // storage map are never used by paimon, but the fail-loud contract is pinned here.
        // MUTATION: swallowing the error and returning the raw path -> red.
        DefaultConnectorContext noStorage = new DefaultConnectorContext("c", 1L);
        Assertions.assertThrows(RuntimeException.class,
                () -> noStorage.normalizeStorageUri("oss://bkt/a/part-0.parquet"));
    }

    // ---- FIX-REST-VENDED-URI-NORMALIZE (P9-1): the 2-arg overload normalizes via the per-table
    //      vended token, which is the ONLY storage map a REST catalog has (its static map is empty). ----

    /** The raw per-table OSS vended token shape a REST catalog returns (mirrors
     *  DefaultConnectorContextVendTest / PaimonVendedCredentialsProviderTest). */
    private static Map<String, String> ossVendedToken() {
        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "STS.testAccessKey123");
        token.put("fs.oss.accessKeySecret", "testSecretKey456");
        token.put("fs.oss.securityToken", "testSessionToken789");
        token.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        return token;
    }

    @Test
    public void vendedRestCredentialsNormalizeUnderEmptyStaticMap() {
        // THE BUG (P9-1, BLOCKER): a REST catalog's static storage map is EMPTY by design (vended creds
        // are per-table/dynamic), so the static-only path throws "No storage properties found for schema:
        // oss" on a native ORC/Parquet read — the exact corner DV-025 deferred but never closed. The
        // 2-arg overload normalizes against the per-table VENDED token instead (legacy
        // VendedCredentialsFactory: the vended map REPLACES the empty static map). MUTATION: ignoring the
        // token (the old static-only path) -> throws -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L); // empty static map = REST
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                restCtx.normalizeStorageUri("oss://bkt/warehouse/db/t/part-0.parquet", ossVendedToken()));
    }

    @Test
    public void emptyTokenUnderEmptyStaticStillFailsLoud() {
        // WHY: prove the fix is the TOKEN, not a swallow — with an empty static map AND no vended token
        // there is genuinely no credential, so normalization must still FAIL LOUD (legacy parity) rather
        // than ship the raw oss:// to BE (silent read corruption). MUTATION: swallowing to the raw path
        // when the token is empty -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        Assertions.assertThrows(RuntimeException.class,
                () -> restCtx.normalizeStorageUri("oss://bkt/a/part-0.parquet", Collections.emptyMap()));
    }

    @Test
    public void staticMapPathUnaffectedByEmptyToken() throws Exception {
        // WHY: the 2-arg overload with an EMPTY token must fold to the static-map path byte-identically
        // to the 1-arg form, so non-REST (static-cred) reads are unchanged. MUTATION: an empty token
        // suppressing the static map -> no normalization / throw -> red.
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                ossContext().normalizeStorageUri(
                        "oss://bkt/warehouse/db/t/part-0.parquet", Collections.emptyMap()));
    }

    // ---- T06 write-sink file type: getBackendFileType resolves the BE file type via the SAME
    //      LocationPath the legacy IcebergTableSink used (broker-aware), returned as the enum NAME. ----

    @Test
    public void backendFileTypeForOssResolvesToS3ViaLocationPath() throws Exception {
        // WHY: the iceberg write sink must tell BE which file-system family opens the output path. The
        // engine resolves it through LocationPath.getTFileTypeForBE() (same as legacy), so an OSS data
        // location yields FILE_S3 (object store). Returned as the enum NAME (the SPI is Thrift-free).
        // MUTATION: scheme-only default that can't see storage props, or a wrong family -> red.
        Assertions.assertEquals(TFileType.FILE_S3.name(),
                ossContext().getBackendFileType("oss://bkt/warehouse/db/t/data", null));
    }

    @Test
    public void backendFileTypeVendedRestResolvesUnderEmptyStaticMap() {
        // WHY: a REST catalog's static storage map is empty; the vended token resolves the file type the
        // same way the vended-aware normalizeStorageUri resolves the path. MUTATION: ignoring the token
        // (static-only) throws "no storage properties" -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        Assertions.assertEquals(TFileType.FILE_S3.name(),
                restCtx.getBackendFileType("oss://bkt/warehouse/db/t/data", ossVendedToken()));
    }
}
