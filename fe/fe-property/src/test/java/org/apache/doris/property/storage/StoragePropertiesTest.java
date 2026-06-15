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

package org.apache.doris.property.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Behavior tests for the extracted fe-property storage layer.
 *
 * <p>WHY these matter: the connector (e.g. {@code PaimonCatalogFactory}) cannot import fe-core's
 * {@code StorageProperties}, so today it hand-re-ports the {@code fs.s3a.*} derivation (the source of the MinIO
 * credential bug). This module exists so a connector can instead call
 * {@code StorageProperties.create(props).getHadoopConfigMap()}. These tests pin the two outputs a connector relies
 * on — the Hadoop config map ({@code fs.s3a.*}, applied to the connector's own {@code Configuration}) and the
 * BE-facing map ({@code AWS_*}) — so the derivation can never silently drift away from the legacy behavior.
 */
public class StoragePropertiesTest {

    private static Map<String, String> minioProps() {
        Map<String, String> p = new HashMap<>();
        // fs.minio.support pins MinIO selection deterministically (explicit flag disables guessIsMe heuristics).
        p.put("fs.minio.support", "true");
        p.put("minio.endpoint", "http://minio:9000");
        p.put("minio.access_key", "myak");
        p.put("minio.secret_key", "mysk");
        return p;
    }

    @Test
    public void minioProducesS3aHadoopConfigMap() {
        StorageProperties sp = StorageProperties.createPrimary(minioProps());
        Assertions.assertEquals(StorageProperties.Type.MINIO, sp.getType());

        // The module produces a Map (NOT a live Hadoop Configuration) for the connector to overlay.
        Map<String, String> hadoop = sp.getHadoopConfigMap();
        Assertions.assertNotNull(hadoop);
        Assertions.assertEquals("http://minio:9000", hadoop.get("fs.s3a.endpoint"));
        Assertions.assertEquals("myak", hadoop.get("fs.s3a.access.key"));
        Assertions.assertEquals("mysk", hadoop.get("fs.s3a.secret.key"));
        Assertions.assertEquals("us-east-1", hadoop.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", hadoop.get("fs.s3a.impl"));
        // disable-cache must be on for object stores (per-credential FileSystem isolation).
        Assertions.assertEquals("true", hadoop.get("fs.s3a.impl.disable.cache"));
    }

    @Test
    public void minioProducesBackendAwsMap() {
        StorageProperties sp = StorageProperties.createPrimary(minioProps());
        Map<String, String> be = sp.getBackendConfigProperties();
        Assertions.assertEquals("http://minio:9000", be.get("AWS_ENDPOINT"));
        Assertions.assertEquals("myak", be.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("mysk", be.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("us-east-1", be.get("AWS_REGION"));
        // MinIO tuning defaults (100/10000/10000) — the exact values the connector re-port had to match.
        Assertions.assertEquals("100", be.get("AWS_MAX_CONNECTIONS"));
    }

    @Test
    public void s3IsSelectedAndNormalizesUri() {
        Map<String, String> p = new HashMap<>();
        p.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        p.put("s3.access_key", "ak");
        p.put("s3.secret_key", "sk");
        p.put("s3.region", "us-east-1");
        StorageProperties sp = StorageProperties.createPrimary(p);
        Assertions.assertEquals(StorageProperties.Type.S3, sp.getType());
        Assertions.assertEquals("s3.us-east-1.amazonaws.com", sp.getHadoopConfigMap().get("fs.s3a.endpoint"));
        // Non-canonical schemes normalize to the canonical s3:// form BE understands.
        Assertions.assertEquals("s3://bucket/key", sp.validateAndNormalizeUri("s3a://bucket/key"));
    }

    @Test
    public void guessOrderingKeepsMinioAndS3Distinct() {
        // An amazonaws endpoint is S3, NOT MinIO: MinIO must defer to S3 so detection isn't hijacked.
        Map<String, String> s3 = new HashMap<>();
        s3.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        Assertions.assertTrue(S3Properties.guessIsMe(s3));
        Assertions.assertFalse(MinioProperties.guessIsMe(s3));

        // A dedicated minio.* key with a non-amazonaws endpoint is MinIO.
        Map<String, String> mi = new HashMap<>();
        mi.put("minio.endpoint", "http://minio:9000");
        mi.put("minio.access_key", "ak");
        Assertions.assertTrue(MinioProperties.guessIsMe(mi));
        Assertions.assertFalse(S3Properties.guessIsMe(mi));
    }

    @Test
    public void hadoopConfigMapIsNullForHttp() {
        // HTTP contributes no Hadoop storage config — the null map signals "skip overlay" to consumers.
        Map<String, String> p = new HashMap<>();
        p.put("fs.http.support", "true");
        p.put("uri", "https://example.com/a.csv");
        StorageProperties sp = StorageProperties.createPrimary(p);
        Assertions.assertEquals(StorageProperties.Type.HTTP, sp.getType());
        Assertions.assertNull(sp.getHadoopConfigMap());
    }
}
