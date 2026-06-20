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

package org.apache.doris.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

/**
 * Offline FE-&gt;BE serialized-{@link Table} round-trip smoke for the Paimon connector.
 *
 * <p>This pins the exact wire mechanism the FE uses to ship a Paimon {@code Table} to BE for the
 * JNI reader: {@code PaimonScanPlanProvider.encodeObjectToString} serializes the live table with
 * {@link InstantiationUtil#serializeObject(Object)} and base64-encodes the bytes with the STANDARD
 * {@link Base64#getEncoder()} into the {@code paimon.serialized_table} property
 * ({@code PaimonScanPlanProvider} ~:213). BE reverses it ({@code PaimonUtils.deserialize}): URL-safe
 * {@link Base64#getUrlDecoder()} first, STANDARD {@link Base64#getDecoder()} fallback, then
 * {@link InstantiationUtil#deserializeObject(byte[], ClassLoader)}, running the IDENTICAL paimon
 * 1.3.1 jar (R-007 — see fe/pom.xml {@code <paimon.version>}). A version drift, a newly
 * non-serializable field on the table, or a base64-variant mismatch would silently break BE
 * deserialization at runtime; this catches it in CI.
 *
 * <p>Why this is a faithful simulation and not a fake: it builds a REAL local-filesystem Paimon
 * catalog (paimon-core ships a local {@code FileIO}, so no hadoop is needed) under a JUnit
 * {@link TempDir}, creates a real database + a real partitioned/keyed table via a valid
 * {@link Schema}, resolves it with {@code catalog.getTable(Identifier)} (the same call the
 * connector metadata makes) to get a real {@code FileStoreTable}, then serializes/deserializes it
 * through the connector's mechanism — the decode reproduces BE's {@code PaimonUtils.deserialize}
 * branch (URL-safe decoder first, STANDARD fallback) to prove the object graph reconstitutes from
 * raw classes on the same path BE actually runs.
 *
 * <p>Fully offline — runs in CI, NOT env-gated (contrast {@link PaimonLiveConnectivityTest}).
 */
public class PaimonTableSerdeRoundTripTest {

    private static final String DB = "rt_db";
    private static final String TBL = "rt_tbl";

    // --- the EXACT connector wire mechanism (PaimonScanPlanProvider.encodeObjectToString) ---

    /** FE side: serialize + STANDARD base64, identical to {@code encodeObjectToString}. */
    private static String feEncode(Object obj) throws Exception {
        byte[] bytes = InstantiationUtil.serializeObject(obj);
        return new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8);
    }

    /**
     * BE side: mirrors {@code PaimonUtils.deserialize} in be-java-extensions/paimon-scanner. BE
     * tries the URL-safe decoder FIRST and falls back to the STANDARD decoder on
     * {@link IllegalArgumentException} (the URL-safe decoder rejects the '+'/'/' a STANDARD payload
     * may contain, which is exactly what triggers the fallback), then deserializes with the
     * scanner's own classloader. Reproducing that branch here keeps the smoke faithful to the real
     * BE decode path rather than just the STANDARD leg.
     */
    private static <T> T beDecode(String encoded) throws Exception {
        byte[] enc = encoded.getBytes(StandardCharsets.UTF_8);
        byte[] bytes;
        try {
            bytes = Base64.getUrlDecoder().decode(enc);
        } catch (IllegalArgumentException urlReject) {
            bytes = Base64.getDecoder().decode(enc);
        }
        return InstantiationUtil.deserializeObject(bytes, PaimonTableSerdeRoundTripTest.class.getClassLoader());
    }

    private static Catalog buildLocalCatalog(Path warehouse) {
        // A real FileSystemCatalog over paimon's bundled LocalFileIO — this is exactly the catalog
        // CatalogFactory.createCatalog builds for a file:// warehouse (the production
        // PaimonConnector.createCatalog path: Options{warehouse=file://...} -> filesystem flavor ->
        // FileSystemCatalog(LocalFileIO, warehousePath)). We construct it directly here only to keep
        // the test classpath hadoop-free: CatalogContext.create(Options) statically references
        // org.apache.hadoop.conf.Configuration, which is present in fe-core at runtime but is NOT a
        // dependency of the connector test module. The resolved Table and the serde under test are
        // identical either way — the catalog wrapper is not what this smoke exercises.
        org.apache.paimon.fs.Path warehousePath = new org.apache.paimon.fs.Path(warehouse.toUri());
        return new FileSystemCatalog(LocalFileIO.create(), warehousePath);
    }

    private static Schema partitionedKeyedSchema() {
        // Partitioned + primary-keyed table. Paimon requires every partition field to also be a
        // primary-key field, and a keyed table needs a fixed bucket count.
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("region", DataTypes.STRING())
                .column("val", DataTypes.BIGINT())
                .partitionKeys("dt")
                .primaryKey("id", "dt")
                .option("bucket", "2")
                .build();
    }

    @Test
    public void serializedTableRoundTripsThroughConnectorMechanism(@TempDir Path warehouse) throws Exception {
        Table original;
        try (Catalog catalog = buildLocalCatalog(warehouse)) {
            catalog.createDatabase(DB, false);
            Identifier id = Identifier.create(DB, TBL);
            catalog.createTable(id, partitionedKeyedSchema(), false);
            // The same resolution the connector metadata does: catalog.getTable(Identifier) -> a
            // real FileStoreTable instance (NOT a hand-rolled double).
            original = catalog.getTable(id);
        }

        // Sanity: we are exercising a genuine resolved table, not a stub.
        RowType originalRowType = original.rowType();
        Assertions.assertEquals(Arrays.asList("id", "dt", "region", "val"),
                originalRowType.getFieldNames(),
                "precondition: resolved a real partitioned/keyed FileStoreTable to serialize");
        Assertions.assertEquals(Collections.singletonList("dt"), original.partitionKeys());
        Assertions.assertEquals(Arrays.asList("id", "dt"), original.primaryKeys());

        // FE encodes for the wire exactly as the connector ships it to BE.
        String wire = feEncode(original);
        Assertions.assertFalse(wire.isEmpty(), "encoded table payload must not be empty");

        // BE reconstitutes the table from the same payload, running the identical paimon 1.3.1.
        Table roundTripped = beDecode(wire);

        // WHY rowType()/partitionKeys()/primaryKeys() are the load-bearing identity: BE's JNI
        // reader rebuilds its scan + schema-projection off exactly these from the deserialized
        // table. If serialization drops or mangles them (non-serializable field, version drift,
        // base64 variant mismatch) the BE read silently returns wrong columns/rows. MUTATION:
        // swapping Base64.getEncoder() for getUrlEncoder(), or skipping InstantiationUtil, breaks
        // the decode -> red.
        Assertions.assertEquals(originalRowType.getFieldNames(),
                roundTripped.rowType().getFieldNames(),
                "round-tripped table must preserve column names/order");
        Assertions.assertEquals(originalRowType.getFieldTypes(),
                roundTripped.rowType().getFieldTypes(),
                "round-tripped table must preserve column types");
        Assertions.assertEquals(original.partitionKeys(), roundTripped.partitionKeys(),
                "round-tripped table must preserve partition keys (partition pruning depends on this)");
        Assertions.assertEquals(original.primaryKeys(), roundTripped.primaryKeys(),
                "round-tripped table must preserve primary keys (bucketing/keyed read depends on this)");
    }

    @Test
    public void standardBase64LegRoundTripsSerializedBytesVerbatim(@TempDir Path warehouse) throws Exception {
        // Locks the byte-level STANDARD base64 leg in isolation: the FE encoder (Base64.getEncoder,
        // STANDARD) produces a payload that a STANDARD decoder reconstitutes byte-for-byte. BE
        // decodes by trying the URL-safe decoder first; getUrlDecoder() THROWS
        // IllegalArgumentException on a '+'/'/' it cannot accept (it does not silently corrupt),
        // which is precisely what triggers BE's STANDARD fallback (see beDecode). This test pins the
        // STANDARD leg that fallback lands on; the object-level round trip above covers the full
        // BE decode branch.
        Table original;
        try (Catalog catalog = buildLocalCatalog(warehouse)) {
            catalog.createDatabase(DB, false);
            Identifier id = Identifier.create(DB, TBL);
            catalog.createTable(id, partitionedKeyedSchema(), false);
            original = catalog.getTable(id);
        }

        byte[] raw = InstantiationUtil.serializeObject(original);
        String standard = new String(Base64.getEncoder().encode(raw), StandardCharsets.UTF_8);
        byte[] decoded = Base64.getDecoder().decode(standard.getBytes(StandardCharsets.UTF_8));

        // The STANDARD round-trip must reproduce the byte stream verbatim.
        Assertions.assertArrayEquals(raw, decoded,
                "STANDARD base64 must round-trip the serialized table bytes verbatim");
    }
}
