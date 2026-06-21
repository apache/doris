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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link IcebergCatalogFactory}, the pure flavor-resolution core. Mirrors the role
 * of {@code PaimonCatalogFactoryTest}: every method is a pure transform over plain Maps / Strings
 * (no env, no clock, no live catalog), so the tests are entirely offline. No Mockito.
 *
 * <p>P6.1 baseline: the per-flavor catalog-impl class names MUST mirror the legacy fe-core
 * {@code IcebergConnector.resolveCatalogImpl} switch. The s3tables/dlf bespoke instantiation fixes
 * are later tasks; here we pin the impl-name routing the current production code performs.
 */
public class IcebergCatalogFactoryTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // ---------------------------------------------------------------------
    // resolveFlavor — lower-cased iceberg.catalog.type, null when absent/blank
    // ---------------------------------------------------------------------

    @Test
    public void resolveFlavorLowerCasesTheCatalogType() {
        // WHY: the second-level dispatch keys off the flavor; the legacy code lower-cases the raw
        // iceberg.catalog.type so a user who writes "REST"/"Hms" still routes correctly. MUTATION:
        // returning the raw value (no toLowerCase) -> "REST" != "rest" -> red.
        Assertions.assertEquals("rest",
                IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "REST")));
        Assertions.assertEquals("hms",
                IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "Hms")));
        Assertions.assertEquals("glue",
                IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "glue")));
    }

    @Test
    public void resolveFlavorReturnsNullWhenAbsent() {
        // WHY: an absent iceberg.catalog.type must resolve to null (the "no second-level flavor"
        // signal), NOT throw or invent a default. MUTATION: defaulting to a flavor string -> red.
        Assertions.assertNull(IcebergCatalogFactory.resolveFlavor(Collections.emptyMap()));
    }

    @Test
    public void resolveFlavorReturnsNullWhenBlank() {
        // WHY: a present-but-empty value is treated as absent (the production guard is
        // null-or-isEmpty), so it must also fold to null. MUTATION: dropping the isEmpty() guard ->
        // returns "" -> red.
        Assertions.assertNull(IcebergCatalogFactory.resolveFlavor(props("iceberg.catalog.type", "")));
    }

    // ---------------------------------------------------------------------
    // resolveCatalogImpl — per-flavor catalog-impl class name
    // ---------------------------------------------------------------------

    @Test
    public void resolveCatalogImplMapsRestToRestCatalog() {
        // WHY: each flavor must resolve to the EXACT catalog-impl class CatalogUtil/the bespoke path
        // instantiates; a wrong class name would build the wrong catalog backend. These names are the
        // parity contract with legacy IcebergConnector.resolveCatalogImpl. MUTATION: any wrong/empty
        // class name -> red.
        Assertions.assertEquals("org.apache.iceberg.rest.RESTCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("rest"));
    }

    @Test
    public void resolveCatalogImplMapsHmsToHiveCatalog() {
        Assertions.assertEquals("org.apache.iceberg.hive.HiveCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("hms"));
    }

    @Test
    public void resolveCatalogImplMapsGlueToGlueCatalog() {
        Assertions.assertEquals("org.apache.iceberg.aws.glue.GlueCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("glue"));
    }

    @Test
    public void resolveCatalogImplMapsHadoopToHadoopCatalog() {
        Assertions.assertEquals("org.apache.iceberg.hadoop.HadoopCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("hadoop"));
    }

    @Test
    public void resolveCatalogImplMapsJdbcToJdbcCatalog() {
        Assertions.assertEquals("org.apache.iceberg.jdbc.JdbcCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("jdbc"));
    }

    @Test
    public void resolveCatalogImplMapsS3TablesToS3TablesCatalog() {
        Assertions.assertEquals("software.amazon.s3tables.iceberg.S3TablesCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("s3tables"));
    }

    @Test
    public void resolveCatalogImplMapsDlfToDorisDlfCatalog() {
        Assertions.assertEquals("org.apache.doris.connector.iceberg.dlf.DLFCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("dlf"));
    }

    @Test
    public void resolveCatalogImplIsCaseInsensitive() {
        // WHY: the switch lower-cases its input, so mixed/upper-case flavors (e.g. from a user who
        // typed "REST"/"Hms") must still resolve. MUTATION: removing the toLowerCase in the switch ->
        // the default branch throws on "REST" -> red.
        Assertions.assertEquals("org.apache.iceberg.rest.RESTCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("REST"));
        Assertions.assertEquals("org.apache.iceberg.hive.HiveCatalog",
                IcebergCatalogFactory.resolveCatalogImpl("Hms"));
    }

    @Test
    public void resolveCatalogImplThrowsOnNull() {
        // WHY: a null catalogType means the required iceberg.catalog.type property is missing; the
        // factory must fail fast with a DorisConnectorException rather than NPE or return null.
        // MUTATION: removing the null guard -> NPE on toLowerCase -> red (wrong exception type).
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergCatalogFactory.resolveCatalogImpl(null));
        Assertions.assertTrue(ex.getMessage().contains("iceberg.catalog.type"),
                "the missing-property error must name the iceberg.catalog.type key");
    }

    @Test
    public void resolveCatalogImplThrowsOnUnknownType() {
        // WHY: an unrecognized flavor must be rejected loudly (not silently mapped to a default
        // backend), so a typo surfaces at catalog creation. MUTATION: a default branch that returns
        // some impl instead of throwing -> no exception -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> IcebergCatalogFactory.resolveCatalogImpl("nosuchcatalog"));
        Assertions.assertTrue(ex.getMessage().contains("nosuchcatalog"),
                "the unknown-type error must echo the offending flavor value");
    }
}
