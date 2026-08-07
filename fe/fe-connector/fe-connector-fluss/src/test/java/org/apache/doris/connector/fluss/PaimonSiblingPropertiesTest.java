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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The translation from a fluss table's properties to the paimon sibling's catalog properties.
 *
 * <p>Assertions compare the WHOLE map, not a key or two: the sibling is handed exactly this map and
 * nothing else, so a leaked fluss property or a dropped storage key is a bug either way, and only whole-map
 * equality catches both. The keys are written out as literals rather than referenced from the class under
 * test — a renamed constant must break these tests, since the names are what the paimon connector reads.
 */
public class PaimonSiblingPropertiesTest {

    /** The properties a fluss coordinator injects for a filesystem-backed paimon lake. */
    private static Map<String, String> flussTableProperties() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("table.datalake.enabled", "true");
        properties.put("table.datalake.format", "paimon");
        properties.put("table.datalake.paimon.metastore", "filesystem");
        properties.put("table.datalake.paimon.warehouse", "/lake/warehouse");
        return properties;
    }

    @Test
    public void lakeOptionsBecomeThePaimonCatalogProperties() {
        Map<String, String> expected = new HashMap<>();
        // Paimon calls the flavor "metastore"; Doris's paimon connector calls it paimon.catalog.type.
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");

        Assertions.assertEquals(expected,
                PaimonSiblingProperties.synthesize(flussTableProperties()));
    }

    @Test
    public void nonLakePropertiesAreNotForwarded() {
        Map<String, String> properties = flussTableProperties();
        // The kind of thing a fluss table carries alongside its lake settings. Handing these to the paimon
        // connector would make it reject the catalog (or, worse, silently treat them as paimon options).
        properties.put("bucket.num", "8");
        properties.put("table.datalake.freshness", "3min");
        properties.put("table.log.ttl", "7d");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties),
                "only the table.datalake.paimon.* namespace describes the lake catalog");
    }

    @Test
    public void remainingLakeOptionsKeepTheirNamesWithoutThePrefix() {
        Map<String, String> properties = flussTableProperties();
        // A real object-store deployment: the paimon connector reads these storage keys under exactly
        // these names, so the prefix has to come off and nothing may be added.
        properties.put("table.datalake.paimon.fs.s3a.endpoint", "http://minio:9000");
        properties.put("table.datalake.paimon.fs.s3a.access.key", "ak");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("fs.s3a.endpoint", "http://minio:9000");
        expected.put("fs.s3a.access.key", "ak");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties));
    }

    @Test
    public void anAbsentMetastoreIsTheFilesystemDefault() {
        Map<String, String> properties = flussTableProperties();
        properties.remove("table.datalake.paimon.metastore");

        // Paimon's own default is filesystem, so silence means the flavor this connector supports. The
        // catalog type must still be stated explicitly: the paimon connector defaults on its own, and
        // leaving it unset would make the sibling depend on that default staying put.
        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties));
    }

    @Test
    public void anUnsupportedMetastoreFailsLoud() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "hive");

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSiblingProperties.synthesize(properties));
        // Naming both the flavor found and the one supported is what makes this actionable: the fix is in
        // the fluss cluster's configuration, not in Doris.
        Assertions.assertTrue(failure.getMessage().contains("hive"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("filesystem"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("datalake.paimon.metastore"),
                failure.getMessage());
    }

    @Test
    public void missingWarehouseFailsLoud() {
        Map<String, String> properties = flussTableProperties();
        properties.remove("table.datalake.paimon.warehouse");

        // Without a warehouse the paimon catalog cannot be built at all. Failing here names the fluss
        // setting to fix; letting it through would surface as a paimon error about a catalog nobody created.
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSiblingProperties.synthesize(properties));
        Assertions.assertTrue(failure.getMessage().contains("table.datalake.paimon.warehouse"),
                failure.getMessage());
    }

    @Test
    public void anEmptyWarehouseIsTreatedAsMissing() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.warehouse", "");

        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSiblingProperties.synthesize(properties));
    }

    @Test
    public void theInputMapIsNeverMutated() {
        Map<String, String> properties = flussTableProperties();
        Map<String, String> before = new LinkedHashMap<>(properties);

        PaimonSiblingProperties.synthesize(properties);

        // The caller's map is a table handle's property map, shared and unmodifiable in production; a
        // synthesis that consumed entries from it would corrupt every later read of that table.
        Assertions.assertEquals(before, properties);
    }
}
