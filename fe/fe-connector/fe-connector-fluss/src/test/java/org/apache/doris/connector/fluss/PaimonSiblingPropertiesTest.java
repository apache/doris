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

import org.apache.doris.connector.paimon.PaimonCatalogFactory;
import org.apache.doris.connector.paimon.PaimonCatalogProperties;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
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

    /** A catalog that states nothing about the lake: the fluss cluster's configuration is used as is. */
    private static Map<String, String> noOverrides() {
        return Collections.emptyMap();
    }

    /** A catalog that states {@code key} about the lake, spelled the way paimon spells it. */
    private static Map<String, String> overrides(String key, String value) {
        return Collections.singletonMap(key, value);
    }

    @Test
    public void lakeOptionsBecomeThePaimonCatalogProperties() {
        Map<String, String> expected = new HashMap<>();
        // Paimon calls the flavor "metastore"; Doris's paimon connector calls it paimon.catalog.type.
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");

        Assertions.assertEquals(expected,
                PaimonSiblingProperties.synthesize(flussTableProperties(), noOverrides()));
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

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()),
                "only the table.datalake.paimon.* namespace describes the lake catalog");
    }

    @Test
    public void storageOptionsKeepTheirNativeNamesForTheConnectorBoundary() {
        Map<String, String> properties = flussTableProperties();
        // FlussConnector consumes these at the last boundary before constructing the sibling: it translates
        // them into shared FE/BE storage and removes them from the map the Paimon factory receives.
        properties.put("table.datalake.paimon.fs.s3a.endpoint", "http://minio:9000");
        properties.put("table.datalake.paimon.fs.s3a.access.key", "ak");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("fs.s3a.endpoint", "http://minio:9000");
        expected.put("fs.s3a.access.key", "ak");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()));
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

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()));

        // A key left empty in the cluster's configuration file says the same thing as an absent one;
        // reading it as a flavor name would refuse the lake over a blank line.
        properties.put("table.datalake.paimon.metastore", "  ");
        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()));
    }

    @Test
    public void hiveMetastoreLakeIsReadAsAnHmsCatalog() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "hive");
        properties.put("table.datalake.paimon.uri", "thrift://hms:9083");

        Map<String, String> expected = new HashMap<>();
        // Same metastore, different spelling: paimon calls it "hive", Doris's paimon connector "hms".
        // Passing "hive" through unchanged would make the connector reject a catalog it can serve.
        expected.put("paimon.catalog.type", "hms");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("uri", "thrift://hms:9083");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()));
    }

    @Test
    public void restCatalogLakeUsesTheConsumersNamespace() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "rest");
        properties.put("table.datalake.paimon.uri", "https://rest:8080");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "rest");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("paimon.rest.uri", "https://rest:8080");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()));
    }

    @Test
    public void nativeRestBearerOptionsReachTheConsumerNamespace() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "rest");
        properties.put("table.datalake.paimon.uri", "https://rest:8080");
        properties.put("table.datalake.paimon.token.provider", "bear");
        properties.put("table.datalake.paimon.token", "cluster-token");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "rest");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("paimon.rest.uri", "https://rest:8080");
        expected.put("paimon.rest.token.provider", "bear");
        expected.put("paimon.rest.token", "cluster-token");

        Map<String, String> synthesized =
                PaimonSiblingProperties.synthesize(properties, noOverrides());
        Assertions.assertEquals(expected, synthesized);

        // Cross the actual consumer boundary as well: PaimonCatalogFactory strips paimon.rest.
        // before handing these Options to the SDK's REST catalog. A bare token.provider in the
        // synthesized map would pass the assertion above only if expected were changed with it,
        // but it would disappear here and reproduce the authenticated-catalog failure.
        Options options = PaimonCatalogFactory.buildCatalogOptions(
                PaimonCatalogProperties.of(synthesized));
        Assertions.assertEquals("rest", options.get("metastore"));
        Assertions.assertEquals("https://rest:8080", options.get("uri"));
        Assertions.assertEquals("bear", options.get("token.provider"));
        Assertions.assertEquals("cluster-token", options.get("token"));
    }

    @Test
    public void nativeRestDlfOptionsReachTheConsumerNamespace() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "rest");
        properties.put("table.datalake.paimon.uri", "https://rest:8080");
        properties.put("table.datalake.paimon.token.provider", "dlf");
        properties.put("table.datalake.paimon.dlf.access-key-id", "id");
        properties.put("table.datalake.paimon.dlf.access-key-secret", "secret");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "rest");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("paimon.rest.uri", "https://rest:8080");
        expected.put("paimon.rest.token.provider", "dlf");
        expected.put("paimon.rest.dlf.access-key-id", "id");
        expected.put("paimon.rest.dlf.access-key-secret", "secret");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties, noOverrides()));
    }

    @Test
    public void restOptionPrecedenceIsClusterThenGatewayThenCatalogOverride() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "rest");
        properties.put("table.datalake.paimon.uri", "https://cluster:8080");
        properties.put("table.datalake.paimon.token", "cluster-token");

        Map<String, String> gateway = new LinkedHashMap<>();
        gateway.put("paimon.rest.uri", "https://gateway:8080");
        gateway.put("paimon.rest.token", "gateway-token");
        Map<String, String> catalogOverrides = new LinkedHashMap<>();
        catalogOverrides.put("rest.token", "catalog-token");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "rest");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("paimon.rest.uri", "https://gateway:8080");
        expected.put("paimon.rest.token", "catalog-token");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(
                gateway, properties, catalogOverrides));
    }

    @Test
    public void unrelatedGatewayAliasesCannotOverrideNativeRestOptions() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "rest");
        properties.put("table.datalake.paimon.uri", "https://cluster:8080");
        properties.put("table.datalake.paimon.token", "cluster-token");

        Map<String, String> gateway = new LinkedHashMap<>();
        gateway.put("uri", "https://unrelated-gateway:8080");
        gateway.put("token", "unrelated-token");

        Map<String, String> synthesized = PaimonSiblingProperties.synthesize(
                gateway, properties, Collections.emptyMap());
        Assertions.assertEquals("https://cluster:8080", synthesized.get("paimon.rest.uri"));
        Assertions.assertEquals("cluster-token", synthesized.get("paimon.rest.token"));
        Assertions.assertFalse(synthesized.containsKey("uri"));
        Assertions.assertFalse(synthesized.containsKey("token"));
    }

    @Test
    public void theCatalogCanStateTheFlavorItself() {
        Map<String, String> properties = flussTableProperties();

        Map<String, String> catalogOverrides = new LinkedHashMap<>();
        catalogOverrides.put("metastore", "hive");
        catalogOverrides.put("uri", "thrift://hms:9083");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "hms");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("uri", "thrift://hms:9083");

        // The flavor is read AFTER the override is applied, so a catalog can point a lake at a metastore
        // the fluss cluster does not know it is registered in.
        Assertions.assertEquals(expected,
                PaimonSiblingProperties.synthesize(properties, catalogOverrides));
    }

    @Test
    public void anUnsupportedMetastoreFailsLoud() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", "jdbc");

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSiblingProperties.synthesize(properties, noOverrides()));
        // Naming both the flavor found and the ones supported is what makes this actionable, and refusing
        // by name is what keeps an unknown flavor from being read as a filesystem lake that is not there.
        Assertions.assertTrue(failure.getMessage().contains("jdbc"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("filesystem"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("hive"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("rest"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("datalake.paimon.metastore"),
                failure.getMessage());
    }

    @Test
    public void theFlavorIsReadWithoutRegardToCaseOrPadding() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.metastore", " Hive ");

        // A value out of a YAML file can carry either; refusing one of these would look like the flavor
        // itself is unsupported.
        Assertions.assertEquals("hms",
                PaimonSiblingProperties.synthesize(properties, noOverrides()).get("paimon.catalog.type"));
    }

    @Test
    public void missingWarehouseFailsLoud() {
        Map<String, String> properties = flussTableProperties();
        properties.remove("table.datalake.paimon.warehouse");

        // Without a warehouse the paimon catalog cannot be built at all. Failing here names the fluss
        // setting to fix; letting it through would surface as a paimon error about a catalog nobody created.
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSiblingProperties.synthesize(properties, noOverrides()));
        Assertions.assertTrue(failure.getMessage().contains("table.datalake.paimon.warehouse"),
                failure.getMessage());
    }

    @Test
    public void anEmptyWarehouseIsTreatedAsMissing() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.warehouse", "");

        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSiblingProperties.synthesize(properties, noOverrides()));
    }

    @Test
    public void theInputMapIsNeverMutated() {
        Map<String, String> properties = flussTableProperties();
        Map<String, String> before = new LinkedHashMap<>(properties);

        PaimonSiblingProperties.synthesize(properties, noOverrides());

        // The caller's map is a table handle's property map, shared and unmodifiable in production; a
        // synthesis that consumed entries from it would corrupt every later read of that table.
        Assertions.assertEquals(before, properties);
    }

    @Test
    public void catalogSettingsOverrideTheClusterKeyByKey() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.client.pool-size", "4");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        // The catalog states the endpoint, so its value is used...
        expected.put("client.pool-size", "8");
        // ...and the warehouse it says nothing about still comes from the cluster. Replacing the whole
        // configuration instead would mean every catalog that fixes one setting has to restate all of them.
        expected.put("warehouse", "/lake/warehouse");

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties,
                overrides("client.pool-size", "8")));
    }

    @Test
    public void gatewayAndStorageSettingsReachTheConnectorBoundary() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        properties.put("table.datalake.paimon.s3.path.style.access", "true");

        Map<String, String> gateway = new LinkedHashMap<>();
        gateway.put("type", "fluss");
        gateway.put("enable.mapping.varbinary", "true");
        gateway.put("meta.cache.paimon.table.ttl-second", "0");
        gateway.put("hadoop.security.authentication", "kerberos");
        gateway.put("fs.defaultFS", "hdfs://nameservice1");

        Map<String, String> expected = new HashMap<>(gateway);
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");
        expected.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        expected.put("s3.path.style.access", "true");
        expected.put("s3.access-key", "AK");
        // synthesize returns the complete effective map. FlussConnector extracts these storage settings
        // immediately before constructing the sibling, into the shared FE/BE storage context.

        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(
                gateway, properties, overrides("s3.access-key", "AK")));
    }

    @Test
    public void directKerberosGateWinsOverAClusterSimpleDefault() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.hadoop.security.authentication", "simple");
        Map<String, String> gateway = new LinkedHashMap<>();
        gateway.put("hadoop.security.authentication", "kerberos");

        Map<String, String> synthesized = PaimonSiblingProperties.synthesize(
                gateway, properties, Collections.emptyMap());

        Assertions.assertEquals("kerberos",
                synthesized.get("hadoop.security.authentication"),
                "the raw Paimon gate must use the same direct-over-derived priority as storage");
    }

    @Test
    public void directSimpleGateWinsOverAClusterKerberosDefault() {
        Map<String, String> properties = flussTableProperties();
        properties.put("table.datalake.paimon.hadoop.security.authentication", "kerberos");
        Map<String, String> gateway = new LinkedHashMap<>();
        gateway.put("hadoop.security.authentication", "simple");

        Map<String, String> synthesized = PaimonSiblingProperties.synthesize(
                gateway, properties, Collections.emptyMap());

        Assertions.assertEquals("simple", synthesized.get("hadoop.security.authentication"));
    }

    @Test
    public void catalogOverridesAreAppliedBeforeTheChecks() {
        Map<String, String> properties = flussTableProperties();
        properties.remove("table.datalake.paimon.warehouse");

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/catalog/warehouse");

        // Checking first would reject the configuration this feature is for: the cluster reports no
        // warehouse (or an unreachable one) and the catalog is what says where the lake really is.
        Assertions.assertEquals(expected, PaimonSiblingProperties.synthesize(properties,
                overrides("warehouse", "/catalog/warehouse")));
    }

    @Test
    public void neitherInputMapIsMutatedByAnOverride() {
        Map<String, String> properties = flussTableProperties();
        Map<String, String> propertiesBefore = new LinkedHashMap<>(properties);
        Map<String, String> catalogOverrides = new LinkedHashMap<>();
        catalogOverrides.put("warehouse", "/catalog/warehouse");
        catalogOverrides.put("metastore", "filesystem");
        Map<String, String> overridesBefore = new LinkedHashMap<>(catalogOverrides);

        PaimonSiblingProperties.synthesize(properties, catalogOverrides);

        // Both are long-lived, shared and unmodifiable in production — the table handle's properties and
        // the catalog's own. Consuming an entry from either would change what the NEXT table synthesizes.
        Assertions.assertEquals(propertiesBefore, properties);
        Assertions.assertEquals(overridesBefore, catalogOverrides);
    }
}
