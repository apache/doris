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
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link PaimonTableOptions}, the connector port of the {@code paimon.table-option.*}
 * namespace upstream #65955 added to the (now deleted) fe-core {@code AbstractPaimonProperties}.
 *
 * <p>Mirrors the upstream {@code AbstractPaimonPropertiesTest} table-option cases one-for-one, plus
 * the two connector-specific wirings the fe-core class did not own: exclusion from the catalog
 * {@link Options} passthrough ({@link PaimonCatalogFactory}) and the fail-fast at CREATE CATALOG.
 * Entirely offline — extraction/validation/narrowing are pure Map transforms.
 */
public class PaimonTableOptionsTest {

    @Test
    public void extractStripsPrefixAndValidatesAgainstCoreOptions() {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", "s3://tmp/warehouse");
        props.put("paimon.jni.enable_jni_io_manager", "true");
        props.put("paimon.table-option.read.batch-size", "4096");
        props.put("paimon.table-option.file.compression.per.level", "0:lz4,1:zstd");

        Map<String, String> extracted = PaimonTableOptions.extract(props);

        // WHY: Paimon consumes the bare option name, so the "paimon.table-option." namespace marker
        // must be stripped exactly once; anything outside the namespace must not be picked up.
        Assertions.assertEquals("4096", extracted.get("read.batch-size"));
        Assertions.assertEquals("0:lz4,1:zstd", extracted.get("file.compression.per.level"));
        Assertions.assertEquals(2, extracted.size());
    }

    @Test
    public void tableOptionAndJniNamespacesAreExcludedFromCatalogOptions() {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", "s3://tmp/warehouse");
        props.put("paimon.table-option.read.batch-size", "4096");
        props.put("paimon.jni.enable_jni_io_manager", "true");
        props.put("paimon.client-pool-size", "7");

        Options options = PaimonCatalogFactory.buildCatalogOptions(props);

        // WHY (#65955): both namespaces are re-keyed by the generic "paimon.*" passthrough unless
        // excluded, which would push a per-TABLE option and a BE scanner knob into the Paimon CATALOG
        // config as unknown keys. Only the exclusion keeps them out; the ordinary paimon.* passthrough
        // must keep working, which is what the client-pool-size assertion pins.
        // MUTATION: dropping either isTableOptionProperty/isJniProperty from the excluded condition
        // -> the stripped key appears in the catalog Options -> red.
        Assertions.assertFalse(options.toMap().containsKey("table-option.read.batch-size"));
        Assertions.assertFalse(options.toMap().containsKey("jni.enable_jni_io_manager"));
        Assertions.assertEquals("7", options.toMap().get("client-pool-size"));
    }

    @Test
    public void forCopyKeepsOptionsThePaimonTableSetsItself() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.table-option.read.batch-size", "4096");
        props.put("paimon.table-option.write.batch-size", "2048");
        props.put("paimon.table-option.file.compression.per.level", "0:lz4,1:zstd");
        Map<String, String> extracted = PaimonTableOptions.extract(props);

        Map<String, String> currentTableOptions = new HashMap<>();
        currentTableOptions.put("read.batch-size", "1024");
        // "orc.write.batch-size" is a FALLBACK key of the write.batch-size ConfigOption.
        currentTableOptions.put("orc.write.batch-size", "512");
        currentTableOptions.put("file.compression.per.level", "0:snappy");

        Map<String, String> optionsForCopy = PaimonTableOptions.forCopy(extracted, currentTableOptions);

        // WHY: the catalog value is a DEFAULT, not an override — a table that states an option must keep
        // its own value, or a catalog-wide default would silently rewrite per-table tuning. The
        // orc.write.batch-size case is why the check goes through the ConfigOption instead of a plain
        // map lookup: a fallback spelling still counts as "the table set it".
        // MUTATION: comparing raw keys (currentTableOptions.containsKey) -> write.batch-size leaks
        // through and overrides the table's own orc.write.batch-size -> red.
        Assertions.assertTrue(optionsForCopy.isEmpty());
    }

    @Test
    public void forCopyFillsOptionsThePaimonTableLeavesUnset() {
        Map<String, String> extracted = PaimonTableOptions.extract(
                Collections.singletonMap("paimon.table-option.read.batch-size", "4096"));

        Map<String, String> optionsForCopy = PaimonTableOptions.forCopy(
                extracted, Collections.singletonMap("path", "s3://tmp/warehouse/test.db/test"));

        // WHY: this is the whole point of the feature — an option the table does not mention is filled
        // from the catalog. A table always carries at least "path", so the non-empty-currentOptions
        // branch is the normal case, not an edge case.
        Assertions.assertEquals("4096", optionsForCopy.get("read.batch-size"));
    }

    @Test
    public void rejectUnknownTableOption() {
        Map<String, String> props =
                Collections.singletonMap("paimon.table-option.option-does-not-exist", "value");

        IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class, () -> PaimonTableOptions.extract(props));

        // WHY: an unknown option is silently ignored by Paimon's Options, so without the CoreOptions
        // lookup a typo would look accepted at CREATE CATALOG and never take effect.
        Assertions.assertTrue(e.getMessage().contains("option-does-not-exist"));
    }

    @Test
    public void rejectPrefixMapTableOption() {
        Map<String, String> props =
                Collections.singletonMap("paimon.table-option.file.compression.per.level.0", "lz4");

        IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class, () -> PaimonTableOptions.extract(props));

        // WHY: Paimon prefix-map options are set as ONE key holding the whole map
        // ("file.compression.per.level" = "0:lz4"); the per-entry spelling is not a ConfigOption and
        // must be rejected rather than accepted as a no-op.
        Assertions.assertTrue(e.getMessage().contains("file.compression.per.level.0"));
    }

    @Test
    public void rejectInvalidTableOptionValue() {
        Map<String, String> props =
                Collections.singletonMap("paimon.table-option.read.batch-size", "not-an-integer");

        IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class, () -> PaimonTableOptions.extract(props));

        // WHY: the value is only parsed when the option is USED (deep inside a scan), so without an
        // eager parse a bad value surfaces as a mid-query failure instead of a rejected DDL.
        Assertions.assertTrue(e.getMessage().contains("read.batch-size"));
    }

    @Test
    public void rejectEmptyTableOptionName() {
        Map<String, String> props = Collections.singletonMap("paimon.table-option.", "value");

        IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class, () -> PaimonTableOptions.extract(props));

        Assertions.assertTrue(e.getMessage().contains("must not be empty"));
    }

    @Test
    public void catalogOpsGetTableOverlaysTableOptions(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .option("read.batch-size", "1024")
                    .build(), false);

            Map<String, String> props = new HashMap<>();
            props.put("paimon.table-option.read.batch-size", "4096");
            props.put("paimon.table-option.scan.max-splits-per-task", "17");
            Map<String, String> tableOptions = PaimonTableOptions.extract(props);
            Table table = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog, tableOptions)
                    .getTable(id);

            // WHY (#65955): this is the ONLY Catalog.getTable call the connector makes, so it is the
            // single point where a catalog default can reach a loaded table — legacy
            // PaimonExternalCatalog.getPaimonTable applied it in exactly the same place. Because the
            // serialized Table is what BE's PaimonJniScanner deserializes and reads, an option that is
            // not overlaid HERE never reaches the scanner at all.
            // MUTATION: returning catalog.getTable(identifier) unchanged -> the unset option is absent
            // -> red. Overlaying unconditionally (skipping forCopy) -> the table's own 1024 is
            // overwritten with 4096 -> red.
            Assertions.assertEquals("17", table.options().get("scan.max-splits-per-task"));
            Assertions.assertEquals("1024", table.options().get("read.batch-size"));
        }
    }

    @Test
    public void catalogOpsGetTableIsUntouchedWithoutTableOptions(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "t");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);

            Table table = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog).getTable(id);

            // WHY: the overwhelmingly common catalog configures no table options at all; that path must
            // return the catalog's own Table instance untouched, so nothing about caching or identity
            // changes for it.
            Assertions.assertSame(catalog.getTable(id).getClass(), table.getClass());
            Assertions.assertFalse(table.options().containsKey("scan.max-splits-per-task"));
        }
    }

    @Test
    public void createCatalogRejectsBadTableOption() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "filesystem");
        props.put("warehouse", "s3://tmp/warehouse");
        props.put("paimon.table-option.read.batch-size", "not-an-integer");

        // WHY (#65955): upstream got the fail-fast from AbstractPaimonProperties.initNormalizeAndCheckProps
        // during catalog init; on the SPI path that class is gone and validateProperties is the only
        // CREATE/ALTER CATALOG hook. Without the call the bad option is accepted at DDL time and only
        // explodes on the first query.
        // MUTATION: removing PaimonTableOptions.extract from validateProperties -> no throw -> red.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new PaimonConnectorProvider().validateProperties(props));
    }
}
