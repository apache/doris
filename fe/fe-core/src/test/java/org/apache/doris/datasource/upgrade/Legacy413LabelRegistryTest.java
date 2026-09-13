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

package org.apache.doris.datasource.upgrade;

import org.apache.doris.datasource.Legacy413Fixtures;
import org.apache.doris.datasource.Legacy413Fixtures.LegacyLabel;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalDatabase;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.gson.RuntimeTypeAdapterFactory;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Every GSON {@code clazz} label a 4.1.3 FE could have written must still resolve on this branch.
 *
 * <p>Those labels are bare string literals in {@code GsonUtils}: nothing in the compiler or in any other
 * test notices if one is deleted or mistyped, and the consequence is not a degraded catalog but an FE that
 * cannot start at all, because an unregistered label makes the whole image fail to parse.
 *
 * <p>The expectation is not hand-maintained. {@code labels.*.txt} is dumped reflectively out of 4.1.3's own
 * registries by the fixture generator, so this test compares one machine-read registry against another.
 * That matters: the obvious alternative -- scraping GsonUtils with a regex -- silently drops
 * {@code IcebergS3TablesExternalCatalog}, whose class name contains a digit.
 */
public class Legacy413LabelRegistryTest {

    /**
     * The one thing that genuinely is a decision rather than a mechanical consequence: which legacy table
     * labels land on the MVCC-capable table class. Getting this wrong is silent -- the catalog still loads,
     * queries still plan, and only snapshot/time-travel/MTMV freshness quietly misbehaves.
     */
    private static final Map<String, Class<?>> EXPECTED_TABLE_TARGETS = ImmutableMap.<String, Class<?>>builder()
            .put("HMSExternalTable", PluginDrivenMvccExternalTable.class)
            .put("IcebergExternalTable", PluginDrivenMvccExternalTable.class)
            .put("PaimonExternalTable", PluginDrivenMvccExternalTable.class)
            .put("EsExternalTable", PluginDrivenExternalTable.class)
            .put("JdbcExternalTable", PluginDrivenExternalTable.class)
            .put("TrinoConnectorExternalTable", PluginDrivenExternalTable.class)
            .put("MaxComputeExternalTable", PluginDrivenExternalTable.class)
            .put("LakeSoulExternalTable", PluginDrivenExternalTable.class)
            .build();

    @Test
    public void everyCatalogLabelWrittenBy413StillResolves() throws Exception {
        assertRegistryCoversLegacy("ds", "dsTypeAdapterFactory", PluginDrivenExternalCatalog.class);
    }

    @Test
    public void everyDatabaseLabelWrittenBy413StillResolves() throws Exception {
        assertRegistryCoversLegacy("db", "dbTypeAdapterFactory", PluginDrivenExternalDatabase.class);
    }

    @Test
    public void everyTableLabelWrittenBy413StillResolves() throws Exception {
        assertRegistryCoversLegacy("tbl", "tblTypeAdapterFactory", PluginDrivenExternalTable.class);
    }

    /**
     * For each label 4.1.3 registered: this branch must register it too, and it must resolve to the same
     * class when that class still exists, or to a plugin-driven replacement when it was deleted by the
     * cutover. Encoding the rule rather than a frozen list means a newly deleted connector class is caught
     * without anyone remembering to update a table here.
     */
    private void assertRegistryCoversLegacy(String which, String factoryField, Class<?> pluginDrivenBase)
            throws Exception {
        Map<String, LegacyLabel> legacy = Legacy413Fixtures.legacyLabels(which);
        Map<String, Class<?>> current = currentRegistry(factoryField);

        List<String> problems = new ArrayList<>();
        for (LegacyLabel label : legacy.values()) {
            Class<?> target = current.get(label.label);
            if (target == null) {
                problems.add("label '" + label.label + "' was written by 4.1.3 but is not registered on this "
                        + "branch: any image containing it fails to parse and the FE cannot start");
                continue;
            }
            Class<?> legacyClass = classOrNull(label.legacyClassName);
            if (legacyClass != null) {
                problems.add(checkSurvivingClass(label, target, legacyClass));
            } else {
                problems.add(checkMigratedClass(label, target, pluginDrivenBase));
            }
        }
        problems.removeIf(java.util.Objects::isNull);
        Assertions.assertTrue(problems.isEmpty(), String.join("\n", problems));

        Assertions.assertFalse(legacy.isEmpty(), "labels." + which + ".txt is empty; the fixture is broken");
    }

    private String checkSurvivingClass(LegacyLabel label, Class<?> target, Class<?> legacyClass) {
        // The class survived the cutover, so the label must still point at it -- silently repointing a
        // surviving class at a plugin-driven one would change behaviour for catalogs the engine still owns.
        if (!target.equals(legacyClass)) {
            return "label '" + label.label + "' pointed at " + legacyClass.getName()
                    + " in 4.1.3 and that class still exists, but this branch resolves it to "
                    + target.getName();
        }
        return null;
    }

    private String checkMigratedClass(LegacyLabel label, Class<?> target, Class<?> pluginDrivenBase) {
        if (!pluginDrivenBase.isAssignableFrom(target)) {
            return "label '" + label.label + "' lost its 4.1.3 class (" + label.legacyClassName
                    + ") to the connector cutover, so it must resolve to a " + pluginDrivenBase.getSimpleName()
                    + ", but resolves to " + target.getName();
        }
        Class<?> expected = EXPECTED_TABLE_TARGETS.get(label.label);
        if (expected != null && !expected.equals(target)) {
            // assertSame-style, deliberately: PluginDrivenMvccExternalTable extends PluginDrivenExternalTable,
            // so an isAssignableFrom check alone accepts a silent downgrade to the non-MVCC variant.
            return "label '" + label.label + "' must resolve to " + expected.getSimpleName()
                    + " (MVCC capability is a decision, not a default), but resolves to " + target.getSimpleName();
        }
        return null;
    }

    @Test
    public void abstractLegacyClassesAreRecordedAsUnreachable() throws Exception {
        // IcebergExternalCatalog is registered by 4.1.3 but abstract, so GSON writes the concrete flavour's
        // label instead and this one can never appear in an image. Recording that in the fixture keeps a
        // future reader from "fixing" its absence from the image fixture.
        Map<String, LegacyLabel> legacy = Legacy413Fixtures.legacyLabels("ds");
        LegacyLabel iceberg = legacy.get("IcebergExternalCatalog");
        Assertions.assertNotNull(iceberg, "4.1.3 registered IcebergExternalCatalog");
        Assertions.assertTrue(iceberg.isAbstract,
                "IcebergExternalCatalog was abstract in 4.1.3; if it is now concrete the image fixture must "
                        + "gain an entry for it");
    }

    private static Map<String, Class<?>> currentRegistry(String factoryField) throws Exception {
        Field field = GsonUtils.class.getDeclaredField(factoryField);
        field.setAccessible(true);
        RuntimeTypeAdapterFactory<?> factory = (RuntimeTypeAdapterFactory<?>) field.get(null);
        Field labelToSubtype = RuntimeTypeAdapterFactory.class.getDeclaredField("labelToSubtype");
        labelToSubtype.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Class<?>> registry = (Map<String, Class<?>>) labelToSubtype.get(factory);
        return registry;
    }

    private static Class<?> classOrNull(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            return null;
        }
    }
}
