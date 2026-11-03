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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class IcebergLineageDdlCoverageTest {
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private IcebergCoverageTestFixture fixture;

    @AfterEach
    void closeFixture() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test
    public void addDoesNotExposeDuplicateRowIdColumns() {
        assertAddPreservesUniqueMetadataNames("_row_id");
    }

    @Test
    public void addDoesNotExposeDuplicateSequenceColumns() {
        assertAddPreservesUniqueMetadataNames("_last_updated_sequence_number");
    }

    @Test
    public void renameDoesNotExposeDuplicateRowIdColumns() {
        assertRenamePreservesUniqueMetadataNames("_row_id");
    }

    @Test
    public void renameDoesNotExposeDuplicateSequenceColumns() {
        assertRenamePreservesUniqueMetadataNames("_last_updated_sequence_number");
    }

    private void assertAddPreservesUniqueMetadataNames(String reservedName) {
        InMemoryCatalog catalog = catalog();
        createV3(catalog);
        IcebergCoverageTestFixture metadata = metadata(catalog);
        String previousMetadata = metadataLocation(catalog);
        UserException failure = Assertions.assertThrows(UserException.class,
                () -> metadata.ops.addColumn(metadata.dorisTable,
                        new Column(reservedName, Type.BIGINT), null, 1L));
        Assertions.assertTrue(failure.getMessage().contains("reserved row lineage column"));
        Assertions.assertEquals(previousMetadata, metadataLocation(catalog));
        Assertions.assertNull(catalog.loadTable(IDENTIFIER).schema().findField(reservedName));
        assertUniqueColumnName(metadata, reservedName);
    }

    private void assertRenamePreservesUniqueMetadataNames(String reservedName) {
        InMemoryCatalog catalog = catalog();
        createV3(catalog);
        IcebergCoverageTestFixture metadata = metadata(catalog);
        String previousMetadata = metadataLocation(catalog);
        UserException failure = Assertions.assertThrows(UserException.class,
                () -> metadata.ops.renameColumn(metadata.dorisTable, "id", reservedName, 1L));
        Assertions.assertTrue(failure.getMessage().contains("reserved row lineage column"));
        Assertions.assertEquals(previousMetadata, metadataLocation(catalog));
        Assertions.assertNotNull(catalog.loadTable(IDENTIFIER).schema().findField("id"));
        Assertions.assertNull(catalog.loadTable(IDENTIFIER).schema().findField(reservedName));
        assertUniqueColumnName(metadata, reservedName);
    }

    private static void assertUniqueColumnName(IcebergCoverageTestFixture metadata, String reservedName) {
        // A public schema must not contain both a physical user field and hidden metadata under one name.
        long matchingColumns = IcebergUtils.appendRowLineageColumnsForV3(
                IcebergUtils.parseSchema(metadata.table.schema(), false, false), metadata.table).stream()
                .filter(column -> reservedName.equalsIgnoreCase(column.getName())).count();
        Assertions.assertEquals(1L, matchingColumns,
                "The connector exposed duplicate physical and hidden columns named " + reservedName);
    }

    private static String metadataLocation(InMemoryCatalog catalog) {
        Table table = catalog.loadTable(IDENTIFIER);
        return ((HasTableOperations) table).operations().current().metadataFileLocation();
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("repro", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        return catalog;
    }

    private static void createV3(InMemoryCatalog catalog) {
        catalog.createTable(IDENTIFIER, new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));
    }

    private IcebergCoverageTestFixture metadata(InMemoryCatalog catalog) {
        fixture = new IcebergCoverageTestFixture(catalog, catalog.loadTable(IDENTIFIER));
        return fixture;
    }
}
