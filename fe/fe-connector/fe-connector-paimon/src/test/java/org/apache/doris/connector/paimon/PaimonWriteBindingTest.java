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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Pins how {@link PaimonWriteBinding#configureTableForWrite} maps a Doris INSERT OVERWRITE onto Paimon's
 * {@code dynamic-partition-overwrite} option. Paimon defaults that option to true, which replaces only the
 * partitions the new rows reach; a Doris full-table overwrite replaces the whole table and a static-partition
 * overwrite replaces exactly the named partition.
 */
public class PaimonWriteBindingTest {

    private static final String DYNAMIC_OVERWRITE = CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key();

    @TempDir
    Path warehouse;

    @Test
    public void fullOverwriteUsesStaticSemanticsByDefault() {
        FileStoreTable table = partitionedTable(Collections.emptyMap());

        FileStoreTable configured = PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.emptyMap());

        // Left at Paimon's default, the overwrite would keep every partition the new rows do not reach.
        Assertions.assertEquals("false", configured.options().get(DYNAMIC_OVERWRITE));
        Assertions.assertFalse(configured.coreOptions().dynamicPartitionOverwrite());
    }

    @Test
    public void fullOverwriteHonorsExplicitDynamicSemantics() {
        // A table that sets the option itself asked for dynamic overwrite, so the binding keeps the
        // loaded table instead of copying it with the static override.
        FileStoreTable table = partitionedTable(Collections.singletonMap(DYNAMIC_OVERWRITE, "true"));

        Assertions.assertSame(table, PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.emptyMap()));
    }

    @Test
    public void staticPartitionOverwriteAlwaysUsesStaticSemantics() {
        // A static partition names the partition to replace. Dynamic semantics would leave it intact
        // when the query produces no row for it, so the table option does not apply here.
        FileStoreTable table = partitionedTable(Collections.singletonMap(DYNAMIC_OVERWRITE, "true"));

        FileStoreTable configured = PaimonWriteBinding.configureTableForWrite(
                table, true, Collections.singletonMap("pt", "1"));

        Assertions.assertEquals("false", configured.options().get(DYNAMIC_OVERWRITE));
        Assertions.assertFalse(configured.coreOptions().dynamicPartitionOverwrite());
    }

    private FileStoreTable partitionedTable(Map<String, String> options) {
        TableSchema schema = new TableSchema(0L,
                Arrays.asList(new DataField(0, "id", DataTypes.INT()), new DataField(1, "pt", DataTypes.STRING())),
                1, Collections.singletonList("pt"), Collections.emptyList(), options, "");
        return FileStoreTableFactory.create(LocalFileIO.create(),
                new org.apache.paimon.fs.Path("file://" + warehouse + "/db.db/tbl"), schema);
    }
}
