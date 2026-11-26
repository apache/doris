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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.datasource.connector.converter.ConnectorColumnConverter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.commands.ConnectorWriteSchemaUtils;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyColumnOp;
import org.apache.doris.qe.SqlModeHelper;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class PaimonAlterDefaultTest {
    @Test
    void omittedColumnReparsesThePersistedModifyDefaultOnce(@TempDir Path warehouse) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(),
                new org.apache.paimon.fs.Path(warehouse.toUri()));
                MockedStatic<SqlModeHelper> sqlMode = Mockito.mockStatic(SqlModeHelper.class)) {
            sqlMode.when(SqlModeHelper::hasNoBackSlashEscapes).thenReturn(false);
            catalog.createDatabase("db", false);
            catalog.createTable(Identifier.create("db", "t"), Schema.newBuilder()
                    .column("id", DataTypes.INT()).column("v", DataTypes.STRING()).build(), false);
            PaimonCatalogOps ops = new PaimonCatalogOps.CatalogBackedPaimonCatalogOps(catalog);
            PaimonCatalogProperties properties = PaimonCatalogProperties.of(Collections.emptyMap());
            ConnectorContext context = Mockito.mock(ConnectorContext.class, Mockito.CALLS_REAL_METHODS);
            PaimonTableHandle handle = new PaimonTableHandle("db", "t", List.of(), List.of());

            AlterTableCommand alter = (AlterTableCommand) new NereidsParser().parseSingle(
                    "ALTER TABLE t MODIFY COLUMN v STRING NULL DEFAULT 'a\\\\b'");
            ColumnDefinition definition = ((ModifyColumnOp) alter.getOps().get(0)).getColumnDef();
            definition.validate(false, Collections.emptySet(), Collections.emptySet(), false, KeysType.DUP_KEYS);
            new PaimonConnectorMetadata(ops, properties, context).modifyColumn(null, handle,
                    ConnectorColumnConverter.toConnectorColumn(definition.translateToCatalogStyleForSchemaChange()),
                    null);

            ConnectorColumn writeColumn = new PaimonWritePlanProvider(properties, ops, context)
                    .getWriteColumns(null, handle, Optional.empty()).get().get(1);
            Assertions.assertEquals("a\\\\b", writeColumn.getDefaultValue());
            Column pinned = ConnectorColumnConverter.convertColumn(writeColumn);
            StringLikeLiteral bound = Assertions.assertInstanceOf(StringLikeLiteral.class,
                    ConnectorWriteSchemaUtils.resolveDefault(pinned));
            Assertions.assertEquals("a\\b", bound.getStringValue());
        }
    }
}
