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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One immutable view of the current Paimon write target.
 *
 * <p>A time-travel snapshot attached to the Doris table is read-side state and must not affect
 * sink analysis. Capturing the latest remote table once also keeps schema binding, writer
 * distribution and the serialized JNI table on the same table generation.
 */
public final class PaimonWriteTarget {
    private final PaimonExternalTable dorisTable;
    private final FileStoreTable table;
    private final List<Column> schema;
    private final Map<String, Column> columnsByName;
    private final Map<String, Type> columnTypes;
    private final Set<String> partitionColumnNames;

    private PaimonWriteTarget(PaimonExternalTable dorisTable, FileStoreTable table)
            throws AnalysisException {
        this.dorisTable = dorisTable;
        this.table = table;
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) dorisTable.getCatalog();

        ImmutableList.Builder<Column> schemaBuilder = ImmutableList.builder();
        Map<String, Column> columns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Type> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DataField field : table.rowType().getFields()) {
            Column conflictingColumn = columns.get(field.name());
            if (conflictingColumn != null) {
                throw new AnalysisException("Paimon table contains columns which differ only by case: "
                        + conflictingColumn.getName() + " and " + field.name());
            }
            Type type = PaimonUtil.paimonTypeToDorisType(
                    field.type(), catalog.getEnableMappingVarbinary(), false);
            // A Paimon schema has one logical VARIANT type. Doris uses the compute-V2
            // representation for the Paimon write protocol, including nested VARIANT nodes.
            DataType writeType = VariantType.toComputeV2(DataType.fromCatalogType(type));
            type = writeType.toCatalogDataType();
            // Doris exposes external-table columns as nullable. The real Paimon nullability and
            // defaults remain in the pinned FileStoreTable and are enforced by the Paimon writer.
            Column column = new Column(field.name(), type, true, null, true,
                    field.description(), true, -1);
            PaimonUtil.updatePaimonColumnUniqueId(column, field);
            if (field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                column.setWithTZExtraInfo();
            }
            schemaBuilder.add(column);
            columns.put(field.name(), column);
            types.put(field.name(), type);
        }
        this.schema = schemaBuilder.build();
        this.columnsByName = Collections.unmodifiableMap(columns);
        this.columnTypes = Collections.unmodifiableMap(types);

        Set<String> partitionColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        partitionColumns.addAll(table.partitionKeys());
        this.partitionColumnNames = Collections.unmodifiableSet(partitionColumns);
    }

    public static PaimonWriteTarget create(PaimonExternalTable dorisTable)
            throws AnalysisException {
        try {
            Table table = dorisTable.getPaimonTableForWrite();
            if (!(table instanceof FileStoreTable)) {
                throw new AnalysisException("Paimon write requires a file store table");
            }
            return new PaimonWriteTarget(dorisTable, (FileStoreTable) table);
        } catch (AnalysisException e) {
            throw e;
        } catch (Exception e) {
            throw new AnalysisException("Failed to load the latest Paimon write target: "
                    + e.getMessage(), e);
        }
    }

    public PaimonExternalTable getDorisTable() {
        return dorisTable;
    }

    public FileStoreTable getTable() {
        return table;
    }

    public List<Column> getSchema() {
        return schema;
    }

    public Column getColumn(String name) {
        return columnsByName.get(name);
    }

    public Map<String, Type> getColumnTypes() {
        return columnTypes;
    }

    public Set<String> getPartitionColumnNames() {
        return partitionColumnNames;
    }
}
