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

package org.apache.doris.datasource.fluss;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FlussUtils {
    private static final Logger LOG = LogManager.getLogger(FlussUtils.class);

    /**
     * Load schema cache value from Fluss table
     */
    public static Optional<SchemaCacheValue> loadSchemaCacheValue(FlussExternalTable table) {
        try {
            FlussExternalCatalog catalog = (FlussExternalCatalog) table.getCatalog();
            FlussMetadataOps metadataOps = (FlussMetadataOps) catalog.getMetadataOps();
            
            TableInfo tableInfo = metadataOps.getTableInfo(
                    table.getRemoteDbName(), table.getRemoteName());
            RowType rowType = tableInfo.getRowType();
            
            List<Column> columns = new ArrayList<>();
            List<Column> partitionColumns = new ArrayList<>();
            Set<String> partitionKeys = tableInfo.getPartitionKeys() != null 
                    ? tableInfo.getPartitionKeys().stream().collect(Collectors.toSet())
                    : java.util.Collections.emptySet();
            
            for (DataField field : rowType.getFields()) {
                String fieldName = field.getName();
                DataType fieldType = field.getType();
                Type dorisType = flussTypeToDorisType(fieldType, catalog.getEnableMappingVarbinary());
                
                Column column = new Column(
                        fieldName.toLowerCase(),
                        dorisType,
                        fieldType.isNullable(),
                        null,
                        true,
                        field.getDescription().orElse(null),
                        true,
                        -1);
                
                columns.add(column);
                if (partitionKeys.contains(fieldName)) {
                    partitionColumns.add(column);
                }
            }
            
            return Optional.of(new SchemaCacheValue(columns, partitionColumns));
        } catch (Exception e) {
            LOG.warn("Failed to load schema for Fluss table: {}.{}", 
                    table.getDbName(), table.getName(), e);
            throw new RuntimeException("Failed to load Fluss table schema: " 
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * Convert Fluss DataType to Doris Type
     */
    public static Type flussTypeToDorisType(DataType flussType, boolean enableMappingVarbinary) {
        DataTypeRoot typeRoot = flussType.getTypeRoot();
        
        switch (typeRoot) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case TINYINT:
                return Type.TINYINT;
            case SMALLINT:
                return Type.SMALLINT;
            case INT:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case STRING:
                return Type.STRING;
            case CHAR:
                CharType charType = (CharType) flussType;
                return ScalarType.createCharType(charType.getLength());
            case BINARY:
            case BYTES:
                if (enableMappingVarbinary) {
                    return ScalarType.createVarbinaryType(VarBinaryType.MAX_VARBINARY_LENGTH);
                } else {
                    return Type.STRING;
                }
            case DECIMAL:
                DecimalType decimalType = (DecimalType) flussType;
                return ScalarType.createDecimalV3Type(
                        decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP:
                TimestampType timestampType = (TimestampType) flussType;
                int precision = timestampType.getPrecision();
                return ScalarType.createDatetimeV2Type(Math.min(precision, 6));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedType = (LocalZonedTimestampType) flussType;
                int tzPrecision = localZonedType.getPrecision();
                return ScalarType.createDatetimeV2Type(Math.min(tzPrecision, 6));
            case ARRAY:
                org.apache.fluss.types.ArrayType arrayType = (org.apache.fluss.types.ArrayType) flussType;
                Type elementType = flussTypeToDorisType(arrayType.getElementType(), enableMappingVarbinary);
                return ArrayType.create(elementType, arrayType.getElementType().isNullable());
            case MAP:
                org.apache.fluss.types.MapType mapType = (org.apache.fluss.types.MapType) flussType;
                Type keyType = flussTypeToDorisType(mapType.getKeyType(), enableMappingVarbinary);
                Type valueType = flussTypeToDorisType(mapType.getValueType(), enableMappingVarbinary);
                return new MapType(keyType, valueType);
            case ROW:
                RowType rowType = (RowType) flussType;
                List<StructField> structFields = new ArrayList<>();
                for (DataField field : rowType.getFields()) {
                    Type fieldType = flussTypeToDorisType(field.getType(), enableMappingVarbinary);
                    structFields.add(new StructField(field.getName(), fieldType));
                }
                return new StructType(structFields);
            default:
                throw new IllegalArgumentException("Unsupported Fluss type: " + typeRoot);
        }
    }

    /**
     * Get Fluss table instance
     */
    public static org.apache.fluss.client.table.Table getFlussTable(FlussExternalTable table) {
        FlussExternalCatalog catalog = (FlussExternalCatalog) table.getCatalog();
        org.apache.fluss.metadata.TablePath tablePath = 
                org.apache.fluss.metadata.TablePath.of(table.getRemoteDbName(), table.getRemoteName());
        FlussMetadataOps metadataOps = (FlussMetadataOps) catalog.getMetadataOps();
        TableInfo tableInfo = metadataOps.getTableInfo(table.getRemoteDbName(), table.getRemoteName());
        return new org.apache.fluss.client.table.FlussTable(
                catalog.getFlussConnection(), tablePath, tableInfo);
    }
}

