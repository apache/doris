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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Constraints of the Doris spatial Parquet writer, checked before DDL commits and data writes. */
final class IcebergSpatialSchemaValidator {
    private IcebergSpatialSchemaValidator() {
    }

    /** Returns whether any leaf is spatial so V3/Parquet checks also apply to nested columns. */
    static boolean containsSpatial(Type type) {
        if (type.typeId() == Type.TypeID.GEOMETRY || type.typeId() == Type.TypeID.GEOGRAPHY) {
            return true;
        }
        boolean spatial = false;
        if (type.isNestedType()) {
            for (Types.NestedField field : type.asNestedType().fields()) {
                spatial |= containsSpatial(field.type());
            }
        }
        return spatial;
    }

    static boolean containsSpatial(Schema schema) {
        return schema.columns().stream().anyMatch(field -> containsSpatial(field.type()));
    }

    static void validatePartitionSpec(Schema schema, PartitionSpec spec) {
        for (PartitionField field : spec.fields()) {
            // Dropped v1 partition fields retain a void transform, which writes NULL without a source value.
            if (field.transform().isVoid()) {
                continue;
            }
            // Identity accepts spatial types in Iceberg, but Doris has no spatial partition value/path codec.
            if (containsSpatial(schema.findType(field.sourceId()))) {
                throw new DorisConnectorException("Iceberg spatial columns cannot be partition sources: "
                        + schema.findColumnName(field.sourceId()) + " (" + field.transform() + ")");
            }
        }
    }

    static void validateTable(int formatVersion, String fileFormat) {
        if (formatVersion < 3) {
            throw new DorisConnectorException("Iceberg spatial columns require format-version >= 3");
        }
        String effectiveFormat = fileFormat == null ? TableProperties.DEFAULT_FILE_FORMAT_DEFAULT : fileFormat;
        if (!"parquet".equalsIgnoreCase(effectiveFormat)) {
            throw new DorisConnectorException("Iceberg spatial columns require Parquet, but the write format is "
                    + effectiveFormat);
        }
    }
}
