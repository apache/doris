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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.api.ConnectorType;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps Trino SPI types to Doris {@link ConnectorType}.
 *
 * <p>Adapted from {@code TrinoConnectorExternalTable.trinoConnectorTypeToDorisType()}
 * in fe-core, but produces ConnectorType (fe-connector-api) instead of
 * Doris Column/Type (fe-core).</p>
 */
public final class TrinoTypeMapping {

    private TrinoTypeMapping() {
    }

    /**
     * Converts a Trino SPI {@link Type} to a Doris {@link ConnectorType}.
     */
    public static ConnectorType toConnectorType(Type type) {
        if (type instanceof BooleanType) {
            return new ConnectorType("BOOLEAN");
        } else if (type instanceof TinyintType) {
            return new ConnectorType("TINYINT");
        } else if (type instanceof SmallintType) {
            return new ConnectorType("SMALLINT");
        } else if (type instanceof IntegerType) {
            return new ConnectorType("INT");
        } else if (type instanceof BigintType) {
            return new ConnectorType("BIGINT");
        } else if (type instanceof RealType) {
            return new ConnectorType("FLOAT");
        } else if (type instanceof DoubleType) {
            return new ConnectorType("DOUBLE");
        } else if (type instanceof CharType) {
            return new ConnectorType("CHAR");
        } else if (type instanceof VarcharType) {
            return new ConnectorType("STRING");
        } else if (type instanceof VarbinaryType) {
            return new ConnectorType("STRING");
        } else if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return new ConnectorType("DECIMALV3", decimal.getPrecision(), decimal.getScale());
        } else if (type instanceof TimeType) {
            return new ConnectorType("STRING");
        } else if (type instanceof DateType) {
            return new ConnectorType("DATEV2");
        } else if (type instanceof TimestampType) {
            int precision = Math.min(((TimestampType) type).getPrecision(), 6);
            return new ConnectorType("DATETIMEV2", precision, -1);
        } else if (type instanceof TimestampWithTimeZoneType) {
            int precision = Math.min(((TimestampWithTimeZoneType) type).getPrecision(), 6);
            return new ConnectorType("DATETIMEV2", precision, -1);
        } else if (type instanceof io.trino.spi.type.ArrayType) {
            ConnectorType elementType = toConnectorType(
                    ((io.trino.spi.type.ArrayType) type).getElementType());
            List<ConnectorType> children = new ArrayList<>();
            children.add(elementType);
            return new ConnectorType("ARRAY", -1, -1, children);
        } else if (type instanceof io.trino.spi.type.MapType) {
            ConnectorType keyType = toConnectorType(
                    ((io.trino.spi.type.MapType) type).getKeyType());
            ConnectorType valueType = toConnectorType(
                    ((io.trino.spi.type.MapType) type).getValueType());
            List<ConnectorType> children = new ArrayList<>();
            children.add(keyType);
            children.add(valueType);
            return new ConnectorType("MAP", -1, -1, children);
        } else if (type instanceof RowType) {
            List<ConnectorType> children = new ArrayList<>();
            for (RowType.Field field : ((RowType) type).getFields()) {
                children.add(toConnectorType(field.getType()));
            }
            return new ConnectorType("STRUCT", -1, -1, children);
        } else {
            throw new IllegalArgumentException("Cannot transform unknown Trino type: " + type);
        }
    }
}
