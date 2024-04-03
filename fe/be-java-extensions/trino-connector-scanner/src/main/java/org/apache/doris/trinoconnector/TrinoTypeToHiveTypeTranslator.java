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

package org.apache.doris.trinoconnector;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
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

public final class TrinoTypeToHiveTypeTranslator {
    private TrinoTypeToHiveTypeTranslator() {
    }

    // trino do not support LargeInt
    public static String fromTrinoTypeToHiveType(Type type) {
        if (type instanceof BooleanType) {
            return "boolean";
        } else if (type instanceof TinyintType) {
            return "tinyint";
        } else if (type instanceof SmallintType) {
            return "smallint";
        } else if (type instanceof IntegerType) {
            return "int";
        } else if (type instanceof BigintType) {
            return "bigint";
        } else if (type instanceof RealType) {
            return "float";
        } else if (type instanceof DoubleType) {
            return "double";
        } else if (type instanceof CharType) {
            return type.toString();
        } else if (type instanceof VarcharType) {
            return "string";
        } else if (type instanceof VarbinaryType) {
            return "string";
        } else if (type instanceof DecimalType) {
            StringBuilder sb = new StringBuilder("decimal");
            sb.append("(");
            sb.append(((DecimalType) type).getPrecision());
            sb.append(", ");
            sb.append(((DecimalType) type).getScale());
            sb.append(")");
            return sb.toString();
        } else if (type instanceof TimeType) {
            return "string";
        } else if (type instanceof DateType) {
            return "date";
        } else if (type instanceof TimestampType) {
            return "timestamp";
        } else if (type instanceof TimestampWithTimeZoneType) {
            return "timestamp";
        } else if (type instanceof ArrayType) {
            StringBuilder sb = new StringBuilder();
            sb.append("array<")
                    .append(fromTrinoTypeToHiveType(((ArrayType) type).getElementType()))
                    .append(">");
            return sb.toString();
        } else if (type instanceof MapType) {
            StringBuilder sb = new StringBuilder();
            sb.append("map<")
                    .append(fromTrinoTypeToHiveType(((MapType) type).getKeyType()))
                    .append(",")
                    .append(fromTrinoTypeToHiveType(((MapType) type).getValueType()));
            sb.append(">");
            return sb.toString();
        } else if (type instanceof RowType) {
            StringBuilder sb = new StringBuilder();
            sb.append("struct<");
            for (int i = 0; i < type.getTypeParameters().size(); ++i) {
                if (i != 0) {
                    sb.append(",");
                }
                Type field = type.getTypeParameters().get(i);
                sb.append(field.getDisplayName())
                        .append(":")
                        .append(fromTrinoTypeToHiveType(field));
            }
            sb.append(">");
            return sb.toString();
        } else {
            throw new IllegalArgumentException("Cannot transform unknown type: " + type);
        }
    }
}

