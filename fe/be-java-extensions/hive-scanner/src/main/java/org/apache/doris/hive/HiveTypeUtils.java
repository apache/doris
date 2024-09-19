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

package org.apache.doris.hive;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema.SchemaColumn;
import org.apache.doris.thrift.TPrimitiveType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HiveTypeUtils {

    public static TPrimitiveType typeFromColumnType(ColumnType columnType, SchemaColumn schemaColumn)
            throws UnsupportedOperationException {
        switch (columnType.getType()) {
            case UNSUPPORTED:
                return TPrimitiveType.UNSUPPORTED;
            case BOOLEAN:
                return TPrimitiveType.BOOLEAN;
            case TINYINT:
                return TPrimitiveType.TINYINT;
            case SMALLINT:
                return TPrimitiveType.SMALLINT;
            case INT:
                return TPrimitiveType.INT;
            case BIGINT:
                return TPrimitiveType.BIGINT;
            case LARGEINT:
                return TPrimitiveType.LARGEINT;
            case FLOAT:
                return TPrimitiveType.FLOAT;
            case DOUBLE:
                return TPrimitiveType.DOUBLE;
            case DATE:
                return TPrimitiveType.DATE;
            case DATEV2:
                return TPrimitiveType.DATEV2;
            case DATETIME:
                return TPrimitiveType.DATETIME;
            case DATETIMEV2:
                return TPrimitiveType.DATETIMEV2;
            case CHAR:
                return TPrimitiveType.CHAR;
            case VARCHAR:
                return TPrimitiveType.VARCHAR;
            case STRING:
                return TPrimitiveType.STRING;
            case DECIMALV2:
                return TPrimitiveType.DECIMALV2;
            case DECIMAL32:
                return TPrimitiveType.DECIMAL32;
            case DECIMAL64:
                return TPrimitiveType.DECIMAL64;
            case DECIMAL128:
                return TPrimitiveType.DECIMAL128I;
            case ARRAY:
                SchemaColumn arrayChildColumn = new SchemaColumn();
                schemaColumn.addChildColumns(Collections.singletonList(arrayChildColumn));
                arrayChildColumn.setType(typeFromColumnType(columnType.getChildTypes().get(0), arrayChildColumn));
                return TPrimitiveType.ARRAY;
            case MAP:
                SchemaColumn keyChildColumn = new SchemaColumn();
                keyChildColumn.setType(TPrimitiveType.STRING);
                SchemaColumn valueChildColumn = new SchemaColumn();
                valueChildColumn.setType(typeFromColumnType(columnType.getChildTypes().get(1), valueChildColumn));
                schemaColumn.addChildColumns(Arrays.asList(keyChildColumn, valueChildColumn));
                return TPrimitiveType.MAP;
            case STRUCT:
                List<ColumnType> fields = columnType.getChildTypes();
                List<SchemaColumn> childSchemaColumn = new ArrayList<>();
                for (ColumnType field : fields) {
                    SchemaColumn structChildColumn = new SchemaColumn();
                    structChildColumn.setName(field.getName());
                    structChildColumn.setType(typeFromColumnType(field, structChildColumn));
                    childSchemaColumn.add(structChildColumn);
                }
                schemaColumn.addChildColumns(childSchemaColumn);
                return TPrimitiveType.STRUCT;
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + columnType.getType());
        }
    }
}
