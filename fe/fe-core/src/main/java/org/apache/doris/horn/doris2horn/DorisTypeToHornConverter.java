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

package org.apache.doris.horn.doris2horn;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.horn4j.thrift.TColumnType;
import org.apache.horn4j.thrift.TPrimitiveType;
import org.apache.horn4j.thrift.TScalarType;
import org.apache.horn4j.thrift.TTypeNode;
import org.apache.horn4j.thrift.TTypeNodeType;
import org.apache.doris.nereids.types.DataType;

import java.util.Collections;

/** Converts Doris Nereids/catalog types to Horn TColumnType (Thrift) */
public class DorisTypeToHornConverter {

    /** Convert a Nereids DataType to Horn TColumnType */
    public static TColumnType convert(DataType dataType) {
        return wrap(mapPrimitive(dataType.toCatalogDataType()));
    }

    /** Convert a legacy Doris catalog Type to Horn TColumnType (used for table metadata) */
    public static TColumnType convertCatalogType(Type catalogType) {
        return wrap(mapPrimitive(catalogType));
    }

    /** Map a Doris {@link Type} to a Horn {@link TScalarType} */
    private static TScalarType mapPrimitive(Type catalogType) {
        TScalarType scalarType = new TScalarType();
        switch (catalogType.getPrimitiveType()) {
            case BOOLEAN:
                scalarType.setType(TPrimitiveType.BOOLEAN);
                return scalarType;
            case TINYINT:
                scalarType.setType(TPrimitiveType.TINYINT);
                return scalarType;
            case SMALLINT:
                scalarType.setType(TPrimitiveType.SMALLINT);
                return scalarType;
            case INT:
                scalarType.setType(TPrimitiveType.INT);
                return scalarType;
            case BIGINT:
                scalarType.setType(TPrimitiveType.BIGINT);
                return scalarType;
            case FLOAT:
                scalarType.setType(TPrimitiveType.FLOAT);
                return scalarType;
            case DOUBLE:
                scalarType.setType(TPrimitiveType.DOUBLE);
                return scalarType;
            case DATE:
            case DATEV2:
                scalarType.setType(TPrimitiveType.DATE);
                return scalarType;
            case NULL_TYPE:
                scalarType.setType(TPrimitiveType.NULL_TYPE);
                return scalarType;
            case INVALID_TYPE:
                // 无值表达式（WindowFrame / FrameBoundary 等）的 placeholder：
                scalarType.setType(TPrimitiveType.INVALID_TYPE);
                return scalarType;
            case STRING:
                scalarType.setType(TPrimitiveType.STRING);
                return scalarType;
            case CHAR:
                scalarType.setType(TPrimitiveType.CHAR);
                scalarType.setLen(((ScalarType) catalogType).getLength());
                return scalarType;
            case VARCHAR:
                scalarType.setType(TPrimitiveType.VARCHAR);
                scalarType.setLen(((ScalarType) catalogType).getLength());
                return scalarType;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                scalarType.setType(TPrimitiveType.DECIMAL);
                scalarType.setPrecision(((ScalarType) catalogType).getPrecision());
                scalarType.setScale(((ScalarType) catalogType).getScalarScale());
                return scalarType;
            default:
                // 暂不支持 LARGEINT、DECIMAL256、TIME、TIMESTAMP 等类型
                throw new UnsupportedOperationException(
                        "Horn: unsupported Doris type: " + catalogType);
        }
    }

    private static TColumnType wrap(TScalarType scalarType) {
        TTypeNode node = new TTypeNode();
        node.setType(TTypeNodeType.SCALAR);
        node.setScalar_type(scalarType);
        TColumnType result = new TColumnType();
        result.setTypes(Collections.singletonList(node));
        return result;
    }
}
