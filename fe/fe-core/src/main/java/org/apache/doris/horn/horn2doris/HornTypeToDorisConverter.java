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

package org.apache.doris.horn.horn2doris;

import org.apache.horn4j.thrift.TColumnType;
import org.apache.horn4j.thrift.TPrimitiveType;
import org.apache.horn4j.thrift.TScalarType;
import org.apache.horn4j.thrift.TTypeNode;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

/** Converts Horn/Impala TColumnType (Thrift) back to Doris Nereids DataType */
public class HornTypeToDorisConverter {

    /** Convert Horn TColumnType to Doris DataType */
    public static DataType convert(TColumnType hornType) {
        if (hornType == null || hornType.getTypes() == null || hornType.getTypes().isEmpty()) {
            throw new UnsupportedOperationException("Horn: empty TColumnType");
        }

        TTypeNode typeNode = hornType.getTypes().get(0);
        if (!typeNode.isSetScalar_type()) {
            throw new UnsupportedOperationException(
                    "Horn: non-scalar TColumnType not supported: " + hornType);
        }

        TScalarType scalarType = typeNode.getScalar_type();
        TPrimitiveType primType = scalarType.getType();

        switch (primType) {
            case BOOLEAN:
                return BooleanType.INSTANCE;
            case TINYINT:
                return TinyIntType.INSTANCE;
            case SMALLINT:
                return SmallIntType.INSTANCE;
            case INT:
                return IntegerType.INSTANCE;
            case BIGINT:
                return BigIntType.INSTANCE;
            case FLOAT:
                return FloatType.INSTANCE;
            case DOUBLE:
                return DoubleType.INSTANCE;
            case DECIMAL:
                // 反译统一回 DecimalV3
                return DecimalV3Type.createDecimalV3Type(
                        scalarType.getPrecision(), scalarType.getScale());
            case CHAR:
                return CharType.createCharType(scalarType.getLen());
            case VARCHAR:
                return VarcharType.createVarcharType(scalarType.getLen());
            case STRING:
                return StringType.INSTANCE;
            case DATE:
                // 反译统一回 DateV2
                return DateV2Type.INSTANCE;
            case NULL_TYPE:
                return NullType.INSTANCE;
            default:
                // 暂不支持 TIMESTAMP
                throw new UnsupportedOperationException(
                        "Horn: unsupported TPrimitiveType: " + primType);
        }
    }
}
