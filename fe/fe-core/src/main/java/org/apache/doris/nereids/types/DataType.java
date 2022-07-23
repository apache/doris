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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.MultiRowType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;

/**
 * Abstract class for all data type in Nereids.
 */
public abstract class DataType {
    /**
     * Convert data type in Doris catalog to data type in Nereids.
     * TODO: throw exception when cannot convert catalog type to Nereids type
     *
     * @param catalogType data type in Doris catalog
     * @return data type in Nereids
     */
    public static DataType convertFromCatalogDataType(Type catalogType) {
        if (catalogType instanceof ScalarType) {
            ScalarType scalarType = (ScalarType) catalogType;
            switch (scalarType.getPrimitiveType()) {
                case BOOLEAN:
                    return BooleanType.INSTANCE;
                case INT:
                    return IntegerType.INSTANCE;
                case BIGINT:
                    return BigIntType.INSTANCE;
                case DOUBLE:
                    return DoubleType.INSTANCE;
                case VARCHAR:
                    return VarcharType.createVarcharType(scalarType.getLength());
                case STRING:
                    return StringType.INSTANCE;
                case NULL_TYPE:
                    return NullType.INSTANCE;
                default:
                    throw new AnalysisException("Nereids do not support type: " + scalarType.getPrimitiveType());
            }
        } else if (catalogType instanceof MapType) {
            throw new AnalysisException("Nereids do not support map type.");
        } else if (catalogType instanceof StructType) {
            throw new AnalysisException("Nereids do not support struct type.");
        } else if (catalogType instanceof ArrayType) {
            throw new AnalysisException("Nereids do not support array type.");
        } else if (catalogType instanceof MultiRowType) {
            throw new AnalysisException("Nereids do not support multi row type.");
        } else {
            throw new AnalysisException("Nereids do not support type: " + catalogType);
        }
    }

    public abstract Type toCatalogDataType();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
