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

/**
 * Abstract class for all data type in Nereids.
 */
public abstract class DataType {
    /**
     * Convert data type in Doris catalog to data type in Nereids.
     *
     * @param catalogType data type in Doris catalog
     * @return data type in Nereids
     */
    public static DataType convertFromCatalogDataType(Type catalogType) {
        if (catalogType instanceof ScalarType) {
            ScalarType scalarType = (ScalarType) catalogType;
            switch (scalarType.getPrimitiveType()) {
                case INT:
                    return IntegerType.INSTANCE;
                case DOUBLE:
                    return DoubleType.INSTANCE;
                case STRING:
                    return StringType.INSTANCE;
                default:
                    return null;
            }
        } else if (catalogType instanceof MapType) {
            return null;
        } else if (catalogType instanceof StructType) {
            return null;
        } else if (catalogType instanceof ArrayType) {
            return null;
        } else if (catalogType instanceof MultiRowType) {
            return null;
        } else {
            return null;
        }
    }

    public abstract Type toCatalogDataType();
}
