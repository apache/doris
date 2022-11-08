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

package org.apache.doris.nereids.types.coercion;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.types.DataType;

/**
 * Represent a set of equality concrete data type.
 */
public interface AbstractDataType {

    /**
     * the default concrete type cast to when do implicit cast
     */
    DataType defaultConcreteType();

    /**
     * This AbstractDataType could accept other without implicit cast
     */
    boolean acceptsType(AbstractDataType other);

    /**
     * convert nereids's data type to legacy catalog data type
     * @return legacy catalog data type
     */
    Type toCatalogDataType();

    /**
     * simple string used to print error message
     */
    String simpleString();

    /**
     * whether the target dataType is assignable to this dataType
     * @param targetDataType the target data type
     * @return true if assignable
     */
    @Developing
    default boolean isAssignableFrom(AbstractDataType targetDataType) {
        if (this.equals(targetDataType)) {
            return true;
        }
        if (this instanceof CharacterType) {
            return true;
        }
        return false;
    }
}
