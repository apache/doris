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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.DataType;

import java.util.Locale;

/**
 * Represent any datatype in type coercion.
 */
public class AnyDataType extends DataType {

    public static final int INDEX_OF_INSTANCE_WITHOUT_INDEX = -1;
    public static final AnyDataType INSTANCE_WITHOUT_INDEX = new AnyDataType(INDEX_OF_INSTANCE_WITHOUT_INDEX);

    private final int index;

    public AnyDataType(int index) {
        if (index < 0) {
            index = -1;
        }
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(simpleString().toUpperCase(Locale.ROOT));
        if (index >= 0) {
            sb.append("#").append(index);
        }
        return sb.toString();
    }

    @Override
    public DataType defaultConcreteType() {
        throw new RuntimeException("Unsupported operation.");
    }

    @Override
    public boolean acceptsType(DataType other) {
        return true;
    }

    @Override
    public Type toCatalogDataType() {
        throw new AnalysisException("AnyDataType can not cast to catalog data type");
    }

    @Override
    public String simpleString() {
        return "any";
    }

    @Override
    public int width() {
        return -1;
    }
}
