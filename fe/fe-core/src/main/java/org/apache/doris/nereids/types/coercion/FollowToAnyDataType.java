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
import org.apache.doris.nereids.types.DataType;

import java.util.Locale;

/**
 * FollowToAnyDataType is used to auto compute the return type by the AnyDataType in signature.
 */
public class FollowToAnyDataType extends DataType {

    public final int index;

    public FollowToAnyDataType(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toSql() {
        return simpleString().toUpperCase(Locale.ROOT);
    }

    @Override
    public DataType defaultConcreteType() {
        throw new RuntimeException("Unsupported operation.");
    }

    @Override
    public boolean acceptsType(DataType other) {
        throw new RuntimeException("Unsupported operation.");
    }

    @Override
    public Type toCatalogDataType() {
        throw new RuntimeException("Unsupported operation.");
    }

    @Override
    public String simpleString() {
        return "followAnyDataType#" + index;
    }

    @Override
    public int width() {
        return -1;
    }
}
