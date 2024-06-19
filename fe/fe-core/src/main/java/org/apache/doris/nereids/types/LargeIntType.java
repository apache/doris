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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.coercion.IntegralType;

import java.math.BigInteger;

/**
 * LargeInt type in Nereids.
 */
public class LargeIntType extends IntegralType {

    public static final LargeIntType INSTANCE = new LargeIntType("largeint");
    public static final LargeIntType UNSIGNED = new LargeIntType("unsigned");

    public static final BigInteger MAX_VALUE = new BigInteger("170141183460469231731687303715884105727");

    public static final BigInteger MIN_VALUE = new BigInteger("-170141183460469231731687303715884105728");

    private static final int WIDTH = 16;

    private final String simpleName;

    private LargeIntType(String simpleName) {
        this.simpleName = simpleName;
    }

    @Override
    public Type toCatalogDataType() {
        return Type.LARGEINT;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LargeIntType;
    }

    @Override
    public String simpleString() {
        return simpleName;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof LargeIntType;
    }

    @Override
    public DataType defaultConcreteType() {
        return this;
    }

    @Override
    public int width() {
        return WIDTH;
    }
}
