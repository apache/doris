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
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

/**
 * Abstract type for all characters type in Nereids.
 */
public class CharacterType extends PrimitiveType {

    public static final CharacterType INSTANCE = new CharacterType(-1);

    private static final int WIDTH = 16;

    protected final int len;

    public CharacterType(int len) {
        this.len = len;
    }

    public int getLen() {
        return len;
    }

    @Override
    public Type toCatalogDataType() {
        throw new RuntimeException("CharacterType is only used for implicit cast.");
    }

    @Override
    public boolean acceptsType(AbstractDataType other) {
        return other instanceof CharacterType;
    }

    @Override
    public DataType defaultConcreteType() {
        return StringType.INSTANCE;
    }

    @Override
    public int width() {
        return WIDTH;
    }

    /**
     * find the wider character type for type coercion.
     */
    public static CharacterType widerCharacterType(CharacterType left, CharacterType right) {
        if (left.equals(right)) {
            return left;
        }
        if (left instanceof StringType || right instanceof StringType) {
            return StringType.INSTANCE;
        }
        if (left.getLen() == -1 || right.getLen() == -1) {
            return VarcharType.SYSTEM_DEFAULT;
        }
        return new VarcharType(Math.max(left.getLen(), right.getLen()));
    }
}
