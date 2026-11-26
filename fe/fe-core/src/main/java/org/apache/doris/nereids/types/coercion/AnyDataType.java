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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Represent any datatype in type coercion.
 * with index = -1: individual slot, just accept any input type. process in `implementAnyDataTypeWithOutIndex`
 * with index != -1: slots with same index are treated as type-equal. target type is the common type of them.
 * for example:
 *  two slots with AnyDataType(0) has type int and bigint separately, then the target type of both is bigint.
 *  process in `implementAnyDataTypeWithIndex`.
 * AnyDataType could NOT be return type. see more in FollowToAnyDataType.java
 */
public class AnyDataType extends DataType {

    public static final int INDEX_OF_INSTANCE_WITHOUT_INDEX = -1;
    public static final AnyDataType INSTANCE_WITHOUT_INDEX = new AnyDataType(INDEX_OF_INSTANCE_WITHOUT_INDEX);
    public static final AnyDataType ORDERING_WITHOUT_INDEX
            = new AnyDataType(INDEX_OF_INSTANCE_WITHOUT_INDEX, KeyUse.ORDERING);
    public static final AnyDataType GROUP_EQUALITY_WITHOUT_INDEX
            = new AnyDataType(INDEX_OF_INSTANCE_WITHOUT_INDEX, KeyUse.GROUP_EQUALITY);

    private final int index;
    @Nullable
    private final KeyUse requiredKeyUse;

    public AnyDataType(int index) {
        this(index, null);
    }

    /** Construct an ANY placeholder optionally constrained to types allowing {@code requiredKeyUse}. */
    public AnyDataType(int index, @Nullable KeyUse requiredKeyUse) {
        if (index < 0) {
            index = -1;
        }
        this.index = index;
        this.requiredKeyUse = requiredKeyUse;
    }

    public static AnyDataType ordering(int index) {
        return index < 0 ? ORDERING_WITHOUT_INDEX : new AnyDataType(index, KeyUse.ORDERING);
    }

    public static AnyDataType groupEquality(int index) {
        return index < 0 ? GROUP_EQUALITY_WITHOUT_INDEX : new AnyDataType(index, KeyUse.GROUP_EQUALITY);
    }

    public int getIndex() {
        return index;
    }

    @Nullable
    public KeyUse getRequiredKeyUse() {
        return requiredKeyUse;
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
        return requiredKeyUse == null || other.allowsKeyUse(requiredKeyUse);
    }

    @Override
    public Type toCatalogDataType() {
        throw new AnalysisException("AnyDataType can not cast to catalog data type");
    }

    @Override
    public String simpleString() {
        if (requiredKeyUse == KeyUse.ORDERING) {
            return "orderableAny";
        } else if (requiredKeyUse == KeyUse.GROUP_EQUALITY) {
            return "equatableAny";
        }
        return "any";
    }

    @Override
    public int width() {
        return -1;
    }

    @Override
    public Optional<Expression> zeroValue() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnyDataType that = (AnyDataType) o;
        return index == that.index && requiredKeyUse == that.requiredKeyUse;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, requiredKeyUse);
    }
}
