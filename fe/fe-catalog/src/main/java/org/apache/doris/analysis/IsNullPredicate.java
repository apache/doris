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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/IsNullPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class IsNullPredicate extends Predicate {
    private static final String IS_NULL = "is_null_pred";
    private static final String IS_NOT_NULL = "is_not_null_pred";

    @SerializedName("inn")
    private boolean isNotNull;

    private IsNullPredicate() {
        // use for serde only
    }

    /**
     * use for Nereids ONLY
     */
    public IsNullPredicate(Expr e, boolean isNotNull) {
        super();
        this.isNotNull = isNotNull;
        Preconditions.checkNotNull(e);
        children.add(e);
        fn = new Function(new FunctionName(isNotNull ? IS_NOT_NULL : IS_NULL), Lists.newArrayList(e.getType()),
                Type.BOOLEAN, false, true, NullableMode.ALWAYS_NOT_NULLABLE);
        this.nullable = false;
    }

    protected IsNullPredicate(IsNullPredicate other) {
        super(other);
        this.isNotNull = other.isNotNull;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public Expr clone() {
        return new IsNullPredicate(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isNotNull);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return ((IsNullPredicate) obj).isNotNull == isNotNull;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitIsNullPredicate(this, context);
    }

    public boolean isSlotRefChildren() {
        return (children.get(0) instanceof SlotRef);
    }
}
