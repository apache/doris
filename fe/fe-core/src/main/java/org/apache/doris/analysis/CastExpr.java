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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/CastExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;

import com.google.gson.annotations.SerializedName;

public class CastExpr extends Expr {
    // True if this is a "pre-analyzed" implicit cast.
    @SerializedName("ii")
    protected boolean isImplicit;

    // True if this cast does not change the type.
    @SerializedName("noOp")
    protected boolean noOp = false;

    // only used restore from readFields.
    private CastExpr() {

    }

    public CastExpr(Type targetType, Expr e, boolean nullable) {
        type = targetType;
        isImplicit = true;
        children.add(e);

        noOp = Type.matchExactType(e.type, type, true);
        if (noOp) {
            // For decimalv2, we do not perform an actual cast between different precision/scale. Instead, we just
            // set the target type as the child's type.
            if (type.isDecimalV2() && e.type.isDecimalV2()) {
                getChild(0).setType(type);
            }
            // as the targetType have struct field name, if use the default name will be
            // like col1,col2, col3... in struct, and the filed name is import in BE.
            if (type.isStructType() && e.type.isStructType()) {
                getChild(0).setType(type);
            }
        }
        this.nullable = nullable;
    }

    protected CastExpr(CastExpr other) {
        super(other);
        isImplicit = other.isImplicit;
        noOp = other.noOp;
    }

    @Override
    public Expr clone() {
        return new CastExpr(this);
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitCastExpr(this, context);
    }

    public boolean isImplicit() {
        return isImplicit;
    }

    public void setImplicit(boolean implicit) {
        isImplicit = implicit;
    }

    public boolean isNoOp() {
        return noOp;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public boolean canHashPartition() {
        if (type.isFixedPointType() && getChild(0).getType().isFixedPointType()) {
            return true;
        }
        if (type.isDateType() && getChild(0).getType().isDateType()) {
            return true;
        }
        return false;
    }

}
