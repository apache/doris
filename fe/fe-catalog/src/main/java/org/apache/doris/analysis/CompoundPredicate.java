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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/CompoundPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * &&, ||, ! predicates.
 */
public class CompoundPredicate extends Predicate {
    @SerializedName("op")
    private Operator op;

    private CompoundPredicate() {
        // use for serde only
    }

    /**
     * use only for test.
     */
    public CompoundPredicate(Operator op, Expr e1, Expr e2) {
        super();
        this.op = op;
        children.add(e1);
        if (e2 != null) {
            children.add(e2);
        }
        this.nullable = true;
    }

    public CompoundPredicate(Operator op, Expr e1, Expr e2, boolean nullable) {
        super();
        this.op = op;
        children.add(e1);
        if (e2 != null) {
            children.add(e2);
        }
        this.nullable = nullable;
    }

    protected CompoundPredicate(CompoundPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new CompoundPredicate(this);
    }

    public Operator getOp() {
        return op;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ((CompoundPredicate) obj).op == op;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitCompoundPredicate(this, context);
    }

    public enum Operator {
        AND("AND"),
        OR("OR"),
        NOT("NOT");

        private final String description;

        Operator(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    @Override
    public String toString() {
        if (getChildren().size() == 1) {
            Preconditions.checkState(getOp() == Operator.NOT);
            return "NOT " + getChild(0);
        } else {
            return "(" + getChild(0)
                    + " " + getOp().toString()
                    + " " + getChild(1) + ")";
        }
    }
}
