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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;

import java.util.Objects;

/**
 * All data type literal expression in Nereids.
 * TODO: Increase the implementation of sub expression. such as Integer.
 */
public class Literal extends Expression implements LeafExpression {
    public static final Literal TRUE_LITERAL = new Literal(true);
    public static final Literal FALSE_LITERAL = new Literal(false);
    private final DataType dataType;
    private final Object value;

    /**
     * Constructor for Literal.
     *
     * @param value    real value stored in java object
     * @param dataType logical data type in Nereids
     */
    public Literal(Object value, DataType dataType) {
        super(NodeType.LITERAL);
        this.dataType = dataType;
        this.value = value;
    }

    /**
     * Constructor for Literal. Recognize data type Automatically.
     *
     * @param value real value stored in java object
     */
    public Literal(Object value) {
        super(NodeType.LITERAL);
        this.value = value;
        if (value == null) {
            dataType = NullType.INSTANCE;
        } else if (value instanceof Integer) {
            dataType = IntegerType.INSTANCE;
        } else if (value instanceof Boolean) {
            dataType = BooleanType.INSTANCE;
        } else if (value instanceof String) {
            dataType = StringType.INSTANCE;
        } else {
            throw new RuntimeException();
        }
    }

    public Object getValue() {
        return value;
    }

    /**
     * Convert to legacy literal expression in Doris.
     *
     * @return legacy literal expression in Doris
     */
    public Expr toExpr() {
        if (dataType instanceof IntegerType) {
            return new IntLiteral((Integer) value);
        } else {
            return null;
        }
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return dataType;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return value == null;
    }

    public static Literal of(Object value) {
        return new Literal(value);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public String sql() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Literal other = (Literal) o;
        return Objects.equals(value, other.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
