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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

/**
 * Abstract class for all integral data type in Nereids.
 */
public class IntegralType extends NumericType {

    public static final IntegralType INSTANCE = new IntegralType();

    @Override
    public DataType defaultConcreteType() {
        return BigIntType.INSTANCE;
    }

    @Override
    public boolean acceptsType(DataType other) {
        return other instanceof IntegralType;
    }

    @Override
    public String simpleString() {
        return "integral";
    }

    public boolean widerThan(IntegralType other) {
        return this.width() > other.width();
    }

    /**
     * parse string to integer like literal.
     */
    public IntegerLikeLiteral fromString(String s) {
        if (this instanceof TinyIntType) {
            return new TinyIntLiteral(Byte.parseByte(s));
        } else if (this instanceof SmallIntType) {
            return new SmallIntLiteral(Short.parseShort(s));
        } else if (this instanceof IntegerType) {
            return new IntegerLiteral(Integer.parseInt(s));
        } else if (this instanceof BigIntType) {
            return new BigIntLiteral(Long.parseLong(s));
        } else {
            throw new AnalysisException("unknown integer like type");
        }
    }
}
