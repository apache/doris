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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.TernaryExpression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * substring function.
 */
public class Substring extends BoundFunction implements TernaryExpression {

    public Substring(Expression str, Expression pos, Expression len) {
        super("substring", str, pos, len);
    }

    public Substring(Expression str, Expression pos) {
        super("substring", str, pos, new IntegerLiteral(Integer.MAX_VALUE));
    }

    @Override
    public DataType getDataType() {
        return StringType.INSTANCE;
    }

    @Override
    public boolean nullable() {
        return first().nullable();
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        if (children.size() == 2) {
            return new Substring(children.get(0), children.get(1));
        }
        return new Substring(children.get(0), children.get(1), children.get(2));
    }
}
