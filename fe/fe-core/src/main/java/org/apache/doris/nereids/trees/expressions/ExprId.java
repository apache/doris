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

import org.apache.doris.common.Id;
import org.apache.doris.common.IdGenerator;

import java.util.Objects;

/**
 * UUID for Expression in Nereids.
 */
public class ExprId extends Id<ExprId> {

    public ExprId(int id) {
        super(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExprId exprId = (ExprId) o;
        return id == exprId.id;
    }

    /**
     * Should be only called by {@link org.apache.doris.nereids.trees.expressions.NamedExpressionUtil}.
     */
    public static IdGenerator<ExprId> createGenerator() {
        return new IdGenerator<ExprId>() {
            @Override
            public ExprId getNextId() {
                return new ExprId(nextId++);
            }

            @Override
            public ExprId getMaxId() {
                return new ExprId(nextId++);
            }
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "" + id;
    }
}
