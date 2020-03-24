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

package org.apache.doris.analysis;

import org.apache.doris.thrift.TExprNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Class representing a [NOT] EXISTS predicate.
 */
public class ExistsPredicate extends Predicate {
    private static final Logger LOG = LoggerFactory.getLogger(
        ExistsPredicate.class);
    private boolean notExists = false;

    public boolean isNotExists() { return notExists; }

    public ExistsPredicate(Subquery subquery, boolean notExists) {
        Preconditions.checkNotNull(subquery);
        children.add(subquery);
        this.notExists = notExists;
    }

    public ExistsPredicate(ExistsPredicate other) {
        super(other);
        notExists = other.notExists;
    }

    @Override
    public Expr negate() {
        return new ExistsPredicate((Subquery) getChild(0), !notExists);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // Cannot serialize a nested predicate
        Preconditions.checkState(false);
    }

    @Override
    public Expr clone() { return new ExistsPredicate(this); }

    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        if (notExists) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(getChild(0).toSql());
        return strBuilder.toString();
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(notExists);
    }
}

