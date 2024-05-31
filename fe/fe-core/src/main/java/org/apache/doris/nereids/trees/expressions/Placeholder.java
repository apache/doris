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

import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;

import java.util.Optional;

/**
 * Placeholder for prepared statement
 */
public class Placeholder extends Expression implements LeafExpression {
    private final PlaceholderId placeholderId;
    private final Optional<MysqlColType> mysqlColType;

    public Placeholder(PlaceholderId placeholderId) {
        this.placeholderId = placeholderId;
        this.mysqlColType = Optional.empty();
    }

    public Placeholder(PlaceholderId placeholderId, MysqlColType mysqlColType) {
        this.placeholderId = placeholderId;
        this.mysqlColType = Optional.of(mysqlColType);
    }

    public PlaceholderId getPlaceholderId() {
        return placeholderId;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPlaceholder(this, context);
    }

    @Override
    public boolean nullable() {
        return true;
    }

    @Override
    public String toSql() {
        return "?";
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return NullType.INSTANCE;
    }

    public Placeholder withNewMysqlColType(MysqlColType mysqlColType) {
        return new Placeholder(getPlaceholderId(), mysqlColType);
    }

    public MysqlColType getMysqlColType() {
        return mysqlColType.get();
    }
}
