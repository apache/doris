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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.TVFProperties;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** TableValuedFunction */
public abstract class TableValuedFunction extends BoundFunction implements UnaryExpression, CustomSignature {
    protected final Supplier<TableValuedFunctionIf> catalogFunctionCache = Suppliers.memoize(() -> toCatalogFunction());
    protected final Supplier<FunctionGenTable> tableCache = Suppliers.memoize(() -> {
        try {
            return catalogFunctionCache.get().getTable();
        } catch (AnalysisException e) {
            throw e;
        } catch (Throwable t) {
            throw new AnalysisException("Can not build FunctionGenTable by " + this + ": " + t.getMessage(), t);
        }
    });

    public TableValuedFunction(String functionName, TVFProperties tvfProperties) {
        super(functionName, tvfProperties);
    }

    protected abstract TableValuedFunctionIf toCatalogFunction();

    public abstract StatsDeriveResult computeStats(List<Slot> slots);

    public TVFProperties getTVFProperties() {
        return (TVFProperties) child(0);
    }

    public final String getTableName() {
        return tableCache.get().getName();
    }

    public final List<Column> getTableColumns() {
        return ImmutableList.copyOf(tableCache.get().getBaseSchema());
    }

    public final TableValuedFunctionIf getCatalogFunction() {
        return catalogFunctionCache.get();
    }

    public final FunctionGenTable getTable() {
        return tableCache.get();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTableValuedFunction(this, context);
    }

    @Override
    public boolean nullable() {
        throw new UnboundException("TableValuedFunction can not compute nullable");
    }

    @Override
    public DataType getDataType() throws UnboundException {
        throw new UnboundException("TableValuedFunction can not compute data type");
    }

    @Override
    public String toSql() {
        String args = getTVFProperties()
                .getMap()
                .entrySet()
                .stream()
                .map(kv -> "'" + kv.getKey() + "' = '" + kv.getValue() + "'")
                .collect(Collectors.joining(", "));
        return getName() + "(" + args + ")";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
