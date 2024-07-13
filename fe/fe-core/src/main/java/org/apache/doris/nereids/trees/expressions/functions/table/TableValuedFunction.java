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
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** TableValuedFunction */
public abstract class TableValuedFunction extends BoundFunction
        implements UnaryExpression, CustomSignature, Nondeterministic {

    protected final Supplier<TableValuedFunctionIf> catalogFunctionCache = Suppliers.memoize(this::toCatalogFunction);
    protected final Supplier<FunctionGenTable> tableCache = Suppliers.memoize(() -> {
        try {
            return catalogFunctionCache.get().getTable();
        } catch (AnalysisException e) {
            throw e;
        } catch (Throwable t) {
            // Do not print the whole stmt, it is too long and may contain sensitive information
            throw new AnalysisException(
                    "Can not build FunctionGenTable '" + this.getName() + "'. error: " + t.getMessage(), t);
        }
    });

    public TableValuedFunction(String functionName, Properties tvfProperties) {
        super(functionName, tvfProperties);
    }

    protected abstract TableValuedFunctionIf toCatalogFunction();

    /**
     * For most of tvf, eg, s3/local/hdfs, the column stats is unknown.
     * The derived tvf can override this method to compute the column stats.
     *
     * @param slots the slots of the tvf
     * @return the column stats of the tvf
     */
    public Statistics computeStats(List<Slot> slots) {
        Map<Expression, ColumnStatistic> columnToStatistics = Maps.newHashMap();
        for (Slot slot : slots) {
            columnToStatistics.put(slot, ColumnStatistic.UNKNOWN);
        }
        return new Statistics(0, columnToStatistics);
    }

    public Properties getTVFProperties() {
        return (Properties) child(0);
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

    public final void checkAuth(ConnectContext ctx) {
        getCatalogFunction().checkAuth(ctx);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTableValuedFunction(this, context);
    }

    @Override
    public boolean nullable() {
        throw new UnboundException("TableValuedFunction can not compute nullable");
    }

    public PhysicalProperties getPhysicalProperties() {
        if (SessionVariable.canUseNereidsDistributePlanner()) {
            return PhysicalProperties.ANY;
        }
        return PhysicalProperties.STORAGE_ANY;
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
