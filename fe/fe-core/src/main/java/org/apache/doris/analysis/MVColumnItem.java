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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a result of semantic analysis for AddMaterializedViewClause.
 * It is used to construct real mv column in MaterializedViewHandler.
 * It does not include all of column properties.
 * It just a intermediate variable between semantic analysis and final handler.
 */
public class MVColumnItem {
    private String name;
    // the origin type of slot ref
    private Type type;
    private boolean isKey;
    private AggregateType aggregationType;
    private boolean isAggregationTypeImplicit;
    private Expr defineExpr;
    private Set<String> baseColumnNames;
    private String baseTableName;

    public MVColumnItem(String name, Type type, AggregateType aggregateType, boolean isAggregationTypeImplicit,
            Expr defineExpr, String baseColumnName) {
        this(name, type, aggregateType, isAggregationTypeImplicit, defineExpr);
    }

    public MVColumnItem(Type type, AggregateType aggregateType, Expr defineExpr, String baseColumnName) {
        this(CreateMaterializedViewStmt.mvColumnBuilder(aggregateType, baseColumnName), type, aggregateType, false,
                defineExpr);
    }

    public MVColumnItem(String name, Type type, AggregateType aggregateType, boolean isAggregationTypeImplicit,
            Expr defineExpr) {
        this.name = name;
        this.type = type;
        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
        this.defineExpr = defineExpr;
        baseColumnNames = new HashSet<>();

        Map<Long, Set<String>> tableIdToColumnNames = defineExpr.getTableIdToColumnNames();
        if (defineExpr instanceof SlotRef) {
            baseColumnNames = new HashSet<>();
            baseColumnNames.add(this.name);
        } else if (tableIdToColumnNames.size() == 1) {
            for (Map.Entry<Long, Set<String>> entry : tableIdToColumnNames.entrySet()) {
                baseColumnNames = entry.getValue();
            }
        }
    }

    public MVColumnItem(String name, Type type) {
        this.name = name;
        this.type = type;
        baseColumnNames = new HashSet<>();
        baseColumnNames.add(name);
    }

    public MVColumnItem(Expr defineExpr) throws AnalysisException {
        this.name = CreateMaterializedViewStmt.mvColumnBuilder(defineExpr.toSql());

        if (this.name == null) {
            throw new AnalysisException("defineExpr.toSql() is null");
        }

        this.name = MaterializedIndexMeta.normalizeName(this.name);

        this.defineExpr = defineExpr;

        this.type = defineExpr.getType();
        if (this.type instanceof ScalarType && this.type.isStringType()) {
            this.type = new ScalarType(type.getPrimitiveType());
            ((ScalarType) this.type).setMaxLength();
        }

        Map<Long, Set<String>> tableIdToColumnNames = defineExpr.getTableIdToColumnNames();
        if (defineExpr instanceof SlotRef) {
            baseColumnNames = new HashSet<>();
            baseColumnNames.add(this.name);
        } else if (tableIdToColumnNames.size() == 1) {
            for (Map.Entry<Long, Set<String>> entry : tableIdToColumnNames.entrySet()) {
                baseColumnNames = entry.getValue();
            }
        }
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setIsKey(boolean key) {
        isKey = key;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setAggregationType(AggregateType aggregationType, boolean isAggregationTypeImplicit) {
        this.aggregationType = aggregationType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public AggregateType getAggregationType() {
        return aggregationType;
    }

    public boolean isAggregationTypeImplicit() {
        return isAggregationTypeImplicit;
    }

    public Expr getDefineExpr() {
        return defineExpr;
    }

    public void setDefineExpr(Expr defineExpr) {
        this.defineExpr = defineExpr;
    }

    public Set<String> getBaseColumnNames() {
        return baseColumnNames;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public Column toMVColumn(Table table) throws DdlException {
        if (table instanceof OlapTable) {
            return toMVColumn((OlapTable) table);
        } else if (table instanceof InlineView) {
            return toMVColumn((InlineView) table);
        } else {
            throw new DdlException("Failed to convert to a materialized view column, column=" + getName() + ".");
        }
    }

    public Column toMVColumn(OlapTable olapTable) throws DdlException {
        Column baseColumn = olapTable.getBaseColumn(name);
        Column result;
        if (baseColumn == null && defineExpr == null) {
            // Some mtmv column have name diffrent with base column
            baseColumn = olapTable.getBaseColumn(baseColumnNames.iterator().next());
        }
        if (baseColumn != null) {
            result = new Column(baseColumn);
            if (result.getType() == null) {
                throw new DdlException("base column's type is null, column=" + result.getName());
            }
            result.setIsKey(isKey);
        } else {
            if (type == null) {
                throw new DdlException("MVColumnItem type is null");
            }
            result = new Column(name, type, isKey, aggregationType, null, "");
            if (defineExpr != null) {
                result.setIsAllowNull(defineExpr.isNullable());
            }
        }
        result.setName(name);
        result.setAggregationType(aggregationType, isAggregationTypeImplicit);
        result.setDefineExpr(defineExpr);
        return result;
    }

    public Column toMVColumn(InlineView view) throws DdlException {
        Column baseColumn = view.getColumn(name);
        if (baseColumn == null) {
            throw new DdlException("Failed to get the column (" + name + ") in a view (" + view + ").");
        }
        return new Column(baseColumn);
    }
}
