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
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;

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
    private String baseColumnName;

    public MVColumnItem(String name, Type type, AggregateType aggregateType, boolean isAggregationTypeImplicit,
            Expr defineExpr, String baseColumnName) {
        this.name = name;
        this.type = type;
        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
        this.defineExpr = defineExpr;
        this.baseColumnName = baseColumnName;
    }

    public MVColumnItem(String name, Type type) {
        this.name = name;
        this.type = type;
        this.baseColumnName = name;
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

    public String getBaseColumnName() {
        return baseColumnName;
    }

    public Column toMVColumn(OlapTable olapTable) throws DdlException {
        Column baseColumn = olapTable.getBaseColumn(name);
        if (baseColumn == null) {
            Preconditions.checkNotNull(defineExpr != null);
            Column result = new Column(name, type, isKey, aggregationType, ColumnDef.DefaultValue.ZERO, "");
            result.setDefineExpr(defineExpr);
            return result;
        } else {
            Column result = new Column(baseColumn);
            result.setIsKey(isKey);
            // If the mv column type is inconsistent with the base column type, the daily test will core.
            // So, I comment this line firstly.
//            result.setType(type);
            result.setAggregationType(aggregationType, isAggregationTypeImplicit);
            return result;
        }
    }
}
