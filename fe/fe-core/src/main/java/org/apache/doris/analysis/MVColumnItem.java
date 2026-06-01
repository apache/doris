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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This is a result of semantic analysis for AddMaterializedViewClause.
 * It is used to construct real mv column in MaterializedViewHandler.
 * It does not include all of column properties.
 * It just a intermediate variable between semantic analysis and final handler.
 */
public class MVColumnItem {
    private final String name;
    // the origin type of slot ref
    private Type type;
    private boolean isKey;
    private AggregateType aggregationType;
    private boolean isAggregationTypeImplicit;
    private final Expr defineExpr;
    private final Set<String> baseColumnNames;

    public MVColumnItem(String name, Type type, AggregateType aggregateType, Expr defineExpr) {
        this.name = name;
        this.type = type;
        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = false;
        this.defineExpr = Objects.requireNonNull(defineExpr, "defineExpr is null");
        baseColumnNames = extractBaseColumnNames(defineExpr);
    }

    public MVColumnItem(Expr defineExpr) throws AnalysisException {
        this(defineExpr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE), defineExpr);
    }

    public MVColumnItem(String name, Expr defineExpr) {
        this.name = name;
        this.isAggregationTypeImplicit = false;
        this.defineExpr = Objects.requireNonNull(defineExpr, "defineExpr is null");

        this.type = defineExpr.getType();
        if (this.type instanceof ScalarType && this.type.isStringType()) {
            this.type = new ScalarType(type.getPrimitiveType());
            ((ScalarType) this.type).setMaxLength();
        }
        baseColumnNames = extractBaseColumnNames(defineExpr);
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

    public Expr getDefineExpr() {
        return defineExpr;
    }

    public Set<String> getBaseColumnNames() {
        return baseColumnNames;
    }

    public Column toMVColumn(Map<String, String> sessionVars) throws DdlException {
        Column result;
        if (type == null) {
            throw new DdlException("MVColumnItem type is null");
        }
        result = new Column(name, type, isKey, aggregationType, null, "");
        result.setIsAllowNull(defineExpr.isNullable());
        result.setName(name);
        result.setAggregationType(aggregationType, isAggregationTypeImplicit);
        result.setDefineExpr(defineExpr);
        result.setSessionVariables(sessionVars);
        return result;
    }

    private Set<String> extractBaseColumnNames(Expr defineExpr) {
        Set<String> result = new HashSet<>();
        Set<SlotRef> slotRefs = new HashSet<>();
        defineExpr.collect(SlotRef.class, slotRefs);
        for (SlotRef slotRef : slotRefs) {
            if (slotRef.getCol() != null) {
                result.add(slotRef.getCol());
            }
        }
        return result;
    }
}
