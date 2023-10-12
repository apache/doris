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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * relation generated from TableIf
 */
public abstract class PhysicalCatalogRelation extends PhysicalRelation implements CatalogRelation {

    protected final TableIf table;
    protected final ImmutableList<String> qualifier;

    /**
     * Constructor for PhysicalCatalogRelation.
     *
     * @param table Doris table
     * @param qualifier qualified relation name
     */
    public PhysicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties) {
        super(relationId, type, groupExpression, logicalProperties);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
    }

    /**
     * Constructor for PhysicalCatalogRelation.
     *
     * @param table Doris table
     * @param qualifier qualified relation name
     */
    public PhysicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics) {
        super(relationId, type, groupExpression, logicalProperties, physicalProperties, statistics);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
    }

    @Override
    public TableIf getTable() {
        return table;
    }

    @Override
    public DatabaseIf getDatabase() throws AnalysisException {
        Preconditions.checkArgument(!qualifier.isEmpty(), "qualifier can not be empty");
        try {
            CatalogIf catalog = qualifier.size() == 3
                    ? Env.getCurrentEnv().getCatalogMgr().getCatalogOrException(qualifier.get(0),
                        s -> new Exception("Catalog [" + qualifier.get(0) + "] does not exist."))
                    : Env.getCurrentEnv().getCurrentCatalog();
            return catalog.getDbOrException(qualifier.size() == 3 ? qualifier.get(1) : qualifier.get(0),
                    s -> new Exception("Database [" + qualifier.get(1) + "] does not exist in catalog ["
                        + qualifier.get(0) + "]."));
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public List<Slot> computeOutput() {
        return table.getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(col, qualified()))
                .collect(ImmutableList.toImmutableList());
    }

    public List<String> getQualifier() {
        return qualifier;
    }

    /**
     * Full qualified name parts, i.e., concat qualifier and name into a list.
     */
    public List<String> qualified() {
        return Utils.qualifiedNameParts(qualifier, table.getName());
    }

    /**
     * Full qualified table name, concat qualifier and name with `.` as separator.
     */
    public String qualifiedName() {
        return Utils.qualifiedName(qualifier, table.getName());
    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return true;
    }
}
