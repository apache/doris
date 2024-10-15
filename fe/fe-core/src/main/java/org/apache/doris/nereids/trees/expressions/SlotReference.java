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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Reference to slot in expression.
 */
public class SlotReference extends Slot {
    protected final ExprId exprId;
    protected final Supplier<String> name;
    protected final DataType dataType;
    protected final boolean nullable;
    protected final List<String> qualifier;

    // The sub column path to access type like struct or variant
    // e.g. For accessing variant["a"]["b"], the parsed paths is ["a", "b"]
    protected final List<String> subPath;

    // the unique string representation of a SlotReference
    // different SlotReference will have different internalName
    // TODO: remove this member variable after mv selection is refactored
    protected final Supplier<Optional<String>> internalName;

    private final TableIf table;
    private final Column column;

    public SlotReference(String name, DataType dataType) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, true, ImmutableList.of(),
                null, null, Optional.empty(), ImmutableList.of());
    }

    public SlotReference(String name, DataType dataType, boolean nullable) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, nullable, ImmutableList.of(),
                null, null, Optional.empty(), ImmutableList.of());
    }

    public SlotReference(String name, DataType dataType, boolean nullable, List<String> qualifier) {
        this(StatementScopeIdGenerator.newExprId(), name, dataType, nullable,
                qualifier, null, null, Optional.empty(), ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable, List<String> qualifier) {
        this(exprId, name, dataType, nullable, qualifier, null, null, Optional.empty(), ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable,
            List<String> qualifier, @Nullable TableIf table, @Nullable Column column) {
        this(exprId, name, dataType, nullable, qualifier, table, column, Optional.empty(), ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable,
            List<String> qualifier, @Nullable TableIf table, @Nullable Column column, Optional<String> internalName) {
        this(exprId, name, dataType, nullable, qualifier, table, column, internalName, ImmutableList.of());
    }

    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable,
            List<String> qualifier, @Nullable TableIf table, @Nullable Column column,
            Optional<String> internalName, List<String> subColLabels) {
        this(exprId, () -> name, dataType, nullable, qualifier, table, column,
                buildInternalName(() -> name, subColLabels, internalName), subColLabels, Optional.empty());
    }

    /**
     * Constructor for SlotReference.
     *
     * @param exprId UUID for this slot reference
     * @param name slot reference name
     * @param dataType slot reference logical data type
     * @param nullable true if nullable
     * @param qualifier slot reference qualifier
     * @param column the column which this slot come from
     * @param internalName the internalName of this slot
     * @param subPath subColumn access labels
     */
    public SlotReference(ExprId exprId, Supplier<String> name, DataType dataType, boolean nullable,
            List<String> qualifier, @Nullable TableIf table, @Nullable Column column,
            Supplier<Optional<String>> internalName, List<String> subPath,
            Optional<Pair<Integer, Integer>> indexInSql) {
        super(indexInSql);
        this.exprId = exprId;
        this.name = name;
        this.dataType = dataType;
        this.qualifier = Utils.fastToImmutableList(
                Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.nullable = nullable;
        this.table = table;
        this.column = column;
        this.subPath = Objects.requireNonNull(subPath, "subPath can not be null");
        this.internalName = internalName;
    }

    public static SlotReference of(String name, DataType type) {
        return new SlotReference(name, type);
    }

    /**
     * get SlotReference from a column
     * @param column the column which contains type info
     * @param qualifier the qualifier of SlotReference
     */
    public static SlotReference fromColumn(TableIf table, Column column, List<String> qualifier) {
        DataType dataType = DataType.fromCatalogType(column.getType());
        return new SlotReference(StatementScopeIdGenerator.newExprId(), column::getName, dataType,
                column.isAllowNull(), qualifier, table, column,
                () -> Optional.of(column.getName()), ImmutableList.of(), Optional.empty());
    }

    public static SlotReference fromColumn(TableIf table, Column column, String name, List<String> qualifier) {
        DataType dataType = DataType.fromCatalogType(column.getType());
        return new SlotReference(StatementScopeIdGenerator.newExprId(), name, dataType,
            column.isAllowNull(), qualifier, table, column, Optional.empty(), ImmutableList.of());
    }

    @Override
    public String getName() {
        return name.get();
    }

    @Override
    public ExprId getExprId() {
        return exprId;
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    @Override
    public DataType getDataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return nullable;
    }

    @Override
    public String getInternalName() {
        return internalName.get().get();
    }

    public Optional<Column> getColumn() {
        return Optional.ofNullable(column);
    }

    public Optional<TableIf> getTable() {
        return Optional.ofNullable(table);
    }

    @Override
    public String toSql() {
        if (subPath.isEmpty()) {
            return name.get();
        } else {
            return name.get() + "['" + String.join("']['", subPath) + "']";
        }
    }

    @Override
    public String toString() {
        if (subPath.isEmpty()) {
            // Just return name and exprId, add another method to show fully qualified name when it's necessary.
            return name.get() + "#" + exprId;
        }
        return name.get() + "['" + String.join("']['", subPath) + "']" + "#" + exprId;
    }

    @Override
    public String shapeInfo() {
        if (qualifier.isEmpty()) {
            return name.get();
        } else {
            return qualifier.get(qualifier.size() - 1) + "." + name.get();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotReference that = (SlotReference) o;
        // The equals of slotReference only compares exprId,
        // because in subqueries with aliases,
        // there will be scenarios where the same exprId but different qualifiers are used,
        // resulting in an error due to different qualifiers during comparison.
        // eg:
        // select * from t6 where t6.k1 < (select max(aa) from (select v1 as aa from t7 where t6.k2=t7.v2) t2 )
        //
        // For aa, the qualifier of aa in the subquery is empty, but in the output column of agg,
        // the qualifier of aa is t2. but both actually represent the same column.
        return exprId.equals(that.exprId);
    }

    // The contains method needs to use hashCode, so similar to equals, it only compares exprId
    @Override
    public int hashCode() {
        // direct return exprId to speed up
        return exprId.asInt();
    }

    @Override
    public int fastChildrenHashCode() {
        return exprId.asInt();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSlotReference(this, context);
    }

    @Override
    public SlotReference withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.isEmpty());
        return this;
    }

    @Override
    public SlotReference withNullable(boolean nullable) {
        if (this.nullable == nullable) {
            return this;
        }
        return new SlotReference(exprId, name, dataType, nullable,
                qualifier, table, column, internalName, subPath, indexInSqlString);
    }

    @Override
    public Slot withNullableAndDataType(boolean nullable, DataType dataType) {
        if (this.nullable == nullable && this.dataType.equals(dataType)) {
            return this;
        }
        return new SlotReference(exprId, name, dataType, nullable,
                qualifier, table, column, internalName, subPath, indexInSqlString);
    }

    @Override
    public SlotReference withQualifier(List<String> qualifier) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier, table, column, internalName, subPath,
                indexInSqlString);
    }

    @Override
    public SlotReference withName(String name) {
        if (this.name.get().equals(name)) {
            return this;
        }
        return new SlotReference(
                exprId, () -> name, dataType, nullable, qualifier, table, column, internalName, subPath,
                indexInSqlString);
    }

    @Override
    public SlotReference withExprId(ExprId exprId) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier, table, column, internalName, subPath,
                indexInSqlString);
    }

    public SlotReference withSubPath(List<String> subPath) {
        return new SlotReference(exprId, name, dataType, !subPath.isEmpty() || nullable,
                qualifier, table, column, internalName, subPath, indexInSqlString);
    }

    @Override
    public Slot withIndexInSql(Pair<Integer, Integer> index) {
        return new SlotReference(exprId, name, dataType, nullable, qualifier, table, column, internalName, subPath,
                Optional.ofNullable(index));
    }

    public boolean isVisible() {
        return column == null || column.isVisible();
    }

    public List<String> getSubPath() {
        return subPath;
    }

    public boolean hasSubColPath() {
        return !subPath.isEmpty();
    }

    private static Supplier<Optional<String>> buildInternalName(
            Supplier<String> name, List<String> subColLabels, Optional<String> internalName) {
        if (subColLabels != null && !subColLabels.isEmpty()) {
            // Modify internal name to distinguish from different sub-columns of same top level column,
            // using the `.` to connect each part of paths
            return Suppliers.memoize(() ->
                    Optional.of(internalName.orElse(name.get()) + String.join(".", subColLabels)));
        } else {
            return Suppliers.memoize(() ->
                    internalName.isPresent() ? internalName : Optional.of(name.get()));
        }
    }

    public String getQualifiedNameWithBackquote() throws UnboundException {
        return Utils.qualifiedNameWithBackquote(getQualifier(), getName());
    }
}
