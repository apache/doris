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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.DataType;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * Reference to slot in expression.
 */
public class SlotReference extends Slot {
    private final ExprId exprId;
    private final String name;
    private final List<String> qualifier;
    private final DataType dataType;
    private final boolean nullable;

    public SlotReference(String name, DataType dataType, boolean nullable, List<String> qualifier) {
        this(NamedExpression.newExprId(), name, dataType, nullable, qualifier);
    }

    /**
     * Constructor for SlotReference.
     *
     * @param exprId UUID for this slot reference
     * @param name slot reference name
     * @param dataType slot reference logical data type
     * @param nullable true if nullable
     * @param qualifier slot reference qualifier
     */
    public SlotReference(ExprId exprId, String name, DataType dataType, boolean nullable, List<String> qualifier) {
        super(NodeType.SLOT_REFERENCE);
        this.exprId = exprId;
        this.name = name;
        this.dataType = dataType;
        this.qualifier = qualifier;
        this.nullable = nullable;
    }

    public static SlotReference fromColumn(Column column, List<String> qualifier) {
        DataType dataType = DataType.convertFromCatalogDataType(column.getType());
        return new SlotReference(column.getName(), dataType, column.isAllowNull(), qualifier);
    }

    @Override
    public String getName() {
        return name;
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
    public DataType getDataType() throws UnboundException {
        return dataType;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return nullable;
    }

    @Override
    public String sql() {
        return null;
    }

    @Override
    public String toString() {
        String uniqueName = name + "#" + exprId;
        if (qualifier.isEmpty()) {
            return uniqueName;
        } else {
            return StringUtils.join(qualifier, ".") + "." + uniqueName;
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
        return nullable == that.nullable
                && exprId.equals(that.exprId)
                && name.equals(that.name)
                && qualifier.equals(that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exprId, name, qualifier, nullable);
    }
}
