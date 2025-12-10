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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SlotDescriptor.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TSlotDescriptor;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class SlotDescriptor {
    private static final Logger LOG = LogManager.getLogger(SlotDescriptor.class);
    private final SlotId id;
    private final TupleDescriptor parent;
    private Type type;
    private Column column;  // underlying column, if there is one

    // used in explain verbose, caption is column name or alias name
    private String caption;

    // for SlotRef.toSql() in the absence of a path
    private String label;
    // for variant column's sub column lables
    private List<String> subColPath;
    // materializedColumnName is the target name of a slot
    // it could be either column name or a composed name for a variant
    // subcolumn like `a.b.c`
    private String materializedColumnName;

    // Expr(s) materialized into this slot; multiple exprs for unions. Should be empty if
    // path_ is set.
    private List<Expr> sourceExprs = Lists.newArrayList();

    // if false, this slot cannot be NULL
    private boolean isNullable;

    private boolean isAutoInc = false;
    private Expr virtualColumn = null;
    private List<TColumnAccessPath> allAccessPaths;
    private List<TColumnAccessPath> predicateAccessPaths;
    private List<TColumnAccessPath> displayAllAccessPaths;
    private List<TColumnAccessPath> displayPredicateAccessPaths;

    public SlotDescriptor(SlotId id, TupleDescriptor parent) {

        this.id = id;
        this.parent = parent;
        this.isNullable = true;
    }

    public SlotDescriptor(SlotId id, TupleDescriptor parent, SlotDescriptor src) {
        this.id = id;
        this.parent = parent;
        this.column = src.column;
        this.isNullable = src.isNullable;
        this.type = src.type;
        this.sourceExprs.add(new SlotRef(src));
    }

    public SlotId getId() {
        return id;
    }

    public void setSubColLables(List<String> subColPath) {
        this.subColPath = subColPath;
    }

    public List<String> getSubColLables() {
        return this.subColPath;
    }

    public List<TColumnAccessPath> getAllAccessPaths() {
        return allAccessPaths;
    }

    public void setAllAccessPaths(List<TColumnAccessPath> allAccessPaths) {
        this.allAccessPaths = allAccessPaths;
    }

    public List<TColumnAccessPath> getPredicateAccessPaths() {
        return predicateAccessPaths;
    }

    public void setPredicateAccessPaths(List<TColumnAccessPath> predicateAccessPaths) {
        this.predicateAccessPaths = predicateAccessPaths;
    }

    public List<TColumnAccessPath> getDisplayAllAccessPaths() {
        return displayAllAccessPaths;
    }

    public void setDisplayAllAccessPaths(List<TColumnAccessPath> displayAllAccessPaths) {
        this.displayAllAccessPaths = displayAllAccessPaths;
    }

    public List<TColumnAccessPath> getDisplayPredicateAccessPaths() {
        return displayPredicateAccessPaths;
    }

    public void setDisplayPredicateAccessPaths(List<TColumnAccessPath> displayPredicateAccessPaths) {
        this.displayPredicateAccessPaths = displayPredicateAccessPaths;
    }

    public TupleDescriptor getParent() {
        return parent;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
        this.type = column.getType();
        this.caption = column.getName();
    }

    public void setSrcColumn(Column column) {
        this.column = column;
    }

    public boolean isAutoInc() {
        return isAutoInc;
    }

    public void setAutoInc(boolean isAutoInc) {
        this.isAutoInc = isAutoInc;
    }

    public boolean getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(boolean value) {
        isNullable = value;
    }

    public void setMaterializedColumnName(String name) {
        this.materializedColumnName = name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setSourceExpr(Expr expr) {
        sourceExprs = Collections.singletonList(expr);
    }

    public List<Expr> getSourceExprs() {
        return sourceExprs;
    }

    public int getUniqueId() {
        if (column == null) {
            return -1;
        }
        return column.getUniqueId();
    }

    public Expr getVirtualColumn() {
        return virtualColumn;
    }

    public void setVirtualColumn(Expr virtualColumn) {
        this.virtualColumn = virtualColumn;
    }

    public TSlotDescriptor toThrift() {
        // Non-nullable slots will have 0 for the byte offset and -1 for the bit mask
        String colName = materializedColumnName != null ? materializedColumnName :
                                     ((column != null) ? column.getNonShadowName() : "");
        TSlotDescriptor tSlotDescriptor = new TSlotDescriptor(id.asInt(), parent.getId().asInt(), type.toThrift(), -1,
                0, 0, getIsNullable() ? 0 : -1, colName, -1,
                true);
        tSlotDescriptor.setIsAutoIncrement(isAutoInc);
        if (column != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("column name:{}, column unique id:{}", column.getNonShadowName(), column.getUniqueId());
            }
            tSlotDescriptor.setColUniqueId(column.getUniqueId());
            tSlotDescriptor.setPrimitiveType(column.getDataType().toThrift());
            tSlotDescriptor.setIsKey(column.isKey());
            tSlotDescriptor.setColDefaultValue(column.getDefaultValue());
        }
        if (subColPath != null) {
            tSlotDescriptor.setColumnPaths(subColPath);
        }
        if (virtualColumn != null) {
            tSlotDescriptor.setVirtualColumnExpr(virtualColumn.treeToThrift());
        }
        if (allAccessPaths != null) {
            tSlotDescriptor.setAllAccessPaths(allAccessPaths);
        }
        if (predicateAccessPaths != null) {
            tSlotDescriptor.setPredicateAccessPaths(predicateAccessPaths);
        }
        return tSlotDescriptor;
    }

    private String normalizeCaption(String caption) {
        int maxLength = 15;
        if (caption == null || caption.length() <= maxLength) {
            return caption;
        }

        String normalized = caption.replaceAll("\\s+", " ");

        if (normalized.length() <= maxLength) {
            return normalized;
        }

        int lastHashIndex = normalized.lastIndexOf('#');

        if (lastHashIndex == -1) {
            return normalized.substring(0, maxLength);
        }

        String suffixWithHash = normalized.substring(lastHashIndex);
        int prefixLength = maxLength - suffixWithHash.length();

        if (prefixLength <= 0) {
            return suffixWithHash;
        }

        return normalized.substring(0, prefixLength) + suffixWithHash;
    }

    public void setCaptionAndNormalize(String caption) {
        this.caption = normalizeCaption(caption);
    }

    public String debugString() {
        String typeStr = (type == null ? "null" : type.toString());
        String parentTupleId = (parent == null) ? "null" : parent.getId().toString();
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("parent", parentTupleId).add("col", caption)
                .add("type", typeStr).add("nullable", getIsNullable())
                .add("isAutoIncrement", isAutoInc).add("subColPath", subColPath)
                .add("virtualColumn", virtualColumn == null ? null : virtualColumn.toSql()).toString();
    }

    @Override
    public String toString() {
        return debugString();
    }

    public String getExplainString(String prefix) {
        return new StringBuilder()
                .append(prefix).append("SlotDescriptor{")
                .append("id=").append(id)
                .append(", col=").append(caption)
                .append(", colUniqueId=").append(column == null ? "null" : column.getUniqueId())
                .append(", type=").append(type == null ? "null" : type.toSql())
                .append(", nullable=").append(isNullable)
                .append(", isAutoIncrement=").append(isAutoInc)
                .append(", subColPath=").append(subColPath)
                .append(", virtualColumn=").append(virtualColumn == null ? null : virtualColumn.toSql())
                .append("}")
                .toString();
    }

    public boolean isScanSlot() {
        return parent.getTable() instanceof OlapTable;
    }

}
