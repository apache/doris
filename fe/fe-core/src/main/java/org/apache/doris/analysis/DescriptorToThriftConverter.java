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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TMetaAccessPath;
import org.apache.doris.thrift.TSlotDescriptor;
import org.apache.doris.thrift.TTupleDescriptor;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts {@link SlotDescriptor}, {@link TupleDescriptor}, and {@link DescriptorTable}
 * to their Thrift representations.
 */
public final class DescriptorToThriftConverter {
    private static final Logger LOG = LogManager.getLogger(DescriptorToThriftConverter.class);

    private DescriptorToThriftConverter() {
    }

    /**
     * Converts a {@link SlotDescriptor} to its Thrift representation.
     */
    public static TSlotDescriptor toThrift(SlotDescriptor slotDesc) {
        String materializedColumnName = slotDesc.getMaterializedColumnName();
        Column column = slotDesc.getColumn();
        String colName = materializedColumnName != null ? materializedColumnName :
                                     ((column != null) ? column.getNonShadowName() : "");
        TSlotDescriptor tSlotDescriptor = new TSlotDescriptor(slotDesc.getId().asInt(),
                slotDesc.getParentId().asInt(), slotDesc.getType().toThrift(), -1,
                0, 0, slotDesc.getIsNullable() ? 0 : -1, colName, -1,
                true);
        tSlotDescriptor.setIsAutoIncrement(slotDesc.isAutoInc());
        if (column != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("column name:{}, column unique id:{}", column.getNonShadowName(), column.getUniqueId());
            }
            tSlotDescriptor.setColUniqueId(column.getUniqueId());
            tSlotDescriptor.setPrimitiveType(column.getDataType().toThrift());
            tSlotDescriptor.setIsKey(column.isKey());
            tSlotDescriptor.setColDefaultValue(column.getDefaultValue());
        }
        if (slotDesc.getSubColLables() != null) {
            tSlotDescriptor.setColumnPaths(slotDesc.getSubColLables());
        }
        if (slotDesc.getVirtualColumn() != null) {
            tSlotDescriptor.setVirtualColumnExpr(ExprToThriftVisitor.treeToThrift(slotDesc.getVirtualColumn()));
        }
        if (slotDesc.getAllAccessPaths() != null) {
            tSlotDescriptor.setAllAccessPaths(toThrift(slotDesc.getAllAccessPaths()));
        }
        if (slotDesc.getPredicateAccessPaths() != null) {
            tSlotDescriptor.setPredicateAccessPaths(toThrift(slotDesc.getPredicateAccessPaths()));
        }
        return tSlotDescriptor;
    }

    /**
     * Converts a {@link ColumnAccessPath} to its Thrift representation.
     */
    public static TColumnAccessPath toThrift(ColumnAccessPath accessPath) {
        TColumnAccessPath result = new TColumnAccessPath(
                accessPath.getType() == ColumnAccessPathType.DATA ? TAccessPathType.DATA : TAccessPathType.META);
        if (accessPath.getType() == ColumnAccessPathType.DATA) {
            result.setDataAccessPath(new TDataAccessPath(accessPath.getPath()));
        } else {
            result.setMetaAccessPath(new TMetaAccessPath(accessPath.getPath()));
        }
        return result;
    }

    /**
     * Converts a list of {@link ColumnAccessPath} to Thrift representations.
     */
    public static List<TColumnAccessPath> toThrift(List<ColumnAccessPath> accessPaths) {
        List<TColumnAccessPath> result = new ArrayList<>(accessPaths.size());
        for (ColumnAccessPath accessPath : accessPaths) {
            result.add(toThrift(accessPath));
        }
        return result;
    }

    /**
     * Converts a {@link TupleDescriptor} to its Thrift representation.
     */
    public static TTupleDescriptor toThrift(TupleDescriptor tupleDesc) {
        TTupleDescriptor tTupleDesc = new TTupleDescriptor(tupleDesc.getId().asInt(), 0, 0);
        if (tupleDesc.getTable() != null && tupleDesc.getTable().getId() >= 0) {
            tTupleDesc.setTableId((int) tupleDesc.getTable().getId());
        }
        return tTupleDesc;
    }

    /**
     * Converts a {@link DescriptorTable} to its Thrift representation.
     */
    public static TDescriptorTable toThrift(DescriptorTable descTable) {
        TDescriptorTable result = new TDescriptorTable();
        Map<Long, TableIf> referencedTbls = Maps.newHashMap();
        for (TupleDescriptor tupleD : descTable.getTupleDescs()) {
            result.addToTupleDescriptors(toThrift(tupleD));
            if (tupleD.getTable() != null && tupleD.getTable().getId() >= 0) {
                referencedTbls.put(tupleD.getTable().getId(), tupleD.getTable());
            }
            for (SlotDescriptor slotD : tupleD.getSlots()) {
                result.addToSlotDescriptors(toThrift(slotD));
            }
        }

        for (TableIf tbl : referencedTbls.values()) {
            result.addToTableDescriptors(tbl.toThrift());
        }
        return result;
    }
}
