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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is used for {@link org.apache.doris.catalog.MaterializedView}.
 * When we create a materialized view for multiple tables, we can specify the partition of the view
 * which is as consistent as one of base tables.
 */
public class ColumnPartitionDesc extends PartitionDesc {
    private final List<SlotRef> columns;

    public ColumnPartitionDesc(List<SlotRef> columns)
            throws AnalysisException {
        this.columns = columns;
    }

    public void analyze(Analyzer analyzer, CreateMultiTableMaterializedViewStmt stmt) throws AnalysisException {
        for (SlotRef column : columns) {
            column.analyze(analyzer);
        }
        OlapTable olapTable = matchTable(stmt.getOlapTables());
        PartitionDesc partitionDesc = olapTable.getPartitionInfo().toPartitionDesc(olapTable);
        type = partitionDesc.getType();
        partitionColNames = toMVPartitionColumnNames(olapTable.getName(), partitionDesc.getPartitionColNames(),
                stmt.getQueryStmt());
        singlePartitionDescs = partitionDesc.getSinglePartitionDescs();
    }

    private OlapTable matchTable(Map<String, OlapTable> olapTables) throws AnalysisException {
        OlapTable matched = null;
        for (SlotRef column : columns) {
            OlapTable olapTable = olapTables.get(column.getTableName().getTbl());
            if (olapTable != null) {
                if (matched != null && !matched.getName().equals(olapTable.getName())) {
                    throw new AnalysisException("The partition columns must be in the same table.");
                } else if (matched == null) {
                    matched = olapTable;
                }
            }
        }
        PartitionInfo partitionInfo = matched.getPartitionInfo();
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        if (!columns.stream().map(SlotRef::getColumn).collect(Collectors.toList()).equals(partitionColumns)) {
            throw new AnalysisException("The partition columns doesn't match the ones in base table "
                    + matched.getName() + ".");
        }
        return matched;
    }

    private List<String> toMVPartitionColumnNames(String tableName, List<String> partitionColNames, QueryStmt queryStmt)
            throws AnalysisException {
        List<String> mvPartitionColumnNames = Lists.newArrayListWithCapacity(partitionColNames.size());
        if (queryStmt instanceof SelectStmt) {
            List<SelectListItem> items = ((SelectStmt) queryStmt).getSelectList().getItems();
            for (String partitionColName : partitionColNames) {
                String mvColumnName = null;
                for (int i = 0; mvColumnName == null && i < items.size(); ++ i) {
                    SelectListItem item = items.get(i);
                    if (item.isStar()) {
                        mvColumnName = partitionColName;
                    } else if (item.getExpr() instanceof SlotRef) {
                        SlotRef slotRef = (SlotRef) item.getExpr();
                        if (slotRef.getTableName().getTbl().equals(tableName) && slotRef.getColumnName()
                                .equals(partitionColName)) {
                            mvColumnName = item.getAlias() == null ? partitionColName : item.getAlias();
                        }
                    }
                }
                if (mvColumnName != null) {
                    mvPartitionColumnNames.add(mvColumnName);
                } else {
                    throw new AnalysisException(
                            "Failed to map the partition column name " + partitionColName + " to mv column");
                }
            }
        } else {
            throw new AnalysisException("Only select statement is supported.");
        }
        return mvPartitionColumnNames;
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException, AnalysisException {
        switch (type) {
            case RANGE:
                return new RangePartitionDesc(partitionColNames,
                        singlePartitionDescs.stream().map(desc -> (AllPartitionDesc) desc)
                                .collect(Collectors.toList())).toPartitionInfo(schema, partitionNameToId, isTemp);
            case LIST:
                return new ListPartitionDesc(partitionColNames,
                        singlePartitionDescs.stream().map(desc -> (AllPartitionDesc) desc)
                                .collect(Collectors.toList())).toPartitionInfo(schema, partitionNameToId, isTemp);
            default:
                throw new RuntimeException("Invalid partition type.");
        }
    }
}
