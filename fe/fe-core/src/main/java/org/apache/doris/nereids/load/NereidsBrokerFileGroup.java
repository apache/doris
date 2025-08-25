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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.OrcFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.ParquetFileFormatProperties;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A broker file group information for Nereids, one @NereidsDataDescription will
 * produce one BrokerFileGroup. After parsed by broker, detailed
 * broker file information will be saved here.
 */
public class NereidsBrokerFileGroup {
    private long tableId;
    private String columnSeparator;
    private String lineDelimiter;
    // fileFormat may be null, which means format will be decided by file's suffix
    private String fileFormat;
    private boolean isNegative;
    private List<Long> partitionIds; // can be null, means no partition specified
    private List<String> filePaths;

    // this only used in multi load, all filePaths is file not dir
    private List<Long> fileSize;

    private List<String> fileFieldNames;
    // partition columnNames
    private List<String> columnNamesFromPath;
    // columnExprList includes all fileFieldNames, columnsFromPath and column mappings
    // this param will be recreated by data desc when the log replay
    private List<NereidsImportColumnDesc> columnExprList;
    // this is only for hadoop function check
    private Map<String, Pair<String, List<String>>> columnToHadoopFunction;
    // filter the data from source directly
    private Expression precedingFilterExpr;
    // filter the data which has been mapped and transformed
    private Expression whereExpr;
    private Expression deleteCondition;
    private LoadTask.MergeType mergeType;
    // sequence column name
    private String sequenceCol;

    // load from table
    private long srcTableId = -1;
    private boolean isLoadFromTable = false;

    private boolean ignoreCsvRedundantCol = false;

    private FileFormatProperties fileFormatProperties;

    // for unit test and edit log persistence
    private NereidsBrokerFileGroup() {
    }

    /**
     * NereidsBrokerFileGroup
     */
    public NereidsBrokerFileGroup(NereidsDataDescription dataDescription) {
        this.fileFieldNames = dataDescription.getFileFieldNames();
        this.columnNamesFromPath = dataDescription.getColumnsFromPath();
        this.columnExprList = dataDescription.getParsedColumnExprList() != null
                ? dataDescription.getParsedColumnExprList()
                : new ArrayList<>();
        this.columnToHadoopFunction = dataDescription.getColumnToHadoopFunction();
        this.precedingFilterExpr = dataDescription.getPrecdingFilterExpr();
        this.whereExpr = dataDescription.getWhereExpr();
        this.deleteCondition = dataDescription.getDeleteCondition();
        this.mergeType = dataDescription.getMergeType();
        this.sequenceCol = dataDescription.getSequenceCol();
        this.filePaths = dataDescription.getFilePaths();
        // use for cloud copy into
        this.ignoreCsvRedundantCol = dataDescription.getIgnoreCsvRedundantCol();
    }

    /**
     * NereidsBrokerFileGroup
     */
    public NereidsBrokerFileGroup(long tableId, boolean isNegative,
            List<Long> partitionIds, List<String> filePaths, List<Long> fileSize, List<String> fileFieldNames,
            List<String> columnNamesFromPath, List<NereidsImportColumnDesc> columnExprList,
            Map<String, Pair<String, List<String>>> columnToHadoopFunction,
            Expression precedingFilterExpr, Expression whereExpr, Expression deleteCondition,
            LoadTask.MergeType mergeType, String sequenceCol, long srcTableId,
            boolean isLoadFromTable, boolean ignoreCsvRedundantCol, FileFormatProperties fileFormatProperties) {
        this.tableId = tableId;
        this.isNegative = isNegative;
        this.partitionIds = partitionIds;
        this.filePaths = filePaths;
        this.fileSize = fileSize;
        this.fileFieldNames = fileFieldNames;
        this.columnNamesFromPath = columnNamesFromPath;
        this.columnExprList = columnExprList != null ? columnExprList : new ArrayList<>();
        this.columnToHadoopFunction = columnToHadoopFunction;
        this.precedingFilterExpr = precedingFilterExpr;
        this.whereExpr = whereExpr;
        this.deleteCondition = deleteCondition;
        this.mergeType = mergeType;
        this.sequenceCol = sequenceCol;
        this.srcTableId = srcTableId;
        this.isLoadFromTable = isLoadFromTable;
        this.ignoreCsvRedundantCol = ignoreCsvRedundantCol;
        this.fileFormatProperties = fileFormatProperties;
    }

    /**
     * This will parse the input NereidsDataDescription to list for NereidsBrokerFileGroup
     */
    public void parse(Database db, NereidsDataDescription dataDescription) throws DdlException {
        // tableId
        OlapTable olapTable = db.getOlapTableOrDdlException(dataDescription.getTableName());
        tableId = olapTable.getId();
        olapTable.readLock();
        try {
            // partitionId
            PartitionNames partitionNames = dataDescription.getPartitionNames();
            if (partitionNames != null) {
                partitionIds = Lists.newArrayList();
                for (String pName : partitionNames.getPartitionNames()) {
                    Partition partition = olapTable.getPartition(pName, partitionNames.isTemp());
                    if (partition == null) {
                        throw new DdlException("Unknown partition '" + pName
                                + "' in table '" + olapTable.getName() + "'");
                    }
                    // partition which need load data
                    if (partition.getState() == Partition.PartitionState.RESTORE) {
                        throw new DdlException("Table [" + olapTable.getName()
                                + "], Partition[" + partition.getName() + "] is under restore");
                    }
                    partitionIds.add(partition.getId());
                }
            }

            // only do check when here's restore on this table now
            if (olapTable.getState() == OlapTable.OlapTableState.RESTORE) {
                boolean hasPartitionRestoring = olapTable.getPartitions().stream()
                        .anyMatch(partition -> partition.getState() == Partition.PartitionState.RESTORE);
                // tbl RESTORE && all partition NOT RESTORE -> whole table restore
                // tbl RESTORE && some partition RESTORE -> just partitions restore, NOT WHOLE TABLE
                // so check wether the whole table restore here
                if (!hasPartitionRestoring) {
                    throw new DdlException("Table [" + olapTable.getName() + "] is under restore");
                }
            }

            if (olapTable.getKeysType() != KeysType.AGG_KEYS && dataDescription.isNegative()) {
                throw new DdlException("Load for AGG_KEYS table should not specify NEGATIVE");
            }

            // check negative for sum aggregate type
            if (dataDescription.isNegative()) {
                for (Column column : olapTable.getBaseSchema()) {
                    if (!column.isKey() && column.getAggregationType() != AggregateType.SUM) {
                        throw new DdlException("Column is not SUM AggregateType. column:" + column.getName());
                    }
                }
            }
        } finally {
            olapTable.readUnlock();
        }

        fileFormatProperties = dataDescription.getFileFormatProperties();
        fileFormat = fileFormatProperties.getFormatName();
        if (fileFormatProperties instanceof CsvFileFormatProperties) {
            columnSeparator = ((CsvFileFormatProperties) fileFormatProperties).getColumnSeparator();
            lineDelimiter = ((CsvFileFormatProperties) fileFormatProperties).getLineDelimiter();
        }

        isNegative = dataDescription.isNegative();

        // FilePath
        filePaths = dataDescription.getFilePaths();
        fileSize = dataDescription.getFileSize();

        if (dataDescription.isLoadFromTable()) {
            String srcTableName = dataDescription.getSrcTableName();
            // src table should be hive table
            Table srcTable = db.getTableOrDdlException(srcTableName);
            if (!(srcTable instanceof HiveTable)) {
                throw new DdlException("Source table " + srcTableName + " is not HiveTable");
            }
            // src table columns should include all columns of loaded table
            for (Column column : olapTable.getBaseSchema()) {
                boolean isIncluded = false;
                for (Column srcColumn : srcTable.getBaseSchema()) {
                    if (srcColumn.getName().equalsIgnoreCase(column.getName())) {
                        isIncluded = true;
                        break;
                    }
                }
                if (!isIncluded) {
                    throw new DdlException("Column " + column.getName() + " is not in Source table");
                }
            }
            srcTableId = srcTable.getId();
            isLoadFromTable = true;
        }
    }

    public long getTableId() {
        return tableId;
    }

    public boolean isNegative() {
        return isNegative;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public Expression getPrecedingFilterExpr() {
        return precedingFilterExpr;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public List<String> getColumnNamesFromPath() {
        return columnNamesFromPath;
    }

    public List<NereidsImportColumnDesc> getColumnExprList() {
        return columnExprList;
    }

    public List<String> getFileFieldNames() {
        return fileFieldNames;
    }

    public Map<String, Pair<String, List<String>>> getColumnToHadoopFunction() {
        return columnToHadoopFunction;
    }

    public long getSrcTableId() {
        return srcTableId;
    }

    public boolean isLoadFromTable() {
        return isLoadFromTable;
    }

    public Expression getDeleteCondition() {
        return deleteCondition;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    public String getSequenceCol() {
        return sequenceCol;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    public List<Long> getFileSize() {
        return fileSize;
    }

    public void setFileSize(List<Long> fileSize) {
        this.fileSize = fileSize;
    }

    public boolean isBinaryFileFormat() {
        return fileFormatProperties instanceof ParquetFileFormatProperties
                || fileFormatProperties instanceof OrcFileFormatProperties;
    }

    public FileFormatProperties getFileFormatProperties() {
        return fileFormatProperties;
    }

    public boolean getIgnoreCsvRedundantCol() {
        return ignoreCsvRedundantCol;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BrokerFileGroup{tableId=").append(tableId);
        if (partitionIds != null) {
            sb.append(",partitionIds=[");
            int idx = 0;
            for (long id : partitionIds) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(id);
            }
            sb.append("]");
        }
        if (columnNamesFromPath != null) {
            sb.append(",columnsFromPath=[");
            int idx = 0;
            for (String name : columnNamesFromPath) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(name);
            }
            sb.append("]");
        }
        if (fileFieldNames != null) {
            sb.append(",fileFieldNames=[");
            int idx = 0;
            for (String name : fileFieldNames) {
                if (idx++ != 0) {
                    sb.append(",");
                }
                sb.append(name);
            }
            sb.append("]");
        }
        sb.append(",valueSeparator=").append(columnSeparator)
                .append(",lineDelimiter=").append(lineDelimiter)
                .append(",fileFormat=").append(fileFormat)
                .append(",isNegative=").append(isNegative);
        sb.append(",fileInfos=[");
        int idx = 0;
        for (String path : filePaths) {
            if (idx++ != 0) {
                sb.append(",");
            }
            sb.append(path);
        }
        sb.append("]");
        sb.append(",srcTableId=").append(srcTableId);
        sb.append(",isLoadFromTable=").append(isLoadFromTable);
        sb.append("}");

        return sb.toString();
    }
}
