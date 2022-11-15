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

package org.apache.doris.load;

import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BrokerTable;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HiveTable;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A broker file group information, one @DataDescription will
 * produce one BrokerFileGroup. After parsed by broker, detailed
 * broker file information will be saved here.
 */
public class BrokerFileGroup implements Writable {
    private static final Logger LOG = LogManager.getLogger(BrokerFileGroup.class);

    private long tableId;
    private String columnSeparator;
    private String lineDelimiter;
    // fileFormat may be null, which means format will be decided by file's suffix
    private String fileFormat;
    private TFileCompressType compressType = TFileCompressType.UNKNOWN;
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
    private List<ImportColumnDesc> columnExprList;
    // this is only for hadoop function check
    private Map<String, Pair<String, List<String>>> columnToHadoopFunction;
    // filter the data from source directly
    private Expr precedingFilterExpr;
    // filter the data which has been mapped and transformed
    private Expr whereExpr;
    private Expr deleteCondition;
    private LoadTask.MergeType mergeType;
    // sequence column name
    private String sequenceCol;

    // load from table
    private long srcTableId = -1;
    private boolean isLoadFromTable = false;

    // for multi load
    private TNetworkAddress beAddr;
    private long backendID;
    private boolean stripOuterArray = false;
    private String jsonPaths = "";
    private String jsonRoot = "";
    private boolean fuzzyParse = true;
    private boolean readJsonByLine = false;
    private boolean numAsString = false;

    // for unit test and edit log persistence
    private BrokerFileGroup() {
    }

    // Used for broker table, no need to parse
    public BrokerFileGroup(BrokerTable table) throws AnalysisException {
        this.tableId = table.getId();
        this.columnSeparator = Separator.convertSeparator(table.getColumnSeparator());
        this.lineDelimiter = Separator.convertSeparator(table.getLineDelimiter());
        this.isNegative = false;
        this.filePaths = table.getPaths();
        this.fileFormat = table.getFileFormat();
    }

    /**
     * Should used for hive/iceberg/hudi external table.
     */
    public BrokerFileGroup(long tableId,
                           String filePath,
                           String fileFormat) throws AnalysisException {
        this(tableId,  "|", "\n", filePath, fileFormat, null, null);
    }

    /**
     * Should used for hive/iceberg/hudi external table.
     */
    public BrokerFileGroup(long tableId, String columnSeparator, String lineDelimiter, String filePath,
            String fileFormat, List<String> columnNamesFromPath, List<ImportColumnDesc> columnExprList)
            throws AnalysisException {
        this.tableId = tableId;
        this.columnSeparator = Separator.convertSeparator(columnSeparator);
        this.lineDelimiter = Separator.convertSeparator(lineDelimiter);
        this.isNegative = false;
        this.filePaths = Lists.newArrayList(filePath);
        this.fileFormat = fileFormat;
        this.columnNamesFromPath = columnNamesFromPath;
        this.columnExprList = columnExprList;
    }

    public BrokerFileGroup(DataDescription dataDescription) {
        this.fileFieldNames = dataDescription.getFileFieldNames();
        this.columnNamesFromPath = dataDescription.getColumnsFromPath();
        this.columnExprList = dataDescription.getParsedColumnExprList();
        this.columnToHadoopFunction = dataDescription.getColumnToHadoopFunction();
        this.precedingFilterExpr = dataDescription.getPrecdingFilterExpr();
        this.whereExpr = dataDescription.getWhereExpr();
        this.deleteCondition = dataDescription.getDeleteCondition();
        this.mergeType = dataDescription.getMergeType();
        this.sequenceCol = dataDescription.getSequenceCol();
    }

    // NOTE: DBLock will be held
    // This will parse the input DataDescription to list for BrokerFileInfo
    public void parse(Database db, DataDescription dataDescription) throws DdlException {
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
                    partitionIds.add(partition.getId());
                }
            }

            if (olapTable.getState() == OlapTableState.RESTORE) {
                throw new DdlException("Table [" + olapTable.getName() + "] is under restore");
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

        // column
        columnSeparator = dataDescription.getColumnSeparator();
        if (columnSeparator == null) {
            columnSeparator = "\t";
        }
        lineDelimiter = dataDescription.getLineDelimiter();
        if (lineDelimiter == null) {
            lineDelimiter = "\n";
        }

        fileFormat = dataDescription.getFileFormat();
        if (fileFormat != null) {
            if (!fileFormat.equalsIgnoreCase("parquet") && !fileFormat.equalsIgnoreCase(FeConstants.csv)
                    && !fileFormat.equalsIgnoreCase("orc")
                    && !fileFormat.equalsIgnoreCase("json")
                    && !fileFormat.equalsIgnoreCase(FeConstants.csv_with_names)
                    && !fileFormat.equalsIgnoreCase(FeConstants.csv_with_names_and_types)) {
                throw new DdlException("File Format Type " + fileFormat + " is invalid.");
            }
        }
        compressType = dataDescription.getCompressType();
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
        beAddr = dataDescription.getBeAddr();
        backendID = dataDescription.getBackendId();
        if (fileFormat != null && fileFormat.equalsIgnoreCase("json")) {
            stripOuterArray = dataDescription.isStripOuterArray();
            jsonPaths = dataDescription.getJsonPaths();
            jsonRoot = dataDescription.getJsonRoot();
            fuzzyParse = dataDescription.isFuzzyParse();
            // ATTN: for broker load, we only support reading json format data line by line,
            // so if this is set to false, it must be stream load.
            readJsonByLine = dataDescription.isReadJsonByLine();
            numAsString = dataDescription.isNumAsString();
        }
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public TFileCompressType getCompressType() {
        return compressType;
    }

    public boolean isNegative() {
        return isNegative;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public Expr getPrecedingFilterExpr() {
        return precedingFilterExpr;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public List<String> getColumnNamesFromPath() {
        return columnNamesFromPath;
    }

    public List<ImportColumnDesc> getColumnExprList() {
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

    public Expr getDeleteCondition() {
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

    public TNetworkAddress getBeAddr() {
        return beAddr;
    }

    public long getBackendID() {
        return backendID;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public void setStripOuterArray(boolean stripOuterArray) {
        this.stripOuterArray = stripOuterArray;
    }

    public boolean isFuzzyParse() {
        return fuzzyParse;
    }

    public void setFuzzyParse(boolean fuzzyParse) {
        this.fuzzyParse = fuzzyParse;
    }

    public boolean isReadJsonByLine() {
        return readJsonByLine;
    }

    public void setReadJsonByLine(boolean readJsonByLine) {
        this.readJsonByLine = readJsonByLine;
    }

    public boolean isNumAsString() {
        return numAsString;
    }

    public void setNumAsString(boolean numAsString) {
        this.numAsString = numAsString;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPaths(String jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public void setJsonRoot(String jsonRoot) {
        this.jsonRoot = jsonRoot;
    }

    public boolean isBinaryFileFormat() {
        if (fileFormat == null) {
            // null means default: csv
            return false;
        }
        return fileFormat.equalsIgnoreCase("parquet") || fileFormat.equalsIgnoreCase("orc");
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

    @Deprecated
    @Override
    public void write(DataOutput out) throws IOException {
        // tableId
        out.writeLong(tableId);
        // valueSeparator
        Text.writeString(out, columnSeparator);
        // lineDelimiter
        Text.writeString(out, lineDelimiter);
        // isNegative
        out.writeBoolean(isNegative);
        // partitionIds
        if (partitionIds == null) {
            out.writeInt(0);
        } else {
            out.writeInt(partitionIds.size());
            for (long id : partitionIds) {
                out.writeLong(id);
            }
        }
        // fileFieldNames
        if (fileFieldNames == null) {
            out.writeInt(0);
        } else {
            out.writeInt(fileFieldNames.size());
            for (String name : fileFieldNames) {
                Text.writeString(out, name);
            }
        }
        // filePaths
        out.writeInt(filePaths.size());
        for (String path : filePaths) {
            Text.writeString(out, path);
        }
        // expr column map will be null after broker load supports function
        out.writeInt(0);

        // fileFormat
        if (fileFormat == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, fileFormat);
        }

        // src table
        out.writeLong(srcTableId);
        out.writeBoolean(isLoadFromTable);
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        tableId = in.readLong();
        columnSeparator = Text.readString(in);
        lineDelimiter = Text.readString(in);
        isNegative = in.readBoolean();
        // partitionIds
        { // CHECKSTYLE IGNORE THIS LINE
            int partSize = in.readInt();
            if (partSize > 0) {
                partitionIds = Lists.newArrayList();
                for (int i = 0; i < partSize; ++i) {
                    partitionIds.add(in.readLong());
                }
            }
        } // CHECKSTYLE IGNORE THIS LINE
        // fileFieldName
        { // CHECKSTYLE IGNORE THIS LINE
            int fileFieldNameSize = in.readInt();
            if (fileFieldNameSize > 0) {
                fileFieldNames = Lists.newArrayList();
                for (int i = 0; i < fileFieldNameSize; ++i) {
                    fileFieldNames.add(Text.readString(in));
                }
            }
        } // CHECKSTYLE IGNORE THIS LINE
        // fileInfos
        { // CHECKSTYLE IGNORE THIS LINE
            int size = in.readInt();
            filePaths = Lists.newArrayList();
            for (int i = 0; i < size; ++i) {
                filePaths.add(Text.readString(in));
            }
        } // CHECKSTYLE IGNORE THIS LINE
        // expr column map
        Map<String, Expr> exprColumnMap = Maps.newHashMap();
        {  // CHECKSTYLE IGNORE THIS LINE
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                final String name = Text.readString(in);
                exprColumnMap.put(name, Expr.readIn(in));
            }
        } // CHECKSTYLE IGNORE THIS LINE
        // file format
        if (in.readBoolean()) {
            fileFormat = Text.readString(in);
        }
        srcTableId = in.readLong();
        isLoadFromTable = in.readBoolean();

        // There are no columnExprList in the previous load job which is created before function is supported.
        // The columnExprList could not be analyzed without origin stmt in the previous load job.
        // So, the columnExprList need to be merged in here.
        if (fileFieldNames == null || fileFieldNames.isEmpty()) {
            return;
        }
        // Order of columnExprList: fileFieldNames + columnsFromPath
        columnExprList = Lists.newArrayList();
        for (String columnName : fileFieldNames) {
            columnExprList.add(new ImportColumnDesc(columnName, null));
        }
        if (exprColumnMap == null || exprColumnMap.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Expr> columnExpr : exprColumnMap.entrySet()) {
            columnExprList.add(new ImportColumnDesc(columnExpr.getKey(), columnExpr.getValue()));
        }
    }

    @Deprecated
    public static BrokerFileGroup read(DataInput in) throws IOException {
        BrokerFileGroup fileGroup = new BrokerFileGroup();
        fileGroup.readFields(in);
        return fileGroup;
    }
}
