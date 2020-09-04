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

package org.apache.doris.task;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.util.List;

public class StreamLoadTask implements LoadTaskInfo {

    private static final Logger LOG = LogManager.getLogger(StreamLoadTask.class);

    private TUniqueId id;
    private long txnId;
    private TFileType fileType;
    private TFileFormatType formatType;
    private boolean stripOuterArray;
    private String jsonPaths;
    private String jsonRoot;

    // optional
    private List<ImportColumnDesc> columnExprDescs = Lists.newArrayList();
    private Expr whereExpr;
    private ColumnSeparator columnSeparator;
    private PartitionNames partitions;
    private String path;
    private boolean negative;
    private boolean strictMode = false; // default is false
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int timeout = Config.stream_load_default_timeout_second;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L; // default is 2GB
    private LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND; // default is all data is load no delete
    private Expr deleteCondition;
    private String sequenceCol;

    public StreamLoadTask(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
    }

    public TUniqueId getId() {
        return id;
    }

    public long getTxnId() {
        return txnId;
    }

    public TFileType getFileType() {
        return fileType;
    }

    public TFileFormatType getFormatType() {
        return formatType;
    }

    public List<ImportColumnDesc> getColumnExprDescs() {
        return columnExprDescs;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public String getPath() {
        return path;
    }

    public boolean getNegative() {
        return negative;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public void setStripOuterArray(boolean stripOuterArray) {
        this.stripOuterArray = stripOuterArray;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPath(String jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public void setJsonRoot(String jsonRoot) {
        this.jsonRoot = jsonRoot;
    }
    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    public Expr getDeleteCondition() {
        return deleteCondition;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    public static StreamLoadTask fromTStreamLoadPutRequest(TStreamLoadPutRequest request, Database db) throws UserException {
        StreamLoadTask streamLoadTask = new StreamLoadTask(request.getLoadId(), request.getTxnId(),
                                                           request.getFileType(), request.getFormatType());
        streamLoadTask.setOptionalFromTSLPutRequest(request, db);
        return streamLoadTask;
    }

    private void setOptionalFromTSLPutRequest(TStreamLoadPutRequest request, Database db) throws UserException {
        if (request.isSetColumns()) {
            setColumnToColumnExpr(request.getColumns());
        }
        if (request.isSetWhere()) {
            whereExpr = parseWhereExpr(request.getWhere());
        }
        if (request.isSetColumnSeparator()) {
            setColumnSeparator(request.getColumnSeparator());
        }
        if (request.isSetPartitions()) {
            String[] partNames = request.getPartitions().trim().split("\\s*,\\s*");
            if (request.isSetIsTempPartition()) {
                partitions = new PartitionNames(request.isIsTempPartition(), Lists.newArrayList(partNames));
            } else {
                partitions = new PartitionNames(false, Lists.newArrayList(partNames));
            }
        }
        switch (request.getFileType()) {
            case FILE_STREAM:
                path = request.getPath();
                break;
            default:
                throw new UserException("unsupported file type, type=" + request.getFileType());
        }
        if (request.isSetNegative()) {
            negative = request.isNegative();
        }
        if (request.isSetTimeout()) {
            timeout = request.getTimeout();
        }
        if (request.isSetStrictMode()) {
            strictMode = request.isStrictMode();
        }
        if (request.isSetTimezone()) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(request.getTimezone());
        }
        if (request.isSetExecMemLimit()) {
            execMemLimit = request.getExecMemLimit();
        }
        if (request.getFormatType() == TFileFormatType.FORMAT_JSON) {
            if (request.getJsonpaths() != null) {
                jsonPaths = request.getJsonpaths();
            }
            if (request.getJsonRoot() != null) {
                jsonRoot = request.getJsonRoot();
            }
            stripOuterArray = request.isStripOuterArray();
        }
        if (request.isSetMergeType()) {
            try {
                mergeType = LoadTask.MergeType.valueOf(request.getMergeType().toString());
            } catch (IllegalArgumentException e) {
                throw new UserException("unknown merge type " + request.getMergeType().toString());
            }
        }
        if (request.isSetDeleteCondition()) {
            deleteCondition = parseWhereExpr(request.getDeleteCondition());
        }
        if (negative && mergeType != LoadTask.MergeType.APPEND) {
            throw new AnalysisException("Negative is only used when merge type is append.");
        }
        if (mergeType == LoadTask.MergeType.MERGE) {
            columnExprDescs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(deleteCondition));
        }  else if (mergeType == LoadTask.MergeType.DELETE) {
            columnExprDescs.add(ImportColumnDesc.newDeleteSignImportColumnDesc(new IntLiteral(1)));
        }
        if (request.isSetSequenceCol()) {
            sequenceCol = request.getSequenceCol();
            // add expr for sequence column
            columnExprDescs.add(new ImportColumnDesc(Column.SEQUENCE_COL, new SlotRef(null, sequenceCol)));
        }
    }

    // used for stream load
    private void setColumnToColumnExpr(String columns) throws UserException {
        String columnsSQL = new String("COLUMNS (" + columns + ")");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(columnsSQL)));
        ImportColumnsStmt columnsStmt;
        try {
            columnsStmt = (ImportColumnsStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            LOG.warn("error happens when parsing columns, sql={}", columnsSQL, e);
            throw new AnalysisException("failed to parsing columns' header, maybe contain unsupported character");
        } catch (AnalysisException e) {
            LOG.warn("analyze columns' statement failed, sql={}, error={}",
                     columnsSQL, parser.getErrorMsg(columnsSQL), e);
            String errorMessage = parser.getErrorMsg(columnsSQL);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (Exception e) {
            LOG.warn("failed to parse columns header, sql={}", columnsSQL, e);
            throw new UserException("parse columns header failed", e);
        }

        if (columnsStmt.getColumns() != null && !columnsStmt.getColumns().isEmpty()) {
            columnExprDescs = columnsStmt.getColumns();
        }
    }

    private Expr parseWhereExpr(String whereString) throws UserException {
        String whereSQL = new String("WHERE " + whereString);
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(whereSQL)));
        ImportWhereStmt whereStmt;
        try {
            whereStmt = (ImportWhereStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            LOG.warn("error happens when parsing where header, sql={}", whereSQL, e);
            throw new AnalysisException("failed to parsing where header, maybe contain unsupported character");
        } catch (AnalysisException e) {
            LOG.warn("analyze where statement failed, sql={}, error={}",
                     whereSQL, parser.getErrorMsg(whereSQL), e);
            String errorMessage = parser.getErrorMsg(whereSQL);
            if (errorMessage == null) {
                throw e;
            } else {
                throw new AnalysisException(errorMessage, e);
            }
        } catch (Exception e) {
            LOG.warn("failed to parse where header, sql={}", whereSQL, e);
            throw new UserException("parse columns header failed", e);
        }
        return whereStmt.getExpr();
    }

    private void setColumnSeparator(String oriSeparator) throws AnalysisException {
        columnSeparator = new ColumnSeparator(oriSeparator);
        columnSeparator.analyze();
    }

    public long getMemLimit() {
        return execMemLimit;
    }
}
