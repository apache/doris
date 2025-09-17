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
import org.apache.doris.analysis.Separator;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Nereids Stream Load Task
 */
public class NereidsStreamLoadTask implements NereidsLoadTaskInfo {
    private TUniqueId id;
    private long txnId;
    private TFileType fileType;
    private TFileFormatType formatType;
    private TFileCompressType compressType = TFileCompressType.UNKNOWN;
    private boolean stripOuterArray;
    private boolean numAsString;
    private String jsonPaths;
    private String jsonRoot;
    private boolean fuzzyParse;
    private boolean readJsonByLine;

    // optional
    private NereidsImportColumnDescs columnExprDescs = new NereidsImportColumnDescs();
    private Expression whereExpr;
    private Separator columnSeparator;
    private Separator lineDelimiter;
    private PartitionNames partitions;
    private String path;
    private long fileSize = 0;
    private boolean negative;
    private boolean strictMode = false; // default is false
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int timeout = Config.stream_load_default_timeout_second;
    private long execMemLimit = 2 * 1024 * 1024 * 1024L; // default is 2GB
    private LoadTask.MergeType mergeType = LoadTask.MergeType.APPEND; // default is all data is load no delete
    private Expression deleteCondition;
    private String sequenceCol;
    private int sendBatchParallelism = 1;
    private double maxFilterRatio = 0.0;
    private boolean loadToSingleTablet = false;
    private String headerType = "";
    private List<String> hiddenColumns;
    private boolean trimDoubleQuotes = false;
    private TUniqueKeyUpdateMode uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
    private TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy = TPartialUpdateNewRowPolicy.APPEND;

    private int skipLines = 0;
    private boolean enableProfile = false;

    private boolean memtableOnSinkNode = false;
    private int streamPerNode = 2;

    private byte enclose = 0;

    private byte escape = 0;

    private String groupCommit;

    private boolean emptyFieldAsNull = false;

    /**
     * NereidsStreamLoadTask
     */
    public NereidsStreamLoadTask(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType,
            TFileCompressType compressType) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.compressType = compressType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
        this.numAsString = false;
        this.fuzzyParse = false;
        this.readJsonByLine = false;
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

    public TFileCompressType getCompressType() {
        return compressType;
    }

    public NereidsImportColumnDescs getColumnExprDescs() {
        return columnExprDescs;
    }

    public Expression getPrecedingFilter() {
        return null;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    public Separator getColumnSeparator() {
        return columnSeparator;
    }

    public String getHeaderType() {
        return headerType;
    }

    public Separator getLineDelimiter() {
        return lineDelimiter;
    }

    @Override
    public byte getEnclose() {
        return enclose;
    }

    public void setEnclose(byte enclose) {
        this.enclose = enclose;
    }

    @Override
    public byte getEscape() {
        return escape;
    }

    public void setEscape(byte escape) {
        this.escape = escape;
    }

    @Override
    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    @Override
    public boolean isLoadToSingleTablet() {
        return loadToSingleTablet;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public String getPath() {
        return path;
    }

    @Override
    public long getFileSize() {
        return fileSize;
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

    @Override
    public boolean isNumAsString() {
        return numAsString;
    }

    @Override
    public boolean isReadJsonByLine() {
        return readJsonByLine;
    }

    @Override
    public boolean isFuzzyParse() {
        return fuzzyParse;
    }

    public void setFuzzyParse(boolean fuzzyParse) {
        this.fuzzyParse = fuzzyParse;
    }

    public void setStripOuterArray(boolean stripOuterArray) {
        this.stripOuterArray = stripOuterArray;
    }

    public void setNumAsString(boolean numAsString) {
        this.numAsString = numAsString;
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

    public Expression getDeleteCondition() {
        return deleteCondition;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    @Override
    public String getSequenceCol() {
        return sequenceCol;
    }

    @Override
    public List<String> getHiddenColumns() {
        return hiddenColumns;
    }

    @Override
    public boolean getTrimDoubleQuotes() {
        return trimDoubleQuotes;
    }

    public int getSkipLines() {
        return skipLines;
    }

    @Override
    public boolean getEnableProfile() {
        return enableProfile;
    }

    @Override
    public boolean isFixedPartialUpdate() {
        return uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS;
    }

    @Override
    public TUniqueKeyUpdateMode getUniqueKeyUpdateMode() {
        return uniquekeyUpdateMode;
    }

    @Override
    public boolean isFlexiblePartialUpdate() {
        return uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS;
    }

    @Override
    public TPartialUpdateNewRowPolicy getPartialUpdateNewRowPolicy() {
        return partialUpdateNewKeyPolicy;
    }

    @Override
    public boolean isMemtableOnSinkNode() {
        return memtableOnSinkNode;
    }

    public void setMemtableOnSinkNode(boolean memtableOnSinkNode) {
        this.memtableOnSinkNode = memtableOnSinkNode;
    }

    @Override
    public int getStreamPerNode() {
        return streamPerNode;
    }

    public void setStreamPerNode(int streamPerNode) {
        this.streamPerNode = streamPerNode;
    }

    @Override
    public boolean getEmptyFieldAsNull() {
        return emptyFieldAsNull;
    }

    public void setEmptyFieldAsNull(boolean emptyFieldAsNull) {
        this.emptyFieldAsNull = emptyFieldAsNull;
    }

    /**
     * fromTStreamLoadPutRequest
     */
    public static NereidsStreamLoadTask fromTStreamLoadPutRequest(TStreamLoadPutRequest request) throws UserException {
        NereidsStreamLoadTask streamLoadTask = new NereidsStreamLoadTask(request.getLoadId(), request.getTxnId(),
                request.getFileType(), request.getFormatType(),
                request.getCompressType());
        streamLoadTask.setOptionalFromTSLPutRequest(request);
        streamLoadTask.setGroupCommit(request.getGroupCommitMode());
        if (request.isSetFileSize()) {
            streamLoadTask.fileSize = request.getFileSize();
        }
        return streamLoadTask;
    }

    /**
     * setMultiTableBaseTaskInfo
     */
    public void setMultiTableBaseTaskInfo(LoadTaskInfo task) throws UserException {
        this.mergeType = task.getMergeType();
        this.columnSeparator = task.getColumnSeparator();
        this.whereExpr = task.getWhereExpr() != null ? parseWhereExpr(task.getWhereExpr().toSqlWithoutTbl()) : null;
        this.partitions = task.getPartitions();
        this.deleteCondition = task.getDeleteCondition() != null
                ? parseWhereExpr(task.getDeleteCondition().toSqlWithoutTbl())
                : null;
        this.lineDelimiter = task.getLineDelimiter();
        this.strictMode = task.isStrictMode();
        this.timezone = task.getTimezone();
        this.formatType = task.getFormatType();
        this.stripOuterArray = task.isStripOuterArray();
        this.jsonRoot = task.getJsonRoot();
        this.sendBatchParallelism = task.getSendBatchParallelism();
        this.loadToSingleTablet = task.isLoadToSingleTablet();
    }

    private void setOptionalFromTSLPutRequest(TStreamLoadPutRequest request) throws UserException {
        if (request.isSetColumns()) {
            setColumnToColumnExpr(request.getColumns());
        }
        if (request.isSetWhere()) {
            whereExpr = parseWhereExpr(request.getWhere());
        }
        if (request.isSetColumnSeparator()) {
            setColumnSeparator(request.getColumnSeparator());
        }
        if (request.isSetLineDelimiter()) {
            setLineDelimiter(request.getLineDelimiter());
        }
        if (request.isSetEnclose()) {
            setEnclose(request.getEnclose());
        }
        if (request.isSetEscape()) {
            setEscape(request.getEscape());
        }
        if (request.isSetHeaderType()) {
            headerType = request.getHeaderType();
        }
        if (request.isSetPartitions()) {
            String[] splitPartNames = request.getPartitions().trim().split(",");
            List<String> partNames = Arrays.stream(splitPartNames).map(String::trim).collect(Collectors.toList());
            if (request.isSetIsTempPartition()) {
                partitions = new PartitionNames(request.isIsTempPartition(), partNames);
            } else {
                partitions = new PartitionNames(false, partNames);
            }
        }
        switch (request.getFileType()) {
            case FILE_STREAM:
                // fall through to case FILE_LOCAL
            case FILE_LOCAL:
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
            numAsString = request.isNumAsString();
            fuzzyParse = request.isFuzzyParse();
            readJsonByLine = request.isReadJsonByLine();
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
            throw new AnalysisException("Negative is only used when merge type is APPEND.");
        }
        if (request.isSetSequenceCol()) {
            sequenceCol = request.getSequenceCol();
        }
        if (request.isSetSendBatchParallelism()) {
            sendBatchParallelism = request.getSendBatchParallelism();
        }
        if (request.isSetMaxFilterRatio()) {
            maxFilterRatio = request.getMaxFilterRatio();
        }
        if (request.isSetLoadToSingleTablet()) {
            loadToSingleTablet = request.isLoadToSingleTablet();
        }
        if (request.isSetHiddenColumns()) {
            hiddenColumns = Arrays.asList(request.getHiddenColumns().replaceAll("\\s+", "").split(","));
        }
        if (request.isSetTrimDoubleQuotes()) {
            trimDoubleQuotes = request.isTrimDoubleQuotes();
        }
        if (request.isSetSkipLines()) {
            skipLines = request.getSkipLines();
        }
        if (request.isSetEnableProfile()) {
            enableProfile = request.isEnableProfile();
        }
        if (request.isSetUniqueKeyUpdateMode()) {
            try {
                uniquekeyUpdateMode = request.getUniqueKeyUpdateMode();
            } catch (IllegalArgumentException e) {
                throw new UserException("unknown unique_key_update_mode: "
                        + request.getUniqueKeyUpdateMode().toString());
            }
        } else {
            if (request.isSetPartialUpdate() && request.isPartialUpdate()) {
                uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS;
            } else {
                uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
            }
        }
        if (request.isSetPartialUpdateNewKeyPolicy()) {
            partialUpdateNewKeyPolicy = request.getPartialUpdateNewKeyPolicy();
        }
        if (request.isSetMemtableOnSinkNode()) {
            this.memtableOnSinkNode = request.isMemtableOnSinkNode();
        } else {
            this.memtableOnSinkNode = Config.stream_load_default_memtable_on_sink_node;
        }
        if (request.isSetStreamPerNode()) {
            this.streamPerNode = request.getStreamPerNode();
        }
        if (request.isSetEmptyFieldAsNull()) {
            emptyFieldAsNull = request.isEmptyFieldAsNull();
        }
    }

    // used for stream load
    private void setColumnToColumnExpr(String columns) throws UserException {
        List<Expression> expressions = NereidsLoadUtils.parseExpressionSeq(columns);
        for (Expression expr : expressions) {
            if (expr instanceof BinaryOperator) {
                if (!(expr.child(0) instanceof UnboundSlot)) {
                    throw new UserException(String.format("% is unsupported", expr));
                }
                columnExprDescs.descs
                        .add(new NereidsImportColumnDesc(((UnboundSlot) expr.child(0)).getName(), expr.child(1)));
            } else {
                if (!(expr instanceof UnboundSlot)) {
                    throw new UserException(String.format("% is unsupported", expr));
                }
                columnExprDescs.descs.add(new NereidsImportColumnDesc(((UnboundSlot) expr).getName()));
            }
        }
    }

    private Expression parseWhereExpr(String whereString) throws UserException {
        return NereidsLoadUtils.parseExpressionSeq(whereString).get(0);
    }

    private void setColumnSeparator(String oriSeparator) throws AnalysisException {
        columnSeparator = new Separator(oriSeparator);
        columnSeparator.analyze();
    }

    private void setLineDelimiter(String oriLineDelimiter) throws AnalysisException {
        lineDelimiter = new Separator(oriLineDelimiter);
        lineDelimiter.analyze();
    }

    @Override
    public long getMemLimit() {
        return execMemLimit;
    }

    @Override
    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public void setGroupCommit(String groupCommit) {
        this.groupCommit = groupCommit;
    }

    public String getGroupCommit() {
        return groupCommit;
    }
}
