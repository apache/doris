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

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

/**
 * NereidsRoutineLoadTaskInfo
 */
public class NereidsRoutineLoadTaskInfo implements NereidsLoadTaskInfo {

    private static final String PROPS_FORMAT = "format";
    private static final String PROPS_STRIP_OUTER_ARRAY = "strip_outer_array";
    private static final String PROPS_NUM_AS_STRING = "num_as_string";
    private static final String PROPS_JSONPATHS = "jsonpaths";
    private static final String PROPS_JSONROOT = "json_root";
    private static final String PROPS_FUZZY_PARSE = "fuzzy_parse";
    private static final boolean DEFAULT_STRICT_MODE = false;

    protected long execMemLimit;
    protected Map<String, String> jobProperties;
    protected long maxBatchIntervalS;
    protected PartitionNames partitions;
    protected LoadTask.MergeType mergeType;
    protected Expression deleteCondition;
    protected String sequenceCol;
    protected double maxFilterRatio;
    protected NereidsImportColumnDescs columnDescs;
    protected Expression precedingFilter;
    protected Expression whereExpr;
    protected Separator columnSeparator;
    protected Separator lineDelimiter;
    protected byte enclose;
    protected byte escape;
    protected int sendBatchParallelism;
    protected boolean loadToSingleTablet;
    protected boolean isPartialUpdate;
    protected boolean memtableOnSinkNode;

    /**
     * NereidsRoutineLoadTaskInfo
     */
    public NereidsRoutineLoadTaskInfo(long execMemLimit, Map<String, String> jobProperties, long maxBatchIntervalS,
            PartitionNames partitions, LoadTask.MergeType mergeType, Expression deleteCondition,
            String sequenceCol, double maxFilterRatio, NereidsImportColumnDescs columnDescs,
            Expression precedingFilter, Expression whereExpr, Separator columnSeparator,
            Separator lineDelimiter, byte enclose, byte escape, int sendBatchParallelism,
            boolean loadToSingleTablet, boolean isPartialUpdate, boolean memtableOnSinkNode) {
        this.execMemLimit = execMemLimit;
        this.jobProperties = jobProperties;
        this.maxBatchIntervalS = maxBatchIntervalS;
        this.partitions = partitions;
        this.mergeType = mergeType;
        this.deleteCondition = deleteCondition;
        this.sequenceCol = sequenceCol;
        this.maxFilterRatio = maxFilterRatio;
        this.columnDescs = columnDescs;
        this.precedingFilter = precedingFilter;
        this.whereExpr = whereExpr;
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.enclose = enclose;
        this.escape = escape;
        this.sendBatchParallelism = sendBatchParallelism;
        this.loadToSingleTablet = loadToSingleTablet;
        this.isPartialUpdate = isPartialUpdate;
        this.memtableOnSinkNode = memtableOnSinkNode;
    }

    @Override
    public boolean getNegative() {
        return false;
    }

    @Override
    public long getTxnId() {
        return -1L;
    }

    @Override
    public int getTimeout() {
        int timeoutSec = (int) maxBatchIntervalS * Config.routine_load_task_timeout_multiplier;
        int realTimeoutSec = timeoutSec < Config.routine_load_task_min_timeout_sec
                ? Config.routine_load_task_min_timeout_sec : timeoutSec;
        return realTimeoutSec;
    }

    @Override
    public long getMemLimit() {
        return execMemLimit;
    }

    @Override
    public String getTimezone() {
        String value = jobProperties.get(LoadStmt.TIMEZONE);
        if (value == null) {
            return TimeUtils.DEFAULT_TIME_ZONE;
        }
        return value;
    }

    @Override
    public PartitionNames getPartitions() {
        return partitions;
    }

    @Override
    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    @Override
    public Expression getDeleteCondition() {
        return deleteCondition;
    }

    @Override
    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceCol);
    }

    @Override
    public String getSequenceCol() {
        return sequenceCol;
    }

    @Override
    public TFileType getFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public TFileFormatType getFormatType() {
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        if (getFormat().equals("json")) {
            fileFormatType = TFileFormatType.FORMAT_JSON;
        }
        return fileFormatType;
    }

    @Override
    public TFileCompressType getCompressType() {
        return TFileCompressType.PLAIN;
    }

    @Override
    public String getJsonPaths() {
        String value = jobProperties.get(PROPS_JSONPATHS);
        if (value == null) {
            return "";
        }
        return value;
    }

    @Override
    public String getJsonRoot() {
        String value = jobProperties.get(PROPS_JSONROOT);
        if (value == null) {
            return "";
        }
        return value;
    }

    @Override
    public boolean isStripOuterArray() {
        return Boolean.parseBoolean(jobProperties.get(PROPS_STRIP_OUTER_ARRAY));
    }

    @Override
    public boolean isFuzzyParse() {
        return Boolean.parseBoolean(jobProperties.get(PROPS_FUZZY_PARSE));
    }

    @Override
    public boolean isNumAsString() {
        return Boolean.parseBoolean(jobProperties.get(PROPS_NUM_AS_STRING));
    }

    @Override
    public boolean isReadJsonByLine() {
        return false;
    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    @Override
    public NereidsImportColumnDescs getColumnExprDescs() {
        if (columnDescs == null) {
            return new NereidsImportColumnDescs();
        }
        return columnDescs;
    }

    @Override
    public boolean isStrictMode() {
        String value = jobProperties.get(LoadStmt.STRICT_MODE);
        if (value == null) {
            return DEFAULT_STRICT_MODE;
        }
        return Boolean.parseBoolean(value);
    }

    @Override
    public Expression getPrecedingFilter() {
        return precedingFilter;
    }

    @Override
    public Expression getWhereExpr() {
        return whereExpr;
    }

    @Override
    public Separator getColumnSeparator() {
        return columnSeparator;
    }

    @Override
    public Separator getLineDelimiter() {
        return lineDelimiter;
    }

    @Override
    public byte getEnclose() {
        return enclose;
    }

    @Override
    public byte getEscape() {
        return escape;
    }

    @Override
    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    @Override
    public boolean isLoadToSingleTablet() {
        return loadToSingleTablet;
    }

    @Override
    public String getHeaderType() {
        return "";
    }

    @Override
    public List<String> getHiddenColumns() {
        return null;
    }

    @Override
    public boolean isFixedPartialUpdate() {
        return isPartialUpdate;
    }

    @Override
    public boolean isMemtableOnSinkNode() {
        return memtableOnSinkNode;
    }

    private String getFormat() {
        String value = jobProperties.get(PROPS_FORMAT);
        if (value == null) {
            return "csv";
        }
        return value;
    }
}
