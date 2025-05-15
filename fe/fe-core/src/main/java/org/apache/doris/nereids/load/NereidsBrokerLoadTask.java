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
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import java.util.List;

/**
 * Nereids Broker Load Task
 */
public class NereidsBrokerLoadTask implements NereidsLoadTaskInfo {
    private long txnId;
    private int timeout;
    private int sendBatchParallelism;
    private boolean strictMode;
    private boolean memtableOnSinkNode;
    private PartitionNames partitions;

    /**
     * NereidsBrokerLoadTask
     */
    public NereidsBrokerLoadTask(long txnId, int timeout, int sendBatchParallelism,
            boolean strictMode, boolean memtableOnSinkNode, PartitionNames partitions) {
        this.txnId = txnId;
        this.timeout = timeout;
        this.sendBatchParallelism = sendBatchParallelism;
        this.strictMode = strictMode;
        this.memtableOnSinkNode = memtableOnSinkNode;
        this.partitions = partitions;
    }

    @Override
    public PartitionNames getPartitions() {
        return partitions;
    }

    @Override
    public LoadTask.MergeType getMergeType() {
        return null;
    }

    @Override
    public Expression getDeleteCondition() {
        return null;
    }

    @Override
    public boolean hasSequenceCol() {
        return false;
    }

    @Override
    public String getSequenceCol() {
        return null;
    }

    @Override
    public TFileType getFileType() {
        return null;
    }

    @Override
    public TFileFormatType getFormatType() {
        return null;
    }

    @Override
    public TFileCompressType getCompressType() {
        return null;
    }

    @Override
    public String getJsonPaths() {
        return null;
    }

    @Override
    public String getJsonRoot() {
        return null;
    }

    @Override
    public boolean isStripOuterArray() {
        return false;
    }

    @Override
    public boolean isFuzzyParse() {
        return false;
    }

    @Override
    public boolean isNumAsString() {
        return false;
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
        return 0;
    }

    @Override
    public NereidsImportColumnDescs getColumnExprDescs() {
        return null;
    }

    @Override
    public boolean isMemtableOnSinkNode() {
        return memtableOnSinkNode;
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    @Override
    public long getMemLimit() {
        return 0;
    }

    @Override
    public String getTimezone() {
        return null;
    }

    @Override
    public boolean getNegative() {
        return false;
    }

    @Override
    public long getTxnId() {
        return txnId;
    }

    @Override
    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    @Override
    public boolean isLoadToSingleTablet() {
        return false;
    }

    @Override
    public String getHeaderType() {
        return null;
    }

    @Override
    public List<String> getHiddenColumns() {
        return null;
    }

    @Override
    public boolean isFixedPartialUpdate() {
        return false;
    }

    @Override
    public boolean isStrictMode() {
        return strictMode;
    }

    @Override
    public Expression getPrecedingFilter() {
        return null;
    }

    @Override
    public Expression getWhereExpr() {
        return null;
    }

    @Override
    public Separator getColumnSeparator() {
        return null;
    }

    @Override
    public Separator getLineDelimiter() {
        return null;
    }

    @Override
    public byte getEnclose() {
        return 0;
    }

    @Override
    public byte getEscape() {
        return 0;
    }
}
