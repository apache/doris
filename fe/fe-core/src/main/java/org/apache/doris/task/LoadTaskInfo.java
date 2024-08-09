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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public interface LoadTaskInfo {
    boolean getNegative();

    long getTxnId();

    int getTimeout();

    long getMemLimit();

    String getTimezone();

    PartitionNames getPartitions();

    LoadTask.MergeType getMergeType();

    Expr getDeleteCondition();

    boolean hasSequenceCol();

    String getSequenceCol();

    TFileType getFileType();

    TFileFormatType getFormatType();

    TFileCompressType getCompressType();

    String getJsonPaths();

    String getJsonRoot();

    boolean isStripOuterArray();

    boolean isFuzzyParse();

    boolean isNumAsString();

    boolean isReadJsonByLine();

    String getPath();

    default long getFileSize() {
        return 0;
    }

    double getMaxFilterRatio();

    ImportColumnDescs getColumnExprDescs();

    boolean isStrictMode();

    Expr getPrecedingFilter();

    Expr getWhereExpr();

    Separator getColumnSeparator();

    Separator getLineDelimiter();

    /**
     * only for csv
     */
    byte getEnclose();

    /**
     * only for csv
     */
    byte getEscape();

    int getSendBatchParallelism();

    boolean isLoadToSingleTablet();

    String getHeaderType();

    List<String> getHiddenColumns();

    boolean isPartialUpdate();

    default boolean getTrimDoubleQuotes() {
        return false;
    }

    default int getSkipLines() {
        return 0;
    }

    default boolean getEnableProfile() {
        return false;
    }

    default boolean isMemtableOnSinkNode() {
        return false;
    }

    default int getStreamPerNode() {
        return 2;
    }

    class ImportColumnDescs {
        @SerializedName("des")
        public List<ImportColumnDesc> descs = Lists.newArrayList();
        @SerializedName("icdr")
        public boolean isColumnDescsRewrited = false;

        public List<String> getFileColNames() {
            List<String> colNames = Lists.newArrayList();
            for (ImportColumnDesc desc : descs) {
                if (desc.isColumn()) {
                    colNames.add(desc.getColumnName());
                }
            }
            return colNames;
        }

        public List<Expr> getColumnMappingList() {
            List<Expr> exprs = Lists.newArrayList();
            for (ImportColumnDesc desc : descs) {
                if (!desc.isColumn()) {
                    exprs.add(desc.toBinaryPredicate());
                }
            }
            return exprs;
        }
    }
}
