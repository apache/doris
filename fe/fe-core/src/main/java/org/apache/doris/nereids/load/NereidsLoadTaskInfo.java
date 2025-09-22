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
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Abstract class for all load tasks in Nereids.
 */
public interface NereidsLoadTaskInfo {

    boolean getNegative();

    long getTxnId();

    int getTimeout();

    long getMemLimit();

    String getTimezone();

    PartitionNames getPartitions();

    LoadTask.MergeType getMergeType();

    Expression getDeleteCondition();

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

    NereidsImportColumnDescs getColumnExprDescs();

    boolean isStrictMode();

    Expression getPrecedingFilter();

    Expression getWhereExpr();

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

    boolean isFixedPartialUpdate();

    boolean getEmptyFieldAsNull();

    default TUniqueKeyUpdateMode getUniqueKeyUpdateMode() {
        return TUniqueKeyUpdateMode.UPSERT;
    }

    default boolean isFlexiblePartialUpdate() {
        return false;
    }

    default TPartialUpdateNewRowPolicy getPartialUpdateNewRowPolicy() {
        return TPartialUpdateNewRowPolicy.APPEND;
    }

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

    /**
     * NereidsImportColumnDescs
     */
    class NereidsImportColumnDescs {

        public List<NereidsImportColumnDesc> descs = Lists.newArrayList();

        /**
         * getFileColNames
         */
        public List<String> getFileColNames() {
            List<String> colNames = Lists.newArrayList();
            for (NereidsImportColumnDesc desc : descs) {
                if (desc.isColumn()) {
                    colNames.add(desc.getColumnName());
                }
            }
            return colNames;
        }

        /**
         * getColumnMappingList, each one as unboundSlot = expression
         */
        public List<Expression> getColumnMappingList() {
            List<Expression> exprs = Lists.newArrayList();
            for (NereidsImportColumnDesc desc : descs) {
                if (!desc.isColumn()) {
                    exprs.add(desc.toBinaryPredicate());
                }
            }
            return exprs;
        }
    }

}
