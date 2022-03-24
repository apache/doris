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
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.Lists;

import java.util.List;

public interface LoadTaskInfo {
    public boolean getNegative();
    public long getTxnId();
    public int getTimeout();
    public long getMemLimit();
    public String getTimezone();
    public PartitionNames getPartitions();
    public LoadTask.MergeType getMergeType();
    public Expr getDeleteCondition();
    public boolean hasSequenceCol();
    public String getSequenceCol();
    public TFileType getFileType();
    public TFileFormatType getFormatType();
    public String getJsonPaths();
    public String getJsonRoot();
    public boolean isStripOuterArray();
    public boolean isFuzzyParse();
    public boolean isNumAsString();
    public boolean isReadJsonByLine();
    public String getPath();

    public double getMaxFilterRatio();

    public ImportColumnDescs getColumnExprDescs();
    public boolean isStrictMode();

    public Expr getPrecedingFilter();
    public Expr getWhereExpr();
    public Separator getColumnSeparator();
    public Separator getLineDelimiter();
    public int getSendBatchParallelism();
    public boolean isLoadToSingleTablet();

    public static class ImportColumnDescs {
        public List<ImportColumnDesc> descs = Lists.newArrayList();
        public boolean isColumnDescsRewrited = false;
    }
}
