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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.Separator;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.LoadTask;

import com.google.common.base.Strings;

public class RoutineLoadDesc {
    private final Separator columnSeparator;
    private final Separator lineDelimiter;
    private final ImportColumnsStmt columnsInfo;
    private final ImportWhereStmt precedingFilter;
    private final ImportWhereStmt wherePredicate;
    private final Expr deleteCondition;
    private LoadTask.MergeType mergeType;
    // nullable
    private final PartitionNames partitionNames;
    private final String sequenceColName;

    public RoutineLoadDesc(Separator columnSeparator, Separator lineDelimiter, ImportColumnsStmt columnsInfo,
                           ImportWhereStmt precedingFilter, ImportWhereStmt wherePredicate,
                           PartitionNames partitionNames, Expr deleteCondition, LoadTask.MergeType mergeType,
                           String sequenceColName) {
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.columnsInfo = columnsInfo;
        this.precedingFilter = precedingFilter;
        this.wherePredicate = wherePredicate;
        this.partitionNames = partitionNames;
        this.deleteCondition = deleteCondition;
        this.mergeType = mergeType;
        this.sequenceColName = sequenceColName;
    }

    public Separator getColumnSeparator() {
        return columnSeparator;
    }

    public Separator getLineDelimiter() {
        return lineDelimiter;
    }

    public ImportColumnsStmt getColumnsInfo() {
        return columnsInfo;
    }

    public ImportWhereStmt getPrecedingFilter() {
        return precedingFilter;
    }

    public ImportWhereStmt getWherePredicate() {
        return wherePredicate;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    // nullable
    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public Expr getDeleteCondition() {
        return deleteCondition;
    }

    public String getSequenceColName() {
        return sequenceColName;
    }

    public boolean hasSequenceCol() {
        return !Strings.isNullOrEmpty(sequenceColName);
    }

    public void analyze(Analyzer analyzer) throws UserException {
        if (mergeType != LoadTask.MergeType.MERGE && deleteCondition != null) {
            throw new AnalysisException("not support DELETE ON clause when merge type is not MERGE.");
        }
        if (mergeType == LoadTask.MergeType.MERGE && deleteCondition == null) {
            throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
        }
    }
}
