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

import com.google.common.base.Strings;
import org.apache.doris.analysis.ColumnSeparator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.load.loadv2.LoadTask;

public class RoutineLoadDesc {
    private final ColumnSeparator columnSeparator;
    private final ImportColumnsStmt columnsInfo;
    private final ImportWhereStmt wherePredicate;
    private final Expr deleteCondition;
    private LoadTask.MergeType mergeType;
    // nullable
    private final PartitionNames partitionNames;
    private final String sequenceColName;

    public RoutineLoadDesc(ColumnSeparator columnSeparator, ImportColumnsStmt columnsInfo,
                           ImportWhereStmt wherePredicate, PartitionNames partitionNames,
                           Expr deleteCondition, LoadTask.MergeType mergeType,
                           String sequenceColName) {
        this.columnSeparator = columnSeparator;
        this.columnsInfo = columnsInfo;
        this.wherePredicate = wherePredicate;
        this.partitionNames = partitionNames;
        this.deleteCondition = deleteCondition;
        this.mergeType = mergeType;
        this.sequenceColName = sequenceColName;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public ImportColumnsStmt getColumnsInfo() {
        return columnsInfo;
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
}
