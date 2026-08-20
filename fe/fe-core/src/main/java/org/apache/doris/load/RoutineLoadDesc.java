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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RoutineLoadDesc {
    private final Separator columnSeparator;
    private final Separator lineDelimiter;
    private final List<ImportColumnDesc> columnsInfo;
    private final Expr precedingFilter;
    private final Expr filter;
    private final Expr deleteCondition;
    private LoadTask.MergeType mergeType;
    // nullable
    private final PartitionNamesInfo partitionNamesInfo;
    private final String sequenceColName;

    public RoutineLoadDesc(Separator columnSeparator, Separator lineDelimiter, List<ImportColumnDesc> columnsInfo,
                           Expr precedingFilter, Expr filter,
                           PartitionNamesInfo partitionNamesInfo, Expr deleteCondition, LoadTask.MergeType mergeType,
                           String sequenceColName) {
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.columnsInfo = columnsInfo;
        this.precedingFilter = precedingFilter;
        this.filter = filter;
        this.partitionNamesInfo = partitionNamesInfo;
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

    public List<ImportColumnDesc> getColumnsInfo() {
        return columnsInfo;
    }

    public Expr getPrecedingFilter() {
        return precedingFilter;
    }

    public Expr getFilter() {
        return filter;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
    }

    // nullable
    public PartitionNamesInfo getPartitionNamesInfo() {
        return partitionNamesInfo;
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

    /**
     * Convert the effective load clauses to SQL so they can be persisted in RoutineLoadJob.origStmt.
     */
    public String toSql() {
        List<String> clauses = new ArrayList<>();
        // Routine Load SQL does not currently expose a line-delimiter clause. RoutineLoadJob
        // persists that scalar separately until the grammar supports representing it here.
        if (columnSeparator != null) {
            clauses.add("COLUMNS TERMINATED BY " + SqlUtils.quoteStringLiteral(
                    columnSeparator.getOriSeparator(), SqlModeHelper.hasNoBackSlashEscapes()));
        }
        if (columnsInfo != null) {
            clauses.add("COLUMNS(" + columnsInfo.stream()
                    .map(this::columnToSql)
                    .collect(Collectors.joining(", ")) + ")");
        }
        if (precedingFilter != null) {
            clauses.add("PRECEDING FILTER " + precedingFilter.accept(
                    ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
        }
        if (filter != null) {
            clauses.add("WHERE " + filter.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
        }
        if (partitionNamesInfo != null) {
            String prefix = partitionNamesInfo.isTemp() ? "TEMPORARY PARTITION(" : "PARTITION(";
            clauses.add(prefix + partitionNamesInfo.getPartitionNames().stream()
                    .map(SqlUtils::getIdentSql)
                    .collect(Collectors.joining(", ")) + ")");
        }
        if (deleteCondition != null) {
            clauses.add("DELETE ON " + deleteCondition.accept(
                    ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
        }
        if (hasSequenceCol()) {
            clauses.add("ORDER BY " + SqlUtils.getIdentSql(sequenceColName));
        }
        return String.join(", ", clauses);
    }

    private String columnToSql(ImportColumnDesc columnDesc) {
        String sql = SqlUtils.getIdentSql(columnDesc.getColumnName());
        if (columnDesc.getExpr() != null) {
            sql += " = " + columnDesc.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
        }
        return sql;
    }

    public void analyze() throws UserException {
        if (mergeType != LoadTask.MergeType.MERGE && deleteCondition != null) {
            throw new AnalysisException("not support DELETE ON clause when merge type is not MERGE.");
        }
        if (mergeType == LoadTask.MergeType.MERGE && deleteCondition == null) {
            throw new AnalysisException("Excepted DELETE ON clause when merge type is MERGE.");
        }
    }
}
