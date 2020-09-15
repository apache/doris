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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Alter table statement.
public class AlterTableStmt extends DdlStmt {
    private TableName tbl;
    private List<AlterClause> ops;

    public AlterTableStmt(TableName tbl, List<AlterClause> ops) {
        this.tbl = tbl;
        this.ops = ops;
    }

    public void setTableName(String newTableName) {
        tbl = new TableName(tbl.getDb(), newTableName);
    }

    public TableName getTbl() {
        return tbl;
    }

    public List<AlterClause> getOps() {
        return ops;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(analyzer);
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tbl.getDb(), tbl.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tbl.getTbl());
        }
        if (ops == null || ops.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        for (AlterClause op : ops) {
            op.analyze(analyzer);
        }
    }

    public void rewriteAlterClause(OlapTable table) throws UserException {
        List<AlterClause> clauses = new ArrayList<>();
        for (AlterClause alterClause : ops) {
            if (alterClause instanceof EnableFeatureClause) {
                EnableFeatureClause.Features alterFeature  = ((EnableFeatureClause) alterClause).getFeature();
                if (alterFeature == null || alterFeature == EnableFeatureClause.Features.UNKNOWN) {
                    throw new AnalysisException("unknown feature for alter clause");
                }
                if (table.getKeysType() != KeysType.UNIQUE_KEYS && alterFeature == EnableFeatureClause.Features.BATCH_DELETE) {
                    throw new AnalysisException("Batch delete only supported in unique tables.");
                }
                if (table.getKeysType() != KeysType.UNIQUE_KEYS && alterFeature == EnableFeatureClause.Features.SEQUENCE_LOAD) {
                    throw new AnalysisException("Sequence load only supported in unique tables.");
                }
                // analyse sequence column
                Type sequenceColType = null;
                if (alterFeature == EnableFeatureClause.Features.SEQUENCE_LOAD) {
                    Map<String, String> propertyMap = alterClause.getProperties();
                    try {
                        sequenceColType = PropertyAnalyzer.analyzeSequenceType(propertyMap, table.getKeysType());
                        if (sequenceColType == null) {
                            throw new AnalysisException("unknown sequence column type");
                        }
                    } catch (Exception e) {
                        throw new AnalysisException(e.getMessage());
                    }
                }

                // has rollup table
                if (table.getVisibleIndex().size() > 1) {
                    for (MaterializedIndex idx : table.getVisibleIndex()) {
                        // add a column to rollup index it will add to base table automatically,
                        // if add a column here it will duplicated
                        if (idx.getId() == table.getBaseIndexId()) {
                            continue;
                        }
                        AddColumnClause addColumnClause = null;
                        if (alterFeature == EnableFeatureClause.Features.BATCH_DELETE) {
                            addColumnClause = new AddColumnClause(ColumnDef.newDeleteSignColumnDef(), null,
                                    table.getIndexNameById(idx.getId()), null);
                        } else if (alterFeature == EnableFeatureClause.Features.SEQUENCE_LOAD) {
                            addColumnClause = new AddColumnClause(ColumnDef.newSequenceColumnDef(sequenceColType), null,
                                    table.getIndexNameById(idx.getId()), null);
                        } else {
                            throw new AnalysisException("unknown feature : " + alterFeature);
                        }
                        addColumnClause.analyze(analyzer);
                        clauses.add(addColumnClause);
                    }
                } else {
                    // no rollup tables
                    AddColumnClause addColumnClause = null;
                    if (alterFeature == EnableFeatureClause.Features.BATCH_DELETE) {
                        addColumnClause = new AddColumnClause(ColumnDef.newDeleteSignColumnDef(), null,
                                null, null);
                    } else if (alterFeature == EnableFeatureClause.Features.SEQUENCE_LOAD) {
                        addColumnClause = new AddColumnClause(ColumnDef.newSequenceColumnDef(sequenceColType), null,
                                null, null);
                    }
                    addColumnClause.analyze(analyzer);
                    clauses.add(addColumnClause);
                }
            // add hidden column to rollup table
            } else if (alterClause instanceof AddRollupClause && table.getKeysType() == KeysType.UNIQUE_KEYS
                    && table.hasHiddenColumn()) {
                if (table.getColumn(Column.DELETE_SIGN) != null) {
                    if (!((AddRollupClause) alterClause).getColumnNames()
                            .stream()
                            .anyMatch(x -> x.equalsIgnoreCase(Column.DELETE_SIGN))) {
                        ((AddRollupClause) alterClause).getColumnNames().add(Column.DELETE_SIGN);
                    }
                }
                if (table.getColumn(Column.SEQUENCE_COL) != null) {
                    if (!((AddRollupClause) alterClause).getColumnNames()
                            .stream()
                            .anyMatch(x -> x.equalsIgnoreCase(Column.SEQUENCE_COL))) {
                        ((AddRollupClause) alterClause).getColumnNames().add(Column.SEQUENCE_COL);
                    }
                }
                alterClause.analyze(analyzer);
                clauses.add(alterClause);
            } else {
                clauses.add(alterClause);
            }
        }
        ops = clauses;
    }


    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(tbl.toSql()).append(" ");
        int idx = 0;
        for (AlterClause op : ops) {
            if (idx != 0) {
                sb.append(", \n");
            }
            if (op instanceof AddRollupClause) {
                if (idx == 0) {
                    sb.append("ADD ROLLUP");
                }
                sb.append(op.toSql().replace("ADD ROLLUP", ""));
            } else if (op instanceof DropRollupClause) {
                if (idx == 0) {
                    sb.append("DROP ROLLUP ");
                }
                sb.append(((AddRollupClause) op).getRollupName());
            } else {
                sb.append(op.toSql());
            }
            idx++;
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
