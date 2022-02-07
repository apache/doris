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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.LinkedList;
import java.util.List;

public class DeleteStmt extends DdlStmt {
    private final TableName tbl;
    private final PartitionNames partitionNames;
    private Expr wherePredicate;

    private List<Predicate> deleteConditions;

    public DeleteStmt(TableName tableName, PartitionNames partitionNames, Expr wherePredicate) {
        this.tbl = tableName;
        this.partitionNames = partitionNames;
        this.wherePredicate = wherePredicate;
        this.deleteConditions = new LinkedList<Predicate>();
    }
    
    public String getTableName() {
        return tbl.getTbl();
    }
    
    public String getDbName() {
        return tbl.getDb();
    }

    public List<String> getPartitionNames() {
        return partitionNames == null ? Lists.newArrayList() : partitionNames.getPartitionNames();
    }

    public List<Predicate> getDeleteConditions() {
        return deleteConditions;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        
        if (tbl == null) {
            throw new AnalysisException("Table is not set");
        }

        tbl.analyze(analyzer);

        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support deleting temp partitions");
            }
        }

        if (wherePredicate == null) {
            throw new AnalysisException("Where clause is not set");
        }

        // analyze predicate
        analyzePredicate(wherePredicate);

        // check access
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tbl.getDb(), tbl.getTbl(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(), tbl.getDb() + ": " + tbl.getTbl());
        }
    }

    private void analyzePredicate(Expr predicate) throws AnalysisException {
        if (predicate instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
            Expr leftExpr = binaryPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of binary predicate should be column name");
            }
            Expr rightExpr = binaryPredicate.getChild(1);
            if (!(rightExpr instanceof LiteralExpr)) {
                throw new AnalysisException("Right expr of binary predicate should be value");
            }
            deleteConditions.add(binaryPredicate);
        } else if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            if (compoundPredicate.getOp() != Operator.AND) {
                throw new AnalysisException("Compound predicate's op should be AND");
            }

            analyzePredicate(compoundPredicate.getChild(0));
            analyzePredicate(compoundPredicate.getChild(1));
        } else if (predicate instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) predicate;
            Expr leftExpr = isNullPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of is_null predicate should be column name");
            }
            deleteConditions.add(isNullPredicate);
        } else if (predicate instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) predicate;
            Expr leftExpr = inPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr of in predicate should be column name");
            }
            int inElementNum = inPredicate.getInElementNum();
            int maxAllowedInElementNumOfDelete = Config.max_allowed_in_element_num_of_delete;
            if (inElementNum > maxAllowedInElementNumOfDelete) {
                throw new AnalysisException("Element num of in predicate should not be more than " + maxAllowedInElementNumOfDelete);
            }
            for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                Expr expr = inPredicate.getChild(i);
                if (!(expr instanceof LiteralExpr)) {
                    throw new AnalysisException("Child of in predicate should be value");
                }
            }
            deleteConditions.add(inPredicate);
        } else {
            throw new AnalysisException("Where clause only supports compound predicate, binary predicate, is_null predicate or in predicate");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tbl.toSql());
        if (partitionNames != null) {
            sb.append(" PARTITION (");
            sb.append(Joiner.on(", ").join(partitionNames.getPartitionNames()));
            sb.append(")");
        }
        sb.append(" WHERE ").append(wherePredicate.toSql());
        return sb.toString();
    }

}
