// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.analysis.CompoundPredicate.Operator;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.util.PrintableMap;

import com.google.common.base.Strings;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DeleteStmt extends DdlStmt {
    private final TableName tbl;
    private final String partitionName;
    private Expr wherePredicate;

    private List<Predicate> deleteConditions;
    private Map<String, String> properties;

    public DeleteStmt(TableName tableName, String partitionName, Expr wherePredicate, Map<String, String> properties) {
        this.tbl = tableName;
        this.partitionName = partitionName;
        this.wherePredicate = wherePredicate;
        this.deleteConditions = new LinkedList<Predicate>();

        this.properties = properties;
    }
    
    public String getTableName() {
        return tbl.getTbl();
    }
    
    public String getDbName() {
        return tbl.getDb();
    }
    
    public String getPartitionName() {
        return partitionName;
    }
    
    public List<Predicate> getDeleteConditions() {
        return deleteConditions;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        
        if (tbl == null) {
            throw new AnalysisException("Table is not set");
        }

        tbl.analyze(analyzer);
        
        if (Strings.isNullOrEmpty(partitionName)) {
            throw new AnalysisException("Partition is not set");
        }

        if (wherePredicate == null) {
            throw new AnalysisException("Where clause is not set");
        }

        // analyze predicate
        analyzePredicate(wherePredicate);

        // check access
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), tbl.getDb(), AccessPrivilege.READ_WRITE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getUser(), tbl.getDb());
        }
    }

    private void analyzePredicate(Expr predicate) throws AnalysisException {
        if (predicate instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
            Expr leftExpr = binaryPredicate.getChild(0);
            if (!(leftExpr instanceof SlotRef)) {
                throw new AnalysisException("Left expr should be column name");
            }
            Expr rightExpr = binaryPredicate.getChild(1);
            if (!(rightExpr instanceof LiteralExpr)) {
                throw new AnalysisException("Right expr should be value");
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
                throw new AnalysisException("Left expr should be column name");
            }
            deleteConditions.add(isNullPredicate);
        } else {
            throw new AnalysisException("Where clause should be compound or binary predicate");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(tbl.toSql());
        sb.append(" PARTITION ").append(partitionName);
        sb.append(" WHERE ").append(wherePredicate.toSql());
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

}
