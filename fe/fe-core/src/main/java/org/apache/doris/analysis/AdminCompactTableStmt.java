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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;

public class AdminCompactTableStmt extends DdlStmt implements NotFallbackInParser {

    private TableRef tblRef;
    private Expr where;
    private List<String> partitions = Lists.newArrayList();

    public enum CompactionType {
        CUMULATIVE,
        BASE
    }

    private CompactionType typeFilter;
    private BinaryPredicate.Operator op;

    public AdminCompactTableStmt(TableRef tblRef, Expr where) {
        this.tblRef = tblRef;
        this.where = where;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tblRef.getName().analyze(analyzer);
        Util.prohibitExternalCatalog(tblRef.getName().getCtl(), this.getClass().getSimpleName());

        PartitionNames partitionNames = tblRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.getPartitionNames().size() != 1) {
                throw new AnalysisException("Only support single partition for compaction");
            }
            partitions.addAll(partitionNames.getPartitionNames());
        } else {
            throw new AnalysisException("No partition selected for compaction");
        }

        // analyze where clause if not null
        if (where == null) {
            throw new AnalysisException("Compaction type must be specified in"
                    + " Where clause like: type = 'BASE/CUMULATIVE'");
        }

        if (!analyzeWhere()) {
            throw new AnalysisException(
                    "Where clause should looks like: type = 'BASE/CUMULATIVE'");
        }
    }

    private boolean analyzeWhere() throws AnalysisException {

        if (!(where instanceof BinaryPredicate)) {
            return false;
        }

        BinaryPredicate binaryPredicate = (BinaryPredicate) where;
        op = binaryPredicate.getOp();
        if (op != BinaryPredicate.Operator.EQ) {
            return false;
        }

        Expr leftChild = binaryPredicate.getChild(0);
        if (!(leftChild instanceof SlotRef)) {
            return false;
        }

        String leftKey = ((SlotRef) leftChild).getColumnName();
        if (!leftKey.equalsIgnoreCase("type")) {
            return false;
        }

        Expr rightChild = binaryPredicate.getChild(1);
        if (!(rightChild instanceof StringLiteral)) {
            return false;
        }

        try {
            typeFilter = CompactionType.valueOf(((StringLiteral) rightChild).getStringValue().toUpperCase());
        } catch (Exception e) {
            return false;
        }

        if (typeFilter == null || (typeFilter != CompactionType.CUMULATIVE && typeFilter != CompactionType.BASE)) {
            return false;
        }

        return true;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public String getCompactionType() {
        if (typeFilter == CompactionType.CUMULATIVE) {
            return "cumulative";
        } else {
            return "base";
        }
    }
}
