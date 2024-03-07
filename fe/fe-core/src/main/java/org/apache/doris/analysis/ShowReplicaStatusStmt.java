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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class ShowReplicaStatusStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("Version").add("LastFailedVersion")
            .add("LastSuccessVersion").add("CommittedVersion").add("SchemaHash").add("VersionNum")
            .add("IsBad").add("IsUserDrop").add("State").add("Status")
            .build();

    private TableRef tblRef;
    private Expr where;
    private List<String> partitions = Lists.newArrayList();

    private Operator op;
    private ReplicaStatus statusFilter;

    public ShowReplicaStatusStmt(TableRef tblRef, Expr where) {
        this.tblRef = tblRef;
        this.where = where;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tblRef.getName().analyze(analyzer);
        Util.prohibitExternalCatalog(tblRef.getName().getCtl(), this.getClass().getSimpleName());

        PartitionNames partitionNames = tblRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support showing replica status of temporary partitions");
            }
            partitions.addAll(partitionNames.getPartitionNames());
        }

        if (!analyzeWhere()) {
            throw new AnalysisException(
                    "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
        }
    }

    private boolean analyzeWhere() throws AnalysisException {
        // analyze where clause if not null
        if (where == null) {
            return true;
        }

        if (!(where instanceof BinaryPredicate)) {
            return false;
        }

        BinaryPredicate binaryPredicate = (BinaryPredicate) where;
        op = binaryPredicate.getOp();
        if (op != Operator.EQ && op != Operator.NE) {
            return false;
        }

        Expr leftChild = binaryPredicate.getChild(0);
        if (!(leftChild instanceof SlotRef)) {
            return false;
        }

        String leftKey = ((SlotRef) leftChild).getColumnName();
        if (!leftKey.equalsIgnoreCase("status")) {
            return false;
        }

        Expr rightChild = binaryPredicate.getChild(1);
        if (!(rightChild instanceof StringLiteral)) {
            return false;
        }

        try {
            statusFilter = ReplicaStatus.valueOf(((StringLiteral) rightChild).getStringValue().toUpperCase());
        } catch (Exception e) {
            return false;
        }

        if (statusFilter == null) {
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

    public Operator getOp() {
        return op;
    }

    public ReplicaStatus getStatusFilter() {
        return statusFilter;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
