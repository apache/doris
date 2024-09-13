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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShowCloudWarmUpStmt extends ShowStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowCloudWarmUpStmt.class);
    private Expr whereClause;
    private boolean showAllJobs = false;
    private long jobId = -1;

    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId")
            .add("ClusterName")
            .add("Status")
            .add("Type")
            .add("CreateTime")
            .add("FinishBatch")
            .add("AllBatch")
            .add("FinishTime")
            .add("ErrMsg")
            .build();

    public ShowCloudWarmUpStmt(Expr whereClause) {
        this.whereClause = whereClause;
    }

    public long getJobId() {
        return jobId;
    }

    public boolean showAllJobs() {
        return showAllJobs;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (whereClause == null) {
            showAllJobs = true;
            return;
        }
        boolean valid = true;
        CHECK: {
            if (whereClause instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) whereClause.getChild(0)).getColumnName();
            if (leftKey.equalsIgnoreCase("id") && (whereClause.getChild(1) instanceof IntLiteral)) {
                jobId = ((IntLiteral) whereClause.getChild(1)).getLongValue();
            } else {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like one of them: id = 123");
        }

    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW WARM UP JOB ");
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ShowCloudWarmUpStmt.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
