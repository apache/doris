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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

public class CancelCloudWarmUpStmt extends CancelStmt implements NotFallbackInParser {
    private Expr whereClause;
    private long jobId;

    public CancelCloudWarmUpStmt(Expr whereClause) {
        this.whereClause = whereClause;
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (!Config.isCloudMode()) {
            throw new AnalysisException("The sql is illegal in disk mode ");
        }
        if (whereClause == null) {
            throw new AnalysisException("Missing job id");
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
        sb.append("CANCEL WARM UP JOB ");
        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
