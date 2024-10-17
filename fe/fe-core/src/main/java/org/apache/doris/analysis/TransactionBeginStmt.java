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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.transaction.TransactionEntry;

public class TransactionBeginStmt extends TransactionStmt implements NotFallbackInParser {
    private String label = null;

    public TransactionBeginStmt() {
        this.label = "";
    }

    public TransactionBeginStmt(final String label) {
        this.label = label;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (label == null || label.isEmpty()) {
            label = "txn_insert_" + DebugUtil.printId(analyzer.getContext().queryId());
        }
        if (analyzer.getContext().getTxnEntry() == null) {
            analyzer.getContext().setTxnEntry(new TransactionEntry());
        }
        analyzer.getContext().getTxnEntry().setLabel(label);
        super.analyze(analyzer);
    }

}
