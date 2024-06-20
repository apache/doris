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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * Information for a batch insert job
 */
public class BatchInsertJobInfo {

    /**
     * cuncurrent batch size
     */
    private Integer batchSize;

    /**
     * lower bound of the batch
     */
    private Long lowerBound;

    private Long upperBound;

    private SplitColumnInfo splitColumnInfo;

    private InsertIntoTableCommand insertIntoTableCommand;

    private String insertSql;

    /**
     * Constructor for BatchInsertJobInfo
     */
    public BatchInsertJobInfo(Integer batchSize, Long lowerBound, Long upperBound, SplitColumnInfo splitColumnInfo,
                              InsertIntoTableCommand insertIntoTableCommand, String insertSql) {
        this.batchSize = batchSize;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.splitColumnInfo = splitColumnInfo;
        this.insertIntoTableCommand = insertIntoTableCommand;
        this.insertSql = insertSql;
    }

    public void analyze(ConnectContext ctx, StmtExecutor stmtExecutor) throws Exception {
        splitColumnInfo.analyze(ctx);
        //todo Inspect the split column in the source table.
        //check insert into table command
        insertIntoTableCommand.initPlan(ctx, stmtExecutor);
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Long getLowerBound() {
        return lowerBound;
    }

    public Long getUpperBound() {
        return upperBound;
    }

    public SplitColumnInfo getSplitColumnInfo() {
        return splitColumnInfo;
    }

    public InsertIntoTableCommand getInsertIntoTableCommand() {
        return insertIntoTableCommand;
    }

    public String getInsertSql() {
        return insertSql;
    }

}
