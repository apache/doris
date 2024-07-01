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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

    /**
     * analyze job info from insert sql
     * @param ctx ctx
     * @param stmtExecutor executor
     * @throws Exception if analyze failed
     */
    public void analyze(ConnectContext ctx, StmtExecutor stmtExecutor) throws Exception {
        splitColumnInfo.analyze(ctx);
        LogicalPlan logicalPlan = insertIntoTableCommand.getLogicalQuery();
        List<UnboundRelation> unboundRelationList = new ArrayList<>((logicalPlan
                .collect(UnboundRelation.class::isInstance)));
        if (unboundRelationList.isEmpty()) {
            throw new AnalysisException("No unbound relation found, please check your sql: " + this.insertSql);
        }
        boolean splitTableFound = false;
        for (UnboundRelation unboundRelation : unboundRelationList) {
            String tableName = unboundRelation.getTableName();
            List<String> tableNameParts = getTableNameParts(tableName);
            TableNameInfo tableNameInfo = new TableNameInfo(tableNameParts);
            tableNameInfo.analyze(ctx);
            TableIf table = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                    .getDbOrAnalysisException(tableNameInfo.getDb())
                    .getTableOrAnalysisException(tableNameInfo.getTbl());
            if (table.getDatabase().getCatalog().getName().equals(splitColumnInfo.getTableNameInfo().getCtl())
                    && table.getDatabase().getFullName().equals(splitColumnInfo.getTableNameInfo().getDb())
                    && table.getName().equals(splitColumnInfo.getTableNameInfo().getTbl())) {

                splitTableFound = true;
                break;
            }
        }
        if (!splitTableFound) {
            throw new AnalysisException("Split table: " + splitColumnInfo.getTableNameInfo().getTbl()
                    + " not found in the query table. insert sql is: " + insertSql);
        }
        //check insert into table command
        insertIntoTableCommand.initPlan(ctx, stmtExecutor, false);
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

    private List<String> getTableNameParts(String tableName) {
        if (!tableName.contains(".")) {
            return Collections.singletonList(tableName);
        }
        return Arrays.stream(tableName.split("\\."))
                .collect(Collectors.toList());
    }

}
