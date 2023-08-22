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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.Map;

/**
 * export table
 */
public class ExportCommand extends Command {
    private List<String> nameParts;
    private String whereSql;
    private String path;
    private List<String> partitionsNameList;
    private Map<String, String> fileProperties;
    private BrokerDesc brokerDesc;

    /**
     * constructor of ExportCommand
     */
    public ExportCommand(List<String> nameParts, List<String> partitions, String whereSql, String path,
            Map<String, String> fileProperties, BrokerDesc brokerDesc) {
        super(PlanType.EXPORT_COMMAND);
        this.nameParts = nameParts;
        this.partitionsNameList = partitions;
        this.whereSql = whereSql;
        this.path = path.trim();
        this.fileProperties = fileProperties;
        this.brokerDesc = brokerDesc;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        ExportStmt exportStmt = generateExportStmt();
        Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
        exportStmt.analyze(analyzer);
        ctx.getEnv().getExportMgr().addExportJobAndRegisterTask(exportStmt);
    }

    private ExportStmt generateExportStmt() {
        // generate tableRef
        PartitionNames partitionNames = null;
        if (!this.partitionsNameList.isEmpty()) {
            partitionNames = new PartitionNames(false, this.partitionsNameList);
        }
        TableRef tableRef = new TableRef(new TableName(getTableName()), null, partitionNames, null, null, null);
        return new ExportStmt(tableRef, whereSql, path, fileProperties, brokerDesc);
    }

    public String getTableName() {
        return nameParts.stream().map(Utils::quoteIfNeeded)
                .reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExportCommand(this, context);
    }
}
