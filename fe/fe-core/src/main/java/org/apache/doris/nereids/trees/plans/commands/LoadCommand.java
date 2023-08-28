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
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.NereidsLoadStmt;
import org.apache.doris.analysis.S3TvfLoadStmt;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.Map;

/**
 * export table
 */
public class LoadCommand extends Command {
    private String path;
    private BrokerDesc brokerDesc;
    private List<DataDescription> sinkInfos;
    private InsertIntoTableCommand loadByInsert;

    /**
     * constructor of ExportCommand
     */
    public LoadCommand(String labelName, List<DataDescription> sinkInfos, BrokerDesc brokerDesc) {
        super(PlanType.LOAD_COMMAND);
        this.path = path.trim();
        this.sinkInfos = sinkInfos;
        this.brokerDesc = brokerDesc;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        NereidsLoadStmt loadStmt = generateInsertStmt();
        Analyzer analyzer = new Analyzer(ctx.getEnv(), ctx);
        loadStmt.analyze(analyzer);
        loadByInsert(ctx);
        // 1. to insert
        //      1.1 build table sink info
        //      1.2 build select sql, and parse sql to query context
        //      1.3 build sink, and put to insert context
        // 2. execute insert stmt

    }

    private static void loadByInsert(ConnectContext ctx) {
        //        List<String> tableName = visitMultipartIdentifier(ctx.tableName);
        //        String labelName = ctx.labelName == null ? null : ctx.labelName.getText();
        //        List<String> colNames = ctx.cols == null ? ImmutableList.of() : visitIdentifierList(ctx.cols);
        //        List<String> partitions = ctx.partition == null ? ImmutableList.of()
        //        : visitIdentifierList(ctx.partition);
        //        UnboundOlapTableSink<?> sink = new UnboundOlapTableSink<>(
        //                tableName,
        //                colNames,
        //                ImmutableList.of(),
        //                partitions,
        //                visitQuery(ctx.query()));
        //        if (ctx.explain() != null) {
        //            return withExplain(sink, ctx.explain());
        //        }
        // return new InsertIntoTableCommand(sink, Optional.ofNullable(labelName));
    }

    private NereidsLoadStmt generateInsertStmt() {
        // PartitionNames partitionNames = null;
        //        if (!this.partitionsNameList.isEmpty()) {
        //            partitionNames = new PartitionNames(false, this.partitionsNameList);
        //        }
        // TableRef tableRef = new TableRef(new TableName(getTableName()), null, partitionNames, null, null, null);
        return null;
    }

    private void executeInsertStmt() {

    }

    /**
     * test
     */
    public static NereidsLoadStmt buildInsertIntoFromMysql() {
        return null;
    }

    /**
     * s
     */
    public static NereidsLoadStmt buildInsertIntoFromRemote(LabelName label, List<DataDescription> dataDescriptions,
                                                            BrokerDesc brokerDesc,
                                                            Map<String, String> properties, String comment)
                throws DdlException {

        final ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().isEnableUnifiedLoad()) {
            if (brokerDesc != null && brokerDesc.getStorageType() == StorageBackend.StorageType.S3) {
                // for tvf solution validation
                return new NereidsLoadStmt(new S3TvfLoadStmt(label, dataDescriptions, brokerDesc, properties, comment));
            }
        }
        return null;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadCommand(this, context);
    }
}
