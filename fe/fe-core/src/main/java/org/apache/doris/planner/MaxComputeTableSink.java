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

package org.apache.doris.planner;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.MCInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TMaxComputeTableSink;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MaxComputeTableSink extends BaseExternalTableDataSink {

    private final MaxComputeExternalTable targetTable;

    public MaxComputeTableSink(MaxComputeExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return new HashSet<>();
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("MAXCOMPUTE TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("  TABLE: ").append(targetTable.getName()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        TMaxComputeTableSink tSink = new TMaxComputeTableSink();

        MaxComputeExternalCatalog catalog = (MaxComputeExternalCatalog) targetTable.getCatalog();

        tSink.setAccessKey(catalog.getAccessKey());
        tSink.setSecretKey(catalog.getSecretKey());
        tSink.setEndpoint(catalog.getEndpoint());
        tSink.setProject(catalog.getDefaultProject());
        tSink.setTableName(targetTable.getName());
        tSink.setQuota(catalog.getQuota());
        tSink.setConnectTimeout(catalog.getConnectTimeout());
        tSink.setReadTimeout(catalog.getReadTimeout());
        tSink.setRetryCount(catalog.getRetryTimes());

        // Partition columns
        List<String> partitionColumnNames = targetTable.getPartitionColumns().stream()
                .map(col -> col.getName())
                .collect(Collectors.toList());
        if (!partitionColumnNames.isEmpty()) {
            tSink.setPartitionColumns(partitionColumnNames);
        }

        if (insertCtx.isPresent() && insertCtx.get() instanceof MCInsertCommandContext) {
            MCInsertCommandContext mcCtx = (MCInsertCommandContext) insertCtx.get();
            // Static partition spec
            Map<String, String> staticPartitionSpec = mcCtx.getStaticPartitionSpec();
            if (staticPartitionSpec != null && !staticPartitionSpec.isEmpty()) {
                tSink.setStaticPartitionSpec(staticPartitionSpec);
            }
        }

        // Note: writeSessionId is set later by MCInsertExecutor.beforeExec()
        // after MCTransaction.beginInsert() creates the Storage API session.

        tDataSink = new TDataSink(TDataSinkType.MAXCOMPUTE_TABLE_SINK);
        tDataSink.setMaxComputeTableSink(tSink);
    }

    /**
     * Called by MCInsertExecutor.beforeExec() to inject the writeSessionId
     * after MCTransaction.beginInsert() creates the Storage API session.
     * This must be called before fragments are sent to BE (i.e., before execImpl).
     */
    public void setWriteSessionId(String writeSessionId) {
        if (tDataSink != null && tDataSink.isSetMaxComputeTableSink()) {
            tDataSink.getMaxComputeTableSink().setWriteSessionId(writeSessionId);
        }
    }
}
