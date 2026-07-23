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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonTransaction;
import org.apache.doris.datasource.paimon.PaimonWriteBinding;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteBackendType;
import org.apache.doris.thrift.TPaimonWriteMode;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Paimon table sink.
 *
 * Generates TPaimonTableSink payload consumed by BE, including serialized table
 * metadata, Hadoop authentication config, transaction identity, write mode,
 * and sink column names.
 *
 * v1: single-writer architecture; partition/bucket routing delegated to SDK.
 */
public class PaimonTableSink extends BaseExternalTableDataSink {
    private final PaimonExternalTable targetTable;
    private List<Expr> outputExprs;
    private List<Column> cols;

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public PaimonTableSink(PaimonExternalTable targetTable) {
        super();
        this.targetTable = targetTable;
    }

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = outputExprs;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("PAIMON TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("  table: ").append(targetTable.getName()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        TPaimonTableSink tSink = new TPaimonTableSink();
        PaimonInsertCommandContext ctx = (PaimonInsertCommandContext) insertCtx.get();
        Preconditions.checkState(ctx.getTxnId() > 0,
                "Paimon transaction must begin before sink binding");

        PaimonTransaction transaction;
        PaimonWriteBinding binding;
        try {
            transaction = (PaimonTransaction) targetTable.getCatalog()
                    .getTransactionManager().getTransaction(ctx.getTxnId());
            binding = PaimonWriteBinding.create(targetTable, ctx);
        } catch (AnalysisException e) {
            throw e;
        } catch (UserException e) {
            throw new AnalysisException("Failed to bind Paimon write transaction: "
                    + e.getMessage(), e);
        }
        transaction.bind(binding);

        tSink.setTransactionId(ctx.getTxnId());
        tSink.setCommitUser(ctx.getCommitUser());

        // Thrift column_names is the single column-order protocol shared by BE
        // Arrow conversion and the Java writer schema.
        List<String> outputColumnNames = outputColumnNames();

        // FE owns table metadata resolution. BE and the JNI writer consume this
        // exact table instance instead of loading catalog metadata independently.
        tSink.setSerializedTable(binding.getSerializedTable());

        tSink.setBackendType(TPaimonWriteBackendType.JNI);
        if (ctx.isOverwrite()) {
            tSink.setWriteMode(TPaimonWriteMode.OVERWRITE);
        } else {
            tSink.setWriteMode(TPaimonWriteMode.APPEND);
        }

        tSink.setHadoopConfig(binding.getHadoopConfig());

        tSink.setColumnNames(outputColumnNames);

        tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        tDataSink.setPaimonTableSink(tSink);
    }

    private List<String> outputColumnNames() throws AnalysisException {
        if (cols.size() != outputExprs.size()) {
            throw new AnalysisException("Paimon sink output column size mismatch, columns="
                    + cols.size() + ", exprs=" + outputExprs.size());
        }
        List<String> names = new ArrayList<>(cols.size());
        for (Column col : cols) {
            names.add(col.getName());
        }
        return names;
    }

}
