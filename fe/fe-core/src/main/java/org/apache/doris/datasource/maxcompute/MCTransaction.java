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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.MCInsertCommandContext;
import org.apache.doris.thrift.TMCCommitData;
import org.apache.doris.transaction.Transaction;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MCTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(MCTransaction.class);

    private final MaxComputeExternalCatalog catalog;
    private MaxComputeExternalTable table;
    private final List<TMCCommitData> commitDataList = Lists.newArrayList();

    // Storage API write session ID (created in beginInsert, used in finishInsert)
    private String writeSessionId;

    public MCTransaction(MaxComputeExternalCatalog catalog) {
        this.catalog = catalog;
    }

    public void updateMCCommitData(List<TMCCommitData> commitDataList) {
        synchronized (this) {
            this.commitDataList.addAll(commitDataList);
        }
    }

    public void beginInsert(ExternalTable dorisTable, Optional<InsertCommandContext> ctx) throws UserException {
        this.table = (MaxComputeExternalTable) dorisTable;

        try {
            TableIdentifier tableId = TableIdentifier.of(
                    catalog.getDefaultProject(), table.getName());

            boolean isDynamicPartition = !table.getPartitionColumns().isEmpty();
            boolean isStaticPartition = false;
            String staticPartitionSpecStr = null;

            if (ctx.isPresent() && ctx.get() instanceof MCInsertCommandContext) {
                MCInsertCommandContext mcCtx = (MCInsertCommandContext) ctx.get();
                Map<String, String> staticSpec = mcCtx.getStaticPartitionSpec();
                if (staticSpec != null && !staticSpec.isEmpty()) {
                    isStaticPartition = true;
                    staticPartitionSpecStr = staticSpec.entrySet().stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining(","));
                }
            }

            TableWriteSessionBuilder builder = new TableWriteSessionBuilder()
                    .identifier(tableId)
                    .withSettings(catalog.getSettings());

            if (isStaticPartition) {
                builder.partition(new PartitionSpec(staticPartitionSpecStr));
            } else if (isDynamicPartition) {
                builder.withDynamicPartitionOptions(DynamicPartitionOptions.createDefault());
            }

            TableBatchWriteSession writeSession = builder.buildBatchWriteSession();
            writeSessionId = writeSession.getId();

            LOG.info("Created MC Storage API write session: {} for table {}.{}",
                    writeSessionId, catalog.getDefaultProject(), table.getName());
        } catch (Exception e) {
            throw new UserException("Failed to begin insert for MaxCompute table "
                    + dorisTable.getName() + ": " + e.getMessage(), e);
        }
    }

    public String getWriteSessionId() {
        return writeSessionId;
    }

    public void finishInsert() throws UserException {
        try {
            // Collect all WriterCommitMessages from BEs
            List<WriterCommitMessage> allMessages = new ArrayList<>();
            synchronized (this) {
                for (TMCCommitData data : commitDataList) {
                    if (data.isSetCommitMessage() && !data.getCommitMessage().isEmpty()) {
                        byte[] bytes = Base64.getDecoder().decode(data.getCommitMessage());
                        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                        ObjectInputStream ois = new ObjectInputStream(bais);
                        WriterCommitMessage msg = (WriterCommitMessage) ois.readObject();
                        ois.close();
                        allMessages.add(msg);
                    }
                }
            }

            // Restore session and commit all messages
            TableIdentifier tableId = TableIdentifier.of(
                    catalog.getDefaultProject(), table.getName());
            TableBatchWriteSession commitSession = new TableWriteSessionBuilder()
                    .identifier(tableId)
                    .withSessionId(writeSessionId)
                    .withSettings(catalog.getSettings())
                    .buildBatchWriteSession();

            commitSession.commit(allMessages.toArray(new WriterCommitMessage[0]));
            LOG.info("Committed MC write session {} with {} messages for table {}.{}",
                    writeSessionId, allMessages.size(), catalog.getDefaultProject(), table.getName());
        } catch (Exception e) {
            throw new UserException("Failed to commit MaxCompute write session: " + e.getMessage(), e);
        }
    }

    @Override
    public void commit() throws UserException {
        // commit is handled in finishInsert()
    }

    @Override
    public void rollback() {
        // MC sessions auto-expire if not committed; no explicit rollback needed
        LOG.info("MCTransaction rollback called; uncommitted sessions will auto-expire.");
    }

    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TMCCommitData::getRowCount).sum();
    }
}
