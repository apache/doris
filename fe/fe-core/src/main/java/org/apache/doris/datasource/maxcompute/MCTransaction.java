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

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MCTransaction implements Transaction {

    private static final Logger LOG = LogManager.getLogger(MCTransaction.class);

    private final MaxComputeExternalCatalog catalog;
    private MaxComputeExternalTable table;
    private final List<TMCCommitData> commitDataList = Lists.newArrayList();

    // For non-partitioned / static partition: FE pre-creates session
    private TableTunnel tunnel;
    private TableTunnel.UploadSession uploadSession;
    private String preCreatedSessionId;

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
            Odps odps = new Odps(new AliyunAccount(catalog.getAccessKey(), catalog.getSecretKey()));
            odps.setDefaultProject(catalog.getDefaultProject());
            odps.setEndpoint(catalog.getEndpoint());

            tunnel = new TableTunnel(odps);

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

            // Pre-create session for non-partitioned or static partition tables
            if (!isDynamicPartition || isStaticPartition) {
                String project = catalog.getDefaultProject();
                String tableName = table.getName();
                if (isStaticPartition) {
                    PartitionSpec partSpec = new PartitionSpec(staticPartitionSpecStr);
                    uploadSession = tunnel.createUploadSession(project, tableName, partSpec);
                } else {
                    uploadSession = tunnel.createUploadSession(project, tableName);
                }
                preCreatedSessionId = uploadSession.getId();
                LOG.info("Pre-created MC UploadSession: {} for table {}.{}",
                        preCreatedSessionId, project, tableName);

                // Set session ID back to context
                if (ctx.isPresent() && ctx.get() instanceof MCInsertCommandContext) {
                    ((MCInsertCommandContext) ctx.get()).setSessionId(preCreatedSessionId);
                }
            }
        } catch (Exception e) {
            throw new UserException("Failed to begin insert for MaxCompute table "
                    + dorisTable.getName() + ": " + e.getMessage(), e);
        }
    }

    public void finishInsert() throws UserException {
        // Commit all sessions reported by BEs
        Map<String, List<TMCCommitData>> sessionGroups = new HashMap<>();
        synchronized (this) {
            for (TMCCommitData data : commitDataList) {
                String sid = data.isSetSessionId() ? data.getSessionId() : preCreatedSessionId;
                sessionGroups.computeIfAbsent(sid, k -> Lists.newArrayList()).add(data);
            }
        }

        try {
            Odps odps = new Odps(new AliyunAccount(catalog.getAccessKey(), catalog.getSecretKey()));
            odps.setDefaultProject(catalog.getDefaultProject());
            odps.setEndpoint(catalog.getEndpoint());
            TableTunnel commitTunnel = new TableTunnel(odps);

            for (Map.Entry<String, List<TMCCommitData>> entry : sessionGroups.entrySet()) {
                String sessionId = entry.getKey();
                List<TMCCommitData> dataList = entry.getValue();

                // Collect all block IDs for this session
                List<Long> allBlockIds = Lists.newArrayList();
                for (TMCCommitData data : dataList) {
                    if (data.isSetBlockIds()) {
                        allBlockIds.addAll(data.getBlockIds());
                    }
                }

                // Determine partition spec from commit data
                String partitionSpec = null;
                for (TMCCommitData data : dataList) {
                    if (data.isSetPartitionSpec() && !data.getPartitionSpec().isEmpty()) {
                        partitionSpec = data.getPartitionSpec();
                        break;
                    }
                }

                // Get or restore the upload session
                TableTunnel.UploadSession session;
                String project = catalog.getDefaultProject();
                String tableName = table.getName();
                if (partitionSpec != null && !partitionSpec.isEmpty()) {
                    // Convert "key1=val1/key2=val2" to PartitionSpec
                    String specStr = partitionSpec.replace("/", ",");
                    PartitionSpec pSpec = new PartitionSpec(specStr);
                    session = commitTunnel.getUploadSession(project, tableName, pSpec, sessionId);
                } else {
                    session = commitTunnel.getUploadSession(project, tableName, sessionId);
                }

                // Commit the session with block IDs
                Long[] blockArray = allBlockIds.toArray(new Long[0]);
                session.commit(blockArray);
                LOG.info("Committed MC session {} with {} blocks for table {}.{}",
                        sessionId, blockArray.length, project, tableName);
            }
        } catch (Exception e) {
            throw new UserException("Failed to commit MaxCompute upload sessions: " + e.getMessage(), e);
        }
    }

    @Override
    public void commit() throws UserException {
        // commit is handled in finishInsert()
    }

    @Override
    public void rollback() {
        // MC sessions auto-expire after 24h if not committed; no explicit rollback needed
        LOG.info("MCTransaction rollback called; uncommitted sessions will auto-expire.");
    }

    public long getUpdateCnt() {
        return commitDataList.stream().mapToLong(TMCCommitData::getRowCount).sum();
    }
}
