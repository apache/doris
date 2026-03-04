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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TAbortRemoteTxnRequest;
import org.apache.doris.thrift.TAbortRemoteTxnResult;
import org.apache.doris.thrift.TAddOrDropPartitionsRequest;
import org.apache.doris.thrift.TAddOrDropPartitionsResult;
import org.apache.doris.thrift.TBeginRemoteTxnRequest;
import org.apache.doris.thrift.TBeginRemoteTxnResult;
import org.apache.doris.thrift.TCommitRemoteTxnRequest;
import org.apache.doris.thrift.TCommitRemoteTxnResult;
import org.apache.doris.thrift.TGetBackendMetaRequest;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TGetOlapTableMetaRequest;
import org.apache.doris.thrift.TGetOlapTableMetaResult;
import org.apache.doris.thrift.TInsertOverwriteRecordRequest;
import org.apache.doris.thrift.TInsertOverwriteRecordResult;
import org.apache.doris.thrift.TInsertOverwriteRegisterRequest;
import org.apache.doris.thrift.TInsertOverwriteRegisterResult;
import org.apache.doris.thrift.TInsertOverwriteTaskRequest;
import org.apache.doris.thrift.TInsertOverwriteTaskResult;
import org.apache.doris.thrift.TMasterAddressRequest;
import org.apache.doris.thrift.TMasterAddressResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPartitionMeta;
import org.apache.doris.thrift.TRecordFinishedLoadJobRequest;
import org.apache.doris.thrift.TRecordFinishedLoadJobResult;
import org.apache.doris.thrift.TReplacePartitionsRequest;
import org.apache.doris.thrift.TReplacePartitionsResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class FeServiceClient {
    private static final Logger LOG = LogManager.getLogger(FeServiceClient.class);

    private final Random random = new Random(System.currentTimeMillis());
    private final String name;
    private final List<TNetworkAddress> addresses;
    private final String user;
    private final String password;
    private final int retryCount;
    private final int timeoutMs;
    private AtomicReference<TNetworkAddress> master =  new AtomicReference<>(null);

    public FeServiceClient(String name, List<TNetworkAddress> addresses, String user, String password,
                           int retryCount, int timeoutS) {
        this.name = name;
        this.addresses = addresses;
        this.user = user;
        this.password = password;
        this.retryCount = retryCount;
        this.timeoutMs = (int) TimeUnit.SECONDS.toMillis(timeoutS);
    }

    private List<TNetworkAddress> getAddresses() {
        return addresses;
    }

    private FrontendService.Client getRemoteFeClient(TNetworkAddress address, int timeoutMs) {
        try {
            return ClientPool.frontendPool.borrowObject(address, timeoutMs);
        } catch (Exception e) {
            String msg = String.format("failed to get remote doris:%s fe connection", name);
            throw new RuntimeException(msg, e);
        }
    }

    private void returnClient(TNetworkAddress address, FrontendService.Client client, boolean returnObj) {
        if (returnObj) {
            ClientPool.frontendPool.returnObject(address, client);
        } else {
            ClientPool.frontendPool.invalidateObject(address, client);
        }
    }

    private <T> T randomCallWithRetry(ThriftCall<T> call, String errorMsg, int timeout) {
        List<TNetworkAddress> addresses = getAddresses();
        int retries = 0;
        Exception lastException = null;
        while (retries < retryCount) {
            int index = random.nextInt(addresses.size());
            FrontendService.Client client = null;
            for (int i = 0; i < addresses.size() && retries < retryCount; i++) {
                TNetworkAddress address = addresses.get((index + i) % addresses.size());
                client = getRemoteFeClient(address, timeout);
                boolean returnObj = false;
                try {
                    T result = call.call(client);
                    returnObj = true;
                    return result;
                } catch (TException | IOException e) {
                    lastException = e;
                    retries++;
                } catch (Exception e) {
                    throw new RuntimeException(errorMsg + ":" + e.getMessage(), e);
                } finally {
                    returnClient(address, client, returnObj);
                }
            }
        }
        String lastMessage = lastException == null ? "unknown" : lastException.getMessage();
        throw new RuntimeException(errorMsg + ":" + lastMessage, lastException);
    }

    public static class TResultAdapter {
        public static TStatus getStatus(Object obj) {
            if (obj instanceof TBeginRemoteTxnResult) {
                return ((TBeginRemoteTxnResult) obj).getStatus();
            } else if (obj instanceof TCommitRemoteTxnResult) {
                return ((TCommitRemoteTxnResult) obj).getStatus();
            } else if (obj instanceof TAbortRemoteTxnResult) {
                return ((TAbortRemoteTxnResult) obj).getStatus();
            } else if (obj instanceof TMasterAddressResult) {
                return ((TMasterAddressResult) obj).getStatus();
            } else if (obj instanceof TAddOrDropPartitionsResult) {
                return ((TAddOrDropPartitionsResult) obj).getStatus();
            } else if (obj instanceof TReplacePartitionsResult) {
                return ((TReplacePartitionsResult) obj).getStatus();
            } else if (obj instanceof TInsertOverwriteRegisterResult) {
                return ((TInsertOverwriteRegisterResult) obj).getStatus();
            } else if (obj instanceof TInsertOverwriteTaskResult) {
                return ((TInsertOverwriteTaskResult) obj).getStatus();
            } else if (obj instanceof TInsertOverwriteRecordResult) {
                return ((TInsertOverwriteRecordResult) obj).getStatus();
            } else if (obj instanceof TRecordFinishedLoadJobResult) {
                return ((TRecordFinishedLoadJobResult) obj).getStatus();
            }
            throw new IllegalArgumentException("unsupported result type: " + obj.getClass().getName());
        }

        public static TNetworkAddress getMasterAddress(Object obj) {
            if (obj instanceof TBeginRemoteTxnResult) {
                return ((TBeginRemoteTxnResult) obj).getMasterAddress();
            } else if (obj instanceof TCommitRemoteTxnResult) {
                return ((TCommitRemoteTxnResult) obj).getMasterAddress();
            } else if (obj instanceof TAbortRemoteTxnResult) {
                return ((TAbortRemoteTxnResult) obj).getMasterAddress();
            } else if (obj instanceof TMasterAddressResult) {
                return ((TMasterAddressResult) obj).getMasterAddress();
            } else if (obj instanceof TAddOrDropPartitionsResult) {
                return ((TAddOrDropPartitionsResult) obj).getMasterAddress();
            } else if (obj instanceof TReplacePartitionsResult) {
                return ((TReplacePartitionsResult) obj).getMasterAddress();
            } else if (obj instanceof TInsertOverwriteRegisterResult) {
                return ((TInsertOverwriteRegisterResult) obj).getMasterAddress();
            } else if (obj instanceof TInsertOverwriteTaskResult) {
                return ((TInsertOverwriteTaskResult) obj).getMasterAddress();
            } else if (obj instanceof TInsertOverwriteRecordResult) {
                return ((TInsertOverwriteRecordResult) obj).getMasterAddress();
            } else if (obj instanceof TRecordFinishedLoadJobResult) {
                return ((TRecordFinishedLoadJobResult) obj).getMasterAddress();
            }
            throw new IllegalArgumentException("unsupported result type: " + obj.getClass().getName());
        }
    }

    private <T> T masterCallWithRetry(ThriftCall<T> call, String errorMsg, int timeout) {
        List<TNetworkAddress> addresses = getAddresses();
        int retries = 0;
        Exception lastException = null;
        int index = random.nextInt(addresses.size());
        TNetworkAddress address = master.get();
        if (address == null) {
            address = addresses.get((index) % addresses.size());
        }
        FrontendService.Client client = null;
        while (retries < retryCount) {
            TNetworkAddress clientAddr = address;
            client = getRemoteFeClient(clientAddr, timeout);
            boolean returnObj = false;
            try {
                T result = call.call(client);
                returnObj = true;
                if (TResultAdapter.getMasterAddress(result) != null) {
                    master.set(TResultAdapter.getMasterAddress(result));
                    address = (TResultAdapter.getMasterAddress(result));
                }
                if (TResultAdapter.getStatus(result).getStatusCode() == TStatusCode.NOT_MASTER) {
                    if (TResultAdapter.getMasterAddress(result) == null) {
                        index++;
                        address = addresses.get((index) % addresses.size());
                    }
                    retries++;
                    continue;
                }
                return result;
            } catch (TException | IOException e) {
                lastException = e;
                retries++;
                index++;
                address = (addresses.get((index) % addresses.size()));
            } catch (Exception e) {
                throw new RuntimeException(errorMsg + ":" + e.getMessage(), e);
            } finally {
                returnClient(clientAddr, client, returnObj);
            }
        }

        String lastMessage = lastException == null ? "master not found in retry times" : lastException.getMessage();
        throw new RuntimeException(errorMsg + ":" + lastMessage, lastException);
    }

    public List<Backend> listBackends() {
        TGetBackendMetaRequest request = new TGetBackendMetaRequest();
        request.setUser(user);
        request.setPasswd(password);
        String msg = String.format("failed to get backends from remote doris:%s", name);
        return randomCallWithRetry(client -> {
            TGetBackendMetaResult result = client.getBackendMeta(request);
            return result.getBackends().stream()
                    .map(b -> Backend.fromThrift(b))
                    .collect(Collectors.toList());
        }, msg, timeoutMs);
    }

    public RemoteOlapTable getOlapTable(String dbName, String table, long tableId, List<Partition> partitions,
                                        List<Partition> tempPartitions) {
        TGetOlapTableMetaRequest request = new TGetOlapTableMetaRequest();
        request.setDb(dbName);
        request.setTable(table);
        request.setTableId(tableId);
        request.setUser(user);
        request.setPasswd(password);
        request.setVersion(FeConstants.meta_version);
        for (Partition partition : partitions) {
            TPartitionMeta meta = new TPartitionMeta();
            meta.setId(partition.getId());
            meta.setVisibleVersion(partition.getVisibleVersion());
            meta.setVisibleVersionTime(partition.getVisibleVersionTime());
            request.addToPartitions(meta);
        }
        for (Partition partition : tempPartitions) {
            TPartitionMeta meta = new TPartitionMeta();
            meta.setId(partition.getId());
            meta.setVisibleVersion(partition.getVisibleVersion());
            meta.setVisibleVersionTime(partition.getVisibleVersionTime());
            request.addToTempPartitions(meta);
        }
        String msg = String.format("failed to get table meta from remote doris:%s", name);
        return randomCallWithRetry(client -> {
            TGetOlapTableMetaResult result = client.getOlapTableMeta(request);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                throw new UserException(result.getStatus().toString());
            }
            RemoteOlapTable remoteOlapTable = null;
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(result.getTableMeta()))) {
                OlapTable olapTable = OlapTable.read(in);
                remoteOlapTable = RemoteOlapTable.fromOlapTable(olapTable);
            }
            List<Partition> updatedPartitions = new ArrayList<>(result.getUpdatedPartitionsSize());
            if (result.getUpdatedPartitionsSize() > 0) {
                for (ByteBuffer buffer : result.getUpdatedPartitions()) {
                    try (ByteArrayInputStream in =
                            new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
                            DataInputStream dataInputStream = new DataInputStream(in)) {
                        String partitionStr = Text.readString(dataInputStream);
                        Partition partition = GsonUtils.GSON.fromJson(partitionStr, Partition.class);
                        updatedPartitions.add(partition);
                    }
                }
            }
            List<Long> removedPartitions = result.getRemovedPartitions();
            if (removedPartitions == null) {
                removedPartitions = new ArrayList<>();
            }
            remoteOlapTable.rebuildPartitions(partitions, updatedPartitions, removedPartitions);
            // rebuild temp partitions
            if (result.isSetUpdatedTempPartitions() && result.getUpdatedTempPartitionsSize() > 0) {
                updatedPartitions = new ArrayList<>(result.getUpdatedTempPartitionsSize());
                for (ByteBuffer buffer : result.getUpdatedTempPartitions()) {
                    try (ByteArrayInputStream in =
                            new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.remaining());
                            DataInputStream dataInputStream = new DataInputStream(in)) {
                        String partitionStr = Text.readString(dataInputStream);
                        Partition partition = GsonUtils.GSON.fromJson(partitionStr, Partition.class);
                        updatedPartitions.add(partition);
                    }
                }
            }
            removedPartitions = result.getRemovedTempPartitions();
            if (removedPartitions == null) {
                removedPartitions = new ArrayList<>();
            }
            remoteOlapTable.rebuildTempPartitions(tempPartitions, updatedPartitions, removedPartitions);
            return remoteOlapTable;
        }, msg, timeoutMs);
    }

    public TBeginRemoteTxnResult beginRemoteTxn(TBeginRemoteTxnRequest request) throws Exception {
        request.setUser(user);
        request.setPasswd(password);
        String msg = String.format("failed to begin remote txn from remote doris:%s, label=%s", name,
                request.getLabel());
        long startTime = System.currentTimeMillis();
        TBeginRemoteTxnResult result;
        try {
            result = masterCallWithRetry(client -> client.beginRemoteTxn(request), msg,
                    Math.max(timeoutMs, (int) request.getTimeoutMs()));
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("begin remote txn for catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }

        long costMs = System.currentTimeMillis() - startTime;
        LOG.info("begin remote txn for catalog {} finished, cost={}ms, statusCode={}",
                name, costMs, result.getStatus().getStatusCode());
        return result;
    }

    public TCommitRemoteTxnResult commitRemoteTxn(TCommitRemoteTxnRequest request) throws Exception {
        request.setUser(user);
        request.setPasswd(password);
        String msg = String.format("failed to commit remote txn from remote doris:%s, txnId=%d", name,
                request.getTxnId());
        long startTime = System.currentTimeMillis();
        TCommitRemoteTxnResult result;
        try {
            result = masterCallWithRetry(client -> client.commitRemoteTxn(request), msg,
                    Math.max(timeoutMs, (int) request.getInsertVisibleTimeoutMs()));
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("commit remote txn for catalog {} failed, txnId={}, cost={}ms",
                    name, request.getTxnId(), costMs, e);
            throw e;
        }

        long costMs = System.currentTimeMillis() - startTime;
        LOG.info("commit remote txn for catalog {} finished, txnId={}, cost={}ms, statusCode={}",
                name, request.getTxnId(), costMs, result.getStatus().getStatusCode());
        return result;
    }

    public TAbortRemoteTxnResult abortRemoteTxn(TAbortRemoteTxnRequest request) throws Exception {
        request.setUser(user);
        request.setPasswd(password);
        String msg = String.format("failed to abort remote txn from remote doris:%s, txnId=%d", name,
                request.getTxnId());
        long startTime = System.currentTimeMillis();
        TAbortRemoteTxnResult result;
        try {
            result = masterCallWithRetry(client -> client.abortRemoteTxn(request), msg, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("abort remote txn for catalog {} failed, txnId={}, cost={}ms",
                    name, request.getTxnId(), costMs, e);
            throw e;
        }

        long costMs = System.currentTimeMillis() - startTime;
        LOG.info("abort remote txn for catalog {} finished, txnId={}, cost={}ms, statusCode={}",
                name, request.getTxnId(), costMs, result.getStatus().getStatusCode());
        return result;
    }

    private interface ThriftCall<T> {
        public T call(FrontendService.Client client) throws Exception;
    }

    public TNetworkAddress getMasterAddress() throws RuntimeException {
        TMasterAddressRequest request = new TMasterAddressRequest();
        request.setUser(user);
        request.setPasswd(password);
        long startTime = System.currentTimeMillis();
        TMasterAddressResult result;
        try {
            result = masterCallWithRetry(client -> client.getMasterAddress(request),
                    "failed to get master address from remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("get master address for catalog {} failed, cost={}ms", name, costMs, e);
            throw new RuntimeException(Util.getRootCauseMessage(e), e);
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("get master address for catalog {} failed, err={}", name,
                    result.getStatus().getErrorMsgs().get(0));
            throw new RuntimeException(result.getStatus().getErrorMsgs().get(0));
        }
        if (master != null) {
            return master.get();
        } else {
            return null;
        }
    }

    public void addPartitions(String catalogName, String dbName, String tableName, List<String> partitionNames,
                              List<String> tempPartitionNames, boolean isTemp) throws DdlException {
        TAddOrDropPartitionsRequest request = new TAddOrDropPartitionsRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setPartitionNames(partitionNames);
        request.setTempPartitionNames(tempPartitionNames);
        request.setIsTemp(isTemp);
        request.setIsDrop(false);
        long startTime = System.currentTimeMillis();
        TAddOrDropPartitionsResult result;
        try {
            result = masterCallWithRetry(client -> client.addOrDropPartitions(request),
                    "failed to add partitions to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("add partitions to catalog {} failed, cost={}ms", name, costMs, e);
            throw new DdlException(Util.getRootCauseMessage(e), e);
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("add partitions to catalog {} failed, err={}", name,
                    result.getStatus().getErrorMsgs().get(0));
            throw new DdlException(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public boolean dropPartitions(String catalogName, String dbName, String tableName, List<String> partitionNames,
                                  boolean isTemp, boolean isForce) {
        TAddOrDropPartitionsRequest request = new TAddOrDropPartitionsRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setPartitionNames(partitionNames);
        request.setIsTemp(isTemp);
        request.setIsForce(isForce);
        request.setIsDrop(true);
        long startTime = System.currentTimeMillis();
        TAddOrDropPartitionsResult result;
        try {
            result = masterCallWithRetry(client -> client.addOrDropPartitions(request),
                    "failed to drop partitions to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("drop partitions to catalog {} failed, cost={}ms", name, costMs, e);
            return false;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("drop partitions to catalog {} failed, db={}, tbl={}, err={}", name, dbName, tableName,
                    result.getStatus().getErrorMsgs().get(0));
            return false;
        }
        return true;
    }

    public void replacePartitions(String catalogName, String dbName, String tableName, List<String> partitionNames,
                                  List<String> tempPartitionNames, boolean isForce) throws DdlException {
        TReplacePartitionsRequest request = new TReplacePartitionsRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setPartitionNames(partitionNames);
        request.setTempPartitionNames(tempPartitionNames);
        request.setIsForce(isForce);
        long startTime = System.currentTimeMillis();
        TReplacePartitionsResult result;
        try {
            result = masterCallWithRetry(client -> client.replacePartitions(request),
                    "failed to replace partitions to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("replace partitions to catalog {} failed, cost={}ms", name, costMs, e);
            throw new DdlException(Util.getRootCauseMessage(e), e);
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("replace partitions to catalog {} failed, db={}, tbl={}, err={}", name, dbName, tableName,
                    result.getStatus().getErrorMsgs().get(0));
            throw new DdlException(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public long registerTask(String catalogName, String dbName, String tableName,
                             List<String> tempPartitionNames) throws Exception {
        TInsertOverwriteRegisterRequest request = new TInsertOverwriteRegisterRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setPartitionNames(tempPartitionNames);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteRegisterResult result;
        try {
            result = masterCallWithRetry(client -> client.registerInsertOverwriteTask(request),
                    "failed to register insert overwrite task to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("register insert overwrite task to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("register insert overwrite task to catalog {} failed, db={}, tbl={}, err={}", name,
                    dbName, tableName, result.getStatus().getErrorMsgs().get(0));
            throw new DdlException(result.getStatus().getErrorMsgs().get(0));
        }
        return result.getTaskId();
    }

    public long registerTaskGroup(String catalogName, String dbName, String tableName) throws Exception {
        TInsertOverwriteRegisterRequest request = new TInsertOverwriteRegisterRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteRegisterResult result;
        try {
            result = masterCallWithRetry(client -> client.registerInsertOverwriteTask(request),
                    "failed to register insert overwrite task to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("register insert overwrite task to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("register insert overwrite task group to catalog {} failed, db={}, tbl={}, err={}", name,
                    dbName, tableName, result.getStatus().getErrorMsgs().get(0));
            throw new DdlException(result.getStatus().getErrorMsgs().get(0));
        }
        return result.getGroupId();
    }

    public void registerTaskInGroup(long groupId, long taskId) throws Exception {
        TInsertOverwriteRegisterRequest request = new TInsertOverwriteRegisterRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setGroupId(groupId);
        request.setTaskId(taskId);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteRegisterResult result;
        try {
            result = masterCallWithRetry(client -> client.registerInsertOverwriteTask(request),
                    "failed to register insert overwrite task to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("register insert overwrite task to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("register iot in group to catalog {} failed, groupId={}, taskId={}, err={}", name,
                    groupId, taskId, result.getStatus().getErrorMsgs().get(0));
            throw new DdlException(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void taskGroupSuccess(String catalogName, String dbName, String tableName, long groupId)
            throws DdlException {
        TInsertOverwriteTaskRequest request = new TInsertOverwriteTaskRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setGroupId(groupId);
        request.setIsSuccess(true);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteTaskResult result;
        try {
            result = masterCallWithRetry(client -> client.insertOverwriteTaskAction(request),
                    "failed to task group success to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("task group success to catalog {} failed, cost={}ms", name, costMs, e);
            throw new DdlException(Util.getRootCauseMessage(e), e);
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("task group success to catalog {} failed, groupId={}, err={}", name,
                    groupId, result.getStatus().getErrorMsgs().get(0));
            throw new DdlException(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void taskSuccess(long taskId) throws Exception {
        TInsertOverwriteTaskRequest request = new TInsertOverwriteTaskRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setIsSuccess(true);
        request.setTaskId(taskId);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteTaskResult result;
        try {
            result = masterCallWithRetry(client -> client.insertOverwriteTaskAction(request),
                    "failed to task success to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("task success to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("task success to catalog {} failed, taskId={}, err={}", name,
                    taskId, result.getStatus().getErrorMsgs().get(0));
            throw new Exception(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void taskFail(long taskId) throws Exception {
        TInsertOverwriteTaskRequest request = new TInsertOverwriteTaskRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setIsSuccess(false);
        request.setTaskId(taskId);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteTaskResult result;
        try {
            result = masterCallWithRetry(client -> client.insertOverwriteTaskAction(request),
                    "failed to task fail to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("task fail to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("task fail to catalog {} failed, taskId={}, err={}", name,
                    taskId, result.getStatus().getErrorMsgs().get(0));
            throw new Exception(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void taskGroupFail(long groupId) throws Exception {
        TInsertOverwriteTaskRequest request = new TInsertOverwriteTaskRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setGroupId(groupId);
        request.setIsSuccess(false);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteTaskResult result;
        try {
            result = masterCallWithRetry(client -> client.insertOverwriteTaskAction(request),
                    "failed to task group fail to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("task group fail to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("task group fail to catalog {} failed, groupId={}, err={}", name,
                    groupId, result.getStatus().getErrorMsgs().get(0));
            throw new Exception(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void recordRunningTableOrException(String catalogName, String dbName, String tableName) throws Exception {
        TInsertOverwriteRecordRequest request = new TInsertOverwriteRecordRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setIsAdd(true);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteRecordResult result;
        try {
            result = masterCallWithRetry(client -> client.addOrDropInsertOverwriteRecord(request),
                    "failed to record running table or exception to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("record running table or exception to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("record running table or exception to catalog {} failed, err={}", name,
                    result.getStatus().getErrorMsgs().get(0));
            throw new Exception(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void dropRunningRecord(String catalogName, String dbName, String tableName) throws Exception {
        TInsertOverwriteRecordRequest request = new TInsertOverwriteRecordRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setIsAdd(false);
        long startTime = System.currentTimeMillis();
        TInsertOverwriteRecordResult result;
        try {
            result = masterCallWithRetry(client -> client.addOrDropInsertOverwriteRecord(request),
                    "failed to drop running record to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("drop running record to catalog {} failed, cost={}ms", name, costMs, e);
            throw e;
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("drop running record to catalog {} failed, err={}", name,
                    result.getStatus().getErrorMsgs().get(0));
            throw new Exception(result.getStatus().getErrorMsgs().get(0));
        }
    }

    public void recordFinishedLoadJob(String label, long transactionId, String catalogName, String dbName,
                                      String tableName, long createTimestamp, String failMsg, String trackingUrl,
                                      String firstErrorMsg, long jobId) throws MetaNotFoundException {
        TRecordFinishedLoadJobRequest request = new TRecordFinishedLoadJobRequest();
        request.setUser(user);
        request.setPasswd(password);
        request.setCatalog(catalogName);
        request.setLabel(label);
        request.setDb(dbName);
        request.setTbl(tableName);
        request.setTxnId(transactionId);
        request.setCreateTs(createTimestamp);
        request.setFailMsg(failMsg);
        request.setTrackingUrl(trackingUrl);
        request.setFirstErrMsg(firstErrorMsg);
        request.setJobId(jobId);
        long startTime = System.currentTimeMillis();
        TRecordFinishedLoadJobResult result;
        try {
            result = masterCallWithRetry(client -> client.recordFinishedLoadJobRequest(request),
                    "failed to record finished load job to remote doris:" + name, timeoutMs);
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - startTime;
            LOG.warn("record finished load job to catalog {} failed, cost={}ms", name, costMs, e);
            throw new MetaNotFoundException(Util.getRootCauseMessage(e), e);
        }
        if (result.getStatus().getStatusCode() != TStatusCode.OK) {
            LOG.warn("record finished load job to catalog {} failed, err={}", name,
                    result.getStatus().getErrorMsgs().get(0));
            throw new MetaNotFoundException(result.getStatus().getErrorMsgs().get(0));
        }
    }
}
