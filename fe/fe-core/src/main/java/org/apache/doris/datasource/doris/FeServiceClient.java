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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetBackendMetaRequest;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TGetOlapTableMetaRequest;
import org.apache.doris.thrift.TGetOlapTableMetaResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPartitionMeta;
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
import java.util.stream.Collectors;

public class FeServiceClient {
    private static final Logger LOG = LogManager.getLogger(FeServiceClient.class);

    private final Random random = new Random(System.currentTimeMillis());
    private final String name;
    private final List<TNetworkAddress> addresses;
    private volatile TNetworkAddress master;
    private final String user;
    private final String password;
    private final int retryCount;
    private final int timeout;

    public FeServiceClient(String name, List<TNetworkAddress> addresses, String user, String password,
            int retryCount, int timeout) {
        this.name = name;
        this.addresses = addresses;
        this.user = user;
        this.password = password;
        this.retryCount = retryCount;
        this.timeout = timeout;
    }

    private List<TNetworkAddress> getAddresses() {
        return addresses;
    }

    private FrontendService.Client getRemoteFeClient(TNetworkAddress address, int timeout) {
        try {
            return ClientPool.frontendPool.borrowObject(address, timeout);
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
        throw new RuntimeException(errorMsg + ":" + lastException.getMessage(), lastException);
    }

    private <T> T callFromMaster(ThriftCall<MasterResult<T>> call, String errorMsg, int timeout) {
        TNetworkAddress address = master;
        FrontendService.Client client = null;
        Exception lastException = null;
        if (address != null) {
            client = getRemoteFeClient(address, timeout);
            boolean returnObj = false;
            try {
                MasterResult<T> ret = call.call(client);
                returnObj = true;
                if (ret.isMaster) {
                    if (ret.hasError) {
                        throw new RuntimeException(ret.errorMsg);
                    }
                    return ret.result;
                }
            } catch (TException | IOException e) {
                lastException = e;
            } catch (Exception e) {
                throw new RuntimeException(errorMsg + ":" + e.getMessage(), e);
            } finally {
                returnClient(address, client, returnObj);
            }
        }
        master = null;
        List<TNetworkAddress> addresses = getAddresses();
        int retries = 0;
        while (retries < retryCount) {
            int index = random.nextInt(addresses.size());
            for (int i = 0; i < addresses.size() && retries < retryCount; i++) {
                address = addresses.get((index + i) % addresses.size());
                client = getRemoteFeClient(address, timeout);
                boolean returnObj = false;
                try {
                    MasterResult<T> ret = call.call(client);
                    returnObj = true;
                    if (ret.isMaster) {
                        master = address;
                        if (ret.hasError) {
                            throw new RuntimeException(ret.errorMsg);
                        }
                        return ret.result;
                    }
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
        throw new RuntimeException(errorMsg + ":" + lastException.getMessage(), lastException);
    }

    public List<Backend> listBackends() {
        TGetBackendMetaRequest request = new TGetBackendMetaRequest();
        request.setUser(user);
        request.setPasswd(password);
        String msg = String.format("failed to get backends from remote doris:%s", name);
        return callFromMaster(client -> {
            TGetBackendMetaResult result = client.getBackendMeta(request);
            if (result.getStatus().getStatusCode() == TStatusCode.NOT_MASTER) {
                return MasterResult.notMaster();
            }
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                return MasterResult.masterWithError(result.getStatus().toString());
            }
            List<Backend> backends = result.getBackends().stream()
                    .map(b -> Backend.fromThrift(b))
                    .collect(Collectors.toList());
            return MasterResult.withResult(backends);
        }, msg, timeout);
    }

    public RemoteOlapTable getOlapTable(String dbName, String table, long tableId, List<Partition> partitions) {
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
            return remoteOlapTable;
        }, msg, timeout);
    }

    private interface ThriftCall<T> {
        public T call(FrontendService.Client client) throws Exception;
    }

    private static class MasterResult<T> {
        boolean isMaster = true;
        T result;
        boolean hasError = false;
        String errorMsg;

        static <T> MasterResult<T> notMaster() {
            MasterResult<T> ret = new MasterResult();
            ret.isMaster = false;
            return ret;
        }

        static <T> MasterResult<T> withResult(T result) {
            MasterResult<T> ret = new MasterResult();
            ret.isMaster = true;
            ret.hasError = false;
            ret.result = result;
            return ret;
        }

        // is master but has error code
        static <T> MasterResult<T> masterWithError(String errorMsg) {
            MasterResult<T> ret = new MasterResult();
            ret.isMaster = true;
            ret.hasError = true;
            ret.errorMsg = errorMsg;
            return ret;
        }

    }
}
