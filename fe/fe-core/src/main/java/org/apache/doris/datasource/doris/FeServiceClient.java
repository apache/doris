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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetBackendMetaRequest;
import org.apache.doris.thrift.TGetBackendMetaResult;
import org.apache.doris.thrift.TGetDbsParams;
import org.apache.doris.thrift.TGetDbsResult;
import org.apache.doris.thrift.TGetOlapTableMetaRequest;
import org.apache.doris.thrift.TGetOlapTableMetaResult;
import org.apache.doris.thrift.TGetTablesParams;
import org.apache.doris.thrift.TGetTablesResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.TException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class FeServiceClient {
    private final Random random = new Random(System.currentTimeMillis());
    private final String name;
    private final List<TNetworkAddress> addresses;
    private volatile TNetworkAddress master;

    public FeServiceClient(String name, List<TNetworkAddress> addresses) {
        this.name = name;
        this.addresses = addresses;
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
        int index = random.nextInt(addresses.size());
        FrontendService.Client client = null;
        TException lastException = null;
        for (int i = 0; i < addresses.size(); i++) {
            TNetworkAddress address = addresses.get((index + i) % addresses.size());
            client = getRemoteFeClient(address, timeout);
            boolean returnObj = false;
            try {
                T result = call.call(client);
                returnObj = true;
                return result;
            } catch (TException e) {
                lastException = e;
            } catch (Exception e) {
                throw new RuntimeException(errorMsg, e);
            } finally {
                returnClient(address, client, returnObj);
            }
        }
        throw new RuntimeException(errorMsg, lastException);
    }

    private <T> T callFromMaster(ThriftCall<MasterResult<T>> call, String errorMsg, int timeout) {
        TNetworkAddress address = master;
        FrontendService.Client client = null;
        TException lastException = null;
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
            } catch (TException e) {
                lastException = e;
            } catch (Exception e) {
                throw new RuntimeException(errorMsg, e);
            } finally {
                returnClient(address, client, returnObj);
            }
        }
        master = null;
        List<TNetworkAddress> addresses = getAddresses();
        int index = random.nextInt(addresses.size());
        for (int i = 0; i < addresses.size(); i++) {
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
            } catch (TException e) {
                lastException = e;
            } catch (Exception e) {
                throw new RuntimeException(errorMsg, e);
            } finally {
                returnClient(address, client, returnObj);
            }
        }
        throw new RuntimeException(errorMsg, lastException);
    }

    public List<String> listDatabaseNames(String catalog, String user, String userIp, int timeout) {
        TGetDbsParams params = new TGetDbsParams();
        params.setCatalog(catalog);
        params.setUser(user);
        params.setUserIp(userIp);

        String msg = String.format("failed to get db from remote doris:%s", name);
        return randomCallWithRetry(client -> {
            TGetDbsResult tGetDbsResult = client.getDbNames(params);
            return tGetDbsResult.getDbs();
        }, msg, timeout);
    }

    public List<String> listTableNames(String catalog, String dbName, String user, String userIp, int timeout) {
        TGetTablesParams params = new TGetTablesParams();
        params.setCatalog(catalog);
        params.setDb(dbName);
        params.setUser(user);
        params.setUserIp(userIp);
        String msg = String.format("failed to get tables from remote doris:%s db:%s", name, dbName);
        return randomCallWithRetry(client -> {
            TGetTablesResult result = client.getTableNames(params);
            return result.getTables();
        }, msg, timeout);
    }

    public List<Backend> listBackends(String user, String password, int timeout) {
        TGetBackendMetaRequest request = new TGetBackendMetaRequest();
        request.setUser(user);
        request.setPasswd(password);
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
        }, "", timeout);
    }

    public OlapTable getOlapTable(String dbName, String table, String user,
            String password, int timeout) {
        TGetOlapTableMetaRequest request = new TGetOlapTableMetaRequest();
        request.setDb(dbName);
        request.setTable(table);
        request.setUser(user);
        request.setPasswd(password);
        request.setVersion(1);
        String msg = String.format("failed to get table meta from remote doris:%s", name);
        return randomCallWithRetry(client -> {
            TGetOlapTableMetaResult result = client.getOlapTableMeta(request);
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                throw new UserException(result.getStatus().toString());
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(result.getMeta()))) {
                return OlapTable.read(in);
            }
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
            MasterResult ret = new MasterResult();
            ret.isMaster = false;
            return ret;
        }

        static <T> MasterResult<T> withResult(T result) {
            MasterResult ret = new MasterResult();
            ret.isMaster = true;
            ret.hasError = false;
            ret.result = result;
            return ret;
        }

        // is master but has error code
        static <T> MasterResult<T> masterWithError(String errorMsg) {
            MasterResult ret = new MasterResult();
            ret.isMaster = true;
            ret.hasError = true;
            ret.errorMsg = errorMsg;
            return ret;
        }

    }
}
