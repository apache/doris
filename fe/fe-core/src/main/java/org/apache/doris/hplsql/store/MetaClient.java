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

package org.apache.doris.hplsql.store;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TAddHplsqlPackageRequest;
import org.apache.doris.thrift.TAddStoredProcedureRequest;
import org.apache.doris.thrift.THplsqlPackage;
import org.apache.doris.thrift.THplsqlPackageRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStoredKey;
import org.apache.doris.thrift.TStoredProcedure;
import org.apache.doris.thrift.TStoredProcedureRequest;

import org.apache.thrift.TException;

import java.util.Objects;

public class MetaClient {
    public MetaClient() {
    }

    public void addStoredProcedure(String name, String catalogName, String dbName, String ownerName, String source,
            boolean isForce) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getHplsqlManager()
                    .addStoredProcedure(new StoredProcedure(name, catalogName, dbName, ownerName, source), isForce);
        } else {
            addStoredProcedureThrift(name, catalogName, dbName, ownerName, source, isForce);
        }
    }

    public void dropStoredProcedure(String name, String catalogName, String dbName) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getHplsqlManager().dropStoredProcedure(new StoredKey(name, catalogName, dbName));
        } else {
            dropStoredProcedureThrift(name, catalogName, dbName);
        }
    }

    public StoredProcedure getStoredProcedure(String name, String catalogName, String dbName) {
        return Env.getCurrentEnv().getHplsqlManager().getStoredProcedure(new StoredKey(name, catalogName, dbName));
    }

    public void addHplsqlPackage(String name, String catalogName, String dbName, String ownerName, String header,
            String body) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getHplsqlManager()
                    .addPackage(new HplsqlPackage(name, catalogName, dbName, ownerName, header, body),
                            false);
        } else {
            addHplsqlPackageThrift(name, catalogName, dbName, ownerName, header, body);
        }
    }

    public void dropHplsqlPackage(String name, String catalogName, String dbName) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getHplsqlManager().dropPackage(new StoredKey(name, catalogName, dbName));
        } else {
            dropHplsqlPackageThrift(name, catalogName, dbName);
        }
    }

    public HplsqlPackage getHplsqlPackage(String name, String catalogName, String dbName) {
        return Env.getCurrentEnv().getHplsqlManager().getPackage(new StoredKey(name, catalogName, dbName));
    }

    protected void addStoredProcedureThrift(String name, String catalogName, String dbName, String ownerName,
            String source, boolean isForce) {
        TStoredProcedure tStoredProcedure = new TStoredProcedure().setName(name).setCatalogName(catalogName)
                .setDbName(dbName).setOwnerName(ownerName).setSource(source);
        TAddStoredProcedureRequest tAddStoredProcedureRequest = new TAddStoredProcedureRequest()
                .setStoredProcedure(tStoredProcedure);
        tAddStoredProcedureRequest.setIsForce(isForce);

        try {
            sendUpdateRequest(tAddStoredProcedureRequest,
                    (request, client) -> client.addStoredProcedure(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void dropStoredProcedureThrift(String name, String catalogName, String dbName) {
        TStoredKey tStoredKey = new TStoredKey().setName(name).setCatalogName(catalogName).setDbName(dbName);
        TStoredProcedureRequest tStoredProcedureRequest = new TStoredProcedureRequest().setStoredKey(tStoredKey);

        try {
            sendUpdateRequest(tStoredProcedureRequest,
                    (request, client) -> client.dropStoredProcedure(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void addHplsqlPackageThrift(String name, String catalogName, String dbName, String ownerName,
            String header, String body) {
        THplsqlPackage tHplsqlPackage = new THplsqlPackage().setName(name).setCatalogName(catalogName)
                .setDbName(dbName).setOwnerName(ownerName).setHeader(header).setBody(body);
        TAddHplsqlPackageRequest tAddHplsqlPackageRequest = new TAddHplsqlPackageRequest()
                .setHplsqlPackage(tHplsqlPackage);

        try {
            sendUpdateRequest(tAddHplsqlPackageRequest,
                    (request, client) -> client.addHplsqlPackage(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void dropHplsqlPackageThrift(String name, String catalogName, String dbName) {
        TStoredKey tStoredKey = new TStoredKey().setName(name).setCatalogName(catalogName).setDbName(dbName);
        THplsqlPackageRequest tHplsqlPackageRequest = new THplsqlPackageRequest().setStoredKey(tStoredKey);

        try {
            sendUpdateRequest(tHplsqlPackageRequest,
                    (request, client) -> client.dropHplsqlPackage(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkPriv() {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN)) {
            throw new RuntimeException(
                    "Access denied; you need (at least one of) the ADMIN privilege(s) for this operation");
        }
    }

    private <Request> void sendUpdateRequest(Request request,
            BiFunction<Request, FrontendService.Client, TStatus> sendRequest) throws Exception {
        TNetworkAddress masterAddress = new TNetworkAddress(Env.getCurrentEnv().getMasterHost(),
                Env.getCurrentEnv().getMasterRpcPort());
        FrontendService.Client client = ClientPool.frontendPool.borrowObject(masterAddress);
        TStatus status;
        boolean isReturnToPool = true;
        try {
            status = sendRequest.apply(request, client);
            checkResult(status);
        } catch (Exception e) {
            if (!ClientPool.frontendPool.reopen(client)) {
                isReturnToPool = false;
                throw e;
            }

            status = sendRequest.apply(request, client);
            checkResult(status);
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(masterAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(masterAddress, client);
            }
        }
    }

    private void checkResult(TStatus status) throws Exception {
        if (Objects.isNull(status) || !status.isSetStatusCode()) {
            throw new TException("Access master error, no status set.");
        }
        if (status.getStatusCode().equals(TStatusCode.OK)) {
            return;
        }
        throw new Exception(
                "Access fe error, code:" + status.getStatusCode().name() + ", mgs:" + status.getErrorMsgs());
    }

    @FunctionalInterface
    public interface BiFunction<T, U, R> {
        R apply(T t, U u) throws Exception;
    }
}
