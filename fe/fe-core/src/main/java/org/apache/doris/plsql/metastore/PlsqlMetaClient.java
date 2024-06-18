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

package org.apache.doris.plsql.metastore;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TAddPlsqlPackageRequest;
import org.apache.doris.thrift.TAddPlsqlStoredProcedureRequest;
import org.apache.doris.thrift.TDropPlsqlPackageRequest;
import org.apache.doris.thrift.TDropPlsqlStoredProcedureRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlsqlPackage;
import org.apache.doris.thrift.TPlsqlProcedureKey;
import org.apache.doris.thrift.TPlsqlStoredProcedure;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.TException;

import java.util.Map;
import java.util.Objects;

public class PlsqlMetaClient {
    public PlsqlMetaClient() {
    }

    public void addPlsqlStoredProcedure(String name, long catalogId, long dbId, String packageName,
            String ownerName, String source, String createTime, String modifyTime,
            boolean isForce) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getPlsqlManager()
                    .addPlsqlStoredProcedure(
                            new PlsqlStoredProcedure(name, catalogId, dbId, packageName, ownerName, source,
                            createTime, modifyTime),
                            isForce);
        } else {
            addPlsqlStoredProcedureThrift(name, catalogId, dbId, packageName, ownerName, source,
                                            createTime, modifyTime, isForce);
        }
    }

    public void dropPlsqlStoredProcedure(String name, long catalogId, long dbId) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getPlsqlManager()
                    .dropPlsqlStoredProcedure(new PlsqlProcedureKey(name, catalogId, dbId));
        } else {
            dropStoredProcedureThrift(name, catalogId, dbId);
        }
    }

    public PlsqlStoredProcedure getPlsqlStoredProcedure(String name, long catalogId, long dbId) {
        return Env.getCurrentEnv().getPlsqlManager()
                .getPlsqlStoredProcedure(new PlsqlProcedureKey(name, catalogId, dbId));
    }

    public Map<PlsqlProcedureKey, PlsqlStoredProcedure> getAllPlsqlStoredProcedures() {
        return Env.getCurrentEnv().getPlsqlManager()
                .getAllPlsqlStoredProcedures();
    }

    public void addPlsqlPackage(String name, long catalogId, long dbId, String ownerName, String header,
            String body) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getPlsqlManager()
                    .addPackage(new PlsqlPackage(name, catalogId, dbId, ownerName, header, body),
                            false);
        } else {
            addPlsqlPackageThrift(name, catalogId, dbId, ownerName, header, body);
        }
    }

    public void dropPlsqlPackage(String name, long catalogId, long dbId) {
        checkPriv();
        if (Env.getCurrentEnv().isMaster()) {
            Env.getCurrentEnv().getPlsqlManager().dropPackage(new PlsqlProcedureKey(name, catalogId, dbId));
        } else {
            dropPlsqlPackageThrift(name, catalogId, dbId);
        }
    }

    public PlsqlPackage getPlsqlPackage(String name, long catalogId, long dbId) {
        return Env.getCurrentEnv().getPlsqlManager().getPackage(new PlsqlProcedureKey(name, catalogId, dbId));
    }

    protected void addPlsqlStoredProcedureThrift(String name, long catalogId, long dbId, String packageName,
            String ownerName,
            String source, String createTime, String modifyTime, boolean isForce) {
        TPlsqlStoredProcedure tPlsqlStoredProcedure = new TPlsqlStoredProcedure().setName(name)
                .setCatalogId(catalogId).setDbId(dbId)
                .setPackageName(packageName).setOwnerName(ownerName).setSource(source)
                .setCreateTime(createTime).setModifyTime(modifyTime);

        TAddPlsqlStoredProcedureRequest tAddPlsqlStoredProcedureRequest = new TAddPlsqlStoredProcedureRequest()
                .setPlsqlStoredProcedure(tPlsqlStoredProcedure);
        tAddPlsqlStoredProcedureRequest.setIsForce(isForce);

        try {
            sendUpdateRequest(tAddPlsqlStoredProcedureRequest,
                    (request, client) -> client.addPlsqlStoredProcedure(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void dropStoredProcedureThrift(String name, long catalogId, long dbId) {
        TPlsqlProcedureKey tPlsqlProcedureKey = new TPlsqlProcedureKey().setName(name).setCatalogId(catalogId)
                .setDbId(dbId);
        TDropPlsqlStoredProcedureRequest tDropPlsqlStoredProcedureRequest
                = new TDropPlsqlStoredProcedureRequest().setPlsqlProcedureKey(
                tPlsqlProcedureKey);

        try {
            sendUpdateRequest(tDropPlsqlStoredProcedureRequest,
                    (request, client) -> client.dropPlsqlStoredProcedure(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void addPlsqlPackageThrift(String name, long catalogId, long dbId, String ownerName,
            String header, String body) {
        TPlsqlPackage tPlsqlPackage = new TPlsqlPackage().setName(name).setCatalogId(catalogId)
                .setDbId(dbId).setOwnerName(ownerName).setHeader(header).setBody(body);
        TAddPlsqlPackageRequest tAddPlsqlPackageRequest = new TAddPlsqlPackageRequest()
                .setPlsqlPackage(tPlsqlPackage);

        try {
            sendUpdateRequest(tAddPlsqlPackageRequest,
                    (request, client) -> client.addPlsqlPackage(request).getStatus());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void dropPlsqlPackageThrift(String name, long catalogId, long dbId) {
        TPlsqlProcedureKey tPlsqlProcedureKey = new TPlsqlProcedureKey().setName(name).setCatalogId(catalogId)
                .setDbId(dbId);
        TDropPlsqlPackageRequest tDropPlsqlPackageRequest = new TDropPlsqlPackageRequest().setPlsqlProcedureKey(
                tPlsqlProcedureKey);

        try {
            sendUpdateRequest(tDropPlsqlPackageRequest,
                    (request, client) -> client.dropPlsqlPackage(request).getStatus());
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

            status = sendRequest.apply(request, client); // retry once
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
