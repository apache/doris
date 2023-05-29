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

package org.apache.doris.sdk;

import org.apache.doris.sdk.model.Schema;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.utils.RestService;
import org.apache.doris.sdk.utils.SchemaUtils;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.Map;

public class DorisClient {
    private final RestService restService;
    private TDorisExternalService.Client client;
    private TTransport transport;
    private String beHost;
    private Integer bePort;
    private TScanOpenResult tScanOpenResult;
    private Schema schema;
    private String contextId;
    private String database;
    private String table;
    private String username;
    private String password;

    public DorisClient(Map<String, String> requiredParams) throws IOException {
        this.database = requiredParams.get("database");
        this.table = requiredParams.get("table");
        this.username = requiredParams.get("username");
        this.password = requiredParams.get("password");
        this.restService = new RestService(requiredParams);
    }

    public Schema getSchema() {
        return schema;
    }

    public String getContextId() {
        return contextId;
    }

    private void parseBackendInfo(String beInfo) {
        String[] backend = beInfo.split(":");
        if (backend.length != 2) {
            throw new RuntimeException("Failed to parse backend info," + "beInfo=" + beInfo);
        }
        beHost = backend[0];
        bePort = Integer.parseInt(backend[1]);
    }

    public void openScanner() throws TException {
        parseBackendInfo(restService.getBeInfo());

        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        transport = new TSocket(beHost, bePort);
        TProtocol protocol = factory.getProtocol(transport);
        client = new TDorisExternalService.Client(protocol);
        if (!transport.isOpen()) {
            transport.open();
        }
        TScanOpenParams tScanOpenParams = buildScanOpenParams();
        this.tScanOpenResult = client.openScanner(tScanOpenParams);
        this.schema = SchemaUtils.convertToSchema(tScanOpenResult.getSelectedColumns());

        this.contextId = tScanOpenResult.getContextId();
    }

    private TScanOpenParams buildScanOpenParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.cluster = "default_cluster";
        params.database = database;
        params.table = table;
        params.tablet_ids = restService.getTablets();
        params.opaqued_query_plan = restService.getQueryPlan();

        // max row number of one read batch
        params.setBatchSize(1024);
        params.setQueryTimeout(3600);
        params.setMemLimit(2147483648L);

        params.setUser(username);
        params.setPasswd(password);
        return params;
    }

    public TScanBatchResult getNext(TScanNextBatchParams tScanNextBatchParams) throws TException {
        return client.getNext(tScanNextBatchParams);
    }

    public void close() throws TException {
        TScanCloseParams closeParams = new TScanCloseParams();
        closeParams.setContextId(tScanOpenResult.getContextId());
        client.closeScanner(closeParams);

        if ((transport != null) && transport.isOpen()) {
            transport.close();
        }
    }

}
