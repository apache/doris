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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanColumnDesc;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisReaderExample {
    static String dorisUrl = "127.0.0.1:8030";
    static String user = "root";
    static String password = "";
    static String database = "test";
    static String table = "table_1";
    static String sql = "select * from test.table_1";

    static List<List<Object>> result = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        JSONObject queryPlan = getQueryPlan();
        readData(queryPlan);
        System.out.println(result);
        System.out.println(result.size());
    }

    private static JSONObject getQueryPlan() throws Exception {
        try (CloseableHttpClient client = HttpClients.custom().build()) {
            HttpPost put = new HttpPost(String.format("http://%s/api/%s/%s/_query_plan", dorisUrl, database, table));
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION,  "Basic " + new String(Base64.encodeBase64((user + ":" + password).getBytes(StandardCharsets.UTF_8))));

            Map<String,String> params = new HashMap<>();
            params.put("sql",sql);
            StringEntity entity = new StringEntity(JSON.toJSONString(params));
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                if (response.getEntity() != null ) {
                    String loadResult = EntityUtils.toString(response.getEntity());
                    JSONObject queryPlan = JSONObject.parseObject(loadResult);
                    return queryPlan;
                }
            }
        }
        return null;
    }

    private static void readData(JSONObject queryRes) throws Exception {
        JSONObject data = queryRes.getJSONObject("data");
        String queryPlan = data.getString("opaqued_query_plan");
        JSONObject partitions = data.getJSONObject("partitions");
        for(Map.Entry<String, Object> tablet : partitions.entrySet()){
            Long tabletId = Long.parseLong(tablet.getKey());
            JSONObject value = JSONObject.parseObject(JSON.toJSONString(tablet.getValue()));
            //get first backend
            String routingsBackend = value.getJSONArray("routings").getString(0);
            String backendHost = routingsBackend.split(":")[0];
            String backendPort = routingsBackend.split(":")[1];

            //connect backend
            TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
            TTransport transport = new TSocket(new TConfiguration(), backendHost, Integer.parseInt(backendPort));
            TProtocol protocol = factory.getProtocol(transport);
            TDorisExternalService.Client client = new TDorisExternalService.Client(protocol);
            if (!transport.isOpen()) {
                transport.open();
            }

            //build params
            TScanOpenParams params = new TScanOpenParams();
            params.cluster = "default_cluster";
            params.database = database;
            params.table = table;
            params.tablet_ids = Arrays.asList(tabletId);
            params.opaqued_query_plan = queryPlan;
            // max row number of one read batch
            params.setBatchSize(1024);
            params.setQueryTimeout(3600);
            params.setMemLimit(2147483648L);
            params.setUser(user);
            params.setPasswd(password);

            int offset =0;
            //open scanner
            TScanOpenResult tScanOpenResult = client.openScanner(params);
            List<TScanColumnDesc> selectedColumns = tScanOpenResult.getSelectedColumns();

            TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
            nextBatchParams.setContextId(tScanOpenResult.getContextId());
            boolean eos = false;
            //connect
            while(!eos){
                nextBatchParams.setOffset(offset);
                TScanBatchResult next = client.getNext(nextBatchParams);
                eos = next.isEos();
                if(!eos){
                    int i = convertArrow(next, selectedColumns);
                    offset += i;
                }
            }

            //close
            TScanCloseParams closeParams = new TScanCloseParams();
            closeParams.setContextId(tScanOpenResult.getContextId());
            client.closeScanner(closeParams);
            if ((transport != null) && transport.isOpen()) {
                transport.close();
            }
            //System.out.println(String.format("read tablet %s from backend %s finished", tabletId, routingsBackend));
        }
    }

    private static int convertArrow(TScanBatchResult nextResult, List<TScanColumnDesc> selectedColumns) throws Exception {
        int offset = 0;
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        ArrowStreamReader arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);

        VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {
            List<FieldVector>  fieldVectors = root.getFieldVectors();
            //total data rows
            int rowCountInOneBatch = root.getRowCount();
            // init the result
            for (int i = 0; i < rowCountInOneBatch; ++i) {
                result.add(new ArrayList<>(fieldVectors.size()));
            }
            //selectedColumns.forEach(desc -> System.out.print("columnHeader: " + desc.getName()));
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector fieldVector = fieldVectors.get(col);
                for(int row = 0 ; row < rowCountInOneBatch ;row++){
                    //It needs to be converted into data according to the corresponding arrow type
                    Object object = fieldVector.getObject(row);
                    result.get(offset + row).add(object);
                }
            }
            offset += root.getRowCount();
        }
        return offset;
    }

}
