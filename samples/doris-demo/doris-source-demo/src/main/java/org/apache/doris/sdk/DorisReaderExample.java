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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.commons.codec.binary.Base64;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanColumnDesc;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatusCode;
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
import java.math.BigDecimal;
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
    static String database = "db1";
    static String table = "tbl2";
    static String sql = "select * from db1.tbl2";
    static int readRowCount = 0;
    static List<List<Object>> result = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        JSONObject queryPlan = getQueryPlan();
        System.out.println(queryPlan);
        readData(queryPlan);
        System.out.println(result);
        System.out.println(result.size());
    }

    private static JSONObject getQueryPlan() throws Exception {
        try (CloseableHttpClient client = HttpClients.custom().build()) {
            HttpPost post = new HttpPost(String.format("http://%s/api/%s/%s/_query_plan", dorisUrl, database, table));
            post.setHeader(HttpHeaders.EXPECT, "100-continue");
            post.setHeader(HttpHeaders.AUTHORIZATION,  "Basic " + new String(Base64.encodeBase64((user + ":" + password).getBytes(StandardCharsets.UTF_8))));

            //The param is specific SQL, and the query plan is returned
            Map<String,String> params = new HashMap<>();
            params.put("sql",sql);
            StringEntity entity = new StringEntity(JSON.toJSONString(params));
            post.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(post)) {
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

//            int offset =0;
            //open scanner
            TScanOpenResult tScanOpenResult = client.openScanner(params);
            if (!TStatusCode.OK.equals(tScanOpenResult.getStatus().getStatusCode())) {
                throw new RuntimeException(String.format("The status of open scanner result from %s is '%s', error message is: %s.",
                    routingsBackend, tScanOpenResult.getStatus().getStatusCode(), tScanOpenResult.getStatus().getErrorMsgs()));
            }
            List<TScanColumnDesc> selectedColumns = tScanOpenResult.getSelectedColumns();

            TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
            nextBatchParams.setContextId(tScanOpenResult.getContextId());
            boolean eos = false;
            //read data
            int offset = 0;
            while(!eos){
                nextBatchParams.setOffset(offset);
                TScanBatchResult next = client.getNext(nextBatchParams);
                if (!TStatusCode.OK.equals(next.getStatus().getStatusCode())) {
                    throw new RuntimeException(String.format("The status of get next result from %s is '%s', error message is: %s.",
                        routingsBackend, next.getStatus().getStatusCode(), next.getStatus().getErrorMsgs()));
                }
                eos = next.isEos();
                if(!eos){
                    int i = convertArrow(next, selectedColumns);
                    offset += i;
                    readRowCount = offset;
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
            //Arrow returns in column format and needs to be converted to row format
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector fieldVector = fieldVectors.get(col);
                Types.MinorType minorType = fieldVector.getMinorType();
                for(int row = 0 ; row < rowCountInOneBatch ;row++){
                    convertValue(row , minorType, fieldVector);
                }
            }
            offset += root.getRowCount();
        }
        return offset;
    }


    private static void convertValue(int rowIndex,
                              Types.MinorType minorType,
                              FieldVector fieldVector) throws Exception {

        switch (minorType) {
            case BIT:
                BitVector bitVector = (BitVector) fieldVector;
                Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case TINYINT:
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case SMALLINT:
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case INT:
                IntVector intVector = (IntVector) fieldVector;
                fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case BIGINT:
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case FLOAT4:
                Float4Vector float4Vector = (Float4Vector) fieldVector;
                fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case FLOAT8:
                Float8Vector float8Vector = (Float8Vector) fieldVector;
                fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case VARBINARY:
                VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
                fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                result.get(readRowCount + rowIndex).add(fieldValue);
                break;
            case DECIMAL:
                DecimalVector decimalVector = (DecimalVector) fieldVector;
                BigDecimal value = decimalVector.getObject(rowIndex).stripTrailingZeros();
                result.get(readRowCount + rowIndex).add(value);
                break;
            case VARCHAR:
                VarCharVector date = (VarCharVector) fieldVector;
                String stringValue = new String(date.get(rowIndex));
                result.get(readRowCount + rowIndex).add(stringValue);
                break;
            case LIST:
                ListVector listVector = (ListVector) fieldVector;
                List listValue = listVector.isNull(rowIndex) ? null : listVector.getObject(rowIndex);
                result.get(readRowCount + rowIndex).add(listValue);
                break;
            default:
                String errMsg = "Unsupported type " + minorType;
                throw new RuntimeException(errMsg);
        }
    }
}
