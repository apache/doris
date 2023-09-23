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

package org.apache.doris.httpv2.util.streamresponse;

import org.apache.doris.httpv2.rest.RestApiStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

/**
 * Serialize the ResultSet to JSON, and then response to client
 */
public class JsonStreamResponse extends StreamResponseInf {
    private static final Logger LOG = LogManager.getLogger(JsonStreamResponse.class);
    private static final Gson gson = new Gson();
    private JsonWriter jsonWriter;

    public static final String Name = "Json";

    public JsonStreamResponse(HttpServletResponse response) {
        super(response);
    }

    /**
     * Result json sample:
     * {
     *  "data": {
     *      "type": "result_set",
     *      "meta": {},
     *      "data": [],
     *      "time": 10
     *  },
     *  "msg" : "success",
     *  "code" : 0
     * }
     */
    @Override
    public void handleQueryAndShow(ResultSet rs, long startTime) throws Exception {
        response.setContentType("application/json;charset=utf-8");
        out = response.getWriter();
        jsonWriter = new JsonWriter(out);
        jsonWriter.setIndent("    ");
        try {
            // begin write response
            jsonWriter.beginObject();
            // data
            writeResultSetData(rs, jsonWriter, startTime);
            // suffix contains msg, code.
            writeResponseSuffix(jsonWriter);
            jsonWriter.endObject();
        } catch (SQLException e) {
            LOG.warn("Write response error.", e);
        } finally {
            jsonWriter.flush();
            try {
                jsonWriter.close();
            } catch (IOException e) {
                LOG.warn("JSONWriter close exception: ", e);
            }
        }
    }

    /**
     * Result json sample:
     * {
     *  "data": {
     *      "type": "exec_status",
     *      "status": {},
     *      "time": 10
     *  },
     *  "msg" : "success",
     *  "code" : 0
     * }
     */
    @Override
    public void handleDdlAndExport(long startTime) throws Exception {
        response.setContentType("application/json;charset=utf-8");
        out = response.getWriter();
        jsonWriter = new JsonWriter(out);
        jsonWriter.setIndent("    ");
        jsonWriter.beginObject();
        jsonWriter.name("msg").value("success")
                .name("code").value(RestApiStatusCode.OK.code);
        writeExecStatusData(startTime);
        jsonWriter.endObject();

    }

    public StreamResponseType getType() {
        return StreamResponseType.JSON;
    }

    private void writeResultSetData(ResultSet rs, JsonWriter jsonWriter, long startTime)
                                    throws IOException, SQLException {
        // data
        jsonWriter.name("data");
        jsonWriter.beginObject();
        // data-type
        jsonWriter.name("type").value(StreamResponseInf.TYPE_RESULT_SET);
        if (rs == null) {
            jsonWriter.endObject(); // data
            return;
        }
        ResultSetMetaData metaData = rs.getMetaData();
        int colNum = metaData.getColumnCount();
        List<Map<String, String>> metaFields = Lists.newArrayList();
        // index start from 1
        for (int i = 1; i <= colNum; ++i) {
            Map<String, String> field = Maps.newHashMap();
            field.put("name", metaData.getColumnName(i));
            field.put("type", metaData.getColumnTypeName(i));
            metaFields.add(field);
        }
        // data-meta
        String metaJson = gson.toJson(metaFields);
        jsonWriter.name("meta").jsonValue(metaJson);
        // data-data
        jsonWriter.name("data");
        jsonWriter.beginArray();
        // when bufferSize == batchSize, flush jsonWriter.
        int bufferSize = 0;
        long firstRowTime = 0;
        boolean begin = false;
        while (rs.next()) {
            List<Object> row = Lists.newArrayListWithCapacity(colNum);
            // index start from 1
            for (int i = 1; i <= colNum; ++i) {
                String type = rs.getMetaData().getColumnTypeName(i);
                if ("DATE".equalsIgnoreCase(type) || "DATETIME".equalsIgnoreCase(type)
                        || "DATEV2".equalsIgnoreCase(type) || "DATETIMEV2".equalsIgnoreCase(type)) {
                    row.add(rs.getString(i));
                } else {
                    row.add(rs.getObject(i));
                }
            }
            if (begin == false) {
                firstRowTime = (System.currentTimeMillis() - startTime);
                begin = true;
            }
            String rowJson = gson.toJson(row);
            jsonWriter.jsonValue(rowJson);
            ++bufferSize;
            if (bufferSize == streamBatchSize) {
                jsonWriter.flush();
                bufferSize = 0;
            }
        }
        jsonWriter.endArray();
        // data-time
        // the time seems no meaning, because the time contains json serialize time.
        jsonWriter.name("time").value(firstRowTime);
        jsonWriter.endObject(); // data
        jsonWriter.flush();
    }

    private void writeExecStatusData(long startTime) throws IOException {
        // data
        jsonWriter.name("data");
        jsonWriter.beginObject();
        // data-type
        jsonWriter.name("type").value(StreamResponseInf.TYPE_EXEC_STATUS);
        String statusJson = gson.toJson(Maps.newHashMap());
        // data-status
        jsonWriter.name("status").jsonValue(statusJson);
        // data-time
        jsonWriter.name("time").value((System.currentTimeMillis() - startTime));
        jsonWriter.endObject();
    }

    private void writeResponseSuffix(JsonWriter jsonWriter) throws IOException {
        // msg
        jsonWriter.name("msg").value("success");
        // code
        jsonWriter.name("code").value(RestApiStatusCode.OK.code);
    }
}
