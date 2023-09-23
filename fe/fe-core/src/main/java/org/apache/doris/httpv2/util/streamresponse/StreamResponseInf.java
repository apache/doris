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

import java.io.PrintWriter;
import java.sql.ResultSet;
import javax.servlet.http.HttpServletResponse;

/**
 * StreamResponseInf use response.getWriter() to response client
 */
public abstract class StreamResponseInf {
    public static final String TYPE_RESULT_SET = "result_set";
    public static final String TYPE_EXEC_STATUS = "exec_status";

    protected HttpServletResponse response;
    protected PrintWriter out;
    protected int streamBatchSize = 1000;

    public abstract void handleQueryAndShow(ResultSet rs, long startTime) throws Exception;

    public abstract void handleDdlAndExport(long startTime) throws Exception;

    public abstract StreamResponseType getType();

    public StreamResponseInf(HttpServletResponse response) {
        this.response = response;
    }

    public enum StreamResponseType {
        JSON;
        public String toStreamResponseName() {
            switch (this) {
                case JSON:
                    return "Json";
                default:
                    return null;
            }
        }
    }
}
