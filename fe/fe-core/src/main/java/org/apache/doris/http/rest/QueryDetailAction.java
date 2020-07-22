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

package org.apache.doris.http.rest;

import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.List;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.qe.QueryDetail;
import org.apache.doris.qe.QueryDetailQueue;

public class QueryDetailAction extends RestBaseAction {

    public QueryDetailAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/query_detail", new QueryDetailAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String eventTimeStr = request.getSingleParameter("event_time");
        if (eventTimeStr == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        long eventTime = Long.valueOf(eventTimeStr.trim());
        List<QueryDetail> queryDetails = QueryDetailQueue.getQueryDetails(eventTime);
        Gson gson = new Gson();
        String json_string = gson.toJson(queryDetails);
        response.getContent().append(json_string);
        sendResult(request, response);
    }
}
