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

import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.metric.MetricVisitor;
import org.apache.doris.metric.JsonMetricVisitor;
import org.apache.doris.metric.PrometheusMetricVisitor;
import org.apache.doris.metric.SimpleCoreMetricVisitor;

import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;

//fehost:port/metrics
//fehost:port/metrics?type=core
//fehost:port/metrics?type=json
public class MetricsAction extends RestBaseAction {

    private static final String TYPE_PARAM = "type";

    public MetricsAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/metrics", new MetricsAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String type = request.getSingleParameter(TYPE_PARAM);
        MetricVisitor visitor = null;
        if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("core")) {
            visitor = new SimpleCoreMetricVisitor("doris_fe");
        } else if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("json")) {
            visitor = new JsonMetricVisitor("doris_fe");
        } else {
            visitor = new PrometheusMetricVisitor("doris_fe");
        }

        response.setContentType("text/plain");
        response.getContent().append(MetricRepo.getMetric(visitor));
        sendResult(request, response);
    }
}
