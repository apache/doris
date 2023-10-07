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

package org.apache.doris.httpv2.rest;

import org.apache.doris.common.Config;
import org.apache.doris.metric.JsonMetricVisitor;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.metric.MetricVisitor;
import org.apache.doris.metric.PrometheusMetricVisitor;
import org.apache.doris.metric.SimpleCoreMetricVisitor;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//fehost:port/metrics
//fehost:port/metrics?type=core
@RestController
public class MetricsAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(MetricsAction.class);
    private static final String TYPE_PARAM = "type";

    @GetMapping(path = "/metrics")
    public void execute(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        String type = request.getParameter(TYPE_PARAM);
        MetricVisitor visitor = null;
        if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("core")) {
            visitor = new SimpleCoreMetricVisitor();
        } else if (!Strings.isNullOrEmpty(type) && type.equalsIgnoreCase("json")) {
            visitor = new JsonMetricVisitor();
        } else {
            visitor = new PrometheusMetricVisitor();
        }
        response.setContentType("text/plain");
        try {
            response.getWriter().write(MetricRepo.getMetric(visitor));
        } catch (IOException e) {
            LOG.warn("", e);
        }

    }
}
