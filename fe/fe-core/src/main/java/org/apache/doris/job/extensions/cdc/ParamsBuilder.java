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

package org.apache.doris.job.extensions.cdc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ParamsBuilder {
    private final String jobId;
    private final String frontends;
    private final Map<String, String> cdcConfig;

    public ParamsBuilder(String jobId, String frontends, Map<String, String> cdcConfig) {
        this.jobId = jobId;
        this.frontends = frontends;
        this.cdcConfig = cdcConfig;
    }

    public String buildParams() {
        List<String> paramsList = new ArrayList<>();
        paramsList.add("--job-id");
        paramsList.add(jobId);
        paramsList.add("--frontends");
        paramsList.add(frontends);
        paramsList.addAll(cdcConfig.entrySet().stream()
            .map(entry -> String.format("--cdc-conf %s=%s", entry.getKey(), entry.getValue()))
            .collect(Collectors.toList()));
        return String.join(" ", paramsList);
    }
}
