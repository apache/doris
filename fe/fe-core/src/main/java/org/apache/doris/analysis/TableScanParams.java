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

package org.apache.doris.analysis;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TableScanParams {
    public static String INCREMENTAL_READ = "incr";

    private final String paramType;
    private final Map<String, String> params;

    public TableScanParams(String paramType, Map<String, String> params) {
        this.paramType = paramType;
        this.params = params == null ? ImmutableMap.of() : ImmutableMap.copyOf(params);
    }

    public String getParamType() {
        return paramType;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public boolean incrementalRead() {
        return INCREMENTAL_READ.equals(paramType);
    }
}
