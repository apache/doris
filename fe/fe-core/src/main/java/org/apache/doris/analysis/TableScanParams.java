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

import java.util.List;
import java.util.Map;

public class TableScanParams {
    public static String INCREMENTAL_READ = "incr";
    public static String BRANCH = "branch";
    public static String TAG = "tag";

    private final String paramType;
    private final Map<String, String> mapParams;
    private final List<String> listParams;

    public TableScanParams(String paramType, Map<String, String> mapParams, List<String> listParams) {
        this.paramType = paramType;
        this.mapParams = mapParams == null ? ImmutableMap.of() : ImmutableMap.copyOf(mapParams);
        this.listParams = listParams;
    }

    public List<String> getListParams() {
        return listParams;
    }

    public String getParamType() {
        return paramType;
    }

    public Map<String, String> getMapParams() {
        return mapParams;
    }

    public boolean incrementalRead() {
        return INCREMENTAL_READ.equals(paramType);
    }

    public boolean isBranch() {
        return BRANCH.equals(paramType);
    }

    public boolean isTag() {
        return TAG.equals(paramType);
    }
}
