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
import org.bouncycastle.util.Strings;

import java.util.List;
import java.util.Map;

public class TableScanParams {
    public static String READ_MODE = "read_mode";
    public static String INCREMENTAL_READ = "incr";
    public static String BRANCH = "branch";
    public static String TAG = "tag";

    private final String paramType;
    // There are two ways to pass parameters to a function.
    // - One is in map form, where the data is stored in `mapParams`.
    //   such as: @func_name('param1'='value1', 'param2'='value2', 'param3'='value3')
    // - The other is in list form, where the data is stored in `listParams`.
    //   such as: `listParams` is used for @func_name('value1', 'value2', 'value3')
    private final Map<String, String> mapParams;
    private final List<String> listParams;

    public TableScanParams(String paramType, Map<String, String> mapParams, List<String> listParams) {
        this.paramType = Strings.toLowerCase(paramType);
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
