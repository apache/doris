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

package org.apache.doris.httpv2.util;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Map;

public class ExecutionResultSet {

    private Map<String, Object> result;

    public ExecutionResultSet(Map<String, Object> result) {
        this.result = result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public static ExecutionResultSet emptyResult() {
        Map<String, Object> result = Maps.newHashMap();
        result.put("meta", Lists.newArrayList());
        result.put("data", Lists.newArrayList());
        return new ExecutionResultSet(result);
    }
}
