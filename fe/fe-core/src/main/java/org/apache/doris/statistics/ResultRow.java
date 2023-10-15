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

package org.apache.doris.statistics;

import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class ResultRow {
    @SerializedName("values")
    private final List<String> values;

    public ResultRow(List<String> values) {
        this.values = values;
    }

    public List<String> getValues() {
        return values != null ? values : Collections.emptyList();
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(",",  "ResultRow:{", "}");
        for (String val : values) {
            sj.add(val);
        }
        return sj.toString();
    }

    public String get(int idx) {
        return values.get(idx);
    }

    /**
     *  If analyze an empty table, some stats would be null, return a default value
     *  to avoid npe would deserialize it.
     */
    public String getWithDefault(int idx, String defaultVal) {
        String val = values.get(idx);
        return val == null ? defaultVal : val;
    }
}
